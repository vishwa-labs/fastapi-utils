import os
import aiofiles
import aiohttp
from pathlib import Path
from typing import Optional, Union, IO, List

from google.cloud import storage
from google.auth.transport.requests import Request, AuthorizedSession
from google.oauth2 import service_account
from google.auth import default as google_auth_default

from vishwa_labs_fastapi_utils.cloud.storage_base import AsyncStorageClientBase


class GCPStorageClientAsync(AsyncStorageClientBase):
    """
    Async GCP Storage Client implementing AsyncStorageClientBase interface.
    Auth priority:
      1. GOOGLE_APPLICATION_CREDENTIALS (service account JSON)
      2. Application Default Credentials (ADC)
      3. VM / GKE / Cloud Run identity
    """

    def __init__(self,
                 storage_account_name: Optional[str] = None,
                 container_name: Optional[str] = None,
                 return_https_url: Optional[bool] = None):
        super().__init__()
        self.client = self._get_client()
        self._bucket_name = storage_account_name or os.getenv("GCP_STORAGE_BUCKET_NAME")
        if not self._bucket_name:
            raise ValueError("Bucket name is required (set GCP_STORAGE_BUCKET_NAME or pass explicitly).")

        # Treat container as a path prefix inside the bucket
        self._container_prefix = container_name or os.getenv("GCP_STORAGE_CONTAINER_NAME", "")
        if self._container_prefix and not self._container_prefix.endswith("/"):
            self._container_prefix += "/"

        env_mode = os.getenv("STORAGE_URL_MODE", "https").lower()
        self._return_https_url = return_https_url if return_https_url is not None else (env_mode == "https")

        self.bucket = self.client.bucket(self._bucket_name)

    # ───────────────────────── Auth ───────────────────────── #
    def _get_client(self) -> storage.Client:
        key_path = os.getenv("GCP_SERVICE_ACCOUNT_KEY_PATH")

        # ---- Credential loading (unchanged) ----
        if key_path and Path(key_path).exists():
            print(f"Using service account key from {key_path}")
            creds = service_account.Credentials.from_service_account_file(key_path)
        else:
            creds, _ = google_auth_default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
            print("Using Application Default Credentials (ADC) or VM identity.")

        # ---- NEW: Hardened AuthorizedSession ----
        authed_session = AuthorizedSession(creds)

        # Prevent stale TLS reuse issues inside long-lived pods
        authed_session._auth_request.session.headers["Connection"] = "close"

        # Disable HTTP pool reuse (safe under NAT, WireGuard, etc.)
        adapter = authed_session._auth_request.session.adapters["https://"]
        adapter.pool_connections = 1
        adapter.pool_maxsize = 1

        # ---- Pass to storage client ----
        return storage.Client(credentials=creds, _http=authed_session)

    # ----------------------------------------------------------------------
    # URL Formatting
    # ----------------------------------------------------------------------
    def _prefixed_blob_name(self, blob_name: str) -> str:
        """
        Add container prefix (folder path) if defined,
        unless blob_name is already a full GCS URL.
        """
        # Skip prefixing for full URLs
        if "storage.googleapis.com" in blob_name or blob_name.startswith("gs://"):
            return self._resolve_blob_name(blob_name)
        return f"{self._container_prefix}{blob_name}" if self._container_prefix else blob_name

    def _resolve_blob_name(self, blob_name_or_url: str) -> str:
        if "storage.googleapis.com" in blob_name_or_url:
            return blob_name_or_url.split(f"{self._bucket_name}/")[-1]
        if blob_name_or_url.startswith("gs://"):
            return blob_name_or_url.split(f"{self._bucket_name}/")[-1]
        return blob_name_or_url

    def _format_url(self, blob_name: str, already_prefixed: bool = False) -> str:
        """Return GCS URL depending on configured mode."""
        full_name = blob_name if already_prefixed else self._prefixed_blob_name(blob_name)
        if self._return_https_url:
            return f"https://storage.googleapis.com/{self._bucket_name}/{full_name}"
        return f"gs://{self._bucket_name}/{full_name}"

    # ───────────────────────── Upload Methods ───────────────────────── #

    async def upload_file(self, local_file_path: Union[str, Path], blob_name: Optional[str] = None,
                          overwrite: bool = True) -> str:
        local_file_path = Path(local_file_path)
        blob_name = self._prefixed_blob_name(blob_name or local_file_path.name)
        blob = self.bucket.blob(blob_name)
        if not overwrite and blob.exists():
            raise FileExistsError(f"Blob {blob_name} already exists.")
        blob.upload_from_filename(str(local_file_path))
        url = self._format_url(blob_name, already_prefixed=True)
        print(f"Uploaded: {local_file_path} -> {url}")
        return url

    async def upload_bytes(self, data: bytes, blob_name: str, overwrite: bool = True) -> str:
        blob_name = self._prefixed_blob_name(blob_name)
        blob = self.bucket.blob(blob_name)
        if not overwrite and blob.exists():
            raise FileExistsError(f"Blob {blob_name} already exists.")
        blob.upload_from_string(data)
        url = self._format_url(blob_name, already_prefixed=True)
        print(f"Uploaded bytes to: {url}")
        return url

    async def upload_stream(self, stream: IO, blob_name: str, overwrite: bool = True) -> str:
        blob_name = self._prefixed_blob_name(blob_name)
        blob = self.bucket.blob(blob_name)
        if not overwrite and blob.exists():
            raise FileExistsError(f"Blob {blob_name} already exists.")
        blob.upload_from_file(stream)
        url = self._format_url(blob_name, already_prefixed=True)
        print(f"Uploaded stream to: {url}")
        return url

    async def upload_folder(self, local_folder_path: Union[str, Path],
                            remote_folder_path: Optional[str] = None,
                            overwrite: bool = True) -> List[str]:
        local_folder = Path(local_folder_path)
        remote_folder_path = self._prefixed_blob_name(remote_folder_path or local_folder.name)

        uploaded_urls = []
        for file_path in local_folder_path.rglob("*"):
            if file_path.is_file():
                blob_name = str(Path(remote_folder_path) / file_path.relative_to(local_folder_path))
                uploaded_urls.append(await self.upload_file(file_path, blob_name, overwrite))

        print(f"Uploaded folder {local_folder_path} -> remote path {remote_folder_path}")
        return uploaded_urls

    async def upload_from_url(self, source_url: str, blob_name: str, overwrite: bool = True) -> str:
        """Client-side copy from a public URL using aiohttp."""
        blob_name = self._prefixed_blob_name(blob_name)
        blob = self.bucket.blob(blob_name)
        if not overwrite and blob.exists():
            raise FileExistsError(f"Blob {blob_name} already exists.")
        async with aiohttp.ClientSession() as session:
            async with session.get(source_url) as resp:
                resp.raise_for_status()
                content = await resp.read()
                blob.upload_from_string(content)
        url = self._format_url(blob_name, already_prefixed=True)
        print(f"Copied from {source_url} -> {url}")
        return url
    # ───────────────────────── Download Methods ───────────────────────── #

    async def download_blob_to_file(self, blob_name_or_url: str, destination_path: Union[str, Path]) -> None:
        blob_name = self._resolve_blob_name(blob_name_or_url)
        blob = self.bucket.blob(blob_name)
        Path(destination_path).parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(destination_path))

    async def download_blob_to_bytes(self, blob_name_or_url: str) -> bytes:
        blob_name = self._resolve_blob_name(blob_name_or_url)
        blob = self.bucket.blob(blob_name)
        return blob.download_as_bytes()

    async def download_blob_as_text(self, blob_name_or_url: str, encoding: str = "utf-8") -> str:
        """
        Async version — download blob and return its text content.
        """
        blob_name = self._resolve_blob_name(blob_name_or_url)
        blob = self.bucket.blob(blob_name)
        data = blob.download_as_bytes()
        return self._bytes_to_text(data, encoding)

    async def download_blob_from_url(self, blob_url: str, destination_path: str) -> None:
        """Download directly via HTTPS URL asynchronously."""
        Path(destination_path).parent.mkdir(parents=True, exist_ok=True)
        async with aiohttp.ClientSession() as session:
            async with session.get(blob_url) as resp:
                resp.raise_for_status()
                async with aiofiles.open(destination_path, "wb") as f:
                    await f.write(await resp.read())

    async def download_folder_if_not_exists(self, destination_path: str, remote_folder_path: str) -> None:
        local_dir = Path(destination_path)
        if local_dir.exists():
            print("Folder exists locally; skipping download.")
            return

        local_dir.mkdir(parents=True, exist_ok=True)
        print(f"Downloading folder gs://{self._bucket_name}/{remote_folder_path}")

        for blob in self.client.list_blobs(self._bucket_name, prefix=remote_folder_path):
            local_file = local_dir / Path(blob.name).relative_to(remote_folder_path)
            local_file.parent.mkdir(parents=True, exist_ok=True)
            blob.download_to_filename(str(local_file))

    # ───────────────────────── Container Mgmt ───────────────────────── #

    async def check_and_create_container(self, name: Optional[str] = None):
        bucket_name = name or self._bucket_name
        if not self.client.lookup_bucket(bucket_name):
            raise ValueError(f"GCS bucket '{bucket_name}' does not exist (cannot auto-create in async mode).")

    async def close(self):
        # GCP client is stateless; nothing to close
        pass
