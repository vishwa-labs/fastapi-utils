import os
import aiofiles
import aiohttp
from pathlib import Path
from typing import Optional, Union, IO

from google.cloud import storage
from google.auth.transport.requests import Request
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
                 bucket_name: Optional[str] = None,
                 return_https_url: Optional[bool] = None):
        super().__init__()
        self.client = self._get_client()
        self._bucket_name = bucket_name or os.getenv("GCP_STORAGE_BUCKET_NAME")
        if not self._bucket_name:
            raise ValueError("Bucket name is required (set GCP_STORAGE_BUCKET_NAME or pass explicitly).")

        env_mode = os.getenv("STORAGE_URL_MODE", "https").lower()
        self._return_https_url = return_https_url if return_https_url is not None else (env_mode == "https")

        self.bucket = self.client.bucket(self._bucket_name)

    # ───────────────────────── Auth ───────────────────────── #
    def _get_client(self) -> storage.Client:
        key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if key_path and os.path.exists(key_path):
            creds = service_account.Credentials.from_service_account_file(key_path)
            creds.refresh(Request())
            return storage.Client(credentials=creds)
        creds, _ = google_auth_default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        print("Using Application Default Credentials (ADC) or VM identity.")
        return storage.Client(credentials=creds)  # Fallback to ADC

    # ----------------------------------------------------------------------
    # URL Formatting
    # ----------------------------------------------------------------------
    def _format_url(self, blob_name: str) -> str:
        """Return GCS URL depending on configured mode."""
        if self._return_https_url:
            return f"https://storage.googleapis.com/{self._bucket_name}/{blob_name}"
        return f"gs://{self._bucket_name}/{blob_name}"

    # ───────────────────────── Upload Methods ───────────────────────── #

    async def upload_bytes(self, data: bytes, blob_name: str, overwrite: bool = True) -> str:
        blob = self.bucket.blob(blob_name)
        if not overwrite and blob.exists():
            raise FileExistsError(f"Blob {blob_name} already exists.")
        blob.upload_from_string(data)
        return self._format_url(blob_name)

    async def upload_file(self, local_file_path: Union[str, Path], blob_name: Optional[str] = None,
                          overwrite: bool = True) -> str:
        local_file_path = Path(local_file_path)
        blob_name = blob_name or local_file_path.name
        blob = self.bucket.blob(blob_name)
        if not overwrite and blob.exists():
            raise FileExistsError(f"Blob {blob_name} already exists.")
        blob.upload_from_filename(str(local_file_path))
        return self._format_url(blob_name)

    async def upload_stream(self, stream: IO, blob_name: str, overwrite: bool = True) -> str:
        blob = self.bucket.blob(blob_name)
        if not overwrite and blob.exists():
            raise FileExistsError(f"Blob {blob_name} already exists.")
        blob.upload_from_file(stream)
        return self._format_url(blob_name)

    async def upload_folder(self, local_folder_path: Union[str, Path],
                            remote_folder_path: Optional[str] = None,
                            overwrite: bool = True):
        local_folder = Path(local_folder_path)
        remote_folder = remote_folder_path or local_folder.name
        for file_path in local_folder.rglob("*"):
            if file_path.is_file():
                blob_name = str(Path(remote_folder) / file_path.relative_to(local_folder))
                await self.upload_file(file_path, blob_name, overwrite)

    async def upload_from_url(self, source_url: str, blob_name: str, overwrite: bool = True) -> str:
        """Client-side copy from a public URL using aiohttp."""
        blob = self.bucket.blob(blob_name)
        if not overwrite and blob.exists():
            raise FileExistsError(f"Blob {blob_name} already exists.")
        async with aiohttp.ClientSession() as session:
            async with session.get(source_url) as resp:
                resp.raise_for_status()
                content = await resp.read()
                blob.upload_from_string(content)
        return self._format_url(blob_name)

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

    # ───────────────────────── Helpers ───────────────────────── #

    def _resolve_blob_name(self, blob_name_or_url: str) -> str:
        if "storage.googleapis.com" in blob_name_or_url:
            return blob_name_or_url.split(f"{self._bucket_name}/")[-1]
        if blob_name_or_url.startswith("gs://"):
            return blob_name_or_url.split(f"{self._bucket_name}/")[-1]
        return blob_name_or_url
