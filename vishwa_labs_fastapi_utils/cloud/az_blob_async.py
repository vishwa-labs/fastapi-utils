import os
import aiofiles
from azure.identity.aio import DefaultAzureCredential, ClientSecretCredential
from azure.storage.blob.aio import BlobServiceClient, BlobClient
from pathlib import Path
from typing import Union, Optional, IO, List

from vishwa_labs_fastapi_utils.cloud.storage_base import AsyncStorageClientBase


class AzureBlobServiceClientAsync(AsyncStorageClientBase):
    def __init__(self, container_name: Optional[str] = None, storage_account_url: Optional[str] = None,
                 storage_account_name: Optional[str] = None):
        super().__init__()
        self._credential = None

        # Build URL if only account name is provided
        if storage_account_name and not storage_account_url:
            storage_account_url = f"https://{storage_account_name}.blob.core.windows.net"

        # Prefer explicit param, then env
        storage_account_url = storage_account_url or os.getenv("AZURE_STORAGE_ACCOUNT_URL")
        storage_account_name = storage_account_name or os.getenv("AZURE_STORAGE_ACCOUNT_NAME")

        # Extract name from URL if still not available
        if not storage_account_name and storage_account_url:
            try:
                storage_account_name = storage_account_url.split("//")[1].split(".")[0]
            except Exception:
                storage_account_name = None

        if not storage_account_url:
            raise ValueError("Storage account URL is required (pass explicitly or set AZURE_STORAGE_ACCOUNT_URL).")

        if not storage_account_name:
            raise ValueError("Could not determine storage account name from URL or env.")

        self._account_name = storage_account_name
        self._client = self.get_blob_service_client_async(storage_account_url)
        self._container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME") if container_name is None else container_name
        self._container_client = self._client.get_container_client(self._container_name)

    # ─────────────────────────── Auth & Client ───────────────────────────
    def get_blob_service_client_async(self, storage_account_url: Optional[str] = None):
        tenant_id = os.getenv("AZURE_TENANT_ID")
        client_id = os.getenv("AZURE_CLIENT_ID")
        client_secret = os.getenv("AZURE_CLIENT_SECRET")
        storage_account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL") or storage_account_url

        if tenant_id and client_id and client_secret:
            print("Using Service Principal credentials (async).")
            self._credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
        else:
            print("Falling back to DefaultAzureCredential (async).")
            self._credential = DefaultAzureCredential()

        return BlobServiceClient(account_url=storage_account_url, credential=self._credential)

    # ─────────────────────────── Helper ───────────────────────────
    def _format_url(self, blob_name: str) -> str:
        """Build a fully qualified HTTPS blob URL."""
        return f"https://{self._account_name}.blob.core.windows.net/{self._container_name}/{blob_name}"

    def _log_upload(self, blob_name: str) -> str:
        url = self._format_url(blob_name)
        print(f"Uploaded blob available at: {url}")
        return url

    def _get_blob_client(self, blob_name_or_url: str) -> BlobClient:
        """Return BlobClient for blob name or HTTPS URL."""
        if blob_name_or_url.startswith("http"):
            return BlobClient.from_blob_url(blob_name_or_url, credential=self._credential)
        return self._container_client.get_blob_client(blob_name_or_url)

    async def _aio_write_file(self, destination_path: str, data: bytes):
        """Small helper to write asynchronously to a file."""
        import aiofiles
        async with aiofiles.open(destination_path, "wb") as f:
            await f.write(data)
            await f.flush()
        return destination_path

    async def _download_blob_to_file(self, blob_client: BlobClient, destination_path: str):
        """Download a single blob to a specified file path asynchronously."""
        async with aiofiles.open(destination_path, "wb") as f:
            stream = await blob_client.download_blob()
            data = await stream.readall()
            await f.write(data)
        print(f"Downloaded: {destination_path}")

    async def download_blob_to_file(self, blob_name_or_url: str, destination_path: Union[str, Path]):
        """Download blob (by name or URL) to local file."""
        blob_client = self._get_blob_client(blob_name_or_url)
        Path(destination_path).parent.mkdir(parents=True, exist_ok=True)
        async with blob_client:
            stream = await blob_client.download_blob()
            data = await stream.readall()
        async with await self._aio_write_file(destination_path, data):
            pass
        print(f"Downloaded: {blob_name_or_url} -> {destination_path}")

    async def download_blob_to_bytes(self, blob_name_or_url: str) -> bytes:
        """Download blob (by name or URL) to bytes."""
        blob_client = self._get_blob_client(blob_name_or_url)
        async with blob_client:
            stream = await blob_client.download_blob()
            data = await stream.readall()
        print(f"Downloaded blob {blob_name_or_url} (size={len(data)}).")
        return data

    async def download_blob_as_text(self, blob_name_or_url: str, encoding: str = "utf-8") -> str:
        """Download blob (by name or URL) and return decoded text."""
        data = await self.download_blob_to_bytes(blob_name_or_url)
        return self._bytes_to_text(data, encoding)

    async def download_blob_from_url(self, blob_url: str, destination_path: str):
        """Backward-compatible URL downloader."""
        await self.download_blob_to_file(blob_url, destination_path)

    async def download_folder_if_not_exists(self, destination_path: str, remote_folder_path: str) -> None:
        """Download all blobs from a folder if not already downloaded."""
        model_dir = Path(destination_path)
        if model_dir.exists():
            print("Model folder already exists locally. Skipping download.")
            return

        print(f"Model folder does not exist locally. Downloading from Azure Blob Storage...")
        model_dir.mkdir(parents=True, exist_ok=True)

        async for blob in self._container_client.list_blobs(name_starts_with=remote_folder_path):
            local_file_path = Path(destination_path) / Path(blob.name).relative_to(remote_folder_path)
            local_file_path.parent.mkdir(parents=True, exist_ok=True)
            blob_client = self._container_client.get_blob_client(blob)
            await self._download_blob_to_file(blob_client, str(local_file_path))

        print(f"Model folder downloaded to {destination_path}")

    # ----------------------------------------------------------------------
    # Upload Methods
    # ----------------------------------------------------------------------

    async def _upload_blob_from_file(self, blob_client: BlobClient, local_file_path: Union[str, Path],
                                     overwrite: bool = True) -> str:
        """Internal helper to upload a file to blob asynchronously."""
        async with aiofiles.open(local_file_path, "rb") as f:
            data = await f.read()
        await blob_client.upload_blob(data, overwrite=overwrite)
        return self._log_upload(blob_client.blob_name)

    async def upload_file(self, local_file_path: Union[str, Path], blob_name: Optional[str] = None,
                          overwrite: bool = True) -> str:
        """Upload a single local file to blob storage."""
        local_file_path = Path(local_file_path)
        blob_name = blob_name or local_file_path.name
        blob_client = self._container_client.get_blob_client(blob_name)
        return await self._upload_blob_from_file(blob_client, local_file_path, overwrite)

    async def upload_bytes(self, data: bytes, blob_name: str, overwrite: bool = True) -> str:
        """Upload raw bytes as a blob."""
        blob_client = self._container_client.get_blob_client(blob_name)
        await blob_client.upload_blob(data, overwrite=overwrite)
        return self._log_upload(blob_name)

    async def upload_stream(self, stream: IO, blob_name: str, overwrite: bool = True) -> str:
        """Upload from a file-like stream (e.g., BytesIO)."""
        blob_client = self._container_client.get_blob_client(blob_name)
        await blob_client.upload_blob(stream, overwrite=overwrite)
        return self._log_upload(blob_name)

    async def upload_folder(self, local_folder_path: Union[str, Path], remote_folder_path: Optional[str] = None,
                            overwrite: bool = True) -> List[str]:
        """Upload all files in a local folder recursively."""
        local_folder_path = Path(local_folder_path)
        remote_folder_path = remote_folder_path or local_folder_path.name

        uploaded_urls = []
        for file_path in local_folder_path.rglob("*"):
            if file_path.is_file():
                blob_name = str(Path(remote_folder_path) / file_path.relative_to(local_folder_path))
                blob_client = self._container_client.get_blob_client(blob_name)
                uploaded_urls.append(await self._upload_blob_from_file(blob_client, file_path, overwrite))

        print(f"Uploaded folder {local_folder_path} -> remote path {remote_folder_path}")
        return uploaded_urls

    async def upload_from_url(self, source_url: str, blob_name: str, overwrite: bool = True) -> str:
        """Upload a blob by copying directly from a source URL (server-side)."""
        blob_client = self._container_client.get_blob_client(blob_name)
        await blob_client.start_copy_from_url(source_url)
        return self._log_upload(blob_name)

    # ----------------------------------------------------------------------
    # Cleanup
    # ----------------------------------------------------------------------

    async def close(self):
        if self._client:
            await self._client.close()
        if self._credential:
            await self._credential.close()
