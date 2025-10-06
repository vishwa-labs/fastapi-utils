import os
import aiofiles
from azure.identity.aio import DefaultAzureCredential, ClientSecretCredential
from azure.storage.blob.aio import BlobServiceClient, BlobClient
from pathlib import Path
from typing import Union, Optional, IO

from vishwa_labs_fastapi_utils.cloud.storage_base import AsyncStorageClientBase


class AzureBlobServiceClientAsync(AsyncStorageClientBase):
    def __init__(self, container_name: Optional[str] = None, storage_account_url: Optional[str] = None,
                 storage_account_name: Optional[str] = None):
        self._credential = None
        self._client = None
        self._container_client = None

        # Build URL if only account name is provided
        if storage_account_name and not storage_account_url:
            storage_account_url = f"https://{storage_account_name}.blob.core.windows.net"

        self._storage_account_url = storage_account_url or os.getenv("AZURE_STORAGE_ACCOUNT_URL")
        self._container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME") if container_name is None else container_name

    async def get_blob_service_client(self):
        """Authenticate using Service Principal and return an async BlobServiceClient."""
        tenant_id = os.getenv("AZURE_TENANT_ID")
        client_id = os.getenv("AZURE_CLIENT_ID")
        client_secret = os.getenv("AZURE_CLIENT_SECRET")

        if tenant_id and client_id and client_secret:
            print("Using Service Principal credentials from environment variables.")
            self._credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
        else:
            print("Service Principal credentials not found. Falling back to DefaultAzureCredential.")
            self._credential = DefaultAzureCredential()

        # Create BlobServiceClient
        self._client = BlobServiceClient(account_url=self._storage_account_url, credential=self._credential)
        self._container_client = self._client.get_container_client(self._container_name)
        return self._client

    async def _download_blob_to_file(self, blob_client: BlobClient, destination_path: str):
        """Download a single blob to a specified file path asynchronously."""
        async with aiofiles.open(destination_path, "wb") as f:
            stream = await blob_client.download_blob()
            data = await stream.readall()
            await f.write(data)
        print(f"Downloaded: {destination_path}")

    async def download_blob_to_file(self, blob_name: str, destination_path: str):
        blob_client = self._container_client.get_blob_client(blob_name)
        await self._download_blob_to_file(blob_client, destination_path)

    async def download_blob_from_url(self, blob_url: str, destination_path: str) -> None:
        """Download a blob using its full URL."""
        blob_client = BlobClient.from_blob_url(blob_url, credential=self._credential)
        await self._download_blob_to_file(blob_client, destination_path)

    async def download_blob_to_bytes(self, blob_name: str) -> bytes:
        """Download a blob and return its content as bytes."""
        blob_client = self._container_client.get_blob_client(blob_name)
        stream = await blob_client.download_blob()
        data = await stream.readall()
        print(f"Downloaded blob {blob_name} to bytes (size={len(data)}).")
        return data

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
                                     overwrite: bool = True):
        """Internal helper to upload a file to blob asynchronously."""
        async with aiofiles.open(local_file_path, "rb") as f:
            data = await f.read()
        await blob_client.upload_blob(data, overwrite=overwrite)
        print(f"Uploaded: {local_file_path} -> {blob_client.blob_name}")

    async def upload_file(self, local_file_path: Union[str, Path], blob_name: Optional[str] = None,
                          overwrite: bool = True) -> None:
        """Upload a single local file to blob storage."""
        local_file_path = Path(local_file_path)
        blob_name = blob_name or local_file_path.name
        blob_client = self._container_client.get_blob_client(blob_name)
        await self._upload_blob_from_file(blob_client, local_file_path, overwrite)

    async def upload_bytes(self, data: bytes, blob_name: str, overwrite: bool = True) -> None:
        """Upload raw bytes as a blob."""
        blob_client = self._container_client.get_blob_client(blob_name)
        await blob_client.upload_blob(data, overwrite=overwrite)
        print(f"Uploaded bytes to blob: {blob_name}")

    async def upload_stream(self, stream: IO, blob_name: str, overwrite: bool = True) -> None:
        """Upload from a file-like stream (e.g., BytesIO)."""
        blob_client = self._container_client.get_blob_client(blob_name)
        await blob_client.upload_blob(stream, overwrite=overwrite)
        print(f"Uploaded stream to blob: {blob_name}")

    async def upload_folder(self, local_folder_path: Union[str, Path], remote_folder_path: Optional[str] = None,
                            overwrite: bool = True) -> None:
        """Upload all files in a local folder recursively."""
        local_folder_path = Path(local_folder_path)
        remote_folder_path = remote_folder_path or local_folder_path.name

        for file_path in local_folder_path.rglob("*"):
            if file_path.is_file():
                blob_name = str(Path(remote_folder_path) / file_path.relative_to(local_folder_path))
                blob_client = self._container_client.get_blob_client(blob_name)
                await self._upload_blob_from_file(blob_client, file_path, overwrite)

        print(f"Uploaded folder {local_folder_path} -> remote path {remote_folder_path}")

    async def upload_from_url(self, source_url: str, blob_name: str, overwrite: bool = True) -> None:
        """Upload a blob by copying directly from a source URL (server-side)."""
        blob_client = self._container_client.get_blob_client(blob_name)
        await blob_client.start_copy_from_url(source_url)
        print(f"Started copy from {source_url} to {blob_name}")

    # ----------------------------------------------------------------------
    # Cleanup
    # ----------------------------------------------------------------------

    async def close(self):
        if self._client:
            await self._client.close()
        if self._credential:
            await self._credential.close()
