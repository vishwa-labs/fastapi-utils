import os
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from pathlib import Path
from typing import Tuple, Union, Dict, List, Optional, IO
from azure.storage.blob import BlobClient

from vishwa_labs_fastapi_utils.cloud.storage_base import StorageClientBase


class AzureBlobServiceClient(StorageClientBase):
    def __init__(self, container_name: Optional[str] = None, storage_account_url: Optional[str] = None,
                 storage_account_name: Optional[str] = None):
        self._credential = None

        # Build URL if only account name is provided
        if storage_account_name and not storage_account_url:
            storage_account_url = f"https://{storage_account_name}.blob.core.windows.net"

        self._client = self.get_blob_service_client(storage_account_url)
        self._container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME") if container_name is None else container_name
        self._container_client = self._client.get_container_client(self._container_name)

    def get_blob_service_client(self, storage_account_url: Optional[str] = None):
        """Authenticate using Service Principal and return the BlobServiceClient."""
        tenant_id = os.getenv("AZURE_TENANT_ID")
        client_id = os.getenv("AZURE_CLIENT_ID")
        client_secret = os.getenv("AZURE_CLIENT_SECRET")
        storage_account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL") if storage_account_url is None else storage_account_url

        # Check if service principal credentials are available in the environment
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

        # Create BlobServiceClient using the determined credentials
        blob_service_client = BlobServiceClient(account_url=storage_account_url, credential=self._credential)

        return blob_service_client

    def _download_blob_to_file(self, blob_client, destination_path):
        """Download a single blob to a specified file path."""
        with open(destination_path, "wb") as download_file:
            blob_data = blob_client.download_blob()
            blob_data.readinto(download_file)
        print(f"Downloaded: {destination_path}")

    def download_blob_to_file(self, blob_name, destination_path):
        blob_client = self._container_client.get_blob_client(blob_name)
        self._download_blob_to_file(blob_client, destination_path)

    def download_blob_from_url(self, blob_url: str, destination_path: str) -> None:
        """
        Download a blob using its full URL.
    
        :param blob_url: The full URL to the blob. This is not just the blob name; it must include the storage account 
                         and container information, and optionally a SAS token.
        :param destination_path: The local file path where the blob will be saved.
        """
        # Create the BlobClient directly from the blob URL.
        # If the blob URL has a SAS token embedded, the credential parameter is optional.
        # Otherwise, you can supply a credential (here we assume you can get it from the client, e.g. self._client.credential).
        blob_client = BlobClient.from_blob_url(blob_url, credential=self._credential)
        
        # Call the internal helper to download the blob to the given local file path.
        self._download_blob_to_file(blob_client, destination_path)

    def download_blob_to_bytes(self, blob_name: str) -> bytes:
        """Download a blob and return its content as bytes."""
        blob_client = self._container_client.get_blob_client(blob_name)
        data = blob_client.download_blob().readall()
        print(f"Downloaded blob {blob_name} to bytes (size={len(data)}).")
        return data

    def download_folder_if_not_exists(self, destination_path: str, remote_folder_path: str) -> None:
        """Download the model file from Azure Blob Storage if it doesn't exist locally."""
        # Check if the model directory exists locally
        model_dir = Path(destination_path)
        if not model_dir.exists():
            print(f"Model folder does not exist locally. Downloading from Azure Blob Storage...")

            # Create the model directory
            model_dir.mkdir(parents=True, exist_ok=True)

            # Get the container client
            container_client = self._client.get_container_client(self._container_name)

            # List all blobs in the model folder (prefix)
            blobs = container_client.list_blobs(name_starts_with=remote_folder_path)

            # Download each file in the folder
            for blob in blobs:
                # Generate the local file path by replacing the prefix
                local_file_path = Path(destination_path) / Path(blob.name).relative_to(remote_folder_path)

                # Ensure the local directory structure exists
                local_file_path.parent.mkdir(parents=True, exist_ok=True)

                # Download the blob to the local file path
                blob_client = container_client.get_blob_client(blob)
                self._download_blob_to_file(blob_client, local_file_path)

            print(f"Model folder downloaded to {destination_path}")
        else:
            print("Model folder already exists locally. Skipping download.")

    # ----------------------------------------------------------------------
    # Upload Methods
    # ----------------------------------------------------------------------
    def _upload_blob_from_file(self, blob_client: BlobClient, local_file_path: Union[str, Path],
                               overwrite: bool = True):
        """Internal helper to upload a file to blob."""
        with open(local_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=overwrite)
        print(f"Uploaded: {local_file_path} -> {blob_client.blob_name}")

    def upload_file(self, local_file_path: Union[str, Path], blob_name: Optional[str] = None,
                    overwrite: bool = True) -> None:
        """Upload a single local file to blob storage."""
        local_file_path = Path(local_file_path)
        blob_name = blob_name or local_file_path.name
        blob_client = self._container_client.get_blob_client(blob_name)
        self._upload_blob_from_file(blob_client, local_file_path, overwrite)

    def upload_bytes(self, data: bytes, blob_name: str, overwrite: bool = True) -> None:
        """Upload raw bytes as a blob."""
        blob_client = self._container_client.get_blob_client(blob_name)
        blob_client.upload_blob(data, overwrite=overwrite)
        print(f"Uploaded bytes to blob: {blob_name}")

    def upload_stream(self, stream: IO, blob_name: str, overwrite: bool = True) -> None:
        """Upload from a file-like stream (e.g., BytesIO)."""
        blob_client = self._container_client.get_blob_client(blob_name)
        blob_client.upload_blob(stream, overwrite=overwrite)
        print(f"Uploaded stream to blob: {blob_name}")

    def upload_folder(self, local_folder_path: Union[str, Path], remote_folder_path: Optional[str] = None,
                      overwrite: bool = True) -> None:
        """Upload all files in a local folder recursively."""
        local_folder_path = Path(local_folder_path)
        remote_folder_path = remote_folder_path or local_folder_path.name

        for file_path in local_folder_path.rglob("*"):
            if file_path.is_file():
                blob_name = str(Path(remote_folder_path) / file_path.relative_to(local_folder_path))
                blob_client = self._container_client.get_blob_client(blob_name)
                self._upload_blob_from_file(blob_client, file_path, overwrite)

        print(f"Uploaded folder {local_folder_path} -> remote path {remote_folder_path}")

    def upload_from_url(self, source_url: str, blob_name: str, overwrite: bool = True) -> None:
        """Upload a blob by copying directly from a source URL (server-side)."""
        blob_client = self._container_client.get_blob_client(blob_name)
        blob_client.start_copy_from_url(source_url)
        print(f"Started copy from {source_url} to {blob_name}")
    # ----------------------------------------------------------------------
    # Cleanup
    # ----------------------------------------------------------------------

    def close(self):
        if self._client:
            self._client.close()
        if self._credential:
            self._credential.close()
