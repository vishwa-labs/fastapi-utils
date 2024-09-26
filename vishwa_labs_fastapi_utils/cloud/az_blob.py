import os
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from pathlib import Path


class AzureBlobServiceClient:
    def __init__(self):
        self._client = self.get_blob_service_client()
        self._container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")

    def get_blob_service_client(self):
        """Authenticate using Service Principal and return the BlobServiceClient."""
        tenant_id = os.getenv("AZURE_TENANT_ID")
        client_id = os.getenv("AZURE_CLIENT_ID")
        client_secret = os.getenv("AZURE_CLIENT_SECRET")
        storage_account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL")

        # Check if service principal credentials are available in the environment
        if tenant_id and client_id and client_secret:
            print("Using Service Principal credentials from environment variables.")
            credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
        else:
            print("Service Principal credentials not found. Falling back to DefaultAzureCredential.")
            credential = DefaultAzureCredential()

        # Create BlobServiceClient using the determined credentials
        blob_service_client = BlobServiceClient(account_url=storage_account_url, credential=credential)

        return blob_service_client

    def _download_blob_to_file(self, blob_client, destination_path):
        """Download a single blob to a specified file path."""
        with open(destination_path, "wb") as download_file:
            blob_data = blob_client.download_blob()
            blob_data.readinto(download_file)
        print(f"Downloaded: {destination_path}")

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
