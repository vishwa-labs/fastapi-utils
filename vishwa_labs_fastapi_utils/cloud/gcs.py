import os
from pathlib import Path
from typing import Optional, Union, IO

import requests
from google.cloud import storage
from google.auth import default as google_auth_default
from google.oauth2 import service_account

from vishwa_labs_fastapi_utils.cloud.storage_base import StorageClientBase


class GCPStorageClient(StorageClientBase):
    """
    Google Cloud Storage client conforming to StorageClientBase.
    Auth priority:
      1. GCP_SERVICE_ACCOUNT_KEY_PATH (if set)
      2. Application Default Credentials (ADC)
      3. VM / GKE / Cloud Run Identity
    """

    def __init__(self,
                 bucket_name: Optional[str] = None,
                 return_https_url: Optional[bool] = None):
        self._client = self._get_storage_client()
        self._bucket_name = bucket_name or os.getenv("GCP_STORAGE_BUCKET_NAME")
        if not self._bucket_name:
            raise ValueError("Bucket name is required (set GCP_STORAGE_BUCKET_NAME or pass explicitly).")

        # URL flipper
        env_mode = os.getenv("STORAGE_URL_MODE", "https").lower()
        self._return_https_url = return_https_url if return_https_url is not None else (env_mode == "https")

        self._bucket = self._client.bucket(self._bucket_name)

    # ----------------------------------------------------------------------
    # Auth
    # ----------------------------------------------------------------------
    def _get_storage_client(self) -> storage.Client:
        key_path = os.getenv("GCP_SERVICE_ACCOUNT_KEY_PATH")
        if key_path and Path(key_path).exists():
            print(f"Using service account key from {key_path}")
            creds = service_account.Credentials.from_service_account_file(key_path)
            return storage.Client(credentials=creds)
        creds, _ = google_auth_default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        print("Using Application Default Credentials (ADC) or VM identity.")
        return storage.Client(credentials=creds)

    # ----------------------------------------------------------------------
    # URL Formatting
    # ----------------------------------------------------------------------
    def _format_url(self, blob_name: str) -> str:
        """Return GCS URL depending on configured mode."""
        if self._return_https_url:
            return f"https://storage.googleapis.com/{self._bucket_name}/{blob_name}"
        return f"gs://{self._bucket_name}/{blob_name}"

    def _resolve_blob_name(self, blob_name_or_url: str) -> str:
        if "storage.googleapis.com" in blob_name_or_url:
            return blob_name_or_url.split(f"{self._bucket_name}/")[-1]
        if blob_name_or_url.startswith("gs://"):
            return blob_name_or_url.split(f"{self._bucket_name}/")[-1]
        return blob_name_or_url
    # ----------------------------------------------------------------------
    # Download Methods
    # ----------------------------------------------------------------------
    def download_blob_to_file(self, blob_name: str, destination_path: Union[str, Path]) -> None:
        blob = self._bucket.blob(blob_name)
        Path(destination_path).parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(destination_path))
        print(f"Downloaded: {blob_name} -> {destination_path}")

    def download_blob_from_url(self, blob_url: str, destination_path: str) -> None:
        # Extract bucket + object path
        clean_url = blob_url.replace("https://storage.googleapis.com/", "")
        parts = clean_url.split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid GCS URL: {blob_url}")

        bucket_name, blob_name = parts
        bucket = self._client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        Path(destination_path).parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(destination_path)
        print(f"Downloaded from {blob_url} -> {destination_path}")

    def download_blob_to_bytes(self, blob_name: str) -> bytes:
        blob = self._bucket.blob(blob_name)
        data = blob.download_as_bytes()
        print(f"Downloaded blob {blob_name} ({len(data)} bytes).")
        return data

    def download_blob_as_text(self, blob_name_or_url: str, encoding: str = "utf-8") -> str:
        """
        Download blob as text (supports blob name or full HTTPS/gs:// URL).
        """
        blob_name = self._resolve_blob_name(blob_name_or_url)
        blob = self._bucket.blob(blob_name)
        data = blob.download_as_bytes()
        return self._bytes_to_text(data, encoding)

    def download_folder_if_not_exists(self, destination_path: str, remote_folder_path: str) -> None:
        """Download all files under a remote prefix if local folder doesn’t exist."""
        local_dir = Path(destination_path)
        if local_dir.exists():
            print("Folder exists locally; skipping download.")
            return

        local_dir.mkdir(parents=True, exist_ok=True)
        print(f"Downloading folder from gs://{self._bucket_name}/{remote_folder_path}")

        for blob in self._client.list_blobs(self._bucket_name, prefix=remote_folder_path):
            local_file = local_dir / Path(blob.name).relative_to(remote_folder_path)
            local_file.parent.mkdir(parents=True, exist_ok=True)
            blob.download_to_filename(local_file)
            print(f"Downloaded: {blob.name} -> {local_file}")

    # ----------------------------------------------------------------------
    # Upload Methods
    # ----------------------------------------------------------------------
    def upload_file(self, local_file_path: Union[str, Path], blob_name: Optional[str] = None,
                    overwrite: bool = True) -> str:
        local_file_path = Path(local_file_path)
        blob_name = blob_name or local_file_path.name
        blob = self._bucket.blob(blob_name)

        if not overwrite and blob.exists():
            raise FileExistsError(f"Blob {blob_name} already exists.")

        blob.upload_from_filename(str(local_file_path))
        print(f"Uploaded: {local_file_path} -> {self._format_url(blob_name)}")
        return self._format_url(blob_name)

    def upload_bytes(self, data: bytes, blob_name: str, overwrite: bool = True) -> str:
        blob = self._bucket.blob(blob_name)

        if not overwrite and blob.exists():
            raise FileExistsError(f"Blob {blob_name} already exists.")

        blob.upload_from_string(data)
        print(f"Uploaded bytes to: {self._format_url(blob_name)}")
        return self._format_url(blob_name)

    def upload_stream(self, stream: IO, blob_name: str, overwrite: bool = True) -> str:
        blob = self._bucket.blob(blob_name)

        if not overwrite and blob.exists():
            raise FileExistsError(f"Blob {blob_name} already exists.")

        blob.upload_from_file(stream)
        print(f"Uploaded stream to: {self._format_url(blob_name)}")
        return self._format_url(blob_name)

    def upload_folder(self, local_folder_path: Union[str, Path],
                      remote_folder_path: Optional[str] = None,
                      overwrite: bool = True) -> None:
        local_folder_path = Path(local_folder_path)
        remote_folder_path = remote_folder_path or local_folder_path.name

        for file_path in local_folder_path.rglob("*"):
            if file_path.is_file():
                blob_name = str(Path(remote_folder_path) / file_path.relative_to(local_folder_path))
                self.upload_file(file_path, blob_name=blob_name, overwrite=overwrite)

    def upload_from_url(self, source_url: str, blob_name: str, overwrite: bool = True) -> str:
        """Upload by fetching content from an external URL (client-side)."""
        blob = self._bucket.blob(blob_name)

        if not overwrite and blob.exists():
            raise FileExistsError(f"Blob {blob_name} already exists.")

        resp = requests.get(source_url)
        resp.raise_for_status()
        blob.upload_from_string(resp.content)
        print(f"Copied from {source_url} -> {self._format_url(blob_name)}")
        return self._format_url(blob_name)
