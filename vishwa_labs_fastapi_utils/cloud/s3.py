import os
import threading
from pathlib import Path
from typing import Optional, Union, IO, List
from urllib.parse import urlparse

import boto3
import requests
from botocore.config import Config

from vishwa_labs_fastapi_utils.cloud.storage_base import StorageClientBase


class S3StorageClient(StorageClientBase):
    """
    AWS S3 client conforming to StorageClientBase. Mirrors GCPStorageClient:
    `storage_account_name` is the S3 bucket and `container_name` is a key prefix.

    Auth uses the default boto3 credential chain (env vars, ~/.aws, IRSA / EC2 role).
    Honors S3_ENDPOINT_URL (e.g. LocalStack) and AWS_REGION / AWS_DEFAULT_REGION.
    """

    _shared_client = None
    _shared_client_key = None
    _lock = threading.Lock()

    def __init__(self,
                 storage_account_name: Optional[str] = None,
                 container_name: Optional[str] = None,
                 return_https_url: Optional[bool] = None):
        self._region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or ""
        self._endpoint_url = os.getenv("S3_ENDPOINT_URL") or None
        self._client = self._get_s3_client(self._region, self._endpoint_url)

        self._bucket_name = storage_account_name or os.getenv("AWS_S3_BUCKET_NAME")
        if not self._bucket_name:
            raise ValueError("Bucket name is required (set AWS_S3_BUCKET_NAME or pass explicitly).")

        # Treat container as a path prefix inside the bucket (parity with GCS).
        self._container_prefix = container_name or os.getenv("AWS_S3_CONTAINER_NAME", "")
        if self._container_prefix and not self._container_prefix.endswith("/"):
            self._container_prefix += "/"

        env_mode = os.getenv("STORAGE_URL_MODE", "https").lower()
        self._return_https_url = return_https_url if return_https_url is not None else (env_mode == "https")

    # ----------------------------------------------------------------------
    # Auth (shared client, mirrors GCPStorageClient singleton)
    # ----------------------------------------------------------------------
    @classmethod
    def _get_s3_client(cls, region: str, endpoint_url: Optional[str]):
        key = (region, endpoint_url)
        if cls._shared_client is not None and cls._shared_client_key == key:
            return cls._shared_client

        with cls._lock:
            if cls._shared_client is not None and cls._shared_client_key == key:
                return cls._shared_client

            client_kwargs = {}
            if region:
                client_kwargs["region_name"] = region
            if endpoint_url:
                # Path-style addressing is required for most local S3 emulators (LocalStack/MinIO).
                client_kwargs["endpoint_url"] = endpoint_url
                client_kwargs["config"] = Config(s3={"addressing_style": "path"})

            cls._shared_client = boto3.client("s3", **client_kwargs)
            cls._shared_client_key = key
            return cls._shared_client

    # ----------------------------------------------------------------------
    # Key / URL helpers
    # ----------------------------------------------------------------------
    def _prefixed_blob_name(self, blob_name: str) -> str:
        """Add the container prefix unless blob_name is already a full S3 URL."""
        if blob_name.startswith("s3://") or ".amazonaws.com" in blob_name or (self._endpoint_url and self._endpoint_url in blob_name):
            return self._resolve_blob_name(blob_name)
        return f"{self._container_prefix}{blob_name}" if self._container_prefix else blob_name

    def _resolve_blob_name(self, blob_name_or_url: str) -> str:
        """Extract the object key from a full s3:// or https S3 URL (else return as-is)."""
        if blob_name_or_url.startswith("s3://"):
            # s3://bucket/key
            return blob_name_or_url[len("s3://"):].split("/", 1)[-1] if "/" in blob_name_or_url[len("s3://"):] else ""

        if "://" in blob_name_or_url:
            parsed = urlparse(blob_name_or_url)
            host = parsed.netloc.lower()
            path = parsed.path.lstrip("/")
            # Virtual-host style: <bucket>.s3[.<region>].amazonaws.com/<key>
            if ".s3." in host or host.endswith(".s3.amazonaws.com"):
                return path
            # Path-style: s3[.<region>].amazonaws.com/<bucket>/<key>  (also LocalStack/MinIO)
            parts = path.split("/", 1)
            return parts[1] if len(parts) > 1 else ""

        return blob_name_or_url

    def _format_url(self, blob_name: str, already_prefixed: bool = False) -> str:
        full_name = blob_name if already_prefixed else self._prefixed_blob_name(blob_name)
        if self._endpoint_url:
            return f"{self._endpoint_url.rstrip('/')}/{self._bucket_name}/{full_name}"
        if not self._return_https_url:
            return f"s3://{self._bucket_name}/{full_name}"
        if self._region:
            return f"https://{self._bucket_name}.s3.{self._region}.amazonaws.com/{full_name}"
        return f"https://{self._bucket_name}.s3.amazonaws.com/{full_name}"

    # ----------------------------------------------------------------------
    # Download
    # ----------------------------------------------------------------------
    def download_blob_to_file(self, blob_name_or_url: str, destination_path: Union[str, Path]) -> None:
        blob_name = self._resolve_blob_name(blob_name_or_url) if (
            blob_name_or_url.startswith("s3://") or "://" in blob_name_or_url) else self._prefixed_blob_name(blob_name_or_url)
        Path(destination_path).parent.mkdir(parents=True, exist_ok=True)
        self._client.download_file(self._bucket_name, blob_name, str(destination_path))
        print(f"Downloaded: {blob_name} -> {destination_path}")

    def download_blob_from_url(self, blob_url: str, destination_path: str) -> None:
        blob_name = self._resolve_blob_name(blob_url)
        Path(destination_path).parent.mkdir(parents=True, exist_ok=True)
        self._client.download_file(self._bucket_name, blob_name, destination_path)
        print(f"Downloaded from {blob_url} -> {destination_path}")

    def download_blob_to_bytes(self, blob_name_or_url: str) -> bytes:
        blob_name = self._resolve_blob_name(blob_name_or_url) if (
            blob_name_or_url.startswith("s3://") or "://" in blob_name_or_url) else self._prefixed_blob_name(blob_name_or_url)
        resp = self._client.get_object(Bucket=self._bucket_name, Key=blob_name)
        data = resp["Body"].read()
        print(f"Downloaded blob {blob_name} ({len(data)} bytes).")
        return data

    def download_blob_as_text(self, blob_name_or_url: str, encoding: str = "utf-8") -> str:
        return self._bytes_to_text(self.download_blob_to_bytes(blob_name_or_url), encoding)

    def download_folder_if_not_exists(self, destination_path: str, remote_folder_path: str) -> None:
        local_dir = Path(destination_path)
        if local_dir.exists():
            print("Folder exists locally; skipping download.")
            return
        local_dir.mkdir(parents=True, exist_ok=True)
        print(f"Downloading folder from s3://{self._bucket_name}/{remote_folder_path}")

        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket_name, Prefix=remote_folder_path):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                local_file = local_dir / Path(key).relative_to(remote_folder_path)
                local_file.parent.mkdir(parents=True, exist_ok=True)
                self._client.download_file(self._bucket_name, key, str(local_file))
                print(f"Downloaded: {key} -> {local_file}")

    # ----------------------------------------------------------------------
    # Upload
    # ----------------------------------------------------------------------
    def upload_file(self, local_file_path: Union[str, Path], blob_name: Optional[str] = None,
                    overwrite: bool = True) -> str:
        local_file_path = Path(local_file_path)
        blob_name = self._prefixed_blob_name(blob_name or local_file_path.name)
        if not overwrite and self._object_exists(blob_name):
            raise FileExistsError(f"Object {blob_name} already exists.")
        self._client.upload_file(str(local_file_path), self._bucket_name, blob_name)
        url = self._format_url(blob_name, already_prefixed=True)
        print(f"Uploaded: {local_file_path} -> {url}")
        return url

    def upload_bytes(self, data: bytes, blob_name: str, overwrite: bool = True) -> str:
        blob_name = self._prefixed_blob_name(blob_name)
        if not overwrite and self._object_exists(blob_name):
            raise FileExistsError(f"Object {blob_name} already exists.")
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._client.put_object(Bucket=self._bucket_name, Key=blob_name, Body=data)
        url = self._format_url(blob_name, already_prefixed=True)
        print(f"Uploaded bytes to: {url}")
        return url

    def upload_stream(self, stream: IO, blob_name: str, overwrite: bool = True) -> str:
        blob_name = self._prefixed_blob_name(blob_name)
        if not overwrite and self._object_exists(blob_name):
            raise FileExistsError(f"Object {blob_name} already exists.")
        self._client.upload_fileobj(stream, self._bucket_name, blob_name)
        url = self._format_url(blob_name, already_prefixed=True)
        print(f"Uploaded stream to: {url}")
        return url

    def upload_folder(self, local_folder_path: Union[str, Path],
                      remote_folder_path: Optional[str] = None,
                      overwrite: bool = True) -> List[str]:
        local_folder = Path(local_folder_path)
        remote_folder_path = self._prefixed_blob_name(remote_folder_path or local_folder.name)

        uploaded_urls = []
        for file_path in local_folder.rglob("*"):
            if file_path.is_file():
                blob_name = str(Path(remote_folder_path) / file_path.relative_to(local_folder))
                uploaded_urls.append(self.upload_file(file_path, blob_name=blob_name, overwrite=overwrite))

        print(f"Uploaded folder {local_folder_path} -> remote path {remote_folder_path}")
        return uploaded_urls

    def upload_from_url(self, source_url: str, blob_name: str, overwrite: bool = True) -> str:
        blob_name = self._prefixed_blob_name(blob_name)
        if not overwrite and self._object_exists(blob_name):
            raise FileExistsError(f"Object {blob_name} already exists.")
        resp = requests.get(source_url)
        resp.raise_for_status()
        self._client.put_object(Bucket=self._bucket_name, Key=blob_name, Body=resp.content)
        url = self._format_url(blob_name, already_prefixed=True)
        print(f"Copied from {source_url} -> {url}")
        return url

    # ----------------------------------------------------------------------
    # Internal
    # ----------------------------------------------------------------------
    def _object_exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket_name, Key=key)
            return True
        except Exception:
            return False
