import asyncio
from pathlib import Path
from typing import Optional, Union, IO, List

from vishwa_labs_fastapi_utils.cloud.s3 import S3StorageClient
from vishwa_labs_fastapi_utils.cloud.storage_base import AsyncStorageClientBase


class S3StorageClientAsync(AsyncStorageClientBase):
    """
    Async AWS S3 client. boto3 has no first-party async client and aioboto3 is not a
    dependency here, so this wraps the synchronous S3StorageClient via asyncio.to_thread —
    consistent with how the GCS async client leans on the sync Google SDK.
    """

    def __init__(self,
                 storage_account_name: Optional[str] = None,
                 container_name: Optional[str] = None,
                 return_https_url: Optional[bool] = None):
        super().__init__()
        self._sync = S3StorageClient(
            storage_account_name=storage_account_name,
            container_name=container_name,
            return_https_url=return_https_url,
        )

    # ───────────────────────── Upload ───────────────────────── #
    async def upload_file(self, local_file_path: Union[str, Path], blob_name: Optional[str] = None,
                          overwrite: bool = True) -> str:
        return await asyncio.to_thread(self._sync.upload_file, local_file_path, blob_name, overwrite)

    async def upload_bytes(self, data: bytes, blob_name: str, overwrite: bool = True) -> str:
        return await asyncio.to_thread(self._sync.upload_bytes, data, blob_name, overwrite)

    async def upload_stream(self, stream: IO, blob_name: str, overwrite: bool = True) -> str:
        return await asyncio.to_thread(self._sync.upload_stream, stream, blob_name, overwrite)

    async def upload_folder(self, local_folder_path: Union[str, Path], remote_folder_path: Optional[str] = None,
                            overwrite: bool = True) -> List[str]:
        return await asyncio.to_thread(self._sync.upload_folder, local_folder_path, remote_folder_path, overwrite)

    async def upload_from_url(self, source_url: str, blob_name: str, overwrite: bool = True) -> str:
        return await asyncio.to_thread(self._sync.upload_from_url, source_url, blob_name, overwrite)

    # ───────────────────────── Download ───────────────────────── #
    async def download_blob_to_file(self, blob_name: str, destination_path: str) -> None:
        return await asyncio.to_thread(self._sync.download_blob_to_file, blob_name, destination_path)

    async def download_blob_from_url(self, blob_url: str, destination_path: str) -> None:
        return await asyncio.to_thread(self._sync.download_blob_from_url, blob_url, destination_path)

    async def download_blob_to_bytes(self, blob_name: str) -> bytes:
        return await asyncio.to_thread(self._sync.download_blob_to_bytes, blob_name)

    async def download_blob_as_text(self, blob_name: str, encoding: str = "utf-8") -> str:
        return await asyncio.to_thread(self._sync.download_blob_as_text, blob_name, encoding)

    async def download_folder_if_not_exists(self, destination_path: str, remote_folder_path: str) -> None:
        return await asyncio.to_thread(self._sync.download_folder_if_not_exists, destination_path, remote_folder_path)

    # ───────────────────────── Lifecycle ───────────────────────── #
    async def close(self) -> None:
        # boto3 clients use pooled connections with no explicit async close; nothing to do.
        return None
