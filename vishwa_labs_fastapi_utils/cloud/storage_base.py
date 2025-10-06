from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union, Optional, IO


class StorageClientBase(ABC):
    """Synchronous base interface for all storage clients (Azure, GCP, AWS, etc.)"""
    def _bytes_to_text(self, data: bytes, encoding: str = "utf-8") -> str:
        return data.decode(encoding)

    # ───────────────────────────────
    # Setup / Initialization
    # ───────────────────────────────
    @abstractmethod
    def upload_file(self, local_file_path: Union[str, Path], blob_name: Optional[str] = None,
                    overwrite: bool = True) -> None:
        """Upload a local file to blob storage."""
        pass

    @abstractmethod
    def upload_bytes(self, data: bytes, blob_name: str, overwrite: bool = True) -> None:
        """Upload bytes as a blob."""
        pass

    @abstractmethod
    def upload_stream(self, stream: IO, blob_name: str, overwrite: bool = True) -> None:
        """Upload from a file-like stream."""
        pass

    @abstractmethod
    def upload_folder(self, local_folder_path: Union[str, Path], remote_folder_path: Optional[str] = None,
                      overwrite: bool = True) -> None:
        """Upload all files from a local folder recursively."""
        pass

    @abstractmethod
    def upload_from_url(self, source_url: str, blob_name: str, overwrite: bool = True) -> None:
        """Copy/upload a blob directly from an external URL (server-side if supported)."""
        pass

    # ───────────────────────────────
    # Download
    # ───────────────────────────────
    @abstractmethod
    def download_blob_to_file(self, blob_name: str, destination_path: str) -> None:
        """Download a blob to a local file."""
        pass

    @abstractmethod
    def download_blob_from_url(self, blob_url: str, destination_path: str) -> None:
        """Download a blob using its full storage URL."""
        pass

    @abstractmethod
    def download_blob_to_bytes(self, blob_name: str) -> bytes:
        """Download a blob and return it as bytes."""
        pass

    @abstractmethod
    def download_folder_if_not_exists(self, destination_path: str, remote_folder_path: str) -> None:
        """Download all blobs in a folder if local folder does not exist."""
        pass


class AsyncStorageClientBase(ABC):
    """Asynchronous base interface for all async storage clients (Azure, GCP, etc.)"""
    def _bytes_to_text(self, data: bytes, encoding: str = "utf-8") -> str:
        return data.decode(encoding)

    # ───────────────────────────────
    # Upload
    # ───────────────────────────────
    @abstractmethod
    async def upload_file(self, local_file_path: Union[str, Path], blob_name: Optional[str] = None,
                          overwrite: bool = True) -> None:
        pass

    @abstractmethod
    async def upload_bytes(self, data: bytes, blob_name: str, overwrite: bool = True) -> None:
        pass

    @abstractmethod
    async def upload_stream(self, stream: IO, blob_name: str, overwrite: bool = True) -> None:
        pass

    @abstractmethod
    async def upload_folder(self, local_folder_path: Union[str, Path], remote_folder_path: Optional[str] = None,
                            overwrite: bool = True) -> None:
        pass

    @abstractmethod
    async def upload_from_url(self, source_url: str, blob_name: str, overwrite: bool = True) -> None:
        pass

    # ───────────────────────────────
    # Download
    # ───────────────────────────────
    @abstractmethod
    async def download_blob_to_file(self, blob_name: str, destination_path: str) -> None:
        pass

    @abstractmethod
    async def download_blob_from_url(self, blob_url: str, destination_path: str) -> None:
        pass

    @abstractmethod
    async def download_blob_to_bytes(self, blob_name: str) -> bytes:
        pass

    @abstractmethod
    async def download_folder_if_not_exists(self, destination_path: str, remote_folder_path: str) -> None:
        pass

    # ───────────────────────────────
    # Lifecycle
    # ───────────────────────────────
    @abstractmethod
    async def close(self) -> None:
        """Close connections (credential/session cleanup)."""
        pass
