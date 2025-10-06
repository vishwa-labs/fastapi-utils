import os
from typing import Optional

from vishwa_labs_fastapi_utils.cloud.utils import parse_cloud_url


def get_storage_client(storage_provider: Optional[str] = None, **kwargs):
    provider = storage_provider or os.getenv("STORAGE_PROVIDER", "azure").lower()
    if provider in ["gcp", "gcs"]:
        from .gcs import GCPStorageClient
        return GCPStorageClient(**kwargs)
    else:
        from .az_blob import AzureBlobServiceClient
        return AzureBlobServiceClient(**kwargs)


async def get_storage_client_async(storage_provider: Optional[str] = None, **kwargs):
    """
    Factory for async storage clients across clouds.

    Example:
        client = await get_storage_client_async("gcp", bucket_name="my-bucket")
        await client.upload_bytes(b"hello", "test.txt")
    """
    provider = (storage_provider or os.getenv("STORAGE_PROVIDER", "azure")).lower()

    if provider in ["gcp", "gcs"]:
        from .gcs_async import GCPStorageClientAsync
        return GCPStorageClientAsync(**kwargs)
    elif provider in ["azure", "az"]:
        from .az_blob_async import AzureBlobServiceClientAsync
        return AzureBlobServiceClientAsync(**kwargs)
    else:
        raise ValueError(f"Unsupported storage provider: {provider}")

# ────────────────────────────────────────────────
# Auto-detect reader
# ────────────────────────────────────────────────

def get_reader_client_from_url(url: str):
    """Auto-detect cloud provider from URL and return corresponding client (sync)."""
    info = parse_cloud_url(url)
    provider = info["provider"]

    if provider == "azure":
        from .az_blob import AzureBlobServiceClient
        return AzureBlobServiceClient(
            storage_account_name=info["storage_account_name"],
            container_name=info["container"],
        )
    elif provider == "gcp":
        from .gcs import GCPStorageClient
        return GCPStorageClient(
            storage_account_name=info["storage_account_name"],
            container_name=info["container"],
        )
    else:
        raise ValueError(f"Cannot detect valid cloud provider for URL: {url}")


async def get_reader_client_from_url_async(url: str):
    """Auto-detect cloud provider from URL and return async client."""
    info = parse_cloud_url(url)
    provider = info["provider"]

    if provider == "azure":
        from .az_blob_async import AzureBlobServiceClientAsync
        return AzureBlobServiceClientAsync(
            storage_account_name=info["storage_account_name"],
            container_name=info["container"],
        )
    elif provider == "gcp":
        from .gcs_async import GCPStorageClientAsync
        return GCPStorageClientAsync(
            storage_account_name=info["storage_account_name"],
            container_name=info["container"],
        )
    else:
        raise ValueError(f"Cannot detect valid cloud provider for URL: {url}")
