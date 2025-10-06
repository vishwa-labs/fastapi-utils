import os
from typing import Optional


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
