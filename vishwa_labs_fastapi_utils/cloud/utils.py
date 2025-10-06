import re
from urllib.parse import unquote, urlparse


def parse_cloud_url(url: str):
    """
    Detects cloud provider from URL and extracts:
      - storage_account_name (account/bucket)
      - container (Azure container / GCP prefix)
      - blob (object path)
      - provider ("azure" | "gcp" | "aws" | "unknown")

    Returns:
        dict(
            provider,
            storage_account_name,
            container,
            blob,
            clean_url
        )
    """
    if not url:
        raise ValueError("URL cannot be empty")

    decoded = unquote(url)
    parsed = urlparse(decoded)
    netloc = parsed.netloc.lower()
    path = parsed.path.lstrip("/")

    # --- Azure Blob Storage ---
    if "blob.core.windows.net" in netloc:
        # https://<account>.blob.core.windows.net/<container>/<blob_path>
        match = re.match(r"^(?P<account>[^.]+)\.blob\.core\.windows\.net$", netloc)
        account = match.group("account") if match else None
        parts = path.split("/", 1)
        container = parts[0] if parts else None
        blob = parts[1] if len(parts) > 1 else None
        return {
            "provider": "azure",
            "storage_account_name": account,
            "container": container,
            "blob": blob,
            "clean_url": decoded,
        }

    # --- Google Cloud Storage ---
    if "storage.googleapis.com" in netloc:
        # https://storage.googleapis.com/<bucket>/<blob_path>
        parts = path.split("/", 1)
        bucket = parts[0] if parts else None
        blob = parts[1] if len(parts) > 1 else None
        return {
            "provider": "gcp",
            "storage_account_name": bucket,
            "container": None,  # GCS has only bucket (no container)
            "blob": blob,
            "clean_url": decoded,
        }

    if url.startswith("gs://"):
        # gs://<bucket>/<blob_path>
        stripped = url.replace("gs://", "")
        parts = stripped.split("/", 1)
        bucket = parts[0]
        blob = parts[1] if len(parts) > 1 else None
        return {
            "provider": "gcp",
            "storage_account_name": bucket,
            "container": None,
            "blob": blob,
            "clean_url": decoded,
        }

    # --- AWS S3 ---
    if "s3.amazonaws.com" in netloc or netloc.endswith(".s3.amazonaws.com"):
        # https://<bucket>.s3.amazonaws.com/<blob_path>
        match = re.match(r"^(?P<bucket>[^.]+)\.s3\.amazonaws\.com$", netloc)
        bucket = match.group("bucket") if match else parsed.netloc.split(".s3")[0]
        blob = path or None
        return {
            "provider": "aws",
            "storage_account_name": bucket,
            "container": None,
            "blob": blob,
            "clean_url": decoded,
        }

    if url.startswith("s3://"):
        # s3://<bucket>/<blob_path>
        stripped = url.replace("s3://", "")
        parts = stripped.split("/", 1)
        bucket = parts[0]
        blob = parts[1] if len(parts) > 1 else None
        return {
            "provider": "aws",
            "storage_account_name": bucket,
            "container": None,
            "blob": blob,
            "clean_url": decoded,
        }

    # --- Fallback ---
    return {
        "provider": "unknown",
        "storage_account_name": None,
        "container": None,
        "blob": path or None,
        "clean_url": decoded,
    }
