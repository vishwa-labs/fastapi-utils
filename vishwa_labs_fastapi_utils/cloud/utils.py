import re
from urllib.parse import unquote, urlparse


def parse_cloud_url(url: str):
    """
    Detects cloud provider from URL and extracts container/bucket and blob/object name.

    Returns:
        dict(
            provider=<"azure" | "gcp" | "aws" | "unknown">,
            container=<container/bucket name>,
            blob=<blob/object key>,
            clean_url=<decoded original URL>
        )
    """
    if not url:
        raise ValueError("URL cannot be empty")

    decoded = unquote(url)
    parsed = urlparse(decoded)
    netloc = parsed.netloc.lower()

    # --- Azure Blob Storage ---
    if "blob.core.windows.net" in netloc:
        # Example: https://account.blob.core.windows.net/container/path/to/blob.pdf
        parts = parsed.path.lstrip("/").split("/", 1)
        if len(parts) < 2:
            raise ValueError(f"Invalid Azure Blob URL: {url}")
        container, blob = parts
        return {
            "provider": "azure",
            "container": container,
            "blob": blob,
            "clean_url": decoded,
        }

    # --- Google Cloud Storage ---
    if "storage.googleapis.com" in netloc:
        # Example: https://storage.googleapis.com/bucket/path/to/blob.pdf
        parts = parsed.path.lstrip("/").split("/", 1)
        if len(parts) < 2:
            raise ValueError(f"Invalid GCS URL: {url}")
        bucket, blob = parts
        return {
            "provider": "gcp",
            "container": bucket,
            "blob": blob,
            "clean_url": decoded,
        }

    if url.startswith("gs://"):
        # Example: gs://bucket/path/to/blob.pdf
        stripped = url.replace("gs://", "")
        parts = stripped.split("/", 1)
        bucket, blob = parts if len(parts) == 2 else (parts[0], "")
        return {
            "provider": "gcp",
            "container": bucket,
            "blob": blob,
            "clean_url": decoded,
        }

    # --- AWS S3 ---
    if "s3.amazonaws.com" in netloc or netloc.endswith(".s3.amazonaws.com"):
        # Example: https://my-bucket.s3.amazonaws.com/path/to/blob.pdf
        match = re.match(r"^(?P<bucket>[^.]+)\.s3\.amazonaws\.com$", netloc)
        bucket = match.group("bucket") if match else parsed.netloc.split(".s3")[0]
        blob = parsed.path.lstrip("/")
        return {
            "provider": "aws",
            "container": bucket,
            "blob": blob,
            "clean_url": decoded,
        }

    if url.startswith("s3://"):
        # Example: s3://my-bucket/path/to/blob.pdf
        stripped = url.replace("s3://", "")
        parts = stripped.split("/", 1)
        bucket, blob = parts if len(parts) == 2 else (parts[0], "")
        return {
            "provider": "aws",
            "container": bucket,
            "blob": blob,
            "clean_url": decoded,
        }

    # --- Fallback ---
    return {
        "provider": "unknown",
        "container": None,
        "blob": parsed.path.lstrip("/"),
        "clean_url": decoded,
    }
