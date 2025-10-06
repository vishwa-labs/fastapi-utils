import os
import tempfile
from pathlib import Path
import pytest

# Import both sync and async factories
from vishwa_labs_fastapi_utils.cloud.storage import get_storage_client, get_storage_client_async


# Configure test constants
TEST_FILE_CONTENT = b"hello storage!"
TEST_FILE_NAME = "sdk_test_file.txt"
TEST_FOLDER_NAME = "sdk_test_folder"
TEST_BLOB_NAME = f"{TEST_FOLDER_NAME}/{TEST_FILE_NAME}"


@pytest.mark.parametrize("provider", ["gcp",])  # "azure",
def test_sync_storage_lifecycle(provider):
    """Test full lifecycle for sync client (upload → download → delete)."""
    os.environ["STORAGE_PROVIDER"] = provider
    client = get_storage_client()

    # Upload bytes
    url = client.upload_bytes(TEST_FILE_CONTENT, TEST_BLOB_NAME)
    assert url and TEST_BLOB_NAME in url
    print(f"[{provider.upper()}] Uploaded to {url}")

    # Download and validate content
    downloaded = client.download_blob_to_bytes(TEST_BLOB_NAME)
    assert downloaded == TEST_FILE_CONTENT
    print(f"[{provider.upper()}] Download validated successfully")

    # Download to file
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / TEST_FILE_NAME
        client.download_blob_to_file(TEST_BLOB_NAME, local_path)
        assert local_path.exists()
        assert local_path.read_bytes() == TEST_FILE_CONTENT

    print(f"[{provider.upper()}] Download-to-file successful")

    # Upload from file
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(TEST_FILE_CONTENT)
        tmp.flush()
        client.upload_file(tmp.name, TEST_BLOB_NAME)
    print(f"[{provider.upper()}] Upload-from-file successful")

    # Upload folder
    folder = Path(tempfile.mkdtemp()) / "folder_upload"
    folder.mkdir(parents=True)
    (folder / TEST_FILE_NAME).write_bytes(TEST_FILE_CONTENT)
    client.upload_folder(folder, TEST_FOLDER_NAME)
    print(f"[{provider.upper()}] Folder upload successful")

    # Upload from URL (simple public small file)
    client.upload_from_url("https://example.com", f"{TEST_FOLDER_NAME}/test_url.html")
    print(f"[{provider.upper()}] Upload-from-URL successful")

    print(f"[{provider.upper()}] ✅ All sync tests passed.")


@pytest.mark.asyncio
@pytest.mark.parametrize("provider", ["gcp",])  # "azure",
async def test_async_storage_lifecycle(provider):
    """Test full lifecycle for async client."""
    os.environ["STORAGE_PROVIDER"] = provider
    client = await get_storage_client_async()

    # Upload bytes
    url = await client.upload_bytes(TEST_FILE_CONTENT, TEST_BLOB_NAME)
    assert url and TEST_BLOB_NAME in url
    print(f"[{provider.upper()}-ASYNC] Uploaded to {url}")

    # Download bytes
    downloaded = await client.download_blob_to_bytes(TEST_BLOB_NAME)
    assert downloaded == TEST_FILE_CONTENT
    print(f"[{provider.upper()}-ASYNC] Download validated successfully")

    # Download to file
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = Path(tmpdir) / TEST_FILE_NAME
        await client.download_blob_to_file(TEST_BLOB_NAME, local_path)
        assert local_path.exists()
        assert local_path.read_bytes() == TEST_FILE_CONTENT

    print(f"[{provider.upper()}-ASYNC] ✅ All async tests passed.")

    await client.close()
