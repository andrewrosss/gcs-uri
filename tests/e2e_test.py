from __future__ import annotations

import os
import os.path as op
from concurrent.futures import as_completed
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlunparse

import pytest
from google.cloud import storage

from gcs_uri import copy_dir
from gcs_uri import copy_file


TEST_STORAGE_URI_KEY = "GCS_URI_TEST_STORAGE_URI"


@pytest.fixture
def filenames() -> tuple[str, ...]:
    return ("a.txt", "b.txt", "c/d.txt", "c/e.txt")


@pytest.fixture(scope="session")
def storage_client():
    return storage.Client()


@pytest.fixture(scope="session")
def storage_root(storage_client: storage.Client) -> storage.Blob:
    if TEST_STORAGE_URI_KEY not in os.environ:
        msg = (
            f"End-to-end tests require that the {TEST_STORAGE_URI_KEY!r} "
            "environment varibale to be set and point to an existing GCS uri."
        )
        raise ValueError(msg)
    uri = os.environ[TEST_STORAGE_URI_KEY]
    root = storage.Blob.from_string(uri, client=storage_client)
    return root


@pytest.fixture(scope="session", autouse=True)
def manage_storage_objects(storage_client: storage.Client, storage_root: storage.Blob):
    # remove any blobs under storage_root
    _delete_blobs(storage_root, storage_client)
    # run all tests (i.e. scope='session')
    yield
    # clean up any blobs left over by the tests
    _delete_blobs(storage_root, storage_client)


def _delete_blobs(root: storage.Blob, client: storage.Client):
    with ThreadPoolExecutor() as executor:
        future_to_blob: dict[Future[None], storage.Blob] = {}
        for b in client.list_blobs(root.bucket, prefix=root.name):
            future = executor.submit(b.delete)
            future_to_blob[future] = b

        undeleted_blobs: list[storage.Blob] = []
        for future in as_completed(future_to_blob):
            b = future_to_blob[future]
            try:
                future.result()
            except Exception:
                undeleted_blobs.append(b)

        if len(undeleted_blobs) > 0:
            print("Failed to remove some storage blobs:")
            for b in undeleted_blobs:
                print(f"- {b}")


@pytest.fixture
def _clean_remote_blobs(storage_client: storage.Client, storage_root: storage.Blob):
    _delete_blobs(storage_root, storage_client)


@pytest.fixture
def remote_blobs(
    _clean_remote_blobs: None,
    storage_root: storage.Blob,
    filenames: tuple[str, ...],
) -> tuple[storage.Blob, list[storage.Blob]]:
    # create the remote blobs
    sample_dirname = op.join(storage_root.name, "__test_files__")
    sample_dir = storage.Blob(sample_dirname, storage_root.bucket)
    blobs: list[storage.Blob] = []
    for fn in filenames:
        bucket: storage.Bucket = storage_root.bucket
        blob = bucket.blob(op.join(sample_dir.name, fn))
        blob.upload_from_string("")
        blobs.append(blob)

    return sample_dir, blobs


@pytest.fixture
def local_files(
    tmp_path: Path,
    filenames: tuple[str, ...],
) -> tuple[str, tuple[str, ...]]:
    root = tmp_path / "__test_files__"
    paths: list[str] = []
    for filename in filenames:
        path = root / filename
        path.parent.mkdir(exist_ok=True, parents=True)
        path.write_text("")
        paths.append(str(path))

    return str(root), tuple(paths)


# --- TESTS ---


@pytest.mark.e2e
def test_local_file_to_local_file(local_files: tuple[str, tuple[str, ...]]):
    root, filenames = local_files
    src = filenames[0]
    dst = op.join(root, "test.txt")

    assert not op.exists(dst)

    copy_file(src, dst)

    assert op.exists(dst)


@pytest.mark.e2e
def test_local_dir_to_local_dir(
    tmp_path: Path,
    local_files: tuple[str, tuple[str, ...]],
):
    src, filenames = local_files
    dst = tmp_path / "dst"
    dst.mkdir(exist_ok=True, parents=True)

    assert len(list(p for p in Path(dst).rglob("*") if p.is_file())) == 0

    copy_dir(src, dst)

    copied_files = list(p for p in Path(dst).rglob("*") if p.is_file())
    assert len(copied_files) == len(filenames)


@pytest.mark.e2e
def test_local_file_to_remote_file(
    _clean_remote_blobs: None,
    local_files: tuple[str, tuple[str, ...]],
    storage_root: storage.Blob,
):
    _, filenames = local_files
    src = filenames[0]
    bucket = storage_root.bucket.name
    name = op.join(storage_root.name, "test.txt")
    components = ("gs", bucket, name, "", "", "")
    dst = urlunparse(components)

    # assert remote storage is empty
    remote_blobs = list(storage_root.bucket.list_blobs(prefix=storage_root.name))
    assert len(remote_blobs) == 0

    copy_file(src, dst)

    remote_blob_it = storage_root.bucket.list_blobs(prefix=storage_root.name)
    remote_blobs = [b for b in remote_blob_it if b.name.endswith(".txt")]
    assert len(remote_blobs) == 1
    assert storage.Blob.from_string(dst).exists(client=storage_root.client)


@pytest.mark.e2e
def test_local_dir_to_remote_dir(
    _clean_remote_blobs: None,
    local_files: tuple[str, tuple[str, ...]],
    storage_root: storage.Blob,
):
    src, filenames = local_files
    dst = storage_root

    # assert remote storage is empty
    remote_blobs = list(storage_root.bucket.list_blobs(prefix=storage_root.name))
    assert len(remote_blobs) == 0

    copy_dir(src, dst)

    remote_blob_it = storage_root.bucket.list_blobs(prefix=storage_root.name)
    remote_blobs = [b for b in remote_blob_it if b.name.endswith(".txt")]
    assert len(remote_blobs) == len(filenames)


@pytest.mark.e2e
def test_remote_file_to_local_file(
    tmp_path: Path,
    remote_blobs: tuple[storage.Blob, list[storage.Blob]],
):
    _, blobs = remote_blobs
    src = blobs[0]
    dst = tmp_path / "dst" / "test.txt"
    dst.parent.mkdir(exist_ok=True, parents=True)

    local_files = [p for p in dst.rglob("*.txt")]
    assert len(local_files) == 0

    copy_file(src, dst)

    local_files = [p for p in dst.rglob("*.txt")]
    assert len(local_files) == 0
    assert dst.exists()


@pytest.mark.e2e
def test_remote_dir_to_local_dir(
    tmp_path: Path,
    remote_blobs: tuple[storage.Blob, list[storage.Blob]],
):
    src, blobs = remote_blobs
    dst = tmp_path / "dst"
    dst.parent.mkdir(exist_ok=True, parents=True)

    local_files = [p for p in dst.rglob("*.txt")]
    assert len(local_files) == 0

    copy_dir(src, dst)

    local_files = [p for p in dst.rglob("*.txt")]
    assert len(local_files) == len(blobs)


@pytest.mark.e2e
def test_remote_file_to_remote_file(
    storage_root: storage.Blob,
    remote_blobs: tuple[storage.Blob, list[storage.Blob]],
):
    _, blobs = remote_blobs
    src = blobs[0]

    dst = storage.Blob(
        op.join(storage_root.name, "test.txt"),
        bucket=storage_root.bucket,
    )
    dst_uri = f"gs://{dst.bucket.name}/{dst.name}"

    assert not dst.exists()

    copy_file(src, dst_uri)

    assert dst.exists(client=src.client)


@pytest.mark.e2e
def test_remote_dir_to_remote_dir(
    storage_root: storage.Blob,
    remote_blobs: tuple[storage.Blob, list[storage.Blob]],
):
    src, blobs = remote_blobs

    dst = storage.Blob(op.join(storage_root.name, "dst"), bucket=storage_root.bucket)
    dst_uri = f"gs://{dst.bucket.name}/{dst.name}"

    # assert remote destination is empty
    dst_blobs = list(dst.bucket.list_blobs(prefix=dst.name))
    assert len(dst_blobs) == 0

    copy_dir(src, dst_uri)

    dst_blobs = list(dst.bucket.list_blobs(prefix=dst.name))
    assert len(dst_blobs) == len(blobs)
