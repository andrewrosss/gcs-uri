from __future__ import annotations

import os
import os.path as op
from concurrent.futures import as_completed
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable
from urllib.parse import urlunparse

import pytest
from google.cloud import storage

from gcs_uri import _blob_to_uri
from gcs_uri import _flatten
from gcs_uri import copy_dir
from gcs_uri import copy_file
from gcs_uri import copy_files


TEST_STORAGE_URI_KEY = "GCS_URI_TEST_STORAGE_URI"


@pytest.fixture
def filenames() -> tuple[str, ...]:
    """Generate a list of relative file paths."""
    return ("a.txt", "b.txt", "c/d.txt", "c/e.txt")


@pytest.fixture(scope="session")
def storage_client():
    return storage.Client()


@pytest.fixture(scope="session")
def storage_root(storage_client: storage.Client) -> storage.Blob:
    """Setup the remote GCS URI that will be used as the working
    location for running the tests.
    """
    if TEST_STORAGE_URI_KEY not in os.environ:
        msg = (
            f"End-to-end tests require that the {TEST_STORAGE_URI_KEY!r} "
            "environment varibale to be set and point to an existing GCS uri."
        )
        raise KeyError(msg)
    uri = os.environ[TEST_STORAGE_URI_KEY]
    root = storage.Blob.from_string(uri, client=storage_client)
    return root


@pytest.fixture(scope="session", autouse=True)
def manage_storage_objects(storage_client: storage.Client, storage_root: storage.Blob):
    """Clear out all blobs under storage root prior to,
    as well as after, running the test suite."""
    # remove any blobs under storage_root
    _delete_blobs(storage_root, storage_client)
    # run all tests (i.e. scope='session')
    yield
    # clean up any blobs left over by the tests
    _delete_blobs(storage_root, storage_client)


def _delete_blobs(root: storage.Blob, client: storage.Client):
    """Remove all blobs under a GCS URI"""
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
    """Clear out all blobs under storage_root prior to running
    each test that requires this fixture."""
    _delete_blobs(storage_root, storage_client)


@pytest.fixture
def remote_blobs(
    _clean_remote_blobs: None,
    storage_root: storage.Blob,
    filenames: tuple[str, ...],
) -> tuple[storage.Blob, list[storage.Blob]]:
    """Create some remote blobs under `{storage_root}/__test_files__/`"""
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
    """Create some remote blobs under `{tmp_path}/__test_files__/`"""
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
    for fn in filenames:
        rp = op.relpath(fn, src)
        assert dst / rp in copied_files


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
    for fn in filenames:
        rp = op.relpath(fn, src)
        expected_name = op.join(dst.name, rp)
        assert any(expected_name == b.name for b in remote_blobs)


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
    for b in blobs:
        rp = op.relpath(b.name, src.name)
        assert dst / rp in local_files  # type: ignore


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
    for src_blob in blobs:
        rp = op.relpath(src_blob.name, src.name)
        expected_name = op.join(dst.name, rp)
        assert any(dst_blob.name == expected_name for dst_blob in dst_blobs)


@pytest.mark.e2e
@pytest.mark.parametrize(
    ("src_type", "dst_type"),
    [(str, str), (str, Path), (Path, str), (Path, Path)],
)
def test_copy_files_local_list_to_local_list(
    src_type: type[str] | type[Path],
    dst_type: type[str] | type[Path],
    filenames: list[str],
    tmp_path: Path,
    local_files: tuple[str, tuple[str, ...]],
):
    _, _srcs = local_files
    srcs = [src_type(name) for name in _srcs]
    dsts = [dst_type(tmp_path / name) for name in filenames]

    # ensure the directories we'll need to write to exists
    for dst in dsts:
        os.makedirs(op.dirname(dst), exist_ok=True)

    # make sure the dst files dont exist
    assert all(not op.exists(dst) for dst in dsts)

    copy_files(srcs, dsts)

    assert all(op.exists(dst) for dst in dsts)


@pytest.mark.e2e
@pytest.mark.parametrize(
    ("src_type", "dst_type"),
    [(str, str), (str, Path), (Path, str), (Path, Path)],
)
def test_copy_files_local_list_to_local_dir(
    src_type: type[str] | type[Path],
    dst_type: type[str] | type[Path],
    tmp_path: Path,
    local_files: tuple[str, tuple[str, ...]],
):
    _, _srcs = local_files
    srcs = [src_type(name) for name in _srcs]
    dst = dst_type(tmp_path / "out")

    # ensure the directory we'll need to write to exists
    os.makedirs(dst)

    copy_files(srcs, dst)

    dst_files = list(p for p in Path(dst).glob("*") if p.is_file())
    assert len(dst_files) == len(srcs)


@pytest.mark.e2e
@pytest.mark.parametrize(
    ("src_type", "dst_type"),
    [
        (str, str),
        (str, storage.Blob.from_string),
        (Path, str),
        (Path, storage.Blob.from_string),
    ],
)
def test_copy_files_local_list_to_remote_list(
    src_type: type[str] | type[Path],
    dst_type: type[str] | Callable[[str], storage.Blob],
    filenames: list[str],
    local_files: tuple[str, tuple[str, ...]],
    storage_root: storage.Blob,
    storage_client: storage.Client,
    _clean_remote_blobs: None,
):
    # prepare the source and destination lists
    storage_uri = _blob_to_uri(storage_root)
    _, _srcs = local_files
    srcs = [src_type(name) for name in _srcs]
    dsts = [dst_type(op.join(storage_uri, name)) for name in filenames]

    # ensure that the destination root (storage_root) is empty
    dst_blobs = list(storage_root.bucket.list_blobs(prefix=storage_root.name))
    assert len(dst_blobs) == 0

    copy_files(srcs, dsts, client=storage_client)

    # check that all of the expected files have been copied
    for dst in dsts:
        if isinstance(dst, storage.Blob):
            blob = dst
        else:
            blob = storage.Blob.from_string(dst)

        assert blob.exists(client=storage_client)


@pytest.mark.e2e
@pytest.mark.parametrize(
    ("src_type", "dst_type"),
    [
        (str, str),
        (str, storage.Blob.from_string),
        (Path, str),
        (Path, storage.Blob.from_string),
    ],
)
def test_copy_files_local_list_to_remote_dir(
    src_type: type[str] | type[Path],
    dst_type: type[str] | Callable[[str], storage.Blob],
    local_files: tuple[str, tuple[str, ...]],
    storage_root: storage.Blob,
    storage_client: storage.Client,
    _clean_remote_blobs: None,
):
    # prepare the source and destination lists
    storage_uri = _blob_to_uri(storage_root)
    _, _srcs = local_files
    srcs = [src_type(name) for name in _srcs]
    dst = dst_type(op.join(storage_uri, "out"))

    # ensure that the destination root (storage_root) is empty
    dst_blobs = list(storage_root.bucket.list_blobs(prefix=storage_root.name))
    assert len(dst_blobs) == 0

    copy_files(srcs, dst, client=storage_client)

    # check that all of the expected file have been copied
    flat_names = [_flatten(name) for name in _srcs]
    dst_blobs = list(storage_root.bucket.list_blobs(prefix=storage_root.name))
    assert len(dst_blobs) == 4
    for blob in dst_blobs:
        basename = op.basename(blob.name)
        assert basename in flat_names


@pytest.mark.e2e
@pytest.mark.parametrize(
    ("src_type", "dst_type"),
    [
        (lambda blob: blob, str),
        (_blob_to_uri, str),
        (lambda blob: blob, Path),
        (_blob_to_uri, Path),
    ],
)
def test_copy_files_remote_list_to_local_list(
    src_type: Callable[[storage.Blob], storage.Blob] | Callable[[storage.Blob], str],
    dst_type: type[str] | type[Path],
    filenames: list[str],
    tmp_path: Path,
    remote_blobs: tuple[storage.Blob, list[storage.Blob]],
    storage_client: storage.Client,
):
    # make source/destination lists
    _, _srcs = remote_blobs
    srcs = [src_type(blob) for blob in _srcs]
    dsts = [dst_type(tmp_path / name) for name in filenames]

    # make sure all the directories we need to write to exist
    for dst in dsts:
        os.makedirs(op.dirname(dst), exist_ok=True)

    # ensure that none of our destination files exist
    assert all(not op.exists(dst) for dst in dsts)

    copy_files(srcs, dsts, client=storage_client)

    # ensure that all of our destination files exist
    assert all(op.exists(dst) for dst in dsts)


@pytest.mark.e2e
@pytest.mark.parametrize(
    ("src_type", "dst_type"),
    [
        (lambda blob: blob, str),
        (_blob_to_uri, str),
        (lambda blob: blob, Path),
        (_blob_to_uri, Path),
    ],
)
def test_copy_files_remote_list_to_local_dir(
    src_type: Callable[[storage.Blob], storage.Blob] | Callable[[storage.Blob], str],
    dst_type: type[str] | type[Path],
    tmp_path: Path,
    remote_blobs: tuple[storage.Blob, list[storage.Blob]],
    storage_client: storage.Client,
):
    # make source/destination lists
    _, _srcs = remote_blobs
    srcs = [src_type(blob) for blob in _srcs]
    dst = dst_type(tmp_path / "out")

    # create our desintation directory
    os.makedirs(dst)

    copy_files(srcs, dst, client=storage_client)

    # ensure that all of our destination files exist
    dst_files = [p for p in Path(dst).glob("*") if p.is_file()]
    assert len(dst_files) == len(_srcs)


@pytest.mark.e2e
@pytest.mark.parametrize(
    ("src_type", "dst_type"),
    [
        (lambda blob: blob, str),
        (_blob_to_uri, str),
        (lambda blob: blob, storage.Blob.from_string),
        (_blob_to_uri, storage.Blob.from_string),
    ],
)
def test_copy_files_remote_list_to_remote_list(
    src_type: Callable[[storage.Blob], storage.Blob] | Callable[[storage.Blob], str],
    dst_type: Callable[[str], storage.Blob] | Callable[[str], str],
    remote_blobs: tuple[storage.Blob, list[storage.Blob]],
    filenames: list[str],
    storage_root: storage.Blob,
    storage_client: storage.Client,
):
    # prepare the source and destination lists
    storage_uri = op.join(_blob_to_uri(storage_root), "out")
    _, _srcs = remote_blobs
    srcs = [src_type(blob) for blob in _srcs]
    dsts = [dst_type(op.join(storage_uri, name)) for name in filenames]

    # ensure that the destination root (storage_root) is empty
    prefix = storage.Blob.from_string(storage_uri).name
    dst_blobs = list(storage_root.bucket.list_blobs(prefix=prefix))
    assert len(dst_blobs) == 0

    copy_files(srcs, dsts, client=storage_client)

    # check that all of the expected files have been copied
    for dst in dsts:
        if isinstance(dst, storage.Blob):
            blob = dst
        else:
            blob = storage.Blob.from_string(dst)

        assert blob.exists(client=storage_client)


@pytest.mark.e2e
@pytest.mark.parametrize(
    ("src_type", "dst_type"),
    [
        (lambda blob: blob, str),
        (_blob_to_uri, str),
        (lambda blob: blob, storage.Blob.from_string),
        (_blob_to_uri, storage.Blob.from_string),
    ],
)
def test_copy_files_remote_list_to_remote_dir(
    src_type: Callable[[storage.Blob], storage.Blob] | Callable[[storage.Blob], str],
    dst_type: Callable[[str], storage.Blob] | Callable[[str], str],
    remote_blobs: tuple[storage.Blob, list[storage.Blob]],
    storage_root: storage.Blob,
    storage_client: storage.Client,
):
    # prepare the source and destination lists
    storage_uri = op.join(_blob_to_uri(storage_root), "out")
    _, _srcs = remote_blobs
    srcs = [src_type(blob) for blob in _srcs]
    dst = dst_type(op.join(storage_uri, "out"))

    # ensure that the destination root (storage_root) is empty
    prefix = storage.Blob.from_string(storage_uri).name
    dst_blobs = list(storage_root.bucket.list_blobs(prefix=prefix))
    assert len(dst_blobs) == 0

    copy_files(srcs, dst, client=storage_client)

    # check that all of the expected file have been copied
    flat_names = [_flatten(name) for name in _srcs]
    dst_blobs = list(storage_root.bucket.list_blobs(prefix=prefix))
    assert len(dst_blobs) == 4
    for blob in dst_blobs:
        basename = op.basename(blob.name)
        assert basename in flat_names
