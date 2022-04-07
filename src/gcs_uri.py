from __future__ import annotations

import concurrent.futures
import glob
import os
import os.path as op
import shutil
from pathlib import Path
from typing import Callable
from typing import cast
from typing import Literal
from urllib.parse import unquote_plus
from urllib.parse import urlparse
from urllib.parse import urlunparse

from google.cloud import storage


def copy_file(
    src: str | Path | storage.Blob,
    dst: str | Path | storage.Blob,
    *,
    client: storage.Client | None = None,
    quiet: bool = False,
) -> None:
    """Copy a single file.

    If `src` and `dst` are both determined to be local files then `client` is ignored.
    """
    return _copy(src, dst, _scheme_copy_fns=_FILE_FUNCTIONS, client=client, quiet=quiet)


def copy_dir(
    src: str | Path | storage.Blob,
    dst: str | Path | storage.Blob,
    *,
    client: storage.Client | None = None,
    quiet: bool = False,
) -> None:
    """Copy a directory (recursively).

    If `src` and `dst` are both determined to be local directories
    then `client` is ignored.
    """
    return _copy(src, dst, _scheme_copy_fns=_DIR_FUNCTIONS, client=client, quiet=quiet)


# --- PRIVATE API ---


def _copy(
    src: str | Path | storage.Blob,
    dst: str | Path | storage.Blob,
    *,
    _scheme_copy_fns: dict[tuple[str, str], Callable],
    client: storage.Client | None = None,
    quiet: bool = False,
) -> None:
    src_scheme = _parse_scheme(src)
    dst_scheme = _parse_scheme(dst)
    schemes = (src_scheme, dst_scheme)

    copy_fn = _scheme_copy_fns[schemes]

    # local copy operation
    if "gs" not in schemes:
        copy_fn(src, dst, quiet=quiet)  # type: ignore
        return

    # one (or both) of src/dst are remote URIs, need to
    # forward the storag client
    client = client or storage.Client()
    copy_fn(src, dst, client=client, quiet=quiet)  # type: ignore


def _parse_scheme(arg: str | Path | storage.Blob) -> Literal["", "gs"]:
    if isinstance(arg, str):
        scheme = urlparse(arg).scheme
        if scheme in ("", "file"):
            return ""
        elif scheme == "gs":
            return "gs"
        else:
            raise ValueError(f"Failed to determine scheme for {arg!r}")
    elif isinstance(arg, Path):
        return ""
    elif isinstance(arg, storage.Blob):
        return "gs"
    else:
        raise ValueError(f"Failed to determine scheme for {arg!r}")


# --- UTILITY FUNCTIONS ---


def _blob_to_uri(blob: storage.Blob) -> str:
    components = ("gs", blob.bucket.name, blob.name, "", "", "")
    return urlunparse(components)


def _filename_to_uri(filename: str | Path) -> str:
    components = ("file", "", str(filename), "", "", "")
    return urlunparse(components)


def _uri_to_filename(uri: str | Path) -> str:
    uri = unquote_plus(urlparse(uri).path) if isinstance(uri, str) else uri
    return str(uri)


def _log_successful_copy(
    src: str | Path | storage.Blob, *, n: int | None = 1, N: int | None = 1
):
    uri = _filename_to_uri(src) if isinstance(src, (str, Path)) else _blob_to_uri(src)
    prefix = "" if None in (n, N) else f"[{n}/{N}] - "
    print(f"{prefix}Copied {uri!r}")


# --- COPY FUNCTION IMPLEMENTATIONS ---


def _copy_file(src: str | Path, dst: str | Path, *, quiet: bool = False):
    """local file -> local file"""
    shutil.copy2(src, dst)
    if not quiet:
        _log_successful_copy(src, n=None, N=None)


def _sync_files(src: str | Path, dst: str | Path, *, quiet: bool = False):
    """local dir -> local dir"""

    def _copy_fn_factory(quiet: bool):
        def _copy_fn(src: str, dst: str):
            """Wrapper around copy2 with some optional logging"""
            shutil.copy2(src, dst)
            if not quiet:
                _log_successful_copy(src, n=None, N=None)

        return _copy_fn

    shutil.copytree(src, dst, dirs_exist_ok=True, copy_function=_copy_fn_factory(quiet))


def _download_file(
    src: str | storage.Blob,
    dst: str | Path,
    *,
    client: storage.Client | None = None,
    quiet: bool = False,
):
    """remote blob -> local file"""
    client = client or storage.Client()
    _src = storage.Blob.from_string(src, client=client) if isinstance(src, str) else src
    _dst = _uri_to_filename(dst)
    if op.isdir(_dst):
        filename = op.join(_dst, op.basename(_src.name))
    else:
        filename = _dst
    _src.download_to_filename(filename)
    if not quiet:
        _log_successful_copy(_src)


def _upload_file(
    src: str | Path,
    dst: str | storage.Blob,
    *,
    client: storage.Client | None = None,
    quiet: bool = False,
):
    """local file -> remote blob"""
    client = client or storage.Client()
    _src = _uri_to_filename(src)
    _dst = storage.Blob.from_string(dst, client=client) if isinstance(dst, str) else dst
    if cast(str, _dst.name).endswith("/"):
        _dst.name = op.join(_dst.name, op.basename(_src))
    _dst.upload_from_filename(_src)
    if not quiet:
        _log_successful_copy(_src)


def _download_dir(
    src: str | storage.Blob,
    dst: str | Path,
    *,
    client: storage.Client | None = None,
    quiet: bool = False,
):
    """remote dir -> local dir"""
    client = client or storage.Client()
    _src = storage.Blob.from_string(src, client=client) if isinstance(src, str) else src
    _dst = _uri_to_filename(dst)
    blobs: list[storage.Blob] = list(client.list_blobs(_src.bucket, prefix=_src.name))

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # submit each of the download jobs to the thread pool
        future_to_uri = {}
        for b in blobs:
            relpath = op.relpath(b.name, _src.name or "")
            filename = op.join(_dst, relpath)  # type: ignore
            os.makedirs(op.dirname(filename), exist_ok=True)
            future = executor.submit(b.download_to_filename, filename)
            future_to_uri[future] = _blob_to_uri(b)

        # report the status of each downloaded file
        completed_futures = concurrent.futures.as_completed(future_to_uri)
        for i, future in enumerate(completed_futures, 1):
            uri = future_to_uri[future]
            future.result()
            if not quiet:
                _log_successful_copy(uri, n=i, N=len(future_to_uri))


def _upload_dir(
    src: str | Path,
    dst: str | storage.Blob,
    *,
    client: storage.Client | None = None,
    quiet: bool = False,
):
    """local dir -> remote dir"""
    client = client or storage.Client()
    _src = _uri_to_filename(src)
    _dst = storage.Blob.from_string(dst, client=client) if isinstance(dst, str) else dst
    pattern = op.join(_src, "**")
    files = [f for f in glob.glob(pattern, recursive=True) if op.isfile(f)]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # submit each of the download jobs to the thread pool
        future_to_filename = {}
        for filename in files:
            relpath = op.relpath(filename, _src)
            name = op.join(_dst.name, relpath)
            b = storage.Blob(name, _dst.bucket)
            future = executor.submit(b.upload_from_filename, filename)
            future_to_filename[future] = filename

        # report the status of each downloaded file
        completed_futures = concurrent.futures.as_completed(future_to_filename)
        for i, future in enumerate(completed_futures, 1):
            filename = future_to_filename[future]
            future.result()
            if not quiet:
                _log_successful_copy(filename, n=i, N=len(future_to_filename))


def _copy_blob(
    src: str | storage.Blob,
    dst: str | storage.Blob,
    *,
    client: storage.Client | None = None,
    quiet: bool = False,
):
    """remote blob -> remote blob"""
    client = client or storage.Client()
    _src = storage.Blob.from_string(src, client=client) if isinstance(src, str) else src
    _dst = storage.Blob.from_string(dst, client=client) if isinstance(dst, str) else dst
    src_is_dir = cast(str, _src.name).endswith("/")
    dst_is_dir = cast(str, _dst.name).endswith("/")
    if dst_is_dir and not src_is_dir:
        _dst.name = op.join(_dst.name, op.basename(_src.name))
    _src.bucket.copy_blob(_src, _dst.bucket, _dst.name)
    if not quiet:
        _log_successful_copy(_dst)


def _sync_blobs(
    src: str | storage.Blob,
    dst: str | storage.Blob,
    *,
    client: storage.Client | None = None,
    quiet: bool = False,
):
    """remote dir -> remote dir"""
    client = client or storage.Client()
    src = storage.Blob.from_string(src, client=client) if isinstance(src, str) else src
    dst = storage.Blob.from_string(dst, client=client) if isinstance(dst, str) else dst
    src_blobs: list[storage.Blob] = list(client.list_blobs(src.bucket, prefix=src.name))

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # submit each of the download jobs to the thread pool
        future_to_blob: dict[concurrent.futures.Future[None], storage.Blob] = {}
        for src_blob in src_blobs:
            relpath = op.relpath(src_blob.name, src.name)
            new_name = op.join(dst.name, relpath)
            dst_blob = storage.Blob(new_name, dst.bucket)
            _src_uri = _blob_to_uri(src_blob)
            _dst_uri = _blob_to_uri(dst_blob)
            future = executor.submit(
                _copy_blob, _src_uri, _dst_uri, client=client, quiet=True
            )
            future_to_blob[future] = src_blob

        # report the status of each downloaded file
        completed_futures = concurrent.futures.as_completed(future_to_blob)
        for i, future in enumerate(completed_futures, 1):
            blob = future_to_blob[future]
            future.result()
            if not quiet:
                _log_successful_copy(blob, n=i, N=len(future_to_blob))


# --- MAPPING OF SCHEMES TO COPY FUNCTIONS ---

_FILE_FUNCTIONS = {
    ("", ""): _copy_file,  # local file -> local file
    ("gs", ""): _download_file,  # remote file -> local file
    ("", "gs"): _upload_file,  # local file -> remote file
    ("gs", "gs"): _copy_blob,  # remote file -> remote file
}

_DIR_FUNCTIONS = {
    ("", ""): _sync_files,  # local dir -> local dir
    ("gs", ""): _download_dir,  # remote dir -> local dir
    ("", "gs"): _upload_dir,  # local dir -> remote dir
    ("gs", "gs"): _sync_blobs,  # remote dir -> remote dir
}
