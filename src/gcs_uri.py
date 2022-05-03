from __future__ import annotations

import concurrent.futures
import datetime
import functools
import glob
import os
import os.path as op
import re
import shutil
import sys
from pathlib import Path
from typing import Callable
from typing import cast
from typing import Sequence
from typing import TypeVar
from urllib.parse import unquote_plus
from urllib.parse import urlparse
from urllib.parse import urlunparse

from google.cloud.storage import Blob
from google.cloud.storage import Bucket
from google.cloud.storage import Client
from google.cloud.storage.retry import ConditionalRetryPolicy
from google.cloud.storage.retry import DEFAULT_RETRY
from google.cloud.storage.retry import is_generation_specified

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec
else:
    from typing import ParamSpec


__version__ = "1.3.0"

# Resources describing the retry strategy:
# https://cloud.google.com/storage/docs/retry-strategy#client-libraries
# https://cloud.google.com/storage/docs/samples/storage-configure-retries
# https://cloud.google.com/python/docs/reference/storage/latest/retry_timeout#configuring-retries

# Custom retry with more rapid attempts as well as more time to retry
_RETRY = DEFAULT_RETRY.with_deadline(600.0).with_delay(multiplier=1.2)

# Custom conditional retry composing the custom retry defined above
_CONDITIONAL_RETRY = ConditionalRetryPolicy(
    _RETRY,
    is_generation_specified,
    ["query_params"],
)

# To enable uploading on slower connections we use a lower chunk_size
# for uploads and downloads, to understand why see this github issue:
# https://github.com/googleapis/python-storage/issues/74
# and specifically this comment:
# https://github.com/googleapis/python-storage/issues/74#issuecomment-603296568
_CHUNK_SIZE = 10 * 1024 * 1024  # 10 MB


def copy_file(
    src: str | Path | Blob,
    dst: str | Path | Blob,
    *,
    client: Client | None = None,
    quiet: bool = False,
) -> None:
    """Copy a single file.

    If `src` and `dst` are both determined to be local files then `client` is ignored.
    """
    return _copy(src, dst, _scheme_copy_fns=_FILE_FUNCTIONS, client=client, quiet=quiet)


def copy_dir(
    src: str | Path | Blob,
    dst: str | Path | Blob,
    *,
    client: Client | None = None,
    quiet: bool = False,
) -> None:
    """Copy a directory (recursively).

    If `src` and `dst` are both determined to be local directories
    then `client` is ignored.
    """
    return _copy(src, dst, _scheme_copy_fns=_DIR_FUNCTIONS, client=client, quiet=quiet)


def copy_files(
    srcs: Sequence[str | Path | Blob],
    dsts: str | Path | Blob | Sequence[str | Path | Blob],
    *,
    client: Client | None = None,
    quiet: bool = False,
) -> None:
    """Copy a list of files.

    If `dsts` is a `str | Path | Blob` it is treated as a directory
    and each of the files in `srcs` will have its name "flattened" and will be
    copied under `dsts`.

    if `dsts` is a `Sequence[str | Path | Blob]` it is zipped with `srcs`, i.e.
    each file in `srcs` is copied to its corresponding entry in `dsts`.
    """
    if isinstance(dsts, (str, Path)):
        _dsts = [op.join(dsts, _flatten(src)) for src in srcs]
    elif isinstance(dsts, Blob):
        blob_name: str = dsts.name or ""  # type: ignore
        bucket: Bucket = dsts.bucket  # type: ignore
        names = [op.join(blob_name, _flatten(src)) for src in srcs]
        _dsts = [Blob(name, bucket) for name in names]
    else:
        _dsts = dsts

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # submit each of the file-copy jobs to the thread pool
        future_to_src: dict[concurrent.futures.Future[None], str | Path | Blob] = {}
        for src, dst in zip(srcs, _dsts):
            future = executor.submit(copy_file, src, dst, client=client, quiet=True)
            future_to_src[future] = src

        # report the status of each copied file
        completed_futures = concurrent.futures.as_completed(future_to_src)
        for i, future in enumerate(completed_futures, 1):
            src = future_to_src[future]
            future.result()
            if not quiet:
                _log_successful_copy(src, n=i, N=len(future_to_src))


# --- PRIVATE API ---


def _copy(
    src: str | Path | Blob,
    dst: str | Path | Blob,
    *,
    _scheme_copy_fns: dict[tuple[str, str], Callable],
    client: Client | None = None,
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
    client = client or Client()
    copy_fn(src, dst, client=client, quiet=quiet)  # type: ignore


def _parse_scheme(arg: str | Path | Blob) -> str:
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
    elif isinstance(arg, Blob):
        return "gs"
    else:
        raise ValueError(f"Failed to determine scheme for {arg!r}")


# --- UTILITY FUNCTIONS ---


def _blob_to_uri(blob: Blob) -> str:
    components = ("gs", blob.bucket.name, blob.name, "", "", "")
    return urlunparse(components)


def _filename_to_uri(filename: str | Path) -> str:
    components = ("file", "", str(filename), "", "", "")
    return urlunparse(components)


def _uri_to_filename(uri: str | Path) -> str:
    uri = unquote_plus(urlparse(uri).path) if isinstance(uri, str) else uri
    return str(uri)


def _log_successful_copy(
    src: str | Path | Blob,
    *,
    n: int | None = 1,
    N: int | None = 1,
):
    uri = _filename_to_uri(src) if isinstance(src, (str, Path)) else _blob_to_uri(src)
    prefix = "" if None in (n, N) else f"[{n}/{N}] - "
    print(f"{prefix}Copied {uri!r}")


def _log_skipping_file(
    src: str | Path,
    *,
    n: int | None = 1,
    N: int | None = 1,
) -> None:
    uri = _filename_to_uri(src)
    prefix = "" if None in (n, N) else f"[{n}/{N}] - "
    print(f"{prefix}Skipping {uri!r}")


def _flatten(spb: str | Path | Blob) -> str:
    """Convert a URI (local or remote) to a flat filename.

    Examples:
        >>> _flatten('.cache/dir/file.txt')
        '.cache-dir-file.txt'
        >>> _flatten('/abs/path to  some/file.tar.gz')
        'abs-path-to-some-file.tar.gz'
        >>> _flatten('gs://bkt/some/blob/')
        'gs-bkt-some-blob'
        >>> _flatten('gs://bkt')
        'gs-bkt'
        >>> # can pass Path objects:
        >>> from pathlib import Path
        >>> fp = Path('path/to/my/file.csv')
        >>> _flatten(fp)
        'path-to-my-file.csv'
        >>> # can pass storage.Blob
        >>> from google.cloud.storage import Blob
        >>> blob = Blob.from_string('gs://bkt/my/module.py')
        >>> _flatten(blob)
        'bkt-my-module.py'
    """
    s = str(spb) if isinstance(spb, (str, Path)) else _blob_to_uri(spb)
    filename_s = re.sub(r"[^0-9a-zA-Z_\.-]+", "-", s)  # remove non-(alnum | _ | . | -)
    filename_s = re.sub(r"(^-+|-+$)", "", filename_s)  # remove leading/trailing '-'
    return filename_s


P = ParamSpec("P")
R = TypeVar("R")


def _log_elapsed_time_on_error(copy_fn: Callable[P, R]) -> Callable[P, R]:
    @functools.wraps(copy_fn)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        start = datetime.datetime.now()
        try:
            return copy_fn(*args, **kwargs)
        except Exception:
            # first positional arg of copy_fn is always src
            src: str | Path | Blob = args[0]  # type: ignore
            elapsed_time = datetime.datetime.now() - start
            total_seconds = elapsed_time.total_seconds()
            msg = f"Failed to copy {src!r} (attempt took {total_seconds:.6f}s)"
            print(msg)
            raise

    return wrapper


# --- COPY FUNCTION IMPLEMENTATIONS ---


@_log_elapsed_time_on_error
def _copy_file(src: str | Path, dst: str | Path, *, quiet: bool = False):
    """local file -> local file"""
    shutil.copy2(src, dst)
    if not quiet:
        _log_successful_copy(src, n=None, N=None)


@_log_elapsed_time_on_error
def _sync_files(src: str | Path, dst: str | Path, *, quiet: bool = False):
    """local dir -> local dir"""

    # NOTE: to support python3.7 we do our own recursive copying
    # If support for python3.7 is dropped, change this back to the
    # implementation here:
    # https://github.com/andrewrosss/gcs-uri/blob/0fd3d4b8fbae947721c9e099a1239e3c551d13ed/src/gcs_uri.py#L213-L222  # noqa: E501
    srcroot = Path(src)
    srcpaths = list(srcroot.rglob("*"))
    for i, srcpath in enumerate(srcpaths):
        relpath = srcpath.relative_to(srcroot)
        dstpath = Path(dst) / relpath
        if srcpath.is_dir():
            dstpath.mkdir(exist_ok=True)
        elif srcpath.is_file():
            shutil.copy2(srcpath, dstpath)
            if not quiet:
                _log_successful_copy(srcpath, n=i, N=len(srcpaths))
        else:
            if not quiet:
                _log_skipping_file(srcpath, n=i, N=len(srcpaths))


@_log_elapsed_time_on_error
def _download_file(
    src: str | Blob,
    dst: str | Path,
    *,
    client: Client | None = None,
    quiet: bool = False,
):
    """remote blob -> local file"""
    client = client or Client()
    _src = Blob.from_string(src, client=client) if isinstance(src, str) else src
    _src.chunk_size = _CHUNK_SIZE
    _dst = _uri_to_filename(dst)
    if op.isdir(_dst):
        filename = op.join(_dst, op.basename(_src.name))
    else:
        filename = _dst
    _src.download_to_filename(filename, client=client, retry=_RETRY)
    if not quiet:
        _log_successful_copy(_src)


@_log_elapsed_time_on_error
def _upload_file(
    src: str | Path,
    dst: str | Blob,
    *,
    client: Client | None = None,
    quiet: bool = False,
):
    """local file -> remote blob"""
    client = client or Client()
    _src = _uri_to_filename(src)
    _dst = Blob.from_string(dst, client=client) if isinstance(dst, str) else dst
    _dst.chunk_size = _CHUNK_SIZE
    if cast(str, _dst.name).endswith("/"):
        _dst.name = op.join(_dst.name, op.basename(_src))
    _dst.upload_from_filename(_src, client=client, retry=_CONDITIONAL_RETRY)
    if not quiet:
        _log_successful_copy(_src)


@_log_elapsed_time_on_error
def _download_dir(
    src: str | Blob,
    dst: str | Path,
    *,
    client: Client | None = None,
    quiet: bool = False,
):
    """remote dir -> local dir"""
    _client = client or Client()
    _src = Blob.from_string(src, client=_client) if isinstance(src, str) else src
    _dst = _uri_to_filename(dst)
    blobs: list[Blob] = list(_client.list_blobs(_src.bucket, prefix=_src.name))

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # submit each of the download jobs to the thread pool
        future_to_uri = {}
        for b in blobs:
            relpath = op.relpath(b.name, _src.name or "")
            filename = op.join(_dst, relpath)  # type: ignore
            os.makedirs(op.dirname(filename), exist_ok=True)
            b.chunk_size = _CHUNK_SIZE
            future = executor.submit(
                b.download_to_filename,
                filename,
                client=client,
                retry=_RETRY,
            )
            future_to_uri[future] = _blob_to_uri(b)

        # report the status of each downloaded file
        completed_futures = concurrent.futures.as_completed(future_to_uri)
        for i, future in enumerate(completed_futures, 1):
            uri = future_to_uri[future]
            future.result()
            if not quiet:
                _log_successful_copy(uri, n=i, N=len(future_to_uri))


@_log_elapsed_time_on_error
def _upload_dir(
    src: str | Path,
    dst: str | Blob,
    *,
    client: Client | None = None,
    quiet: bool = False,
):
    """local dir -> remote dir"""
    client = client or Client()
    _src = _uri_to_filename(src)
    _dst = Blob.from_string(dst, client=client) if isinstance(dst, str) else dst
    pattern = op.join(_src, "**")
    files = [f for f in glob.glob(pattern, recursive=True) if op.isfile(f)]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # submit each of the download jobs to the thread pool
        future_to_filename = {}
        for filename in files:
            relpath = op.relpath(filename, _src)
            name = op.join(_dst.name, relpath)
            b = Blob(name, _dst.bucket)
            b.chunk_size = _CHUNK_SIZE
            future = executor.submit(
                b.upload_from_filename,
                filename,
                client=client,
                retry=_CONDITIONAL_RETRY,
            )
            future_to_filename[future] = filename

        # report the status of each downloaded file
        completed_futures = concurrent.futures.as_completed(future_to_filename)
        for i, future in enumerate(completed_futures, 1):
            filename = future_to_filename[future]
            future.result()
            if not quiet:
                _log_successful_copy(filename, n=i, N=len(future_to_filename))


@_log_elapsed_time_on_error
def _copy_blob(
    src: str | Blob,
    dst: str | Blob,
    *,
    client: Client | None = None,
    quiet: bool = False,
):
    """remote blob -> remote blob"""
    client = client or Client()
    _src = Blob.from_string(src, client=client) if isinstance(src, str) else src
    _dst = Blob.from_string(dst, client=client) if isinstance(dst, str) else dst
    src_is_dir = cast(str, _src.name).endswith("/")
    dst_is_dir = cast(str, _dst.name).endswith("/")
    if dst_is_dir and not src_is_dir:
        _dst.name = op.join(_dst.name, op.basename(_src.name))
    _src.bucket.copy_blob(_src, _dst.bucket, _dst.name, client=client)
    if not quiet:
        _log_successful_copy(_dst)


@_log_elapsed_time_on_error
def _sync_blobs(
    src: str | Blob,
    dst: str | Blob,
    *,
    client: Client | None = None,
    quiet: bool = False,
):
    """remote dir -> remote dir"""
    _client = client or Client()
    _src = Blob.from_string(src, client=_client) if isinstance(src, str) else src
    _dst = Blob.from_string(dst, client=_client) if isinstance(dst, str) else dst
    src_blobs: list[Blob] = list(_client.list_blobs(_src.bucket, prefix=_src.name))

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # submit each of the download jobs to the thread pool
        future_to_blob: dict[concurrent.futures.Future[None], Blob] = {}
        for src_blob in src_blobs:
            relpath = op.relpath(src_blob.name, _src.name)
            new_name = op.join(_dst.name, relpath)
            dst_blob = Blob(new_name, _dst.bucket)
            _src_uri = _blob_to_uri(src_blob)
            _dst_uri = _blob_to_uri(dst_blob)
            future = executor.submit(
                _copy_blob,
                _src_uri,
                _dst_uri,
                client=_client,
                quiet=True,
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
