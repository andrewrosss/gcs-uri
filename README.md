# gcs-uri

Simple API to copy files to and from Google Cloud Storage

[![PyPI Version](https://img.shields.io/pypi/v/gcs-uri.svg)](https://pypi.org/project/gcs-uri/)

## Installation

```bash
pip install gcs-uri
```

## Usage

`gcs-uri` exposes two functions as its main public API

- `copy_file`
- `copy_dir`

These two functions do exactly what they sound like they do.

`copy_file` will copy a source file (either a local file or a remote blob in GCS) to destination file (either a local file or remote blob in GCS).

`copy_dir` will recursively copy the contents of a directory (either a local directory or a remote "directory" in GCS) to a destination directory (either a local directory or a remote "directory" in GCS)

The idea being that you can pass just about any object to these two functions and the functions will figures how to do the copying.

## Examples

### Local file -> local file

In this case `copy_file` behaves just like `shutil.copy2` or `cp`, copying the source file to the destination file locally.

```python
src = '/my/src/file.txt'
dst = '/my/dst/file.txt'

copy_file(src, dst)
```

`src` and `dst` can also be `pathlib.Path` objects:

```python
from pathlib import Path

src = Path('/my/src/file.txt')
dst = Path('/my/dst/file.txt')

copy_file(src, dst)
```

### Local dir -> local dir

In this case `copy_dir` behaves just like `shutil.copytree` (or somewhat like rsync, but `copy_dir` will "re-copy" all files to the destination whether they exist in the the destination or not).

```python
src = '/my/src'
dst = '/my/dst'

copy_dir(src, dst)

# if there was a file `/my/src/a/b.txt` after `copy_dir`
# there would then be a file `/my/dst/a/b.txt`
```

The source and destination can include or omit a trailing slash and the results are the same as above.

### Local file -> remote file (upload)

To copy a file to a google cloud bucket, barely anything has to change, the destination should simply be a google storage URI:

```python
src = '/my/src/file.txt'
dst = 'gs://my-bkt/dst/file.txt'

copy_file(src, dst)
```

If you would like `gcs-uri` to use a particular Google Storage Client, this can be provided as a keyword(-only) argument (the same applies to `copy_dir`):

```python
from google.cloud import storage

client = storage.Client()

src = '/my/src/file.txt'
dst = 'gs://my-bkt/dst/file.txt'

copy_file(src, dst, client=client)
```

If no client is provided and either of the source or destinations (or both) are determined to represent a remote location then `gcs-uri` will try to instantiate a client by calling `storage.Client()`.

Note, we can provided `gcs-uri` with "richer" objects (instead of just strings):

```python
from pathlib import Path
from google.cloud import storage

client = storage.Client()

src = Path('/my/src/file.txt')
dst = storage.Blob.from_string('gs://my-bkt/dst/file.txt', client=client)

copy_file(src, dst)
```

### Local dir -> remote dir (upload)

The concepts from the previous sections apply here:

```python
src = '/my/src'
dst = 'gs://my-bkt/dst'

copy_dir(src, dst)

# if there was a file `/my/src/a/b.txt` after `copy_dir`
# there would then be a blob `gs://my-bkt/dst/a/b.txt`
```

### Remote file -> local file (download)

```python
src = 'gs://my-bkt/src/file.txt'
dst = '/my/dst/file.txt'

copy_file(src, dst)
```

### Remote dir -> local dir (download)

```python
src = 'gs://my-bkt/src'
dst = '/my/dst'

copy_dir(src, dst)
```

### Remote file -> remote file (transfer)

```python
src = 'gs://my-bkt/src/file.txt'
dst = 'gs://my-other-bkt/dst/file.txt'

copy_file(src, dst)
```

### Remote dir -> remote dir (transfer)

```python
src = 'gs://my-bkt/src'
dst = 'gs://my-other-bkt/dst'

copy_dir(src, dst)
```

## Tests

This package comes with some basic end-to-end (e2e) tests. They require an active google cloud project with the google storage API enabled.

To help with running them there is a utility script in the root of this repo: `run_e2e_tests.py`.

```text
usage: run_e2e_tests.py [-h] [-v] [-c GOOGLE_APPLICATION_CREDENTIALS]
                        [-u TEST_STORAGE_URI]

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         show program's version number and exit
  -c GOOGLE_APPLICATION_CREDENTIALS, --google-application-credentials GOOGLE_APPLICATION_CREDENTIALS
                        Google cloud service account to use.
  -u TEST_STORAGE_URI, --test-storage-uri TEST_STORAGE_URI
                        Google storage uri to use when running e2e tests.
```

This script requires you to provided a service account json file as we'll as a URI to a location in google cloud which the tests will use to copy blobs to/from. (**IMPORTANT**: **_all_** blobs at and beneath the location you specifify will be removed - the bucket itself will **not** be removed).

So, run the e2e tests with something like:

```bash
python -m run_e2e_tests -c "path/to/service-account.json" -u "gs://my-bkt/gcs-uri-tests"
```

## Contributing

1. Have or install a recent version of `poetry` (version >= 1.1)
1. Fork the repo
1. Setup a virtual environment (however you prefer)
1. Run `poetry install`
1. Run `pre-commit install`
1. Add your changes (adding/updating tests is always nice too)
1. Commit your changes + push to your fork
1. Open a PR
