"""
A thin wrapper to run the end-to-end tests for gcs-uri.

This script essentially runs the equivalent of the following shell command:

GOOGLE_APPLICATION_CREDENTIALS=<PATH> GCS_URI_TEST_STORAGE_URI=<URI> pytest -m e2e
"""
from __future__ import annotations

import argparse
import os
import os.path as op
from typing import NoReturn

import pytest


__version__ = "0.1.0"


def cli() -> NoReturn:
    raise SystemExit(main())


def main() -> int:
    parser = create_parser()
    args = parser.parse_args()

    if hasattr(args, "handler"):
        return args.handler(args)

    parser.print_help()
    return 1


def create_parser(
    parser: argparse.ArgumentParser | None = None,
) -> argparse.ArgumentParser:
    parser = parser or argparse.ArgumentParser()
    parser.add_argument("-v", "--version", action="version", version=__version__)
    parser.add_argument(
        "-c",
        "--google-application-credentials",
        help="Google cloud service account to use.",
    )
    parser.add_argument(
        "-u",
        "--test-storage-uri",
        help="Google storage uri to use when running e2e tests.",
    )

    parser.set_defaults(handler=handler)

    return parser


def handler(args: argparse.Namespace) -> int:
    google_application_credentials: str = args.google_application_credentials
    test_storage_uri: str = args.test_storage_uri

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_application_credentials
    os.environ["GCS_URI_TEST_STORAGE_URI"] = test_storage_uri

    retcode = pytest.main(["-m", "e2e"])

    return retcode


if __name__ == "__main__":
    cli()
