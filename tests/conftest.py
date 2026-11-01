# noqa: INP001, D100

import zipfile
from pathlib import Path

import pooch
import pytest


@pytest.fixture(scope="session", autouse=True)
def download_test_data() -> None:
    """Fetch cassettes yaml data from cassettes test release."""
    url = "https://github.com/ioos/erddapy/releases/download"
    version = "v2026.11.01"

    fname = pooch.retrieve(
        url=f"{url}/{version}/test_data.zip",
        known_hash="sha256:5a5974c9c9c58189aad6548ca13a160594bafe96c5d120b49ef5c63a417a2d87",
    )

    here = Path(__file__).resolve().parent
    with zipfile.ZipFile(fname, "r") as zip_ref:
        zip_ref.extractall(here)
