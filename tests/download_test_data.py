# noqa: INP001, D100

import zipfile
from pathlib import Path

import pooch


def download_test_data() -> None:
    """Fetch cassettes yaml data from cassettes test release."""
    url = "https://github.com/ioos/erddapy/releases/download"
    version = "v2026.09.15"

    fname = pooch.retrieve(
        url=f"{url}/{version}/test_data.zip",
        known_hash="sha256:0c9d0c110d8f817ff29977edeadbdfae86ea5ce92eff784998a2a4d3dbbe1ef1",
    )

    here = Path(__file__).resolve().parent
    with zipfile.ZipFile(fname, "r") as zip_ref:
        zip_ref.extractall(here)


if __name__ == "__main__":
    download_test_data()
