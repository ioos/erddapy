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
        known_hash="sha256:f48243749a42bebf6955c3b2e79728de97d200a20d623f0469f30ba2343a3e54",
    )

    here = Path(__file__).resolve().parent
    with zipfile.ZipFile(fname, "r") as zip_ref:
        zip_ref.extractall(here)


if __name__ == "__main__":
    download_test_data()
