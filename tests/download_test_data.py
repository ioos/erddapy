import zipfile
from pathlib import Path

import pooch


def download_test_data():
    url = "https://github.com/ioos/erddapy/releases/download"
    version = "2026.04.28"

    fname = pooch.retrieve(
        url=f"{url}/{version}/test_data.zip",
        known_hash="sha256:edfbddaaa1527e742e6775b8b14fab4ab0be8f8d09a74873524b8ba3cbff8cce",
    )

    here = Path(__file__).resolve().parent
    print(fname)
    print(here)
    with zipfile.ZipFile(fname, "r") as zip_ref:
        zip_ref.extractall(here)


if __name__ == "__main__":
    download_test_data()
