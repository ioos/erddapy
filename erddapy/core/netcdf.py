"""Handles netCDF responses."""

import platform
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO, Dict, Generator, Optional
from urllib.parse import urlparse

from erddapy.core.url import urlopen


def _nc_dataset(url, requests_kwargs: Optional[Dict] = None):
    """Return a netCDF4-python Dataset from memory and fallbacks to disk if that fails."""
    from netCDF4 import Dataset

    data = urlopen(url, requests_kwargs)
    try:
        return Dataset(Path(urlparse(url).path).name, memory=data.read())
    except OSError:
        # if libnetcdf is not compiled with in-memory support fallback to a local tmp file
        data.seek(0)
        with _tempnc(data) as _nc:
            return Dataset(_nc)


@contextmanager
def _tempnc(data: BinaryIO) -> Generator[str, None, None]:
    """Create a temporary netcdf file."""
    from tempfile import NamedTemporaryFile

    # Let windows handle the file cleanup to avoid its aggressive file lock.
    delete = True
    if platform.system().lower() == "windows":
        delete = False

    tmp = None
    try:
        tmp = NamedTemporaryFile(suffix=".nc", prefix="erddapy_", delete=delete)
        tmp.write(data.read())
        tmp.flush()
        yield tmp.name
    finally:
        if tmp is not None:
            tmp.close()
