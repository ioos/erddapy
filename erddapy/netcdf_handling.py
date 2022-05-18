"""Handles netCDF responses."""

from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Generator
from typing.io import BinaryIO
from urllib.parse import urlparse

from erddapy.url_handling import urlopen


def _nc_dataset(url, auth, **requests_kwargs: Dict):
    """Return a netCDF4-python Dataset from memory and fallbacks to disk if that fails."""
    from netCDF4 import Dataset

    data = urlopen(url=url, auth=auth, **requests_kwargs)
    try:
        return Dataset(Path(urlparse(url).path).name, memory=data.read())
    except OSError:
        # if libnetcdf is not compiled with in-memory support fallback to a local tmp file
        with _tempnc(data) as _nc:
            return Dataset(_nc)


@contextmanager
def _tempnc(data: BinaryIO) -> Generator[str, None, None]:
    """Create a temporary netcdf file."""
    from tempfile import NamedTemporaryFile

    tmp = None
    try:
        tmp = NamedTemporaryFile(suffix=".nc", prefix="erddapy_")
        tmp.write(data.read())
        tmp.flush()
        yield tmp.name
    finally:
        if tmp is not None:
            tmp.close()
