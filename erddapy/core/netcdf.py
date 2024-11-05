"""Handles netCDF responses."""

from __future__ import annotations

import platform
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO
from urllib.parse import urlparse

from erddapy.core.url import urlopen

if TYPE_CHECKING:
    from collections.abc import Generator

    import netCDF4


def _nc_dataset(
    url: str,
    requests_kwargs: dict | None = None,
) -> netCDF4.Dataset:
    """Return a netCDF4-python Dataset from memory
    and fallbacks to disk if that fails.

    """
    from netCDF4 import Dataset

    data = urlopen(url, requests_kwargs)
    try:
        return Dataset(Path(urlparse(url).path).name, memory=data.read())
    except OSError:
        # if libnetcdf is not compiled with in-memory support fallback tmp file
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
        with NamedTemporaryFile(
            suffix=".nc",
            prefix="erddapy_",
            delete=delete,
        ) as tmp:
            tmp.write(data.read())
            tmp.flush()
            yield tmp.name
    finally:
        if tmp is not None:
            tmp.close()
