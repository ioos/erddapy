"""Test netCDF loading."""

import platform
from pathlib import Path

import pytest

from erddapy.core.netcdf import _nc_dataset, _tempnc
from erddapy.core.url import urlopen


@pytest.mark.web
# For some reason we cannot use vcr with httpx with in_memory
# (also all the to_objects that uses in_memory).
def test__nc_dataset_in_memory_https():
    """Test loading a netcdf dataset in-memory."""
    from netCDF4 import Dataset  # noqa: PLC0415

    url = "https://erddap.ioos.us/erddap/tabledap/allDatasets.nc"
    _nc = _nc_dataset(url)
    assert isinstance(_nc, Dataset)
    assert _nc.filepath() == url.split("/")[-1]


@pytest.mark.web
@pytest.mark.vcr
@pytest.mark.skipif(
    platform.system().lower() == "windows",
    reason="does not remove the file on windows",
)
def test__tempnc():
    """Test temporary netcdf file."""
    url = "https://erddap.ioos.us/erddap/tabledap/allDatasets.nc"
    data = urlopen(url)
    with _tempnc(data) as tmp:
        # Check that the file was exists.
        assert Path(tmp).exists()
        # Confirm that it is a netCDF file.
        assert tmp.endswith("nc")
    # Check that the file was removed.
    assert not Path(tmp).exists()
