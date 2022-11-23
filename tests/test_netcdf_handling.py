"""Test netCDF loading."""

import os

import pytest

from erddapy.core.netcdf import _nc_dataset, _tempnc
from erddapy.core.url import urlopen


@pytest.mark.web
# For some reason we cannot use vcr with httpx with in_memory
# (also all the to_objects that uses in_memory).
def test__nc_dataset_in_memory_https():
    """Test loading a netcdf dataset in-memory."""
    from netCDF4 import Dataset

    url = "http://erddap.ioos.us/erddap/tabledap/allDatasets.nc"  # noqa
    _nc = _nc_dataset(url)
    assert isinstance(_nc, Dataset)
    assert _nc.filepath() == url.split("/")[-1]


@pytest.mark.web
@pytest.mark.vcr()
def test__tempnc():
    """Test temporary netcdf file."""
    url = "http://erddap.ioos.us/erddap/tabledap/allDatasets.nc"  # noqa
    data = urlopen(url)
    with _tempnc(data) as tmp:
        # Check that the file was exists.
        assert os.path.exists(tmp)
        # Confirm that it is a netCDF file.
        assert tmp.endswith("nc")
    # Check that the file was removed.
    assert not os.path.exists(tmp)
