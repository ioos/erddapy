"""Test netCDF loading."""

import os

import pytest

from erddapy.core.url_handling import urlopen
from erddapy.netcdf_handling import _nc_dataset, _tempnc


@pytest.mark.web
# For some reason we cannot use vcr with httpx with in_memory
# (also all the to_objects that uses in_memory).
def test__nc_dataset_in_memory_https():
    """Test loading a netcdf dataset in-memory."""
    from netCDF4 import Dataset

    url = "https://podaac-opendap.jpl.nasa.gov/opendap/allData/modis/L3/aqua/11um/v2019.0/4km/daily/2017/365/AQUA_MODIS.20171231.L3m.DAY.NSST.sst.4km.nc"  # noqa
    auth = None
    _nc = _nc_dataset(url, auth)
    assert isinstance(_nc, Dataset)
    assert _nc.filepath() == url.split("/")[-1]


@pytest.mark.web
@pytest.mark.vcr()
def test__tempnc():
    """Test temporary netcdf file."""
    url = "https://podaac-opendap.jpl.nasa.gov/opendap/allData/modis/L3/aqua/11um/v2019.0/4km/daily/2017/365/AQUA_MODIS.20171231.L3m.DAY.NSST.sst.4km.nc"  # noqa
    data = urlopen(url)
    with _tempnc(data) as tmp:
        # Check that the file was exists.
        assert os.path.exists(tmp)
        # Confirm that it is a netCDF file.
        assert tmp.endswith("nc")
    # Check that the file was removed.
    assert not os.path.exists(tmp)
