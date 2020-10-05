import os

import pytest

from erddapy.netcdf_handling import _nc_dataset, _tempnc
from erddapy.utilities import urlopen


@pytest.mark.web
@pytest.mark.vcr()
def test__tempnc():
    url = "https://data.ioos.us/gliders/erddap/tabledap/cp_336-20170116T1254.nc"
    data = urlopen(url)
    with _tempnc(data) as tmp:
        # Check that the file was exists.
        assert os.path.exists(tmp)
        # Confirm that it is a netCDF file.
        assert tmp.endswith("nc")
    # Check that the file was removed.
    assert not os.path.exists(tmp)


@pytest.mark.web
@pytest.mark.vcr()
def test__nc_dataset():
    """
    FIXME: we need to test both in-memory and local file.
    That can be achieve with a different libnetcdf but having two environments for testing is cumbersome.
    However, it turns out sometimes a server can fail to provide files can be loaded in memory (#137).
    If we identify the reason we can use them to test this function on both in-memory and disk options.
    """
    from netCDF4 import Dataset

    url = "https://data.ioos.us/gliders/erddap/tabledap/cp_336-20170116T1254.nc"
    auth = None
    _nc = _nc_dataset(url, auth)
    assert isinstance(_nc, Dataset)
