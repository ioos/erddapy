"""Test netCDF loading."""

import platform
from pathlib import Path

try:
    import netCDF4  # noqa: F401

    NETCDF4_INSTALLED = True
except ImportError:
    NETCDF4_INSTALLED = False

import pytest

from erddapy.core.netcdf import _nc_dataset, _tempnc
from erddapy.core.url import urlopen


# For some reason we cannot use vcr with requests with in_memory
# (also all the to_objects that uses in_memory).
@pytest.mark.web
@pytest.mark.skipif(
    not NETCDF4_INSTALLED,
    reason="Optional deps  are tested in coverage and oldest Python only.",
)
def test__nc_dataset_in_memory_https():
    """Test loading a netcdf dataset in-memory."""
    from netCDF4 import Dataset  # noqa: PLC0415

    url = "https://erddap.ioos.us/erddap/tabledap/allDatasets.nc"
    _nc = _nc_dataset(url)
    assert isinstance(_nc, Dataset)
    assert _nc.filepath() == url.rsplit("/", maxsplit=1)[-1]


@pytest.mark.web
@pytest.mark.vcr
@pytest.mark.skipif(
    (platform.system().lower() == "windows" or not NETCDF4_INSTALLED),
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
