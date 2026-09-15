"""Test erddapy xarray backend."""

import urllib.parse

import pytest
import xarray as xr


def test_load_backend():
    """Check if the backend is loaded."""
    backends = xr.backends.list_engines()
    assert "erddapy" in backends
    assert backends["erddapy"].description == "Load ERDDAP URLs in xarray."


@pytest.mark.web
@pytest.mark.vcr
def test_netcdf4_erddapy_same_opendap_dataset():
    """Check if netCDF4 and erddapy opendap responses are the same."""
    url = "https://gliders.ioos.us/erddap/tabledap/amelia-20180501T0000"
    dse = xr.open_dataset(url, engine="erddapy")
    dsn = xr.open_dataset(url, engine="netcdf4")
    assert dsn.attrs["ioos_dac_checksum"] == dse.attrs["ioos_dac_checksum"]


@pytest.mark.web
@pytest.mark.vcr
def test_netcdf_like_response():
    """Check if netCDF response."""
    url = "https://gliders.ioos.us/erddap/tabledap/amelia-20180501T0000.nc"
    ds = xr.open_dataset(url, engine="erddapy")
    assert ds.attrs["ioos_dac_checksum"] == "c3150831158a8ce077b817c2f31ad498"


base = "https://gliders.ioos.us/erddap/tabledap/amelia-20180501T0000"
urls = [
    f"{base}.nc?trajectory%2Csalinity&time%3E=2018-05-07T00%3A00%3A00Z&time%3C=2018-05-14T12%3A29%3A12Z",
    f"{base}.ncCF?trajectory%2Csalinity&time%3E=2018-05-07T00%3A00%3A00Z&time%3C=2018-05-14T12%3A29%3A12Z",
    f"{base}.ncCFMA?trajectory%2Csalinity&time%3E=2018-05-07T00%3A00%3A00Z&time%3C=2018-05-14T12%3A29%3A12Z",
]


@pytest.mark.web
@pytest.mark.vcr
@pytest.mark.parametrize("url", urls)
def test_nc_with_slices_and_variables(url):
    """Check if various netCDF-like response with slices."""
    ds = xr.open_dataset(url, engine="erddapy")
    url_history = ds.attrs["history"].split()[-1]

    # Use parse_qs in lieu of urlparse b/c the vars change with the nc type.
    assert urllib.parse.parse_qs(url_history) == urllib.parse.parse_qs(url)


@pytest.mark.web
@pytest.mark.vcr
def test_griddap_opendap():
    """Check if various griddap response."""
    url = "https://erddap.ioos.us/erddap/griddap/etopo5_EDDGridCopy"
    ds = xr.open_dataset(url, engine="erddapy")
    url_history = ds.attrs["history"].split()[-1]
    # Check the path b/c the copied dataset netloc is netloc 'localhost:8080'.
    assert (
        urllib.parse.urlparse(url_history).path.rstrip(".das")
        == urllib.parse.urlparse(url).path
    )


@pytest.mark.web
@pytest.mark.vcr
def test_griddap_opendap_slice():
    """Check if various griddap response with slices."""
    url = "https://erddap.ioos.us/erddap/griddap/etopo5_EDDGridCopy.nc?ROSE%5B(0.0):1:(42.0)%5D%5B(0.0):1:(42.0)%5D"
    ds = xr.open_dataset(url, engine="erddapy")

    url_history = ds.attrs["history"].split()[-1]
    assert (
        urllib.parse.urlparse(url_history).query
        == urllib.parse.urlparse(url).query
    )
