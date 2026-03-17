"""Test xarray backend entry point."""

from pathlib import Path

from erddapy.xarray_erddap import (
    ERDDAPyBackendEntrypoint,
    _is_netcdf,
    _is_url,
)


def test_is_url_valid_erddap_tabledap():
    """Valid ERDDAP tabledap URL returns True."""
    url = "https://coastwatch.pfeg.noaa.gov/erddap/tabledap/data"
    assert _is_url(url) is True


def test_is_url_valid_erddap_griddap():
    """Valid ERDDAP griddap URL returns True."""
    url = "http://erddap.sensors.ioos.us/erddap/griddap/data"
    assert _is_url(url) is True


def test_is_url_non_erddap():
    """Non-ERDDAP URL returns False."""
    assert _is_url("https://example.com/data") is False


def test_is_url_non_string_none():
    """None input returns False without raising."""
    assert _is_url(None) is False


def test_is_url_non_string_int():
    """Integer input returns False without raising."""
    assert _is_url(42) is False


def test_is_url_empty_string():
    """Empty string returns False."""
    assert _is_url("") is False


def test_is_netcdf_nc_extension():
    """URL ending with .nc is recognized as netcdf."""
    url = "https://host.com/erddap/griddap/id.nc"
    assert _is_netcdf(url) is True


def test_is_netcdf_nccf_query():
    """URL with .ncCF? query is recognized as netcdf."""
    url = "https://host.com/erddap/griddap/id.ncCF?var"
    assert _is_netcdf(url) is True


def test_is_netcdf_csv():
    """CSV URL is not netcdf."""
    url = "https://host.com/erddap/tabledap/id.csv"
    assert _is_netcdf(url) is False


def test_guess_can_open_erddap_url():
    """Backend reports it can open ERDDAP URLs."""
    backend = ERDDAPyBackendEntrypoint()
    url = "https://erddap.ioos.us/erddap/tabledap/data"
    assert backend.guess_can_open(url) is True


def test_guess_can_open_local_path():
    """Backend reports it cannot open local paths."""
    backend = ERDDAPyBackendEntrypoint()
    assert backend.guess_can_open("/tmp/data.nc") is False  # noqa: S108


def test_guess_can_open_with_path_object():
    """Path object returns False without raising."""
    backend = ERDDAPyBackendEntrypoint()
    assert backend.guess_can_open(Path("/tmp/data.nc")) is False  # noqa: S108
