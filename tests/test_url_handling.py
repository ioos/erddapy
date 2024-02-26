"""Test URL handling."""

import io

import httpx
import pytest

from erddapy.core.url import _sort_url, check_url_response, urlopen


@pytest.mark.web
@pytest.mark.vcr()
def test_urlopen():
    """Assure that urlopen is always a BytesIO object."""
    url = "https://standards.sensors.ioos.us/erddap/tabledap/"
    ret = urlopen(url)
    isinstance(ret, io.BytesIO)


@pytest.mark.web
@pytest.mark.vcr()
def test_urlopen_raise():
    """Assure that urlopen will raise for bad URLs."""
    url = "https://developer.mozilla.org/en-US/404"
    with pytest.raises(httpx.HTTPError):
        urlopen(url)


@pytest.mark.web
@pytest.mark.vcr()
def test_check_url_response():
    """Test if a bad request returns HTTPError."""
    bad_request = (
        "https://standards.sensors.ioos.us/erddap/tabledap/"
        "org_cormp_cap2.htmlTable?"
        "time,"
        "&time>=2017-08-29T00:00:00Z"
        "&time<=2015-09-05T19:00:00Z"
    )
    with pytest.raises(httpx.HTTPError):
        check_url_response(bad_request)


def test__sort_url():
    """Test _sort_url with defined variable and constraints."""
    url = "https://erddap.sensors.ioos.us/erddap/tabledap/amelia_20180501t0000.nc?time,temperature&time>=1525737600.0&time<=1526245200.0&latitude>=36&latitude<=38&longitude>=-76&longitude<=-73"
    expected = "https://erddap.sensors.ioos.us/erddap/tabledap/amelia_20180501t0000.nc?temperature,time&latitude<=38&latitude>=36&longitude<=-73&longitude>=-76&time<=1526245200.0&time>=1525737600.0"
    assert _sort_url(url) == expected


def test__sort_url_variables_only():
    """Test _sort_url with undefined constraints."""
    url = "https://erddap.sensors.ioos.us/erddap/tabledap/amelia_20180501t0000.nc?&time>=1525737600.0&time<=1526245200.0&latitude>=36&latitude<=38&longitude>=-76&longitude<=-73"
    expected = "https://erddap.sensors.ioos.us/erddap/tabledap/amelia_20180501t0000.nc?&latitude<=38&latitude>=36&longitude<=-73&longitude>=-76&time<=1526245200.0&time>=1525737600.0"
    assert _sort_url(url) == expected


def test__sort_url_constraints_only():
    """Test _sort_url with undefined variables."""
    url = "https://erddap.sensors.ioos.us/erddap/tabledap/amelia_20180501t0000.nc?time,temperature"
    expected = "https://erddap.sensors.ioos.us/erddap/tabledap/amelia_20180501t0000.nc?temperature,time"
    assert _sort_url(url) == expected


def test__sort_url_undefined_query():
    """Test _sort_url with undefined query."""
    url = "https://erddap.sensors.ioos.us/erddap/tabledap/amelia_20180501t0000.nc?"
    assert _sort_url(url) == url
