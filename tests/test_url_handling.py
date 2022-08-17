"""Test URL handling."""

import io

import httpx
import pytest

from erddapy.core.url import check_url_response, urlopen


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
def test_urlopen_requests_kwargs():
    """Test that urlopen can pass kwargs to requests."""
    base_url = "https://standards.sensors.ioos.us/erddap/tabledap/"
    timeout_seconds = 1  # request timeout in seconds
    slowwly_milliseconds = (timeout_seconds + 1) * 1000
    slowwly_url = f"https://flash-the-slow-api.herokuapp.com/delay/{slowwly_milliseconds}/url/{base_url}"

    with pytest.raises(httpx.ReadTimeout):
        urlopen(slowwly_url, timeout=timeout_seconds)


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
