import io

import pytest
from requests.exceptions import HTTPError, ReadTimeout

from erddapy.url_handling import _clean_response, check_url_response, urlopen


@pytest.mark.web
@pytest.mark.vcr()
def test_urlopen():
    """Assure that urlopen is always a BytesIO object."""
    url = "http://erddap.sensors.ioos.us/erddap/tabledap/"
    ret = urlopen(url)
    isinstance(ret, io.BytesIO)


@pytest.mark.web
@pytest.mark.vcr()
def test_urlopen_raise():
    """Assure that urlopen will raise for bad URLs."""
    url = "https://developer.mozilla.org/en-US/404"
    with pytest.raises(HTTPError):
        urlopen(url)


@pytest.mark.web
def test_urlopen_requests_kwargs():
    """Test that urlopen can pass kwargs to requests"""
    base_url = "http://erddap.sensors.ioos.us/erddap/tabledap/"
    timeout_seconds = 1  # request timeout in seconds
    slowwly_milliseconds = (timeout_seconds + 1) * 1000
    slowwly_url = (
        f"https://flash.siwalik.in/delay/{slowwly_milliseconds}/url/{base_url}"
    )

    with pytest.raises(ReadTimeout):
        urlopen(slowwly_url, timeout=timeout_seconds)


@pytest.mark.web
@pytest.mark.vcr()
def test_check_url_response():
    """Test if a bad request returns HTTPError."""
    bad_request = (
        "http://erddap.sensors.ioos.us/erddap/tabledap/"
        "gov_usgs_waterdata_340800117235901.htmlTable?"
        "time,"
        "&time>=2017-08-29T00:00:00Z"
        "&time<=2015-09-05T19:00:00Z"
    )
    with pytest.raises(HTTPError):
        check_url_response(bad_request)
