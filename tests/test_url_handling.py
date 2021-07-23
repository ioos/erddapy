import io

import pytest
from requests.exceptions import HTTPError, ReadTimeout

from erddapy.url_handling import (
    _clean_response,
    check_url_response,
    format_search_string,
    multi_urlopen,
    urlopen,
)


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


def test__clean_response():
    """Test if users can pass responses with or without the '.'."""
    assert _clean_response("html") == _clean_response(".html")


@pytest.mark.web
@pytest.mark.vcr()
def test_multi_urlopen():
    """Assure that multi_urlopen is always a BytesIO object."""
    url = "http://erddap.sensors.ioos.us/erddap/tabledap/"
    ret = multi_urlopen(url)
    isinstance(ret, io.BytesIO)


@pytest.mark.web
@pytest.mark.vcr()
def test_format_search_string():
    """Check that string is correctly formatted for search"""
    server = "https://gliders.ioos.us/erddap/"
    query = "sst"
    url = format_search_string(server, query)
    assert (
        url
        == 'https://gliders.ioos.us/erddap/search/index.csv?page=1&itemsPerPage=100000&searchFor="sst"'
    )
