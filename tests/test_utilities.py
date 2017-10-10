import io

from erddapy.utilities import _check_url_response, _clean_response, _urlopen

import pytest

from requests.exceptions import HTTPError


@pytest.mark.web
def test__check_url_response():
    bad_request = (
        'http://erddap.sensors.ioos.us/erddap/tabledap/'
        'gov_usgs_waterdata_340800117235901.htmlTable?'
        'time,'
        '&time>=2017-08-29T00:00:00Z'
        '&time<=2015-09-05T19:00:00Z'
    )
    with pytest.raises(HTTPError):
        _check_url_response(bad_request)


def test__clean_response():
    ret0 = _clean_response('html')
    ret1 = _clean_response('.html')
    assert ret0 == ret1


@pytest.mark.web
def test__urlopen():
    url = 'http://erddap.sensors.ioos.us/erddap/tabledap/'
    ret = _urlopen(url)
    isinstance(ret, io.BytesIO)
