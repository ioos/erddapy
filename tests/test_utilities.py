import io
from datetime import datetime

from erddapy.utilities import (
    _check_url_response,
    _clean_response,
    _urlopen,
    parse_dates,
    quote_string_constraints
)

import pendulum

import pytest

import pytz

from requests.exceptions import HTTPError


@pytest.mark.web
def test__check_url_response():
    """Test if a bad request returns HTTPError."""
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
    """Test if users can pass reponses with or without the '.'."""
    assert _clean_response('html') == _clean_response('.html')


@pytest.mark.web
def test__urlopen():
    """Assure that urlopen is always a BytesIO object."""
    url = 'http://erddap.sensors.ioos.us/erddap/tabledap/'
    ret = _urlopen(url)
    isinstance(ret, io.BytesIO)


def test_parse_dates_naive_datetime():
    """Naive timestamp at 1970-1-1 must be 0."""
    d = datetime(1970, 1, 1, 0, 0)
    assert parse_dates(d) == 0


def test_parse_dates_utc_datetime():
    """UTC timestamp at 1970-1-1 must be 0."""
    d = datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc)
    assert parse_dates(d) == 0


def test_parse_dates_utc_pendulum():
    """UTC timestamp at 1970-1-1 must be 0."""
    d = pendulum.create(1970, 1, 1, 0, 0, 0, tz='UTC')
    assert parse_dates(d) == 0


def test_parse_dates_nonutc_datetime():
    """Non-UTC timestamp at 1970-1-1 must have the zone offset."""
    d = datetime(1970, 1, 1, tzinfo=pytz.timezone('US/Eastern'))
    assert parse_dates(d) == abs(d.utcoffset().total_seconds())


def test_parse_dates_nonutc_pendulum():
    """Non-UTC timestamp at 1970-1-1 must have the zone offset."""
    d = pendulum.create(1970, 1, 1, 0, 0, 0, tz='America/Vancouver')
    assert parse_dates(d) == abs(d.utcoffset().total_seconds())


def test_parse_dates_from_string():
    """Test if parse_dates can take string input."""
    assert parse_dates('1970-01-01T00:00:00') == 0
    assert parse_dates('1970-01-01T00:00:00Z') == 0
    assert parse_dates('1970-01-01') == 0
    assert parse_dates('1970/01/01') == 0
    assert parse_dates('1970-1-1') == 0
    assert parse_dates('1970/1/1') == 0


def test_quote_string_constraints():
    """Ensure that only string are quoted."""
    kw = quote_string_constraints(
        {
            'latitude': 42,
            'longitude': 42.,
            'max_time': datetime.utcnow(),
            'min_time': '1970-01-01T00:00:00Z',
            'cdm_data_type': 'trajectoryprofile',
        }
    )

    assert isinstance(kw['latitude'], int)
    assert isinstance(kw['longitude'], float)
    assert isinstance(kw['max_time'], datetime)
    assert isinstance(kw['min_time'], str)
    assert isinstance(kw['cdm_data_type'], str)

    assert kw['min_time'].startswith('"') and kw['min_time'].endswith('"')
    assert kw['cdm_data_type'].startswith('"') and kw['cdm_data_type'].endswith('"')

    for k, v in kw.items():
        if isinstance(v, str):
            assert v.startswith('"') and v.endswith('"')
