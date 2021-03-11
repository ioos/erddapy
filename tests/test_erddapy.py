from datetime import datetime

import pendulum
import pytest
import pytz
import requests
from requests.exceptions import ReadTimeout

from erddapy.erddapy import (ERDDAP, _format_constraints_url,
                             _quote_string_constraints, parse_dates)


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
    d = pendulum.datetime(1970, 1, 1, 0, 0, 0, tz="UTC")
    assert parse_dates(d) == 0


def test_parse_dates_nonutc_datetime():
    """Non-UTC timestamp at 1970-1-1 must have the zone offset."""
    d = datetime(1970, 1, 1, tzinfo=pytz.timezone("US/Eastern"))
    assert parse_dates(d) == abs(d.utcoffset().total_seconds())


def test_parse_dates_nonutc_pendulum():
    """Non-UTC timestamp at 1970-1-1 must have the zone offset."""
    d = pendulum.datetime(1970, 1, 1, 0, 0, 0, tz="America/Vancouver")
    assert parse_dates(d) == abs(d.utcoffset().total_seconds())


def test_parse_dates_from_string():
    """Test if parse_dates can take string input."""
    assert parse_dates("1970-01-01T00:00:00") == 0
    assert parse_dates("1970-01-01T00:00:00Z") == 0
    assert parse_dates("1970-01-01") == 0
    assert parse_dates("1970/01/01") == 0
    assert parse_dates("1970-1-1") == 0
    assert parse_dates("1970/1/1") == 0


def test__quote_string_constraints():
    """Ensure that only string are quoted."""
    kw = _quote_string_constraints(
        {
            "latitude": 42,
            "longitude": 42.0,
            "max_time": datetime.utcnow(),
            "min_time": "1970-01-01T00:00:00Z",
            "cdm_data_type": "trajectoryprofile",
        },
    )

    assert isinstance(kw["latitude"], int)
    assert isinstance(kw["longitude"], float)
    assert isinstance(kw["max_time"], datetime)
    assert isinstance(kw["min_time"], str)
    assert isinstance(kw["cdm_data_type"], str)

    assert kw["min_time"].startswith('"') and kw["min_time"].endswith('"')
    assert kw["cdm_data_type"].startswith('"') and kw["cdm_data_type"].endswith('"')

    for k, v in kw.items():
        if isinstance(v, str):
            assert v.startswith('"') and v.endswith('"')


def test__format_constraints_url():
    kw_url = _format_constraints_url(
        {
            "latitude>=": 42,
            "longitude<=": 42.0,
        },
    )

    assert kw_url == "&latitude>=42&longitude<=42.0"


@pytest.mark.web
@pytest.mark.vcr()
def test_erddap_requests_kwargs():
    """Test that an ERDDAP instance can have requests_kwargs attribute assigned
    and are passed to the underlying methods"""

    base_url = "http://www.neracoos.org/erddap"
    timeout_seconds = 1  # request timeout in seconds
    slowwly_milliseconds = (timeout_seconds + 1) * 1000
    slowwly_url = (
        f"https://flash.siwalik.in/delay/{slowwly_milliseconds}/url/{base_url}"
    )

    connection = ERDDAP(slowwly_url)
    connection.dataset_id = "M01_sbe37_all"
    connection.protocol = "tabledap"

    connection.requests_kwargs["timeout"] = timeout_seconds

    with pytest.raises(ReadTimeout):
        connection.to_xarray()


@pytest.mark.web
@pytest.mark.vcr()
def test_erddap2_10():
    e = ERDDAP(server="https://coastwatch.pfeg.noaa.gov/erddap")
    url = e.get_search_url(search_for="whoi", response="csv")
    r = requests.head(url)
    assert r.raise_for_status() is None
