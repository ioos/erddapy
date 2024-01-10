"""Test ERDDAP functionality."""

import datetime

import httpx
import pytest
import pytz

from erddapy.core.griddap import (
    _griddap_check_constraints,
    _griddap_check_variables,
)
from erddapy.core.url import (
    _format_constraints_url,
    _quote_string_constraints,
    parse_dates,
)
from erddapy.erddapy import ERDDAP


def test_parse_dates_naive_datetime():
    """Naive timestamp at 1970-1-1 must be 0."""
    d = datetime.datetime(1970, 1, 1, 0, 0)
    assert parse_dates(d) == 0


def test_parse_dates_utc_datetime():
    """UTC timestamp at 1970-1-1 must be 0."""
    d = datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc)
    assert parse_dates(d) == 0


def test_parse_dates_nonutc_datetime():
    """Non-UTC timestamp at 1970-1-1 must have the zone offset."""
    d = datetime.datetime(1970, 1, 1, tzinfo=pytz.timezone("US/Eastern"))
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
            "max_time": datetime.datetime.now(datetime.UTC),
            "min_time": "1970-01-01T00:00:00Z",
            "cdm_data_type": "trajectoryprofile",
        },
    )

    assert isinstance(kw["latitude"], int)
    assert isinstance(kw["longitude"], float)
    assert isinstance(kw["max_time"], datetime.datetime)
    assert isinstance(kw["min_time"], str)
    assert isinstance(kw["cdm_data_type"], str)

    assert kw["min_time"].startswith('"') and kw["min_time"].endswith('"')
    assert kw["cdm_data_type"].startswith('"') and kw["cdm_data_type"].endswith('"')

    for _k, v in kw.items():
        if isinstance(v, str):
            assert v.startswith('"') and v.endswith('"')


def test__format_constraints_url():
    """Test constraint formatting."""
    kw_url = _format_constraints_url(
        {
            "latitude>=": 42,
            "longitude<=": 42.0,
        },
    )

    assert kw_url == "&latitude>=42&longitude<=42.0"


@pytest.mark.web
@pytest.mark.vcr()
def test_erddap2_10():
    """Check regression for ERDDAP 2.10."""
    e = ERDDAP(server="http://erddap.ioos.us/erddap/")
    url = e.get_search_url(search_for="NOAA", response="csv")
    r = httpx.head(url)
    assert r.status_code == 200


def test__griddap_check_constraints():
    """Check griddap constraints dict has not changed keys."""
    constraints_dict = {
        "time>=": "2012-01-01T00:00:00Z",
        "time<=": "2021-06-19T05:00:00Z",
        "time_step": 1000,
        "latitude>=": 21.7,
        "latitude<=": 46.49442,
    }
    good_constraints = {
        "time>=": "2012-01-01T00:00:00Z",
        "latitude>=": 21.7,
        "time_step": 1000,
        "time<=": "2021-06-19T05:00:00Z",
        "latitude<=": 46.49442,
    }
    bad_constraints = {
        "time>=": "2012-01-01T00:00:00Z",
        "time<=": "2021-06-19T05:00:00Z",
    }

    _griddap_check_constraints(good_constraints, constraints_dict)
    with pytest.raises(ValueError):
        _griddap_check_constraints(bad_constraints, constraints_dict)


def test__griddap_check_variables():
    """Check all variables for griddap query exist in target dataset."""
    original_variables = ["foo", "bar"]
    good_variables = ["foo"]
    bad_variables = ["foo", "bar", "baz"]

    _griddap_check_variables(good_variables, original_variables)
    with pytest.raises(ValueError):
        _griddap_check_variables(bad_variables, original_variables)
