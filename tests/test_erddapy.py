"""Test ERDDAP functionality."""

import datetime as dt
import io
from zoneinfo import ZoneInfo

import pytest

from erddapy.core.griddap import (
    _griddap_check_constraints,
    _griddap_check_variables,
)
from erddapy.core.url import (
    _format_constraints_url,
    parse_dates,
)
from erddapy.erddapy import ERDDAP


def test_parse_dates_utc_datetime():
    """UTC timestamp at 1970-1-1 must be 0."""
    d = dt.datetime(1970, 1, 1, 0, 0, tzinfo=dt.UTC)
    assert parse_dates(d) == 0


def test_parse_dates_nonutc_datetime():
    """Non-UTC timestamp at 1970-1-1 must have the zone offset."""
    d = dt.datetime(1970, 1, 1, tzinfo=ZoneInfo("US/Eastern"))
    assert parse_dates(d) == abs(d.utcoffset().total_seconds())


def test_parse_dates_from_string():
    """Test if parse_dates can take string input."""
    assert parse_dates("1970-01-01T00:00:00") == 0
    assert parse_dates("1970-01-01T00:00:00Z") == 0
    assert parse_dates("1970-01-01") == 0
    assert parse_dates("1970/01/01") == 0
    assert parse_dates("1970-1-1") == 0
    assert parse_dates("1970/1/1") == 0


def test__format_constraints_url():
    """Test constraint formatting."""
    kw_url = _format_constraints_url(
        {
            "latitude>=": 42,
            "longitude<=": 42.0,
        },
    )

    assert kw_url == "&latitude>=42&longitude<=42.0"


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
    with pytest.raises(
        ValueError,
        match=r"e.constraints have changed. Re-run e.griddap_initialize",
    ):
        _griddap_check_constraints(bad_constraints, constraints_dict)


def test__griddap_check_variables():
    """Check all variables for griddap query exist in target dataset."""
    original_variables = ["foo", "bar"]
    good_variables = ["foo"]
    bad_variables = ["foo", "bar", "baz"]

    _griddap_check_variables(good_variables, original_variables)
    with pytest.raises(
        ValueError,
        match=r"are not present in dataset. Re-run e.griddap_initialize",
    ):
        _griddap_check_variables(bad_variables, original_variables)


def test_get_var_by_attr_follows_dataset_id(monkeypatch):
    """Variables must come from the current dataset_id, not a cached one."""
    header = "Row Type,Variable Name,Attribute Name,Data Type,Value\n"
    info = {
        "dataset_a": header + "attribute,lon_a,axis,String,X\n",
        "dataset_b": header + "attribute,lon_b,axis,String,X\n",
    }

    def fake_urlopen(url, **kwargs: object):  # noqa: ARG001
        dataset_id = url.split("/info/")[1].split("/")[0]
        return io.BytesIO(info[dataset_id].encode())

    monkeypatch.setattr("erddapy.erddapy.urlopen", fake_urlopen)
    e = ERDDAP(server="https://erddap.example.org/erddap", protocol="tabledap")

    e.dataset_id = "dataset_a"
    assert e.get_var_by_attr(axis="X") == ["lon_a"]

    e.dataset_id = "dataset_b"
    assert e.get_var_by_attr(axis="X") == ["lon_b"]
