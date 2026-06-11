"""Test URL builders."""

import re

import pytest
import requests

from erddapy.core.url import _clean_response, check_url_response, parse_dates
from erddapy.erddapy import ERDDAP


def _url_to_dict(url):
    options = {}
    for v in url.split("&")[1:]:
        *left, right = re.split(r"(<=|>=|!=|=~|=|<|>)", v)
        options.update({"".join(left): right})
    return options


@pytest.fixture
def e():
    """Instantiate ERDDAP class for testing."""
    return ERDDAP(
        server="https://erddap.ioos.us/erddap/",
        protocol="tabledap",
        response="htmlTable",
    )


@pytest.mark.web
@pytest.mark.vcr
def test_search_url_bad_request(e):
    """Test if a bad request returns HTTPError."""
    kw = {
        "min_time": "3000-01-01T12:00:00Z",
        "max_time": "3000-01-01T12:00:00Z",
    }
    with pytest.raises(requests.HTTPError):
        check_url_response(e.get_search_url(**kw))


def test_search_normalization(e):
    """Test search text normalization."""
    search_url = e.get_search_url(
        cdm_data_type="TimeSeries",
        standard_name="Sea_Water_Practical_Salinity",
    )
    assert "sea_water_practical_salinity" in search_url
    assert "timeseries" in search_url


@pytest.mark.web
@pytest.mark.vcr
def test_search_url_valid_request(e):
    """Test a valid search URL request."""
    min_time = "2000-03-23T00:00:00Z"
    max_time = "2000-03-30T14:08:00Z"
    kw = {"min_time": min_time, "max_time": max_time}
    url = e.get_search_url(**kw)
    assert url == check_url_response(url)
    assert url.startswith(f"{e.server}/search/advanced.{e.response}?")
    options = _url_to_dict(url)
    assert options.pop("minTime=") == str(parse_dates(min_time))
    assert options.pop("maxTime=") == str(parse_dates(max_time))
    assert options.pop("itemsPerPage=") == str(1000000)
    for k, v in options.items():
        if k == "protocol=":
            assert v == e.protocol
        else:
            assert v == "(ANY)"


@pytest.mark.web
@pytest.mark.vcr
def test_search_url_valid_request_with_relative_time_constraints(e):
    """Test a valid search URL request with relative constraints."""
    min_time = "now-50years"
    max_time = "now-1day"
    kw = {"min_time": min_time, "max_time": max_time}
    url = e.get_search_url(dataset_id="OBIS", **kw)
    assert url == check_url_response(url)
    assert url.startswith(f"{e.server}/search/advanced.{e.response}?")
    options = _url_to_dict(url)
    assert options.pop("minTime=") == min_time
    assert options.pop("maxTime=") == max_time
    assert options.pop("itemsPerPage=") == str(1000000)
    for k, v in options.items():
        if k == "protocol=":
            assert v == e.protocol
        else:
            assert v == "(ANY)"


@pytest.mark.web
@pytest.mark.vcr
def test_search_url_change_protocol(e):
    """Test changing the protocol."""
    # Original is tabledap.
    kw = {"search_for": "etopo"}
    griddap_url = e.get_search_url(protocol="griddap", **kw)
    assert griddap_url == check_url_response(griddap_url)
    options = _url_to_dict(griddap_url)
    assert options.pop("protocol=") == "griddap"

    # Switch to None.
    e.protocol = None
    url = e.get_search_url(**kw)
    assert url == check_url_response(url)
    options = _url_to_dict(url)
    assert options.pop("protocol=") == "(ANY)"


@pytest.mark.web
@pytest.mark.vcr
def test_info_url(e):
    """Check info URL results."""
    dataset_id = "raw_asset_inventory"
    url = e.get_info_url(dataset_id=dataset_id)
    assert url == check_url_response(url)
    assert url == f"{e.server}/info/{dataset_id}/index.{e.response}"

    url = e.get_info_url(dataset_id=dataset_id, response="csv")
    assert url == check_url_response(url)
    assert url == f"{e.server}/info/{dataset_id}/index.csv"


@pytest.mark.web
@pytest.mark.vcr
def test_categorize_url(e):
    """Check categorize URL results."""
    categorize_by = "standard_name"
    url = e.get_categorize_url(categorize_by=categorize_by)
    assert url == f"{e.server}/categorize/{categorize_by}/index.{e.response}"

    url = e.get_categorize_url(categorize_by=categorize_by, response="csv")
    assert url == f"{e.server}/categorize/{categorize_by}/index.csv"


@pytest.mark.web
@pytest.mark.vcr
def test_download_url_unconstrained(e):
    """Check download URL results."""
    dataset_id = "OBIS"
    variables = ["shoredistance", "bathymetry"]
    url = e.get_download_url(dataset_id=dataset_id, variables=variables)
    assert url == check_url_response(url, allow_redirects=True)
    assert url.startswith(
        f"{e.server}/{e.protocol}/{dataset_id}.{e.response}?",
    )
    assert sorted(url.split("?")[1].split(",")) == sorted(variables)


@pytest.mark.web
@pytest.mark.vcr
def test_download_url_constrained(e):
    """Test a constraint download URL."""
    dataset_id = "OBIS"
    variables = ["shoredistance", "bathymetry"]

    min_time = "1982-03-9T23:53:33Z"
    max_time = "2003-11-03T08:01:10Z"
    min_lat = -53
    max_lat = 44
    min_lon = -72
    max_lon = 126

    constraints = {
        "time>=": min_time,
        "time<=": max_time,
        "latitude>=": min_lat,
        "latitude<=": max_lat,
        "longitude>=": min_lon,
        "longitude<=": max_lon,
    }

    url = e.get_download_url(
        dataset_id=dataset_id,
        variables=variables,
        response="csv",
        constraints=constraints,
    )
    assert url == check_url_response(url, allow_redirects=True)
    assert url.startswith(f"{e.server}/{e.protocol}/{dataset_id}.csv?")
    options = _url_to_dict(url)
    assert options["time>="] == str(parse_dates(min_time))
    assert options["time<="] == str(parse_dates(max_time))
    assert options["latitude>="] == str(min_lat)
    assert options["latitude<="] == str(max_lat)
    assert options["longitude>="] == str(min_lon)
    assert options["longitude<="] == str(max_lon)


def test_download_url_relative_constraints(e):
    """Test download URL with relative constraints."""
    dataset_id = "OBIS"
    variables = ["shoredistance", "bathymetry"]

    min_time = "now-56years"
    max_time = "now-2years"
    min_lat = "min(latitude)+5"
    max_lat = "max(latitude)-5"
    min_lon = "min(longitude)+5"
    max_lon = "max(longitude)+10"
    min_depth = "min(depth)"
    max_depth = "max(depth)-40"
    minimumdepthinmeters = 0
    maximumdepthinmeters = 4148

    constraints = {
        "time>=": min_time,
        "time<=": max_time,
        "latitude>=": min_lat,
        "latitude<=": max_lat,
        "longitude>=": min_lon,
        "longitude<=": max_lon,
        "depth>=": min_depth,
        "depth<=": max_depth,
        "minimumdepthinmeters>=": minimumdepthinmeters,
        "maximumdepthinmeters<=": maximumdepthinmeters,
    }

    url = e.get_download_url(
        dataset_id=dataset_id,
        variables=variables,
        response="csv",
        constraints=constraints,
    )
    assert url == check_url_response(url, allow_redirects=True)
    assert url.startswith(f"{e.server}/{e.protocol}/{dataset_id}.csv?")
    options = _url_to_dict(url)
    assert options["time>="] == min_time
    assert options["time<="] == max_time
    assert options["latitude>="] == min_lat
    assert options["latitude<="] == max_lat
    assert options["longitude>="] == min_lon
    assert options["longitude<="] == max_lon


def test_download_url_relative_constraints_non_coordinate(e):
    """Test download URL with relative constraints."""
    dataset_id = "OBIS"

    constraints = {
        "identifiedby=": "Max Hoberg",
    }

    url = e.get_download_url(
        dataset_id=dataset_id,
        response="csv",
        constraints=constraints,
    )
    assert url == check_url_response(url, allow_redirects=True)
    assert url.startswith(f"{e.server}/{e.protocol}/{dataset_id}.csv?")
    options = _url_to_dict(url)
    assert options["identifiedby="] == '"Max Hoberg"'


@pytest.mark.web
@pytest.mark.vcr
def test_get_var_by_attr(e):
    """Test get_var_by_attr."""
    variables = e.get_var_by_attr(dataset_id="OBIS", axis="X")
    assert isinstance(variables, list)
    assert variables == ["longitude"]

    variables = e.get_var_by_attr(
        dataset_id="OBIS",
        axis=lambda v: v in ("X", "Y", "Z", "T"),
    )
    assert sorted(variables) == ["depth", "latitude", "longitude", "time"]

    assert (
        e.get_var_by_attr(
            dataset_id="OBIS",
            standard_name="northward_sea_water_velocity",
        )
        == []
    )
    assert "time" in e.get_var_by_attr(
        dataset_id="OBIS",
        standard_name="time",
    )


@pytest.mark.web
@pytest.mark.vcr
def test_download_url_distinct(e):
    """Check download URL results with and without the distinct option."""
    dataset_id = "OBIS"
    variables = ["shoredistance", "bathymetry"]
    no_distinct_url = e.get_download_url(
        dataset_id=dataset_id,
        variables=variables,
    )
    with_distinct_url = e.get_download_url(
        dataset_id=dataset_id,
        variables=variables,
        distinct=True,
    )
    assert not no_distinct_url.endswith("&distinct()")
    assert with_distinct_url.endswith("&distinct()")
    assert no_distinct_url == check_url_response(
        no_distinct_url,
        allow_redirects=True,
    )
    assert with_distinct_url == check_url_response(
        with_distinct_url,
        allow_redirects=True,
    )


def test__clean_response():
    """Test if users can pass responses with or without the '.'."""
    assert _clean_response("html") == _clean_response(".html")
