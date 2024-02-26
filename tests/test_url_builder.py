"""Test URL builders."""

import httpx
import pytest

from erddapy.core.url import check_url_response, parse_dates
from erddapy.erddapy import ERDDAP


def _url_to_dict(url):
    return {v.split("=")[0]: v.split("=")[1] for v in url.split("&")[1:]}


@pytest.fixture
def e():
    """Instantiate ERDDAP class for testing."""
    yield ERDDAP(
        server="https://standards.sensors.ioos.us/erddap/",
        protocol="tabledap",
        response="htmlTable",
    )


@pytest.mark.web
@pytest.mark.vcr()
def test_search_url_bad_request(e):
    """Test if a bad request returns HTTPError."""
    kw = {
        "min_time": "1700-01-01T12:00:00Z",
        "max_time": "1750-01-01T12:00:00Z",
    }
    with pytest.raises(httpx.HTTPError):
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
@pytest.mark.vcr()
def test_search_url_valid_request(e):
    """Test if a bad request returns HTTPError."""
    min_time = "2000-03-23T00:00:00Z"
    max_time = "2000-03-30T14:08:00Z"
    kw = {"min_time": min_time, "max_time": max_time}
    url = e.get_search_url(**kw)
    assert url == check_url_response(url)
    assert url.startswith(f"{e.server}/search/advanced.{e.response}?")
    options = _url_to_dict(url)
    assert options.pop("minTime") == str(parse_dates(min_time))
    assert options.pop("maxTime") == str(parse_dates(max_time))
    assert options.pop("itemsPerPage") == str(1000000)
    for k, v in options.items():
        if k == "protocol":
            assert v == e.protocol
        else:
            assert v == "(ANY)"


@pytest.mark.web
@pytest.mark.vcr()
def test_search_url_valid_request_with_relative_time_constraints(e):
    """Test if a bad request returns HTTPError."""
    min_time = "now-25years"
    max_time = "now-20years"
    kw = {"min_time": min_time, "max_time": max_time}
    url = e.get_search_url(dataset_id="org_cormp_cap2", **kw)
    assert url == check_url_response(url)
    assert url.startswith(f"{e.server}/search/advanced.{e.response}?")
    options = _url_to_dict(url)
    assert options.pop("minTime") == min_time
    assert options.pop("maxTime") == max_time
    assert options.pop("itemsPerPage") == str(1000000)
    for k, v in options.items():
        if k == "protocol":
            assert v == e.protocol
        else:
            assert v == "(ANY)"


@pytest.mark.web
@pytest.mark.vcr()
def test_search_url_change_protocol(e):
    """Test if we change the protocol it show in the URL."""
    kw = {"search_for": "salinity"}
    tabledap_url = e.get_search_url(protocol="tabledap", **kw)
    assert tabledap_url == check_url_response(tabledap_url)
    options = _url_to_dict(tabledap_url)
    assert options.pop("protocol") == "tabledap"

    griddap_url = e.get_search_url(protocol="griddap", **kw)
    # Turn this off while no griddap datasets are available
    # assert griddap_url == check_url_response(griddap_url)
    assert griddap_url == tabledap_url.replace("tabledap", "griddap")
    options = _url_to_dict(griddap_url)
    assert options.pop("protocol") == "griddap"

    e.protocol = None
    url = e.get_search_url(**kw)
    assert url == check_url_response(url)
    options = _url_to_dict(url)
    assert options.pop("protocol") == "(ANY)"


@pytest.mark.web
@pytest.mark.vcr()
def test_info_url(e):
    """Check info URL results."""
    dataset_id = "org_cormp_cap2"
    url = e.get_info_url(dataset_id=dataset_id)
    assert url == check_url_response(url)
    assert url == f"{e.server}/info/{dataset_id}/index.{e.response}"

    url = e.get_info_url(dataset_id=dataset_id, response="csv")
    assert url == check_url_response(url)
    assert url == f"{e.server}/info/{dataset_id}/index.csv"


@pytest.mark.web
@pytest.mark.vcr()
def test_categorize_url(e):
    """Check categorize URL results."""
    categorize_by = "standard_name"
    url = e.get_categorize_url(categorize_by=categorize_by)
    assert url == f"{e.server}/categorize/{categorize_by}/index.{e.response}"

    url = e.get_categorize_url(categorize_by=categorize_by, response="csv")
    assert url == f"{e.server}/categorize/{categorize_by}/index.csv"


@pytest.mark.web
@pytest.mark.vcr()
def test_download_url_unconstrained(e):
    """Check download URL results."""
    dataset_id = "org_cormp_cap2"
    variables = ["station", "z"]
    url = e.get_download_url(dataset_id=dataset_id, variables=variables)
    assert url == check_url_response(url, follow_redirects=True)
    assert url.startswith(f"{e.server}/{e.protocol}/{dataset_id}.{e.response}?")
    assert sorted(url.split("?")[1].split(",")) == sorted(variables)


@pytest.mark.web
@pytest.mark.vcr()
def test_download_url_constrained(e):
    """Test a constraint download URL."""
    dataset_id = "org_cormp_cap2"
    variables = ["station", "z"]

    min_time = "2000-03-23T00:00:00Z"
    max_time = "2000-03-30T14:08:00Z"
    min_lat = 32.8032
    max_lat = 32.8032
    min_lon = -79.6204
    max_lon = -79.6204

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
    assert url == check_url_response(url, follow_redirects=True)
    assert url.startswith(f"{e.server}/{e.protocol}/{dataset_id}.csv?")
    options = _url_to_dict(url)
    assert options["time>"] == str(parse_dates(min_time))
    assert options["time<"] == str(parse_dates(max_time))
    assert options["latitude>"] == str(min_lat)
    assert options["latitude<"] == str(max_lat)
    assert options["longitude>"] == str(min_lon)
    assert options["longitude<"] == str(max_lon)


def test_download_url_relative_constraints(e):
    """Test download URL with relative constraints."""
    dataset_id = "org_cormp_cap2"
    variables = ["station", "z"]

    min_time = "now-25years"
    max_time = "now-20years"
    min_lat = "min(latitude)+5"
    max_lat = "max(latitude)-5"
    min_lon = "min(longitude)+5"
    max_lon = "min(longitude)+10"
    min_depth = "min(depth)+5"
    max_depth = "max(depth)-40"

    constraints = {
        "time>=": min_time,
        "time<=": max_time,
        "latitude>=": min_lat,
        "latitude<=": max_lat,
        "longitude>=": min_lon,
        "longitude<=": max_lon,
        "depth>=": min_depth,
        "depth<=": max_depth,
    }

    url = e.get_download_url(
        dataset_id=dataset_id,
        variables=variables,
        response="csv",
        constraints=constraints,
    )
    assert url.startswith(f"{e.server}/{e.protocol}/{dataset_id}.csv?")
    options = _url_to_dict(url)
    assert options["time>"] == min_time
    assert options["time<"] == max_time
    assert options["latitude>"] == min_lat
    assert options["latitude<"] == max_lat
    assert options["longitude>"] == min_lon
    assert options["longitude<"] == max_lon


@pytest.mark.web
@pytest.mark.vcr()
def test_get_var_by_attr(e):
    """Test get_var_by_attr."""
    variables = e.get_var_by_attr(dataset_id="org_cormp_cap2", axis="X")
    assert isinstance(variables, list)
    assert variables == ["longitude"]

    variables = e.get_var_by_attr(
        dataset_id="org_cormp_cap2",
        axis=lambda v: v in ["X", "Y", "Z", "T"],
    )
    assert sorted(variables) == ["latitude", "longitude", "time", "z"]

    assert (
        e.get_var_by_attr(
            dataset_id="org_cormp_cap2",
            standard_name="northward_sea_water_velocity",
        )
        == []
    )
    assert e.get_var_by_attr(dataset_id="org_cormp_cap2", standard_name="time") == [
        "time",
    ]


@pytest.mark.web
@pytest.mark.vcr()
def test_download_url_distinct(e):
    """Check download URL results with and without the distinct option."""
    dataset_id = "org_cormp_cap2"
    variables = ["station", "z"]
    no_distinct_url = e.get_download_url(dataset_id=dataset_id, variables=variables)
    with_distinct_url = e.get_download_url(
        dataset_id=dataset_id,
        variables=variables,
        distinct=True,
    )
    assert not no_distinct_url.endswith("&distinct()")
    assert with_distinct_url.endswith("&distinct()")
    assert no_distinct_url == check_url_response(no_distinct_url, follow_redirects=True)
    assert with_distinct_url == check_url_response(
        with_distinct_url,
        follow_redirects=True,
    )
