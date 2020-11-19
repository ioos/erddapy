import pytest

from requests.exceptions import HTTPError

from erddapy.erddapy import ERDDAP, parse_dates
from erddapy.url_handling import check_url_response


def _url_to_dict(url):
    return {v.split("=")[0]: v.split("=")[1] for v in url.split("&")[1:]}


@pytest.fixture
@pytest.mark.web
def e():
    yield ERDDAP(
        server="https://upwell.pfeg.noaa.gov/erddap",
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
    with pytest.raises(HTTPError):
        check_url_response(e.get_search_url(**kw))


def test_search_normalization(e):
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
    min_time = "1800-01-01T12:00:00Z"
    max_time = "1950-01-01T12:00:00Z"
    kw = {"min_time": min_time, "max_time": max_time}
    url = e.get_search_url(**kw)
    assert url.startswith(f"{e.server}/search/advanced.{e.response}?")
    options = _url_to_dict(url)
    assert options.pop("minTime") == str(parse_dates(min_time))
    assert options.pop("maxTime") == str(parse_dates(max_time))
    assert options.pop("itemsPerPage") == str(1000)
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
    url = e.get_search_url(protocol="tabledap", **kw)
    options = _url_to_dict(url)
    assert options.pop("protocol") == "tabledap"

    url = e.get_search_url(protocol="griddap", **kw)
    options = _url_to_dict(url)
    assert options.pop("protocol") == "griddap"

    e.protocol = None
    url = e.get_search_url(**kw)
    options = _url_to_dict(url)
    assert options.pop("protocol") == "(ANY)"


@pytest.mark.web
@pytest.mark.vcr()
def test_info_url(e):
    """Check info URL results."""
    dataset_id = "gtoppAT"
    url = e.get_info_url(dataset_id=dataset_id)
    assert url == f"{e.server}/info/{dataset_id}/index.{e.response}"

    url = e.get_info_url(dataset_id=dataset_id, response="csv")
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
    dataset_id = "gtoppAT"
    variables = ["commonName", "yearDeployed", "serialNumber"]
    url = e.get_download_url(dataset_id=dataset_id, variables=variables)
    assert url.startswith(f"{e.server}/{e.protocol}/{dataset_id}.{e.response}?")
    assert sorted(url.split("?")[1].split(",")) == sorted(variables)


@pytest.mark.web
@pytest.mark.vcr()
def test_download_url_constrained(e):
    dataset_id = "gtoppAT"
    variables = ["commonName", "yearDeployed", "serialNumber"]

    min_time = "2002-06-30T13:53:16Z"
    max_time = "2018-10-27T04:54:00Z"
    min_lat = -42
    max_lat = 42
    min_lon = 0
    max_lon = 360

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
    assert url.startswith(f"{e.server}/{e.protocol}/{dataset_id}.csv?")
    options = _url_to_dict(url)
    assert options["time>"] == str(parse_dates(min_time))
    assert options["time<"] == str(parse_dates(max_time))
    assert options["latitude>"] == str(min_lat)
    assert options["latitude<"] == str(max_lat)
    assert options["longitude>"] == str(min_lon)
    assert options["longitude<"] == str(max_lon)


@pytest.mark.web
@pytest.mark.vcr()
def test_get_var_by_attr(e):
    variables = e.get_var_by_attr(dataset_id="gtoppAT", axis="X")
    assert isinstance(variables, list)
    assert variables == ["longitude"]

    variables = e.get_var_by_attr(
        dataset_id="gtoppAT",
        axis=lambda v: v in ["X", "Y", "Z", "T"],
    )
    assert sorted(variables) == ["latitude", "longitude", "time"]

    assert (
        e.get_var_by_attr(
            dataset_id="pmelTao5dayIso",
            standard_name="northward_sea_water_velocity",
        )
        == []
    )
    assert e.get_var_by_attr(dataset_id="pmelTao5dayIso", standard_name="time") == [
        "time",
    ]


# Test URL modifiers (server-side functions).
def test_download_url_distinct(e):
    """Check download URL results with and without the distinct option."""
    dataset_id = "gtoppAT"
    variables = ["commonName", "yearDeployed", "serialNumber"]
    no_distinct_url = e.get_download_url(dataset_id=dataset_id, variables=variables)
    with_distinct_url = e.get_download_url(
        dataset_id=dataset_id,
        variables=variables,
        distinct=True,
    )
    assert not no_distinct_url.endswith("&distinct()")
    assert with_distinct_url.endswith("&distinct()")

# Test generic sever-side functions
def test_download_url_server_functions(e):
    """Check download URL results with and without the distinct option."""
    dataset_id = "gtoppAT"
    variables = ["commonName", "yearDeployed", "serialNumber"]
    no_distinct_url = e.get_download_url(dataset_id=dataset_id, variables=variables)
    with_distinct_url = e.get_download_url(
        dataset_id=dataset_id,
        variables=variables,
        relative_constraints={"time=": "max(time)+0minutes"},
    )
    assert not no_distinct_url.endswith("&time=max(time)+0minutes")
    assert with_distinct_url.endswith("&time=max(time)+0minutes")