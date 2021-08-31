import pytest

from erddapy.multiple_server_search import fetch_results, search_servers


@pytest.mark.web
@pytest.mark.vcr()
def test_fetch_results():
    "This search should return results"
    url = 'https://gliders.ioos.us/erddap/search/index.csv?page=1&itemsPerPage=100000&searchFor="sst"'
    key = "ioos"
    protocol = "tabledap"
    data = fetch_results(url, key, protocol)
    assert data is not None


@pytest.mark.web
@pytest.mark.vcr()
def test_fetch_no_results():
    """This search should return no results"""
    url = (
        "https://gliders.ioos.us/erddap/search/index.csv?page=1&itemsPerPage=100000&searchFor"
        '="incredibly_long_string_that_should_never_match_a_real_dataset" '
    )
    key = "ioos"
    protocol = "tabledap"
    data = fetch_results(url, key, protocol)
    assert data is None


@pytest.mark.web
@pytest.mark.vcr()
@pytest.mark.parametrize("parallel", [True, False])
def test_search_awesome_erddap_servers(parallel):
    query = "glider"
    protocol = "tabledap"
    df = search_servers(
        query=query,
        protocol=protocol,
        parallel=parallel,
    )
    assert df is not None
    assert not df.empty


@pytest.mark.web
@pytest.mark.vcr()
@pytest.mark.parametrize("parallel", [True, False])
def test_search_servers_with_a_list(parallel):
    """
    Check that downloads are made and that serial and parallel results are similar.
    Ideally they should be identical but the servers are live
    and changes from one request to another can happen.

    """
    servers_list = [
        "https://coastwatch.pfeg.noaa.gov/erddap/",
        "https://gliders.ioos.us/erddap/",
    ]
    query = "sst"
    protocol = "griddap"
    df = search_servers(
        query=query,
        servers_list=servers_list,
        protocol=protocol,
        parallel=parallel,
    )

    assert df is not None
    assert not df.empty
