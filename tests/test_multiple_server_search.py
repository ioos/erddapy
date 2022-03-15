"""Test Multiple ERDDAP search."""

import sys

import pytest

from erddapy.multiple_server_search import fetch_results, search_servers


@pytest.mark.web
@pytest.mark.vcr()
def test_fetch_results():
    """This search should return results."""
    url = 'https://gliders.ioos.us/erddap/search/index.csv?page=1&itemsPerPage=100000&searchFor="sst"'
    key = "ioos"
    protocol = "tabledap"
    data = fetch_results(url, key, protocol)
    assert data is not None


@pytest.mark.web
@pytest.mark.vcr()
def test_fetch_no_results():
    """This search should return no results."""
    url = (
        "https://gliders.ioos.us/erddap/search/index.csv?page=1&itemsPerPage=100000&searchFor"
        '="incredibly_long_string_that_should_never_match_a_real_dataset" '
    )
    key = "ioos"
    protocol = "tabledap"
    data = fetch_results(url, key, protocol)
    assert data is None


# I guess we cannot record vcrs with parallel requests.
# @pytest.mark.vcr()
@pytest.mark.web
@pytest.mark.skipif(
    sys.platform in ["win32", "darwin"],
    reason="run only on linux to avoid extra load on the server",
)
def test_search_awesome_erddap_servers_True():
    """Test multiple server search on awesome ERDDAP list parallel=True."""
    query = "glider"
    protocol = "tabledap"
    df = search_servers(
        query=query,
        protocol=protocol,
        parallel=True,
    )
    assert df is not None
    assert not df.empty


# https://github.com/kevin1024/vcrpy/issues/533
# @pytest.mark.vcr()
@pytest.mark.web
@pytest.mark.skipif(
    sys.platform in ["win32", "darwin"],
    reason="run only on linux to avoid extra load on the server",
)
def test_search_awesome_erddap_servers_False():
    """Test multiple server search on awesome ERDDAP list with parallel=False."""
    query = "glider"
    protocol = "tabledap"
    df = search_servers(
        query=query,
        protocol=protocol,
        parallel=False,
    )
    assert df is not None
    assert not df.empty


# I guess we cannot record vcrs with parallel requests.
# @pytest.mark.vcr()
@pytest.mark.web
@pytest.mark.skipif(
    (sys.platform in ["win32", "darwin"] or sys.version_info < (3, 9)),
    reason="run only on linux and latest to avoid extra load on the server",
)
def test_search_servers_with_a_list_True():
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
        parallel=True,
    )

    assert df is not None
    assert not df.empty


@pytest.mark.vcr()
@pytest.mark.web
@pytest.mark.skipif(
    sys.platform in ["win32", "darwin"],
    reason="run only on linux to avoid extra load on the server",
)
def test_search_servers_with_a_list_False():
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
        parallel=False,
    )

    assert df is not None
    assert not df.empty
