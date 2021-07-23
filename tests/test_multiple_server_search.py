from erddapy.multiple_server_search import fetch_results, search_servers


def test_fetch_results():
    "This search should return results"
    url = 'https://gliders.ioos.us/erddap/search/index.csv?page=1&itemsPerPage=100000&searchFor="sst"'
    key = "ioos"
    data = fetch_results(url, key)
    assert data is not None


def test_fetch_no_results():
    """This search should return no results"""
    url = (
        "https://gliders.ioos.us/erddap/search/index.csv?page=1&itemsPerPage=100000&searchFor"
        '="incredibly_long_string_that_should_never_match_a_real_dataset" '
    )
    key = "ioos"
    data = fetch_results(url, key)
    assert data is None


def test_search_servers():
    """Check that downloads are made and that serial and parallel results are identical"""
    servers = [
        "https://coastwatch.pfeg.noaa.gov/erddap/",
        "https://gliders.ioos.us/erddap/",
    ]
    parallel = search_servers(query="sst", servers_list=servers, protocol="griddap")
    serial = search_servers(
        query="sst",
        servers_list=servers,
        protocol="griddap",
        parallel=False,
    )
    assert parallel is not None
    assert (parallel == serial).all().all()
