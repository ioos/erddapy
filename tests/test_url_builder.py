from erddapy import ERDDAP
from erddapy.utilities import parse_dates

import pytest

from requests.exceptions import HTTPError


def _url_to_dict(url):
    return {v.split('=')[0]: v.split('=')[1] for v in url.split('&')[1:]}


@pytest.fixture
@pytest.mark.web
def server():
    yield ERDDAP(
        server='https://upwell.pfeg.noaa.gov/erddap',
        protocol='tabledap',
        response='htmlTable',
    )


@pytest.mark.web
def test_search_url_bad_request(server):
    """Test if a bad request returns HTTPError."""
    kw = {
        'min_time': '1700-01-01T12:00:00Z',
        'max_time': '1750-01-01T12:00:00Z',
    }
    with pytest.raises(HTTPError):
        server.get_search_url(**kw)


@pytest.mark.web
def test_search_url_valid_request(server):
    """Test if a bad request returns HTTPError."""
    min_time = '1800-01-01T12:00:00Z'
    max_time = '1950-01-01T12:00:00Z'
    kw = {
        'min_time': min_time,
        'max_time': max_time,
    }
    url = server.get_search_url(**kw)
    assert url.startswith(f'{server.server}/search/advanced.{server.response}?')
    options = _url_to_dict(url)
    assert options.pop('minTime') == str(parse_dates(min_time))
    assert options.pop('maxTime') == str(parse_dates(max_time))
    assert options.pop('itemsPerPage') == str(1000)
    for k, v in options.items():
        assert v == '(ANY)'


@pytest.mark.web
def test_info_url(server):
    """Check info URL results."""
    dataset_id = 'gtoppAT'
    url = server.get_info_url(dataset_id=dataset_id)
    assert url == f'{server.server}/info/{dataset_id}/index.{server.response}'

    url = server.get_info_url(dataset_id=dataset_id, response='csv')
    assert url == f'{server.server}/info/{dataset_id}/index.csv'


@pytest.mark.web
def test_categorize_url(server):
    """Check categorize URL results."""
    categorize_by = 'standard_name'
    url = server.get_categorize_url(categorize_by=categorize_by)
    assert url == f'{server.server}/categorize/{categorize_by}/index.{server.response}'

    url = server.get_categorize_url(categorize_by=categorize_by, response='csv')
    assert url == f'{server.server}/categorize/{categorize_by}/index.csv'


@pytest.mark.web
def test_download_url_unconstrained(server):
    """Check download URL results."""
    dataset_id = 'gtoppAT'
    variables = ['commonName', 'yearDeployed', 'serialNumber']
    url = server.get_download_url(dataset_id=dataset_id, variables=variables)
    assert url.startswith(f'{server.server}/{server.protocol}/{dataset_id}.{server.response}?')
    assert sorted(url.split('?')[1].split(',')) == sorted(variables)


@pytest.mark.web
def test_download_url_constrained(server):
    dataset_id = 'gtoppAT'
    variables = ['commonName', 'yearDeployed', 'serialNumber']

    min_time = '2002-06-30T13:53:16Z'
    max_time = '2018-10-27T04:54:00Z'
    min_lat = -42
    max_lat = 42
    min_lon = 0
    max_lon = 360

    constraints = {
        'time>=': min_time,
        'time<=': max_time,
        'latitude>=': min_lat,
        'latitude<=': max_lat,
        'longitude>=': min_lon,
        'longitude<=': max_lon,
    }

    url = server.get_download_url(
        dataset_id=dataset_id,
        variables=variables,
        response='csv',
        constraints=constraints,
    )
    assert url.startswith(f'{server.server}/{server.protocol}/{dataset_id}.csv?')
    options = _url_to_dict(url)
    assert options['time>'] == str(parse_dates(min_time))
    assert options['time<'] == str(parse_dates(max_time))
    assert options['latitude>'] == str(min_lat)
    assert options['latitude<'] == str(max_lat)
    assert options['longitude>'] == str(min_lon)
    assert options['longitude<'] == str(max_lon)
