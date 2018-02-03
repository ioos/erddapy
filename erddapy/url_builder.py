"""
url_builder

See ERDDAP docs for all the response options available,

- tabledap: http://coastwatch.pfeg.noaa.gov/erddap/tabledap/documentation.html
- griddap: http://coastwatch.pfeg.noaa.gov/erddap/griddap/documentation.html

"""

from __future__ import (absolute_import, division, print_function)

import copy

try:
    from urllib.parse import quote_plus
except ImportError:
    from urllib import quote_plus

from erddapy.utilities import (_check_url_response, parse_dates, quote_string_constraints)


def search_url(server, response='html', search_for=None, items_per_page=1000, page=1, **kwargs):
    """The search URL for the `server` endpoint provided.

    Args:
        search_for (str): "Google-like" search of the datasets' metadata.

            - Type the words you want to search for, with spaces between the words.
                ERDDAP will search for the words separately, not as a phrase.
            - To search for a phrase, put double quotes around the phrase
                (for example, `"wind speed"`).
            - To exclude datasets with a specific word, use `-excludedWord`.
            - To exclude datasets with a specific phrase, use `-"excluded phrase"`
            - Searches are not case-sensitive.
            - To find just grid or just table datasets, include `protocol=griddap`
                or `protocol=tabledap` in your search.
            - You can search for any part of a word. For example,
                searching for `spee` will find datasets with `speed` and datasets with
                `WindSpeed`
            - The last word in a phrase may be a partial word. For example,
                to find datasets from a specific website (usually the start of the datasetID),
                include (for example) `"datasetID=erd"` in your search.

        response (str): default is HTML.
        items_per_page (int): how many items per page in the return,
            default is 1000.
        page (int): which page to display, defatul is the first page (1).
        kwargs (dict): extra search constraints based on metadata and/or coordinates ke/value.
            metadata: `cdm_data_type`, `institution`, `ioos_category`,
            `keywords`, `long_name`, `standard_name`, and `variableName`.
            coordinates: `minLon`, `maxLon`, `minLat`, `maxLat`, `minTime`, and `maxTime`.

    Returns:
        url (str): the search URL.

    """
    base = (
        '{server_url}/search/advanced.{response}'
        '?page={page}'
        '&itemsPerPage={itemsPerPage}'
        '&protocol={protocol}'
        '&cdm_data_type={cdm_data_type}'
        '&institution={institution}'
        '&ioos_category={ioos_category}'
        '&keywords={keywords}'
        '&long_name={long_name}'
        '&standard_name={standard_name}'
        '&variableName={variableName}'
        '&minLon={minLon}'
        '&maxLon={maxLon}'
        '&minLat={minLat}'
        '&maxLat={maxLat}'
        '&minTime={minTime}'
        '&maxTime={maxTime}'
        )
    if search_for:
        search_for = quote_plus(search_for)
        base += '&searchFor={searchFor}'

    # Convert dates from datetime to `seconds since 1970-01-01T00:00:00Z`.
    min_time = kwargs.pop('min_time', None)
    max_time = kwargs.pop('max_time', None)
    if min_time:
        kwargs.update({'min_time': parse_dates(min_time)})
    if max_time:
        kwargs.update({'max_time': parse_dates(max_time)})

    default = '(ANY)'
    url = base.format(
        server_url=server,
        response=response,
        page=page,
        itemsPerPage=items_per_page,
        protocol=kwargs.get('protocol', default),
        cdm_data_type=kwargs.get('cdm_data_type', default),
        institution=kwargs.get('institution', default),
        ioos_category=kwargs.get('ioos_category', default),
        keywords=kwargs.get('keywords', default),
        long_name=kwargs.get('long_name', default),
        standard_name=kwargs.get('standard_name', default),
        variableName=kwargs.get('variableName', default),
        minLon=kwargs.get('min_lon', default),
        maxLon=kwargs.get('max_lon', default),
        minLat=kwargs.get('min_lat', default),
        maxLat=kwargs.get('max_lat', default),
        minTime=kwargs.get('min_time', default),
        maxTime=kwargs.get('max_time', default),
        searchFor=search_for,
        )
    return _check_url_response(url)


def info_url(server, dataset_id, response='html'):
    """The info URL for the `server` endpoint.

    Args:
        dataset_id (str): a dataset unique id.
        response (str): default is HTML.

    Returns:
        url (str): the info URL for the `response` chosen.

    """
    base = '{server_url}/info/{dataset_id}/index.{response}'.format
    url = base(
        server_url=server,
        dataset_id=dataset_id,
        response=response
        )
    return _check_url_response(url)


def download_url(server, dataset_id, protocol, variables, response='html', constraints=None):
    """The download URL for the `server` endpoint.

    Args:
        dataset_id (str): a dataset unique id.
        protocol (str): tabledap or griddap.
        variables (list/tuple): a list of the variables to download.
        response (str): default is HTML.
        constraints (dict): download constraints, default None (opendap-like url)
            example: constraints = {'latitude<=': 41.0,
                                    'latitude>=': 38.0,
                                    'longitude<=': -69.0,
                                    'longitude>=': -72.0,
                                    'time<=': '2017-02-10T00:00:00+00:00',
                                    'time>=': '2016-07-10T00:00:00+00:00',
                                    }

    Returns:
        url (str): the download URL for the `response` chosen.

    """
    base = '{server_url}/{protocol}/{dataset_id}'

    if not constraints:
        url = base.format(
            server_url=server,
            protocol=protocol,
            dataset_id=dataset_id
            )
    else:
        base += '.{response}?{variables}{constraints}'

        _constraints = copy.copy(constraints)
        for k, v in _constraints.items():
            if k.startswith('time'):
                _constraints.update({k: parse_dates(v)})

        _constraints = quote_string_constraints(_constraints)
        _constraints = ''.join(['&{}{}'.format(k, v) for k, v in _constraints.items()])
        variables = ','.join(variables)

        url = base.format(
            server_url=server,
            protocol=protocol,
            dataset_id=dataset_id,
            response=response,
            variables=variables,
            constraints=_constraints
        )
    return _check_url_response(url)
