"""URL handling."""

import functools
import io
from typing import Dict, Optional
from typing.io import BinaryIO

import httpx


@functools.lru_cache(maxsize=256)
def _urlopen(url: str, auth: Optional[tuple] = None, **kwargs: Dict) -> BinaryIO:
    response = httpx.get(url, follow_redirects=True, auth=auth, **kwargs)
    try:
        response.raise_for_status()
    except httpx.HTTPError as err:
        raise httpx.HTTPError(f"{response.content.decode()}") from err
    return io.BytesIO(response.content)


def urlopen(url: str, auth: Optional[tuple] = None, **kwargs: Dict) -> BinaryIO:
    """Thin wrapper around httpx get content.

    See httpx.get docs for the `params` and `kwargs` options.

    """
    # Ignoring type checks here b/c mypy does not support decorated functions.
    data = _urlopen(url=url, auth=auth, **kwargs)  # type: ignore
    data.seek(0)
    return data


@functools.lru_cache(maxsize=None)
def check_url_response(url: str, **kwargs: Dict) -> str:
    """
    Shortcut to `raise_for_status` instead of fetching the whole content.

    One should only use this is passing URLs that are known to work is necessary.
    Otherwise let it fail later and avoid fetching the head.

    """
    r = httpx.head(url, **kwargs)
    r.raise_for_status()
    return url


def _distinct(url: str, **kwargs: Dict) -> str:
    """
    Sort all of the rows in the results table.

    Starting with the first requested variable,
    then using the second requested variable if the first variable has a tie, ...,
    then remove all non-unique rows of data.

    For example, a query for the variables ["stationType", "stationID"] with `distinct=True`
    will return a sorted list of "stationIDs" associated with each "stationType".

    See https://coastwatch.pfeg.noaa.gov/erddap/tabledap/documentation.html#distinct

    """
    distinct = kwargs.pop("distinct", False)
    if distinct is True:
        return f"{url}&distinct()"
    return url


def _format_search_string(server: str, query: str) -> str:
    """Generate a search string for an erddap server with user defined query."""
    return f'{server}search/index.csv?page=1&itemsPerPage=100000&searchFor="{query}"'


def _multi_urlopen(url: str) -> BinaryIO:
    """Simpler url open to work with multiprocessing."""
    try:
        data = urlopen(url)
    except (httpx.HTTPError, httpx.ConnectError):
        return None
    return data
