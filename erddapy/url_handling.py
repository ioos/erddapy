"""
URL handling

"""

import functools
import io

from typing import Dict, Optional
from typing.io import BinaryIO

import requests


def urlopen(url, auth: Optional[tuple] = None, **kwargs: Dict) -> BinaryIO:
    """Thin wrapper around requests get content.

    See requests.get docs for the `params` and `kwargs` options.

    """
    response = requests.get(url, allow_redirects=True, auth=auth, **kwargs)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise requests.exceptions.HTTPError(f"{response.content.decode()}") from err
    return io.BytesIO(response.content)


@functools.lru_cache(maxsize=None)
def check_url_response(url: str, **kwargs: Dict) -> str:
    """
    Shortcut to `raise_for_status` instead of fetching the whole content.
    One should only use this is passing URLs that are known to work is necessary.
    Otherwise let it fail later and avoid fetching the head.

    """
    r = requests.head(url, **kwargs)
    r.raise_for_status()
    return url


def _clean_response(response: str) -> str:
    """Allow for `ext` or `.ext` format.

    The user can, for example, use either `.csv` or `csv` in the response kwarg.

    """
    return response.lstrip(".")


def _distinct(url: str, **kwargs: Dict) -> str:
    """
    Sort all of the rows in the results table
    (starting with the first requested variable,
    then using the second requested variable if the first variable has a tie, ...),
    then remove all non-unique rows of data.

    For example, a query for the variables ["stationType", "stationID"] with `distinct=True`
    will return a sorted list of "stationIDs" associated with each "stationType".

    See https://coastwatch.pfeg.noaa.gov/erddap/tabledap/documentation.html#distinct

    """
    distinct = kwargs.pop("distinct", False)
    if distinct is True:
        return f"{url}&distinct()"
    return url
