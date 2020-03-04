"""
utilities

"""

import functools
import io

from collections import namedtuple
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, Generator, Optional, Union
from typing.io import BinaryIO

import pandas as pd
import pytz
import requests

from pandas.core.tools.datetimes import parse_time_string


@functools.lru_cache(maxsize=None)
def servers_list():
    # Replace with the master version when
    # https://github.com/IrishMarineInstitute/awesome-erddap/pull/14 is merged.
    # url = "https://raw.githubusercontent.com/IrishMarineInstitute/awesome-erddap/master/erddaps.json"
    url = "https://raw.githubusercontent.com/ocefpaf/awesome-erddap/short_names/erddaps.json"
    df = pd.read_json(url)
    _server = namedtuple("server", ["description", "url"])
    return {
        row["short_name"]: _server(row["name"], row["url"]) for k, row in df.iterrows()
    }


servers = servers_list()


def urlopen(url, params: Optional[Dict] = None, **kwargs: Dict) -> BinaryIO:
    """Thin wrapper around requests get content.

    See requests.get docs for the `params` and `kwargs` options.

    """
    return io.BytesIO(requests.get(url, params=params, **kwargs).content)


@functools.lru_cache(maxsize=None)
def _check_url_response(url: str, **kwargs: Dict) -> str:
    """Shortcut to `raise_for_status` instead of fetching the whole content."""
    r = requests.head(url, **kwargs)
    r.raise_for_status()
    return url


def _clean_response(response: str) -> str:
    """Allow for `ext` or `.ext` format.

    The user can, for example, use either `.csv` or `csv` in the response kwarg.

    """
    return response.lstrip(".")


def parse_dates(date_time: Union[datetime, str]) -> float:
    """
    ERDDAP ReSTful API standardizes the representation of dates as either ISO
    strings or seconds since 1970, but internally ERDDAPY uses datetime-like
    objects. `timestamp` returns the expected strings in seconds since 1970.

    """
    if isinstance(date_time, str):
        # pandas returns a tuple with datetime, dateutil, and string representation.
        # we want only the datetime obj.
        parse_date_time = parse_time_string(date_time)[0]
    else:
        parse_date_time = date_time

    if not parse_date_time.tzinfo:
        parse_date_time = pytz.utc.localize(parse_date_time)
    else:
        parse_date_time = parse_date_time.astimezone(pytz.utc)

    return parse_date_time.timestamp()


def quote_string_constraints(kwargs: Dict) -> Dict:
    """
    For constraints of String variables,
    the right-hand-side value must be surrounded by double quotes.

    """
    return {k: f'"{v}"' if isinstance(v, str) else v for k, v in kwargs.items()}


@contextmanager
def _tempnc(data: BinaryIO) -> Generator[str, None, None]:
    from tempfile import NamedTemporaryFile

    tmp = None
    try:
        tmp = NamedTemporaryFile(suffix=".nc", prefix="erddapy_")
        tmp.write(data)
        tmp.flush()
        yield tmp.name
    finally:
        if tmp is not None:
            tmp.close()
