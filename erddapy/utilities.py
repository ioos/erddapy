"""
utilities

"""

import functools
import io

from collections import namedtuple
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, Generator, Optional, Union
from typing.io import BinaryIO
from urllib.parse import urlparse

import pandas as pd
import pytz
import requests


try:
    from pandas.core.indexes.period import parse_time_string
except ImportError:
    from pandas._libs.tslibs.parsing import parse_time_string


def _nc_dataset(url, auth, **requests_kwargs: Dict):
    """Returns a netCDF4-python Dataset from memory and fallbacks to disk if that fails."""
    from netCDF4 import Dataset

    data = urlopen(url=url, auth=auth, **requests_kwargs)
    try:
        return Dataset(Path(urlparse(url).path).name, memory=data.read())
    except OSError:
        # if libnetcdf is not compiled with in-memory support fallback to a local tmp file
        with _tempnc(data) as _nc:
            return Dataset(_nc)


@functools.lru_cache(maxsize=None)
def servers_list():
    """
    Download a new server list from awesome-erddap.
    If loading the latest one fails it falls back to the default one shipped with the package.

    """
    from urllib.error import URLError

    try:
        url = "https://raw.githubusercontent.com/IrishMarineInstitute/awesome-erddap/master/erddaps.json"
        df = pd.read_json(url)
    except URLError:
        from pathlib import Path

        path = Path(__file__).absolute().parent
        df = pd.read_json(path.joinpath("erddaps.json"))
    _server = namedtuple("server", ["description", "url"])
    return {
        row["short_name"]: _server(row["name"], row["url"])
        for k, row in df.iterrows()
        if row["short_name"]
    }


servers = servers_list()


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
    """Creates a temporary netcdf file."""
    from tempfile import NamedTemporaryFile

    tmp = None
    try:
        tmp = NamedTemporaryFile(suffix=".nc", prefix="erddapy_")
        tmp.write(data.read())
        tmp.flush()
        yield tmp.name
    finally:
        if tmp is not None:
            tmp.close()


def show_iframe(src):
    """Helper function to show HTML returns."""
    from IPython.display import IFrame

    return IFrame(src=src, width="100%", height=950)
