from __future__ import (absolute_import, division, print_function)


import io
from tempfile import NamedTemporaryFile

from netCDF4 import netcdftime

import requests


def _check_url_response(url):
    """Shortcut to `raise_for_status` instead of fetching the whole content."""
    r = requests.head(url)
    r.raise_for_status()


def _clean_response(response):
    """Allow for `ext` or `.ext` format.

    The user can, for example, use either `.csv` or `csv` in the response kwarg.

    """
    return response.lstrip('.')


def _urlopen(url):
    """Thin wrapper around requests get content."""
    return io.BytesIO(requests.get(url).content)


def parse_dates(date_time, calendar='standard'):
    """
    ERDDAP ReSTful API can take a variety of time constraints.
    For erddapy we chose to use only `seconds since 1970-01-01T00:00:00Z`,
    converted from datetime, so the user can parse the dates in any way they like.

    """

    utime = netcdftime.utime('seconds since 1970-01-01T00:00:00Z', calendar=calendar)
    return utime.date2num(date_time)


def open_dataset(url, **kwargs):
    """
    Load data as a xarray dataset from the .nc format.

    Caveat: this downloads a temporary file! Be careful with the size of the request.

    """
    import xarray as xr
    data = _urlopen(url).read()
    with NamedTemporaryFile(suffix='.nc', prefix='erddapy_') as tmp:
        tmp.write(data)
        tmp.flush()
        return xr.open_dataset(tmp.name, **kwargs)
