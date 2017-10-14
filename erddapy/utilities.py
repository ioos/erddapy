from __future__ import (absolute_import, division, print_function)


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


def parse_dates(date_time, calendar='standard'):
    """
    ERDDAP ReSTful API can take a variety of time constraints,
    for erddapy we chose to use only `seconds since 1970-01-01T00:00:00Z`,
    converted from datetime internally, that way the user can parse the dates in any way they like
    using python datetime like objects..

    """

    utime = netcdftime.utime('seconds since 1970-01-01T00:00:00Z', calendar=calendar)
    return utime.date2num(date_time)
