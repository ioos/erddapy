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


def parse_dates(date_time):
    """
    ERDDAP ReSTful API standardizes the representation of data as either ISO strings or seconds since 1970, 
    but in ERDDAPY we convert all datetime objects to seconds since 1970. 

    """

    return date_time.timestamp()
