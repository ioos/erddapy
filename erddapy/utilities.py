from __future__ import (absolute_import, division, print_function)


import requests


def _check_url_response(url):
    """Shortcut to `raise_for_status` instead of fetching the whole content."""
    r = requests.head(url)
    r.raise_for_status()
    return r.url


def _clean_response(response):
    """Allow for `ext` or `.ext` format.

    The user can, for example, use either `.csv` or `csv` in the response kwarg.

    """
    return response.lstrip('.')


def parse_dates(date_time):
    """
    ERDDAP ReSTful API standardizes the representation of dates as either ISO
    strings or seconds since 1970, but internally ERDDAPY uses datetime-like
    objects. `timestamp` returns the expected strings in seconds since 1970.

    """

    return date_time.timestamp()


def quote_string_constraints(kwargs):
    """
    For constraints of String variables,
    the right-hand-side value must be surrounded by double quotes.

    """
    return {k: '"{}"'.format(v) if isinstance(v, str) else v for k, v in kwargs.items()}
