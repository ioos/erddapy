"""
utilities

"""

from __future__ import (absolute_import, division, print_function)


import io
from collections import namedtuple

from pandas.core.tools.datetimes import parse_time_string

import pytz

import requests


_server = namedtuple('server', ['description', 'url'])

servers = {
    'MDA': _server(
        'Marine Domain Awareness (MDA) - Italy',
        'https://bluehub.jrc.ec.europa.eu/erddap'
    ),
    'MII': _server(
        'Marine Institute - Ireland',
        'http://erddap.marine.ie/erddap'
    ),
    'CSCGOM': _server(
        'CoastWatch Caribbean/Gulf of Mexico Node',
        'http://cwcgom.aoml.noaa.gov/erddap'
    ),
    'CSWC': _server(
        'CoastWatch West Coast Node',
        'https://coastwatch.pfeg.noaa.gov/erddap'
    ),
    'CeNCOOS': _server(
        'NOAA IOOS CeNCOOS (Central and Northern California Ocean Observing System)',
        'http://erddap.axiomalaska.com/erddap'
    ),
    'NERACOOS': _server(
        'NOAA IOOS NERACOOS (Northeastern Regional Association of Coastal and Ocean Observing Systems)',
        'http://www.neracoos.org/erddap'
    ),
    'NGDAC': _server(
        'NOAA IOOS NGDAC (National Glider Data Assembly Center)',
        'http://data.ioos.us/gliders/erddap'
    ),
    'PacIOOS': _server(
        'NOAA IOOS PacIOOS (Pacific Islands Ocean Observing System) at the University of Hawaii (UH)',
        'http://oos.soest.hawaii.edu/erddap'
    ),
    'SECOORA': _server(
        'NOAA IOOS SECOORA (Southeast Coastal Ocean Observing Regional Association)',
        'http://erddap.secoora.org/erddap'
    ),
    'NCEI': _server(
        'NOAA NCEI (National Centers for Environmental Information) / NCDDC',
        'http://ecowatch.ncddc.noaa.gov/erddap'
    ),
    'OSMC': _server(
        'NOAA OSMC (Observing System Monitoring Center)',
        'http://osmc.noaa.gov/erddap'
    ),
    'UAF': _server(
        'NOAA UAF (Unified Access Framework)',
        'https://upwell.pfeg.noaa.gov/erddap'
    ),
    'ONC': _server(
        'ONC (Ocean Networks Canada)',
        'http://dap.onc.uvic.ca/erddap'
    ),
    'BMLSC': _server(
        'UC Davis BML (University of California at Davis, Bodega Marine Laboratory)',
        'http://bmlsc.ucdavis.edu:8080/erddap'
    ),
    'RTECH': _server(
        'R.Tech Engineering',
        'http://meteo.rtech.fr/erddap'
    ),
    'IFREMER': _server(
        'French Research Institute for the Exploitation of the Sea',
        'http://www.ifremer.fr/erddap'
    ),
    'UBC': _server(
        'UBC Earth, Ocean & Atmospheric Sciences SalishSeaCast Project',
        'https://salishsea.eos.ubc.ca/erddap/'
    ),
}


def urlopen(url, params=None, **kwargs):
    """Thin wrapper around requests get content.

    See requests.get docs for the `params` and `kwargs` options.

    """
    return io.BytesIO(requests.get(url, params=params, **kwargs).content)


def _check_url_response(url, **kwargs):
    """Shortcut to `raise_for_status` instead of fetching the whole content."""
    r = requests.head(url, **kwargs)
    r.raise_for_status()
    return url


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
    date_time = parse_time_string(date_time)
    # pandas returns a tuple with datetime, dateutil, and string representation.
    # we want only the datetime obj.
    if isinstance(date_time, tuple):
        date_time = date_time[0]

    if not date_time.tzinfo:
        date_time = pytz.utc.localize(date_time)
    else:
        date_time = date_time.astimezone(pytz.utc)

    return date_time.timestamp()


def quote_string_constraints(kwargs):
    """
    For constraints of String variables,
    the right-hand-side value must be surrounded by double quotes.

    """
    return {k: '"{}"'.format(v) if isinstance(v, str) else v for k, v in kwargs.items()}
