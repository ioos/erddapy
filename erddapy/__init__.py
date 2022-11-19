"""Easier access to scientific data."""

from erddapy.core.interfaces import to_iris, to_ncCF, to_pandas, to_xarray
from erddapy.core.url import (
    get_categorize_url,
    get_download_url,
    get_info_url,
    get_search_url,
    parse_dates,
    urlopen,
)
from erddapy.erddapy import ERDDAP
from erddapy.servers.servers import servers

__all__ = [
    "ERDDAP",
    "servers",
    "get_categorize_url",
    "get_download_url",
    "get_info_url",
    "get_search_url",
    "parse_dates",
    "urlopen",
    "to_ncCF",
    "to_iris",
    "to_pandas",
    "to_xarray",
]

try:
    from ._version import __version__
except ImportError:
    __version__ = "unknown"
