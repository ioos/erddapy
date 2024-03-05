"""Core subpackage for the erddapy parent package.

This package contains the URL and data handling functionalities.
"""

from erddapy.core.interfaces import to_iris, to_ncCF, to_pandas, to_xarray
from erddapy.core.url import (
    get_categorize_url,
    get_download_url,
    get_info_url,
    get_search_url,
    parse_dates,
)

__all__ = [
    "get_categorize_url",
    "get_download_url",
    "get_info_url",
    "get_search_url",
    "parse_dates",
    "to_iris",
    "to_ncCF",
    "to_pandas",
    "to_xarray",
]
