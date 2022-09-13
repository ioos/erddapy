"""Easier access to scientific data."""

from erddapy.erddapy import ERDDAP
from erddapy.objects import ERDDAPConnection, ERDDAPServer, GridDataset, TableDataset
from erddapy.servers.servers import servers

__all__ = [
    "ERDDAP",
    "servers",
    "ERDDAPConnection",
    "ERDDAPServer",
    "TableDataset",
    "GridDataset",
]

try:
    from ._version import __version__
except ImportError:
    __version__ = "unknown"
