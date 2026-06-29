"""Easier access to scientific data."""

from erddapy.erddapy import ERDDAP

__all__ = [
    "ERDDAP",
]

try:
    from ._version import __version__
except ImportError:
    __version__ = "unknown"
