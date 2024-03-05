"""Servers subpackage for the erddapy parent package.

Contains the servers.py module and the erddaps.json file,
which is a fallback when the awesome-erddap URL cannot be reached.
"""

from erddapy.servers.servers import servers

__all__ = [
    "servers",
]
