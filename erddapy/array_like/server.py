"""Class ERDDAPServer to represent an ERDDAP server connection."""

from __future__ import annotations

from erddapy.array_like.connection import ERDDAPConnection
from erddapy.array_like.datasets import ERDDAPDataset


class ERDDAPServer:
    """Instance of an ERDDAP server, with support to ERDDAP's native functionalities."""

    def __init__(self, url: str, connection: ERDDAPConnection | None):
        """Initialize instance of ERDDAPServer."""
        if "http" in url:
            self.url = url
        else:
            # get URL from dict of ERDDAP servers
            self._connection = connection or ERDDAPConnection()

    @property
    def connection(self) -> ERDDAPConnection:
        """Access private ._connection attribute."""
        return self._connection

    @connection.setter
    def connection(self, value: str | ERDDAPConnection):
        """Set private ._connection attribute."""
        self._connection = value or ERDDAPConnection()

    def full_text_search(self, query: str) -> dict[str, ERDDAPDataset]:
        """Search the server with native ERDDAP full text search capabilities."""
        pass

    def search(self, query: str) -> dict[str, ERDDAPDataset]:
        """
        Search the server with native ERDDAP full text search capabilities.

        Also see ERDDAPServer.full_text_search.
        """
        return self.full_text_search(query)

    def advanced_search(self, **kwargs) -> dict[str, ERDDAPDataset]:
        """Search server with ERDDAP advanced search capabilities (may return pre-filtered datasets)."""
        pass
