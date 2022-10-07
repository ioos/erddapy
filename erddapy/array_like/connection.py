"""Class ERDDAPConnection to represent connection to a particular URL."""

from __future__ import annotations

from pathlib import Path
from typing import Union

StrLike = Union[str, bytes]
FilePath = Union[str, Path]


class ERDDAPConnection:
    """
    Manages connection that will be used in ERDDAPServer instances.

    While most ERDDAP servers allow connections via a bare url, some servers may require authentication
    to access data.
    """

    def __init__(self, server: str):
        """Initialize instance of ERDDAPConnection."""
        self._server = self.to_string(server)

    @classmethod
    def to_string(cls, value):
        """Convert an instance of ERDDAPConnection to a string."""
        if isinstance(value, str):
            return value
        elif isinstance(value, cls):
            return value.server
        else:
            raise TypeError(
                f"Server must be either a string or an instance of ERDDAPConnection. '{value}' was "
                f"passed.",
            )

    def get(self, url_part: str) -> StrLike:
        """
        Request data from the server.

        Uses requests by default similar to most of the current erddapy data fetching functionality.

        Can be overridden to use httpx, and potentially aiohttp or other async functionality, which could
        hopefully make anything else async compatible.
        """
        pass

    def open(self, url_part: str) -> FilePath:
        """Yield file-like object for access for file types that don't enjoy getting passed a string."""
        pass

    @property
    def server(self) -> str:
        """Access the private ._server attribute."""
        return self._server

    @server.setter
    def server(self, value: str):
        """Set private ._server attribute."""
        self._server = self.to_string(value)
