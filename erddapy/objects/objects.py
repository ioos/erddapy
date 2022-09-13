"""Main module of the 'objects' subpackage containing most classes."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Union  # noqa

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


class ERDDAPDataset:
    """Base class for more focused table or grid datasets."""

    def __init__(
        self,
        dataset_id: str,
        connection: str | ERDDAPConnection,
        variables,
        constraints,
    ):
        """Initialize instance of ERDDAPDataset."""
        self.dataset_id = dataset_id
        self._connection = ERDDAPConnection(ERDDAPConnection.to_string(connection))
        self._variables = variables
        self._constraints = constraints
        self._meta = None

    @property
    def connection(self) -> ERDDAPConnection:
        """Access private ._connection variable."""
        return self._connection

    @connection.setter
    def connection(self, value: str | ERDDAPConnection):
        """Set private ._connection variable."""
        self._connection = ERDDAPConnection(ERDDAPConnection.to_string(value))

    def get(self, file_type: str) -> StrLike:
        """Request data using underlying connection."""
        return self.connection.get(file_type)

    def open(self, file_type: str) -> FilePath:
        """Download and open dataset using underlying connection."""
        return self.connection.open(file_type)

    def get_meta(self):
        """Request dataset metadata from the server."""
        self._meta = None

    @property
    def meta(self):
        """Access private ._meta attribute. Request metadata if ._meta is empty."""
        return self.get_meta() if (self._meta is None) else self._meta

    @property
    def variables(self):
        """Access private ._variables attribute."""
        return self._variables

    @property
    def constraints(self):
        """Access private ._constraints attribute."""
        return self._constraints

    def url_segment(self, file_type: str) -> str:
        """Return URL segment without the base URL (the portion after 'https://server.com/erddap/')."""
        pass

    def url(self, file_type: str) -> str:
        """
        Return a URL constructed using the underlying ERDDAPConnection.

        The URL will contain information regarding the base class server info, the dataset ID,
        access method (tabledap/griddap), file type, variables, and constraints.

        This allows ERDDAPDataset subclasses to be used as more opinionated URL constructors while still
        not tying users to a specific IO method.

        Not guaranteed to capture all the specifics of formatting a request, such as if a server requires
        specific auth or headers.
        """
        pass

    def to_dataset(self):
        """Open the dataset as xarray dataset by downloading a subset NetCDF."""
        pass

    def opendap_dataset(self):
        """Open the full dataset in xarray via OpenDAP."""
        pass


class TableDataset(ERDDAPDataset):
    """Subclass of ERDDAPDataset specific to TableDAP datasets."""

    def to_dataframe(self):
        """Open the dataset as a Pandas DataFrame."""


class GridDataset(ERDDAPDataset):
    """Subclass of ERDDAPDataset specific to GridDAP datasets."""

    pass


class ERDDAPServer:
    """Instance of an ERDDAP server, with support to ERDDAP's native functionalities."""

    def __init__(self, connection: str | ERDDAPConnection):
        """Initialize instance of ERDDAPServer."""
        self._connection = ERDDAPConnection(ERDDAPConnection.to_string(connection))

    @property
    def connection(self) -> ERDDAPConnection:
        """Access private ._connection attribute."""
        return self._connection

    @connection.setter
    def connection(self, value: str | ERDDAPConnection):
        """Set private ._connection attribute."""
        self._connection = ERDDAPConnection(ERDDAPConnection.to_string(value))

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
