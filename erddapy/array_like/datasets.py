"""Classes to represent ERDDAP datasets."""

from pathlib import Path
from typing import Union

from erddapy.array_like.connection import ERDDAPConnection

StrLike = Union[str, bytes]
FilePath = Union[str, Path]


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
