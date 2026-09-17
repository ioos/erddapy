"""ERDDAPyBackendEntrypoint."""

import urllib.parse
from collections.abc import Iterable

try:
    import xarray as xr
    from xarray.backends.common import T_PathFileOrDataStore

    XARRAY_IMPORT = True
except ImportError:
    XARRAY_IMPORT = False

from erddapy.core.interfaces import to_xarray
from erddapy.core.url import _is_netcdf, _is_url

if XARRAY_IMPORT:

    def _make_opendap(url: str) -> str:
        parts = urllib.parse.urlparse(url)
        opendap_url = urllib.parse.urlunparse(
            [parts.scheme, parts.netloc, parts.path, "", "", ""],
        )
        return opendap_url.split(".nc")[0]

    class ERDDAPyBackendEntrypoint(xr.backends.BackendEntrypoint):
        """Erddapy backend entrypoint for xarray."""

        def open_dataset(
            self,
            filename_or_obj: T_PathFileOrDataStore,
            *,
            drop_variables: str | Iterable[str] | None = None,  # noqa: ARG002
        ) -> xr.Dataset:
            """Open ERDDAP URLs as xarray datasets."""
            return open_erddap_dataset(filename_or_obj)

        open_dataset_parameters = ("filename_or_obj", "drop_variables")

        description = "Load ERDDAP URLs in xarray."

    def open_erddap_dataset(
        filename_or_obj: T_PathFileOrDataStore,
    ) -> xr.Dataset:
        """Open an ERDDAP URL response as an xarray object."""
        if not _is_url(filename_or_obj):
            msg = f"Expected an ERDDAP URL, got {filename_or_obj!r}."
            raise ValueError(msg)

        url = str(filename_or_obj)
        if _is_netcdf(url):
            response = "nc"
        else:
            filename_or_obj = _make_opendap(url)
            response = "opendap"

        return to_xarray(url, response=response)
