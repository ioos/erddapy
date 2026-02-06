"""ERDDAPyBackebdEntrypoint."""

import urllib.parse

import xarray as xr

from erddapy.core.interfaces import to_xarray


def _is_netcdf(url: str) -> bool:
    """Check if it .nc .ncCF, or ncCFMA URL.

    Parameters
    ----------
    url : str or unicode
        ERDDAP netcdf-like URL

    Returns
    -------
    isnetcdf : bool
        True is `url` can be opened by xarray otherwise False.

    """
    isnetcdf = False
    if any(
        ext in url for ext in (".nc?", ".ncCF?", ".ncCFMA?")
    ) or url.endswith(
        (".nc", ".ncCF", ".ncCFMA"),
    ):
        isnetcdf = True
    return isnetcdf


def _is_url(url: str) -> bool:
    """Check if it is a valid ERDDAP URL.

    Parameters
    ----------
    url : str or unicode
        ERDDAP netcdf-like URL

    Returns
    -------
    isurl : bool
        True is `url` is a valid ERDDAP URL otherwise False.

    """
    isurl = False
    if not isinstance(url, str):
        isurl = False
    if url.startswith(("http://", "https://")) and "/erddap/" in url:
        isurl = True
    return isurl


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
        filename_or_obj: str,
        *,
        drop_variables: list | None = None,  # noqa: ARG002
    ) -> xr.Dataset:
        """Open ERDDAP URLs as xarray datasets."""
        return open_erddap_dataset(filename_or_obj)

    open_dataset_parameters = ("filename_or_obj", "drop_variables")

    description = "Load ERDDAP URLs in xarray."


def open_erddap_dataset(filename_or_obj: str) -> xr.Dataset:
    """Open an ERDDAP URL with a netcdf-like response as an xarray object."""
    if not _is_url(filename_or_obj):
        msg = f"Expected an ERDDAP URL, got {filename_or_obj}."
        raise ValueError(msg)

    if _is_netcdf(filename_or_obj):
        response = "nc"
    else:
        filename_or_obj = _make_opendap(filename_or_obj)
        response = "opendap"

    return to_xarray(filename_or_obj, response=response)
