"""ERDDAPyBackendEntrypoint."""

import urllib.parse

import xarray as xr

from erddapy.core.interfaces import to_xarray


def _is_netcdf(url: str) -> bool:
    """Check if URL corresponds to a NetCDF-compatible ERDDAP response.

    Parameters
    ----------
    url : str
        ERDDAP dataset URL.

    Returns
    -------
    bool
        True if URL contains NetCDF-like extensions (.nc, .ncCF, .ncCFMA),
        otherwise False.

    Notes
    -----
    This function is used to determine whether the dataset should be
    loaded directly via NetCDF or via OPeNDAP.
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
    """Check if the input is a valid ERDDAP URL.

    Parameters
    ----------
    url : str
        Input string to validate.

    Returns
    -------
    bool
        True if the string is a valid ERDDAP URL, otherwise False.

    Notes
    -----
    A valid ERDDAP URL must:
    - Be a string
    - Start with http:// or https://
    - Contain '/erddap/' in the path
    """
    isurl = False
    if not isinstance(url, str):
        isurl = False
    if url.startswith(("http://", "https://")) and "/erddap/" in url:
        isurl = True
    return isurl


def _make_opendap(url: str) -> str:
    """Convert a NetCDF ERDDAP URL into an OPeNDAP-compatible URL.

    This function removes query parameters and file extensions
    (such as .nc, .ncCF) to produce a clean OPeNDAP endpoint.

    Parameters
    ----------
    url : str
        ERDDAP dataset URL containing NetCDF response format.

    Returns
    -------
    str
        Clean OPeNDAP URL for remote dataset access.

    Notes
    -----
    OPeNDAP allows efficient access to subsets of remote datasets
    without downloading the entire file.
    """
    parts = urllib.parse.urlparse(url)
    opendap_url = urllib.parse.urlunparse(
        [parts.scheme, parts.netloc, parts.path, "", "", ""],
    )
    return opendap_url.split(".nc")[0]


class ERDDAPyBackendEntrypoint(xr.backends.BackendEntrypoint):
    """Custom xarray backend for loading ERDDAP datasets.

    This backend allows xarray to directly open ERDDAP URLs
    as datasets, supporting both NetCDF and OPeNDAP responses.

    It integrates ERDDAP data access into xarray workflows,
    enabling seamless scientific data analysis.

    Notes
    -----
    This class acts as an entrypoint for xarray's backend system.
    """

    def open_dataset(
        self,
        filename_or_obj: str,
        *,
        drop_variables: list | None = None,
    ) -> xr.Dataset:
        """Open an ERDDAP dataset using xarray backend interface.

        Parameters
        ----------
        filename_or_obj : str
            ERDDAP dataset URL.

        drop_variables : list, optional
            Variables to exclude from dataset (currently unused).

        Returns
        -------
        xr.Dataset
            Loaded dataset as an xarray object.

        Notes
        -----
        This method is required by xarray's backend API and delegates
        loading to `open_erddap_dataset`.
        """
        return open_erddap_dataset(filename_or_obj)

    open_dataset_parameters = ("filename_or_obj", "drop_variables")

    description = "Load ERDDAP URLs in xarray."


def open_erddap_dataset(filename_or_obj: str) -> xr.Dataset:
    """Open an ERDDAP dataset URL as an xarray.Dataset.

    This function determines whether the provided URL corresponds
    to a NetCDF-based ERDDAP response or an OPeNDAP endpoint,
    and loads the dataset accordingly.

    Parameters
    ----------
    filename_or_obj : str
        ERDDAP dataset URL.

    Returns
    -------
    xr.Dataset
        Dataset loaded into an xarray object.

    Raises
    ------
    ValueError
        If the input is not a valid ERDDAP URL.

    Behavior
    --------
    - If URL contains NetCDF response (.nc, .ncCF), uses direct NetCDF loading
    - Otherwise, converts URL to OPeNDAP format for remote access

    Notes
    -----
    This function is a key integration point between ERDDAP and xarray.
    """
    if not _is_url(filename_or_obj):
        msg = f"Expected an ERDDAP URL, got {filename_or_obj}."
        raise ValueError(msg)

    if _is_netcdf(filename_or_obj):
        response = "nc"
    else:
        filename_or_obj = _make_opendap(filename_or_obj)
        response = "opendap"

    return to_xarray(filename_or_obj, response=response)