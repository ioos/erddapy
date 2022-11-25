"""
Interface between URL responses and third-party libraries.

This module takes an URL or the bytes response of a request and converts it to Pandas,
XArray, Iris, etc. objects.
"""
from typing import TYPE_CHECKING

from typing import Dict, Optional

import pandas as pd

from erddapy.core.netcdf import _nc_dataset, _tempnc
from erddapy.core.url import urlopen

if TYPE_CHECKING:
    import xarray as xr
    from netCDF4 import Dataset

<<<<<<< HEAD
def to_pandas(url: str, requests_kwargs: Optional[Dict] = None, **kw) -> "pd.DataFrame":
=======
def to_pandas(
    url: str, requests_kwargs: Optional[Dict] = None, **pandas_kwargs
) -> pd.DataFrame:
>>>>>>> bf7cbb2 (Renaming **kw according to each library)
    """
    Convert a URL to Pandas DataFrame.

    url: URL to request data from.
    requests_kwargs: arguments to be passed to urlopen method.
    **pandas_kwargs: kwargs to be passed to third-party library (pandas).
    """
    if requests_kwargs is None:
        requests_kwargs = {}
    data = urlopen(url, **requests_kwargs)
    try:
        return pd.read_csv(data, **pandas_kwargs)
    except Exception as e:
        raise ValueError(f"Could not read url {url} with Pandas.read_csv.") from e


<<<<<<< HEAD
def to_ncCF(url: str, protocol: str = None, **kw) -> "Dataset":
    """Convert a URL to a netCDF4 Dataset."""
=======
def to_ncCF(
    url: str,
    protocol: str = None,
    requests_kwargs: Optional[Dict] = None,
) -> Dataset:
    """
    Convert a URL to a netCDF4 Dataset.

    url: URL to request data from.
    protocol: 'griddap' or 'tabledap'.
    requests_kwargs: arguments to be passed to urlopen method (including auth).
    """
>>>>>>> bf7cbb2 (Renaming **kw according to each library)
    if protocol == "griddap":
        raise ValueError(
            f"Cannot use .ncCF with griddap protocol. The URL you tried to access is: '{url}'.",
        )
    if requests_kwargs is None:
        requests_kwargs = {}
    return _nc_dataset(url, **requests_kwargs)


def to_xarray(
    url: str,
    response="opendap",
    requests_kwargs: Optional[Dict] = None,
    **xarray_kwargs,
) -> "xr.Dataset":
    """
    Convert a URL to an xarray dataset.

    url: URL to request data from.
    response: type of response to be requested from the server.
    requests_kwargs: arguments to be passed to urlopen method
    xarray_kwargs: kwargs to be passed to third-party library (xarray).
    """
    import xarray as xr

    if requests_kwargs is None:
        requests_kwargs = {}
    if response == "opendap":
        return xr.open_dataset(url, **xarray_kwargs)
    else:
        nc = _nc_dataset(url, **requests_kwargs)
        return xr.open_dataset(xr.backends.NetCDF4DataStore(nc), **xarray_kwargs)


def to_iris(url: str, requests_kwargs: Optional[Dict] = None, **iris_kwargs):
    """
    Convert a URL to an iris CubeList.

    url: URL to request data from.
    requests_kwargs: arguments to be passed to urlopen method.
    iris_kwargs: kwargs to be passed to third-party library (iris).
    """
    import iris

    if requests_kwargs is None:
        requests_kwargs = {}
    data = urlopen(url, **requests_kwargs)
    with _tempnc(data) as tmp:
        cubes = iris.load_raw(tmp, **iris_kwargs)
        _ = [cube.data for cube in cubes]
        return cubes
