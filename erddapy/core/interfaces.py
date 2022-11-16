"""
Interface between URL responses and third-party libraries.

This module takes an URL or the bytes response of a request and converts it to Pandas,
XArray, Iris, etc. objects.
"""

import iris
import pandas as pd
import xarray as xr
from netCDF4 import Dataset

from erddapy.core.netcdf import _nc_dataset, _tempnc
from erddapy.core.url import urlopen


def to_pandas(url: str, requests_kwargs=None, **kw) -> pd.DataFrame:
    """Convert a URL to Pandas DataFrame."""
    if requests_kwargs is None:
        requests_kwargs = dict()
    data = urlopen(url, **requests_kwargs)
    try:
        return pd.read_csv(data, **kw)
    except Exception as e:
        print("Couldn't process response into Pandas DataFrame.")
        print(f"{type(e)} occurred. Please see below for the traceback.")
        raise


def to_ncCF(url: str, protocol: str = None, **kw) -> Dataset:
    """Convert a URL to a netCDF4 Dataset."""
    if protocol == "griddap":
        raise ValueError(
            f"Cannot use ncCF with griddap. The URL you tried to access is: '{url}'.",
        )
    auth = kw.pop("auth", None)
    return _nc_dataset(url, auth=auth, **kw)


def to_xarray(url: str, response="opendap", **kw) -> xr.Dataset:
    """Convert a URL to an xarray dataset."""
    auth = kw.pop("auth", None)
    if response == "opendap":
        return xr.open_dataset(url, **kw)
    else:
        nc = _nc_dataset(url, auth=auth, **kw)
        return xr.open_dataset(xr.backends.NetCDF4DataStore(nc), **kw)


def to_iris(url: str, **kw):
    """Convert a URL to an iris CubeList."""
    data = urlopen(url, **kw)
    with _tempnc(data) as tmp:
        cubes = iris.load_raw(tmp, **kw)
        _ = [cube.data for cube in cubes]
        return cubes
