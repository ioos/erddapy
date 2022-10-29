"""
Interface between URL responses and third-party libraries.

This module takes an URL or the bytes response of a request and converts it to Pandas,
XArray, Iris, etc. objects.
"""

import iris
import pandas as pd
import xarray as xr
from netCDF4 import Dataset as ncDataset

from erddapy.core.netcdf import _nc_dataset, _tempnc
from erddapy.core.url import urlopen


def to_pandas(url: str, **kw) -> pd.DataFrame:
    """Convert a URL to Pandas DataFrame."""
    data = urlopen(url, **kw)
    try:
        return pd.read_csv(data, **kw)
    except Exception:
        print("Couldn't process response into Pandas DataFrame.")
        raise


def to_ncCF(url: str, **kw) -> ncDataset:
    """Convert a URL to a netCDF4 Dataset."""
    return _nc_dataset(url, **kw)


def to_xarray(url: str, response="opendap", **kw) -> xr.Dataset:
    """Convert a URL to an xarray dataset."""
    if response == "opendap":
        return xr.open_dataset(url, **kw)
    else:
        nc = _nc_dataset(url, **kw)
        return xr.open_dataset(xr.backends.NetCDF4DataStore(nc), **kw)


def to_iris(url: str, **kw):
    """Convert a URL to an iris CubeList."""
    data = urlopen(url, **kw)
    with _tempnc(data) as tmp:
        cubes = iris.load_raw(tmp, **kw)
        try:
            cubes.realise_data()
        except ValueError:
            _ = [cube.data for cube in cubes]
        return cubes
