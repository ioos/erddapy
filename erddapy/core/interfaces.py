"""Interface between URL responses and third-party libraries.

This module takes an URL or the bytes response of a request and converts it
to Pandas, XArray, Iris, etc. objects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from erddapy.core.netcdf import _nc_dataset, _tempnc
from erddapy.core.url import urlopen

if TYPE_CHECKING:
    import iris.cube
    import netCDF4
    import xarray as xr


OptionalStr = str | None
OptionalDict = dict | None


def to_pandas(
    url: str,
    requests_kwargs: dict | None = None,
    pandas_kwargs: dict | None = None,
) -> pd.DataFrame:
    """Convert a URL to Pandas DataFrame.

    url: URL to request data from.
    requests_kwargs: arguments to be passed to urlopen method.
    **pandas_kwargs: kwargs to be passed to third-party library (pandas).
    """
    data = urlopen(url, requests_kwargs or {})
    try:
        return pd.read_csv(data, **(pandas_kwargs or {}))
    except Exception as e:
        msg = f"Could not read url {url} with Pandas.read_csv."
        raise ValueError(msg) from e


def to_ncCF(  # noqa: N802
    url: str,
    protocol: OptionalStr = None,
    requests_kwargs: OptionalDict = None,
) -> netCDF4.Dataset:
    """Convert a URL to a netCDF4 Dataset.

    url: URL to request data from.
    protocol: 'griddap' or 'tabledap'.
    requests_kwargs: arguments to be passed to urlopen method (including auth).

    """
    msg = (
        f"Cannot use .ncCF with griddap protocol."
        f"The URL you tried to access is: '{url}'."
    )
    if protocol == "griddap":
        raise ValueError(msg)
    return _nc_dataset(url, requests_kwargs)


def to_xarray(
    url: str,
    response: OptionalStr = "opendap",
    requests_kwargs: dict | None = None,
    xarray_kwargs: dict | None = None,
) -> xr.Dataset:
    """Convert a URL to an xarray dataset.

    url: URL to request data from.
    response: type of response to be requested from the server.
    requests_kwargs: arguments to be passed to urlopen method.
    xarray_kwargs: kwargs to be passed to third-party library (xarray).
    """
    import xarray as xr

    if response == "opendap":
        return xr.open_dataset(url, **(xarray_kwargs or {}))

    nc = _nc_dataset(url, requests_kwargs)
    return xr.open_dataset(
        xr.backends.NetCDF4DataStore(nc),
        **(xarray_kwargs or {}),
    )


def to_iris(
    url: str,
    requests_kwargs: dict | None = None,
    iris_kwargs: dict | None = None,
) -> iris.cube.CubeList:
    """Convert a URL to an iris CubeList.

    url: URL to request data from.
    requests_kwargs: arguments to be passed to urlopen method.
    iris_kwargs: kwargs to be passed to third-party library (iris).
    """
    import iris

    data = urlopen(url, **(requests_kwargs or {}))
    with _tempnc(data) as tmp:
        cubes = iris.load_raw(tmp, **(iris_kwargs or {}))
        _ = [cube.data for cube in cubes]
        return cubes
