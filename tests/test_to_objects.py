"""Test converting to other data models objects."""

import sys

import iris
import pytest
import xarray as xr

from erddapy import ERDDAP


@pytest.fixture
@pytest.mark.web
def e():
    """Instantiate ERDDAP class for testing."""
    yield ERDDAP(
        server="https://upwell.pfeg.noaa.gov/erddap",
        response="htmlTable",
    )


@pytest.fixture
def bermuda_1_msl(e):
    """Load bermuda_1_msl for testing."""
    e.dataset_id = "noaa_ngdc_9cfa_244d_0065"
    e.constraints = None
    e.protocol = "griddap"
    yield e


@pytest.fixture
def taodata(e):
    """Load taodata for testing."""
    e.dataset_id = "pmelTao5dayIso"
    e.protocol = "tabledap"
    e.constraints = {
        "time>=": "1977-11-10T12:00:00Z",
        "time<=": "2019-01-26T12:00:00Z",
        "latitude>=": 10,
        "latitude<=": 10,
        "longitude>=": 265,
        "longitude<=": 265,
    }
    e.variables = ["ISO_6", "time"]
    yield e


@pytest.mark.web
@pytest.mark.vcr()
def test_to_pandas(taodata):
    """Test converting tabledap to a pandas DataFrame."""
    import pandas as pd

    df = taodata.to_pandas(index_col="time (UTC)", parse_dates=True).dropna()

    assert isinstance(df, pd.DataFrame)
    assert df.index.name == "time (UTC)"
    assert len(df.columns) == 1
    assert df.columns[0] == "ISO_6 (m)"


@pytest.mark.web
# @pytest.mark.vcr()
def test_to_xarray_tabledap(taodata):
    """Test converting tabledap to an xarray Dataset."""
    ds = taodata.to_xarray()

    assert isinstance(ds, xr.Dataset)
    assert len(ds.variables) == 7
    assert ds["time"].name == "time"
    assert ds["ISO_6"].name == "ISO_6"


@pytest.mark.web
# @pytest.mark.vcr()
def test_to_xarray_griddap(bermuda_1_msl):
    """Test converting griddap to an xarray Dataset."""
    ds = bermuda_1_msl.to_xarray()

    assert isinstance(ds, xr.Dataset)


@pytest.mark.web
# @pytest.mark.vcr()
@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_to_iris_tabledap(taodata):
    """Test converting tabledap to an iris cube."""
    cubes = taodata.to_iris()

    assert isinstance(cubes, iris.cube.CubeList)
    assert isinstance(cubes.extract_cube("depth"), iris.cube.Cube)
    assert isinstance(cubes.extract_cube("20C Isotherm Depth"), iris.cube.Cube)


@pytest.mark.web
# @pytest.mark.vcr()
@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_to_iris_griddap(bermuda_1_msl):
    """Test converting griddap to an iris cube."""
    cubes = bermuda_1_msl.to_iris()
    assert isinstance(cubes, iris.cube.CubeList)
