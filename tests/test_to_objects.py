"""Test converting to other data models objects."""

import sys

import iris
import pytest
import xarray as xr

from erddapy import ERDDAP


@pytest.fixture
@pytest.mark.web
def sensors():
    """Instantiate ERDDAP class for testing."""
    yield ERDDAP(
        server="https://erddap.sensors.ioos.us/erddap/",
        response="htmlTable",
    )


@pytest.fixture
@pytest.mark.web
def neracoos():
    """Instantiate ERDDAP class for testing."""
    yield ERDDAP(
        server="http://www.neracoos.org/erddap/",
        response="htmlTable",
    )


@pytest.fixture
def dataset_griddap(neracoos):
    """Load griddap data for testing."""
    neracoos.dataset_id = "WW3_EastCoast_latest"
    neracoos.protocol = "griddap"
    neracoos.griddap_initialize()
    yield neracoos


@pytest.fixture
def dataset_opendap(neracoos):
    """Load griddap data with OPeNDAP response for testing."""
    neracoos.dataset_id = "WW3_EastCoast_latest"
    neracoos.protocol = "griddap"
    neracoos.response = "opendap"
    yield neracoos


@pytest.fixture
def dataset_tabledap(sensors):
    """Load tabledap for testing."""
    sensors.dataset_id = "osmc_23091"
    sensors.protocol = "tabledap"
    sensors.variables = ["sea_water_temperature", "time"]
    sensors.constraints = {
        "time>=": "2018-10-01T00:00:00Z",
        "time<=": "2018-10-01T21:00:00Z",
        "latitude>=": 10,
        "latitude<=": 20,
        "longitude>=": 80,
        "longitude<=": 90,
    }
    yield sensors


@pytest.mark.web
@pytest.mark.vcr()
def test_to_pandas(dataset_tabledap):
    """Test converting tabledap to a pandas DataFrame."""
    import pandas as pd

    df = dataset_tabledap.to_pandas(index_col="time (UTC)", parse_dates=True).dropna()

    assert isinstance(df, pd.DataFrame)
    assert df.index.name == "time (UTC)"
    assert len(df.columns) == 1
    assert df.columns[0] == "sea_water_temperature (degree_Celsius)"


@pytest.mark.web
def test_to_xarray_tabledap(dataset_tabledap):
    """Test converting tabledap to an xarray Dataset."""
    ds = dataset_tabledap.to_xarray()

    assert isinstance(ds, xr.Dataset)
    assert len(ds.variables) == 6
    assert ds["time"].name == "time"
    assert ds["sea_water_temperature"].name == "sea_water_temperature"


@pytest.mark.web
def test_to_xarray_griddap(dataset_griddap):
    """Test converting griddap to an xarray Dataset."""
    ds = dataset_griddap.to_xarray()
    assert isinstance(ds, xr.Dataset)


@pytest.mark.web
def test_to_xarray_opendap(dataset_opendap):
    """Test converting griddap to an xarray Dataset, use lazy loading for OPeNDAP response."""
    ds = dataset_opendap.to_xarray()
    assert isinstance(ds, xr.Dataset)


@pytest.mark.web
@pytest.mark.skipif(
    (sys.platform == "win32" or sys.platform == "darwin"),
    reason="run this test only once until we figure out a better way to mock it.",
)
def test_to_iris_tabledap(dataset_tabledap):
    """Test converting tabledap to an iris cube."""
    cubes = dataset_tabledap.to_iris()

    assert isinstance(cubes, iris.cube.CubeList)
    assert isinstance(cubes.extract_cube("23091 - Moored Buoy"), iris.cube.Cube)
    assert isinstance(cubes.extract_cube("sea_water_temperature"), iris.cube.Cube)


@pytest.mark.web
@pytest.mark.skipif(
    (sys.platform == "win32" or sys.platform == "darwin"),
    reason="run this test only once until we figure out a better way to mock it.",
)
def test_to_iris_griddap(dataset_griddap):
    """Test converting griddap to an iris cube."""
    cubes = dataset_griddap.to_iris()
    assert isinstance(cubes, iris.cube.CubeList)
