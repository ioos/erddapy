"""Test converting to other data models objects."""

import sys

import dask
import httpx
import iris
import pytest
import xarray as xr

from erddapy import ERDDAP

# netcdf-c is not thread safe and iris doesn't limit that.
dask.config.set(scheduler="single-threaded")


@pytest.fixture
@pytest.mark.web
def sensors():
    """Instantiate ERDDAP class for testing."""
    return ERDDAP(
        server="https://erddap.sensors.ioos.us/erddap/",
        response="htmlTable",
    )


@pytest.fixture
@pytest.mark.web
def gliders():
    """Instantiate ERDDAP class for testing."""
    # The gliders server has 1244 datasets at time of writing
    return ERDDAP(
        server="https://gliders.ioos.us/erddap/",
        response="htmlTable",
    )


@pytest.fixture
@pytest.mark.web
def neracoos():
    """Instantiate ERDDAP class for testing."""
    return ERDDAP(
        server="http://www.neracoos.org/erddap/",
        response="htmlTable",
    )


@pytest.fixture
def dataset_griddap(neracoos):
    """Load griddap data for testing."""
    neracoos.dataset_id = "WW3_EastCoast_latest"
    neracoos.protocol = "griddap"
    neracoos.griddap_initialize()
    return neracoos


@pytest.fixture
def dataset_opendap(neracoos):
    """Load griddap data with OPeNDAP response for testing."""
    neracoos.dataset_id = "WW3_EastCoast_latest"
    neracoos.protocol = "griddap"
    neracoos.response = "opendap"
    return neracoos


@pytest.fixture
def dataset_tabledap(sensors):
    """Load tabledap for testing."""
    sensors.dataset_id = "amelia_20180501t0000"
    sensors.protocol = "tabledap"
    sensors.variables = ["temperature", "time"]
    sensors.constraints = {
        "time>=": "2018-05-08T00:00:00Z",
        "time<=": "2018-05-13T21:00:00Z",
        "latitude>=": 36,
        "latitude<=": 38,
        "longitude>=": -76,
        "longitude<=": -73,
    }
    return sensors


@pytest.mark.web
def test_csv_search(gliders):
    """Test if a CSV search returns all items (instead of the first 1000)."""
    url = gliders.get_search_url(search_for="all", response="csv")
    handle = httpx.get(url)
    nrows = len(list(handle.iter_lines())) - 1
    expected = 1000
    assert nrows > expected


@pytest.mark.web
def test_json_search(gliders):
    """Test if a JSON search returns all items (instead of the first 1000)."""
    url = gliders.get_search_url(search_for="all", response="json")
    handle = httpx.get(url)
    nrows = len(handle.json()["table"]["rows"])
    expected = 1000
    assert nrows > expected


@pytest.mark.web
@pytest.mark.vcr
def test_to_pandas(dataset_tabledap):
    """Test converting tabledap to a pandas DataFrame."""
    import pandas as pd

    df = dataset_tabledap.to_pandas(
        index_col="time (UTC)",
        parse_dates=True,
    ).dropna()

    assert isinstance(df, pd.DataFrame)
    assert df.index.name == "time (UTC)"
    assert len(df.columns) == 1
    assert df.columns[0] == "temperature (degree_Celsius)"


@pytest.mark.web
@pytest.mark.vcr
def test_to_pandas_requests_kwargs(dataset_tabledap):
    """Test if to_pandas_requests_kwargs are processed as expected."""
    import pandas as pd
    from pandas.api.types import is_datetime64_any_dtype

    df = dataset_tabledap.to_pandas(
        index_col="time (UTC)",
        parse_dates=True,
        requests_kwargs={"timeout": 60},
    )
    assert isinstance(df, pd.DataFrame)
    assert df.index.name == "time (UTC)"
    assert is_datetime64_any_dtype(df.index)


@pytest.mark.web
def test_to_xarray_tabledap(dataset_tabledap):
    """Test converting tabledap to an xarray Dataset."""
    ds = dataset_tabledap.to_xarray()

    assert isinstance(ds, xr.Dataset)
    expected = 9
    assert len(ds.variables) == expected
    assert ds["time"].name == "time"
    assert ds["temperature"].name == "temperature"


@pytest.mark.web
def test_to_xarray_requests_kwargs(dataset_tabledap):
    """Test converting tabledap to an xarray Dataset with manual timeout."""
    ds = dataset_tabledap.to_xarray(requests_kwargs={"timeout": 30})

    assert isinstance(ds, xr.Dataset)
    expected = 9
    assert len(ds.variables) == expected
    assert ds["time"].name == "time"
    assert ds["temperature"].name == "temperature"


@pytest.mark.web
def test_to_xarray_griddap(dataset_griddap):
    """Test converting griddap to an xarray Dataset."""
    ds = dataset_griddap.to_xarray()
    assert isinstance(ds, xr.Dataset)


@pytest.mark.web
def test_to_xarray_opendap(dataset_opendap):
    """Test converting griddap to xarray with the OPeNDAP response."""
    ds = dataset_opendap.to_xarray()
    assert isinstance(ds, xr.Dataset)


@pytest.mark.web
def test_to_xarray_opendap_griddap_initialize(dataset_opendap):
    """Test converting griddap aftert calling griddap_initialize."""
    dataset_opendap.griddap_initialize()
    ds = dataset_opendap.to_xarray()
    assert isinstance(ds, xr.Dataset)


@pytest.mark.web
@pytest.mark.skipif(
    (sys.platform in ("win32", "darwin")),
    reason="run this test until we figure out a way to mock it.",
)
def test_to_iris_tabledap(dataset_tabledap):
    """Test converting tabledap to an iris cube."""
    cubes = dataset_tabledap.to_iris()

    assert isinstance(cubes, iris.cube.CubeList)
    assert isinstance(cubes.extract_cube("Profile ID"), iris.cube.Cube)
    assert isinstance(
        cubes.extract_cube("sea_water_temperature"),
        iris.cube.Cube,
    )


@pytest.mark.web
@pytest.mark.skipif(
    (sys.platform in ("win32", "darwin")),
    reason="run this test until we figure out a way to mock it.",
)
def test_to_iris_griddap(dataset_griddap):
    """Test converting griddap to an iris cube."""
    cubes = dataset_griddap.to_iris()
    assert isinstance(cubes, iris.cube.CubeList)


@pytest.mark.web
def test_download_file(dataset_tabledap):
    """Test file download of tabledap with defined variable and constraints."""
    fn = dataset_tabledap.download_file("nc")
    ds = xr.load_dataset(fn)
    assert ds["time"].name == "time"
    assert ds["temperature"].name == "temperature"
    dataset_tabledap.variables = dataset_tabledap.variables[::-1]
    fn_new = dataset_tabledap.download_file("nc")
    assert fn_new == fn


@pytest.mark.web
def test_download_file_variables_only(dataset_tabledap):
    """Test direct download of tabledap dataset with undefined constraints."""
    dataset_tabledap.constraints = {}
    fn = dataset_tabledap.download_file("nc")
    ds = xr.load_dataset(fn)
    assert ds["time"].name == "time"
    assert ds["temperature"].name == "temperature"
    dataset_tabledap.variables = dataset_tabledap.variables[::-1]
    fn_new = dataset_tabledap.download_file("nc")
    assert fn_new == fn


@pytest.mark.web
def test_download_file_constraints_only(dataset_tabledap):
    """Test direct download of tabledap dataset with undefined variables."""
    dataset_tabledap.variables = []
    fn = dataset_tabledap.download_file("nc")
    ds = xr.load_dataset(fn)
    assert ds["time"].name == "time"
    assert ds["temperature"].name == "temperature"
    dataset_tabledap.variables = dataset_tabledap.variables[::-1]
    fn_new = dataset_tabledap.download_file("nc")
    assert fn_new == fn


@pytest.mark.web
def test_download_file_undefined_query(dataset_tabledap):
    """Test direct download of tabledap dataset with undefined query."""
    dataset_tabledap.variables = []
    dataset_tabledap.constraints = {}
    fn = dataset_tabledap.download_file("nc")
    ds = xr.load_dataset(fn)
    assert ds["time"].name == "time"
    assert ds["temperature"].name == "temperature"
    dataset_tabledap.variables = dataset_tabledap.variables[::-1]
    fn_new = dataset_tabledap.download_file("nc")
    assert fn_new == fn
