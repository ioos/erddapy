"""Test xarray backend engine integration."""

import pytest
import xarray as xr

from erddapy import ERDDAP
from erddapy.xarray_erddap import (
    ERDDAPyBackendEntrypoint,
    _is_netcdf,
    _is_url,
    _make_opendap,
    open_erddap_dataset,
)


class TestURLValidation:
    """Test URL validation helper functions."""
    
    def test__is_url_valid_http(self):
        """Test valid HTTP ERDDAP URL."""
        url = "http://erddap.server.com/erddap/tabledap/dataset.nc"
        assert _is_url(url) is True
    
    def test__is_url_valid_https(self):
        """Test valid HTTPS ERDDAP URL."""
        url = "https://gliders.ioos.us/erddap/tabledap/dataset.nc"
        assert _is_url(url) is True
    
    def test__is_url_invalid_no_erddap(self):
        """Test URL without /erddap/ path."""
        url = "https://example.com/data/dataset.nc"
        assert _is_url(url) is False
    
    def test__is_url_invalid_not_http(self):
        """Test non-HTTP URL."""
        url = "ftp://erddap.server.com/erddap/tabledap/dataset.nc"
        assert _is_url(url) is False
    
    def test__is_url_invalid_non_string(self):
        """Test non-string input."""
        assert _is_url(123) is False
        assert _is_url(None) is False
        assert _is_url([]) is False


class TestNetCDFDetection:
    """Test NetCDF format detection."""
    
    def test__is_netcdf_nc_extension(self):
        """Test .nc extension detection."""
        assert _is_netcdf("https://server/erddap/tabledap/data.nc") is True
    
    def test__is_netcdf_nc_with_query(self):
        """Test .nc with query parameters."""
        assert _is_netcdf("https://server/erddap/tabledap/data.nc?var1,var2") is True
    
    def test__is_netcdf_nccf_extension(self):
        """Test .ncCF extension detection."""
        assert _is_netcdf("https://server/erddap/tabledap/data.ncCF") is True
    
    def test__is_netcdf_nccf_with_query(self):
        """Test .ncCF with query parameters."""
        assert _is_netcdf("https://server/erddap/tabledap/data.ncCF?var1") is True
    
    def test__is_netcdf_nccfma_extension(self):
        """Test .ncCFMA extension detection."""
        assert _is_netcdf("https://server/erddap/tabledap/data.ncCFMA") is True
    
    def test__is_netcdf_nccfma_with_query(self):
        """Test .ncCFMA with query parameters."""
        assert _is_netcdf("https://server/erddap/tabledap/data.ncCFMA?var1") is True
    
    def test__is_netcdf_invalid_extension(self):
        """Test non-NetCDF extension."""
        assert _is_netcdf("https://server/erddap/tabledap/data.csv") is False
        assert _is_netcdf("https://server/erddap/tabledap/data.json") is False
    
    def test__is_netcdf_opendap_url(self):
        """Test OPeNDAP URL (no .nc extension)."""
        assert _is_netcdf("https://server/erddap/griddap/dataset") is False


class TestOPeNDAPConversion:
    """Test OPeNDAP URL conversion."""
    
    def test__make_opendap_simple_nc(self):
        """Test conversion of simple .nc URL."""
        url = "https://server/erddap/tabledap/dataset.nc"
        opendap_url = _make_opendap(url)
        assert opendap_url == "https://server/erddap/tabledap/dataset"
        assert ".nc" not in opendap_url
    
    def test__make_opendap_with_query(self):
        """Test conversion with query parameters."""
        url = "https://server/erddap/tabledap/dataset.nc?var1,var2"
        opendap_url = _make_opendap(url)
        assert opendap_url == "https://server/erddap/tabledap/dataset"
        assert "?" not in opendap_url
    
    def test__make_opendap_nccf(self):
        """Test conversion of .ncCF URL."""
        url = "https://server/erddap/tabledap/dataset.ncCF?var1"
        opendap_url = _make_opendap(url)
        assert opendap_url == "https://server/erddap/tabledap/dataset"
    
    def test__make_opendap_preserves_path(self):
        """Test that conversion preserves the full path."""
        url = "https://server/erddap/griddap/complex/path/dataset.nc"
        opendap_url = _make_opendap(url)
        assert "complex/path" in opendap_url
        assert opendap_url.startswith("https://server/erddap/griddap/")


class TestBackendEntrypoint:
    """Test xarray backend entrypoint class."""
    
    def test_backend_entrypoint_exists(self):
        """Test that backend entrypoint can be instantiated."""
        backend = ERDDAPyBackendEntrypoint()
        assert isinstance(backend, xr.backends.BackendEntrypoint)
    
    def test_backend_has_description(self):
        """Test backend has description attribute."""
        backend = ERDDAPyBackendEntrypoint()
        assert hasattr(backend, "description")
        assert isinstance(backend.description, str)
        assert "ERDDAP" in backend.description
    
    def test_backend_has_open_dataset_method(self):
        """Test backend has open_dataset method."""
        backend = ERDDAPyBackendEntrypoint()
        assert hasattr(backend, "open_dataset")
        assert callable(backend.open_dataset)
    
    def test_backend_open_dataset_parameters(self):
        """Test backend has correct parameters."""
        backend = ERDDAPyBackendEntrypoint()
        assert hasattr(backend, "open_dataset_parameters")
        assert "filename_or_obj" in backend.open_dataset_parameters
        assert "drop_variables" in backend.open_dataset_parameters


class TestOpenERDDAPDataset:
    """Test opening ERDDAP datasets."""
    
    def test_open_erddap_dataset_invalid_url_type(self):
        """Test error with non-URL input."""
        with pytest.raises(ValueError, match="Expected an ERDDAP URL"):
            open_erddap_dataset("not_a_url.nc")
    
    def test_open_erddap_dataset_invalid_url_no_erddap(self):
        """Test error with URL missing /erddap/."""
        with pytest.raises(ValueError, match="Expected an ERDDAP URL"):
            open_erddap_dataset("https://example.com/data/dataset.nc")
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_open_erddap_dataset_netcdf_tabledap(self):
        """Test opening TableDAP .nc URL."""
        url = (
            "https://gliders.ioos.us/erddap/tabledap/"
            "amelia-20180501T0000.nc?temperature,time"
            "&time>=2018-05-08T00:00:00Z&time<=2018-05-13T21:00:00Z"
        )
        ds = open_erddap_dataset(url)
        assert isinstance(ds, xr.Dataset)
        assert "temperature" in ds.variables
        assert "time" in ds.variables
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_open_erddap_dataset_netcdf_griddap(self):
        """Test opening GridDAP .nc URL."""
        url = (
            "https://www.neracoos.org/erddap/griddap/"
            "WW3_EastCoast_latest.nc"
        )
        ds = open_erddap_dataset(url)
        assert isinstance(ds, xr.Dataset)
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_open_erddap_dataset_nccf(self):
        """Test opening .ncCF URL."""
        url = (
            "https://gliders.ioos.us/erddap/tabledap/"
            "amelia-20180501T0000.ncCF?temperature,time"
            "&time>=2018-05-08T00:00:00Z&time<=2018-05-13T21:00:00Z"
        )
        ds = open_erddap_dataset(url)
        assert isinstance(ds, xr.Dataset)


class TestXarrayEngineIntegration:
    """Test xarray engine integration via xr.open_dataset."""
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_xarray_open_dataset_with_engine(self):
        """Test opening ERDDAP URL with engine='erddap'."""
        url = (
            "https://gliders.ioos.us/erddap/tabledap/"
            "amelia-20180501T0000.nc?temperature,time"
            "&time>=2018-05-08T00:00:00Z&time<=2018-05-13T21:00:00Z"
        )
        ds = xr.open_dataset(url, engine="erddap")
        assert isinstance(ds, xr.Dataset)
        assert "temperature" in ds.variables
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_xarray_engine_griddap(self):
        """Test engine with GridDAP dataset."""
        url = "https://www.neracoos.org/erddap/griddap/WW3_EastCoast_latest.nc"
        ds = xr.open_dataset(url, engine="erddap")
        assert isinstance(ds, xr.Dataset)
        assert len(ds.data_vars) > 0
    
    @pytest.mark.web
    def test_xarray_engine_vs_default(self):
        """Test that engine works where default xarray might not."""
        # This tests the value of the plugin: handling non-OPeNDAP URLs
        url = (
            "https://gliders.ioos.us/erddap/tabledap/"
            "amelia-20180501T0000.nc?temperature"
            "&time>=2018-05-08T00:00:00Z&time<=2018-05-08T01:00:00Z"
        )
        # With engine='erddap' should work
        ds = xr.open_dataset(url, engine="erddap")
        assert isinstance(ds, xr.Dataset)
        
        # Default xarray may fail (uncomment to test this behavior)
        # with pytest.raises(Exception):
        #     xr.open_dataset(url)


class TestXarrayEngineWithERDDAP:
    """Test xarray engine with ERDDAP class."""
    
    @pytest.fixture
    def erddap_tabledap(self):
        """ERDDAP instance for TableDAP testing."""
        return ERDDAP(
            server="https://gliders.ioos.us/erddap/",
            protocol="tabledap",
            response="nc",
        )
    
    @pytest.fixture
    def erddap_griddap(self):
        """ERDDAP instance for GridDAP testing."""
        return ERDDAP(
            server="https://www.neracoos.org/erddap/",
            protocol="griddap",
            response="nc",
        )
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_engine_with_erddap_tabledap(self, erddap_tabledap):
        """Test engine with ERDDAP-generated TableDAP URL."""
        erddap_tabledap.dataset_id = "amelia-20180501T0000"
        erddap_tabledap.variables = ["temperature", "time"]
        erddap_tabledap.constraints = {
            "time>=": "2018-05-08T00:00:00Z",
            "time<=": "2018-05-13T21:00:00Z",
        }
        url = erddap_tabledap.get_download_url()
        
        ds = xr.open_dataset(url, engine="erddap")
        assert isinstance(ds, xr.Dataset)
        assert "temperature" in ds.variables
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_engine_with_erddap_griddap(self, erddap_griddap):
        """Test engine with ERDDAP-generated GridDAP URL."""
        erddap_griddap.dataset_id = "WW3_EastCoast_latest"
        erddap_griddap.griddap_initialize()
        url = erddap_griddap.get_download_url()
        
        ds = xr.open_dataset(url, engine="erddap")
        assert isinstance(ds, xr.Dataset)


class TestXarrayEngineAttributes:
    """Test attribute and coordinate handling."""
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_attributes_preserved(self):
        """Test that variable attributes are preserved."""
        url = (
            "https://gliders.ioos.us/erddap/tabledap/"
            "amelia-20180501T0000.nc?temperature"
            "&time>=2018-05-08T00:00:00Z&time<=2018-05-08T01:00:00Z"
        )
        ds = xr.open_dataset(url, engine="erddap")
        
        # Check variable has attributes
        assert hasattr(ds["temperature"], "attrs")
        assert len(ds["temperature"].attrs) > 0
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_coordinates_detected(self):
        """Test that coordinates are properly detected."""
        url = (
            "https://gliders.ioos.us/erddap/tabledap/"
            "amelia-20180501T0000.nc?temperature,time"
            "&time>=2018-05-08T00:00:00Z&time<=2018-05-08T01:00:00Z"
        )
        ds = xr.open_dataset(url, engine="erddap")
        
        # Check that time is in coordinates
        assert "time" in ds.coords or "time" in ds.dims


class TestXarrayEngineOperations:
    """Test xarray operations on datasets opened with engine."""
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_selection_operations(self):
        """Test that xarray selection works."""
        url = (
            "https://gliders.ioos.us/erddap/tabledap/"
            "amelia-20180501T0000.nc?temperature,time"
            "&time>=2018-05-08T00:00:00Z&time<=2018-05-13T21:00:00Z"
        )
        ds = xr.open_dataset(url, engine="erddap")
        
        # Test selection works
        if len(ds.time) > 0:
            subset = ds.isel(time=0)
            assert isinstance(subset, xr.Dataset)
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_arithmetic_operations(self):
        """Test arithmetic operations on opened data."""
        url = (
            "https://gliders.ioos.us/erddap/tabledap/"
            "amelia-20180501T0000.nc?temperature"
            "&time>=2018-05-08T00:00:00Z&time<=2018-05-08T01:00:00Z"
        )
        ds = xr.open_dataset(url, engine="erddap")
        
        # Test arithmetic works
        temp = ds["temperature"]
        temp_kelvin = temp + 273.15
        assert isinstance(temp_kelvin, xr.DataArray)
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_to_dataframe(self):
        """Test conversion to pandas DataFrame."""
        import pandas as pd  # noqa: PLC0415
        
        url = (
            "https://gliders.ioos.us/erddap/tabledap/"
            "amelia-20180501T0000.nc?temperature,time"
            "&time>=2018-05-08T00:00:00Z&time<=2018-05-08T01:00:00Z"
        )
        ds = xr.open_dataset(url, engine="erddap")
        df = ds.to_dataframe()
        
        assert isinstance(df, pd.DataFrame)


class TestXarrayEngineResponseFormats:
    """Test different response formats."""
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_nc_response(self):
        """Test with .nc response."""
        url = (
            "https://gliders.ioos.us/erddap/tabledap/"
            "amelia-20180501T0000.nc?temperature"
            "&time>=2018-05-08T00:00:00Z&time<=2018-05-08T01:00:00Z"
        )
        ds = xr.open_dataset(url, engine="erddap")
        assert isinstance(ds, xr.Dataset)
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_nccf_response(self):
        """Test with .ncCF response."""
        url = (
            "https://gliders.ioos.us/erddap/tabledap/"
            "amelia-20180501T0000.ncCF?temperature"
            "&time>=2018-05-08T00:00:00Z&time<=2018-05-08T01:00:00Z"
        )
        ds = xr.open_dataset(url, engine="erddap")
        assert isinstance(ds, xr.Dataset)


class TestXarrayEngineErrorHandling:
    """Test error handling."""
    
    def test_invalid_url_format(self):
        """Test error with invalid URL format."""
        with pytest.raises(ValueError, match="Expected an ERDDAP URL"):
            xr.open_dataset("not_a_valid_url", engine="erddap")
    
    @pytest.mark.web
    def test_nonexistent_dataset(self):
        """Test error with non-existent dataset."""
        url = (
            "https://gliders.ioos.us/erddap/tabledap/"
            "nonexistent_dataset_12345.nc"
        )
        with pytest.raises(Exception):
            xr.open_dataset(url, engine="erddap")
    
    @pytest.mark.web
    def test_malformed_erddap_url(self):
        """Test error with malformed ERDDAP URL."""
        url = "https://gliders.ioos.us/erddap/tabledap/"  # No dataset
        with pytest.raises(Exception):
            xr.open_dataset(url, engine="erddap")


class TestEdgeCases:
    """Test edge cases and special scenarios."""
    
    def test_url_with_multiple_nc_extensions(self):
        """Test URL with .nc appearing multiple times."""
        url = "https://server/erddap/nc_data/dataset.nc.nc?vars"
        opendap = _make_opendap(url)
        # Should remove only the last .nc
        assert opendap.count(".nc") < url.count(".nc")
    
    def test_is_netcdf_edge_cases(self):
        """Test edge cases in NetCDF detection."""
        # Just "nc" in path but not as extension
        assert _is_netcdf("https://server/erddap/nc/dataset.csv") is False
        
        # .nc in query parameter
        assert _is_netcdf("https://server/data?file=data.nc") is False
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_empty_dataset(self):
        """Test handling of dataset with no matching data."""
        # URL with constraints that return no data
        url = (
            "https://gliders.ioos.us/erddap/tabledap/"
            "amelia-20180501T0000.nc?temperature"
            "&time>=2099-01-01T00:00:00Z&time<=2099-01-01T01:00:00Z"
        )
        # Should open successfully but may have empty arrays
        ds = xr.open_dataset(url, engine="erddap")
        assert isinstance(ds, xr.Dataset)
