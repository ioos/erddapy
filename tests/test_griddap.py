"""Test GridDAP functionality."""

import pytest

from erddapy import ERDDAP
from erddapy.core.griddap import (
    _griddap_check_constraints,
    _griddap_check_variables,
    _griddap_get_constraints,
)


@pytest.fixture
def griddap_erddap():
    """ERDDAP instance for GridDAP testing."""
    return ERDDAP(
        server="https://www.neracoos.org/erddap/",
        protocol="griddap",
    )


class TestGridDAPConstraints:
    """Test GridDAP constraint fetching and validation."""
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test__griddap_get_constraints(self):
        """Test fetching GridDAP metadata and constraints."""
        dataset_url = (
            "https://www.neracoos.org/erddap/griddap/WW3_EastCoast_latest"
        )
        constraints, dim_names, var_names = _griddap_get_constraints(
            dataset_url,
            step=1,
        )
        
        # Check that we got constraints
        assert isinstance(constraints, dict)
        assert len(constraints) > 0
        
        # Check dimension names
        assert isinstance(dim_names, list)
        assert len(dim_names) > 0
        
        # Check variable names
        assert isinstance(var_names, list)
        assert len(var_names) > 0
        
        # Check constraint format
        for key in constraints:
            assert ">=" in key or "<=" in key or "_step" in key
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test__griddap_get_constraints_with_step(self):
        """Test constraint fetching with custom step size."""
        dataset_url = (
            "https://www.neracoos.org/erddap/griddap/WW3_EastCoast_latest"
        )
        constraints_step1, _, _ = _griddap_get_constraints(dataset_url, step=1)
        constraints_step5, _, _ = _griddap_get_constraints(dataset_url, step=5)
        
        # Step constraints should be different
        step_keys = [k for k in constraints_step1 if "_step" in k]
        for key in step_keys:
            assert constraints_step1[key] == 1
            assert constraints_step5[key] == 5


class TestGridDAPConstraintValidation:
    """Test constraint validation."""
    
    def test__griddap_check_constraints_valid(self):
        """Test validation with matching constraints."""
        original = {
            "time>=": "2012-01-01",
            "time<=": "2021-01-01",
            "latitude>=": 21.7,
            "latitude<=": 46.49442,
            "time_step": 1,
        }
        user = {
            "time>=": "2013-01-01",  # Changed value is OK
            "time<=": "2020-01-01",
            "latitude>=": 25.0,
            "latitude<=": 45.0,
            "time_step": 1,
        }
        # Should not raise
        _griddap_check_constraints(user, original)
    
    def test__griddap_check_constraints_missing_key(self):
        """Test validation with missing constraint key."""
        original = {
            "time>=": "2012-01-01",
            "time<=": "2021-01-01",
            "latitude>=": 21.7,
        }
        user = {
            "time>=": "2013-01-01",
            "time<=": "2020-01-01",
            # Missing latitude>=
        }
        with pytest.raises(ValueError, match="Re-run e.griddap_initialize"):
            _griddap_check_constraints(user, original)
    
    def test__griddap_check_constraints_extra_key(self):
        """Test validation with extra constraint key."""
        original = {
            "time>=": "2012-01-01",
            "time<=": "2021-01-01",
        }
        user = {
            "time>=": "2013-01-01",
            "time<=": "2020-01-01",
            "latitude>=": 25.0,  # Extra key
        }
        with pytest.raises(ValueError, match="Re-run e.griddap_initialize"):
            _griddap_check_constraints(user, original)


class TestGridDAPVariableValidation:
    """Test variable validation."""
    
    def test__griddap_check_variables_valid(self):
        """Test validation with valid variables."""
        original = ["temperature", "salinity", "depth"]
        user = ["temperature", "salinity"]  # Subset is OK
        # Should not raise
        _griddap_check_variables(user, original)
    
    def test__griddap_check_variables_single(self):
        """Test validation with single variable."""
        original = ["temperature", "salinity"]
        user = ["temperature"]
        # Should not raise
        _griddap_check_variables(user, original)
    
    def test__griddap_check_variables_invalid(self):
        """Test validation with invalid variable."""
        original = ["temperature", "salinity"]
        user = ["temperature", "pressure"]  # pressure not in original
        with pytest.raises(ValueError, match="Re-run e.griddap_initialize"):
            _griddap_check_variables(user, original)
    
    def test__griddap_check_variables_all_invalid(self):
        """Test validation with all invalid variables."""
        original = ["temperature", "salinity"]
        user = ["pressure", "wind_speed"]
        with pytest.raises(ValueError, match="Re-run e.griddap_initialize"):
            _griddap_check_variables(user, original)


class TestGridDAPInitialization:
    """Test ERDDAP griddap_initialize method."""
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_griddap_initialize(self, griddap_erddap):
        """Test GridDAP initialization."""
        griddap_erddap.dataset_id = "WW3_EastCoast_latest"
        griddap_erddap.griddap_initialize()
        
        # Check that constraints were set
        assert griddap_erddap.constraints is not None
        assert isinstance(griddap_erddap.constraints, dict)
        assert len(griddap_erddap.constraints) > 0
        
        # Check dimension names
        assert griddap_erddap.dim_names is not None
        assert isinstance(griddap_erddap.dim_names, list)
        
        # Check variables
        assert griddap_erddap.variables is not None
        assert isinstance(griddap_erddap.variables, list)
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_griddap_initialize_with_step(self, griddap_erddap):
        """Test GridDAP initialization with custom step."""
        griddap_erddap.dataset_id = "WW3_EastCoast_latest"
        griddap_erddap.griddap_initialize(step=5)
        
        # Check that step was applied
        step_keys = [k for k in griddap_erddap.constraints if "_step" in k]
        for key in step_keys:
            assert griddap_erddap.constraints[key] == 5
    
    def test_griddap_initialize_without_dataset_id(self, griddap_erddap):
        """Test initialization fails without dataset_id."""
        with pytest.raises(ValueError, match="valid dataset_id"):
            griddap_erddap.griddap_initialize()
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_griddap_initialize_opendap_skipped(self):
        """Test that OPeNDAP response skips initialization."""
        e = ERDDAP(
            server="https://www.neracoos.org/erddap/",
            protocol="griddap",
            response="opendap",
        )
        e.dataset_id = "WW3_EastCoast_latest"
        e.griddap_initialize()
        
        # OPeNDAP should skip initialization
        assert e.constraints is None


class TestGridDAPDownload:
    """Test GridDAP download URL generation."""
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_griddap_download_url(self, griddap_erddap):
        """Test generating GridDAP download URL."""
        griddap_erddap.dataset_id = "WW3_EastCoast_latest"
        griddap_erddap.griddap_initialize()
        
        url = griddap_erddap.get_download_url()
        assert "griddap" in url
        assert "WW3_EastCoast_latest" in url
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_griddap_download_url_with_subset(self, griddap_erddap):
        """Test GridDAP download with spatial/temporal subset."""
        griddap_erddap.dataset_id = "WW3_EastCoast_latest"
        griddap_erddap.griddap_initialize()
        
        # Modify constraints to subset data
        if "time>=" in griddap_erddap.constraints:
            # Keep only first half of time range
            original_constraints = griddap_erddap._constraints_original.copy()
            griddap_erddap.constraints["time>="] = (
                original_constraints["time>="]
            )
        
        url = griddap_erddap.get_download_url()
        assert "griddap" in url
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_griddap_download_url_select_variables(self, griddap_erddap):
        """Test GridDAP download with variable selection."""
        griddap_erddap.dataset_id = "WW3_EastCoast_latest"
        griddap_erddap.griddap_initialize()
        
        # Select subset of variables
        if griddap_erddap.variables and len(griddap_erddap.variables) > 1:
            griddap_erddap.variables = [griddap_erddap.variables[0]]
        
        url = griddap_erddap.get_download_url()
        assert "griddap" in url


class TestGridDAPIntegration:
    """Test GridDAP integration with data conversion."""
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_griddap_to_xarray(self, griddap_erddap):
        """Test converting GridDAP to xarray."""
        import xarray as xr  # noqa: PLC0415
        
        griddap_erddap.dataset_id = "WW3_EastCoast_latest"
        griddap_erddap.griddap_initialize()
        
        ds = griddap_erddap.to_xarray()
        assert isinstance(ds, xr.Dataset)


class TestGridDAPEdgeCases:
    """Test GridDAP edge cases."""
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_griddap_constraint_modification(self, griddap_erddap):
        """Test modifying constraints after initialization."""
        griddap_erddap.dataset_id = "WW3_EastCoast_latest"
        griddap_erddap.griddap_initialize()
        
        original = griddap_erddap.constraints.copy()
        
        # Modify a constraint value (should be OK)
        if "time>=" in griddap_erddap.constraints:
            griddap_erddap.constraints["time>="] = "2020-01-01"
        
        # Should still be able to generate URL
        url = griddap_erddap.get_download_url()
        assert url is not None
    
    @pytest.mark.web
    @pytest.mark.vcr
    def test_griddap_variable_modification(self, griddap_erddap):
        """Test modifying variables after initialization."""
        griddap_erddap.dataset_id = "WW3_EastCoast_latest"
        griddap_erddap.griddap_initialize()
        
        original_vars = griddap_erddap.variables.copy()
        
        # Select subset (should be OK)
        if len(griddap_erddap.variables) > 1:
            griddap_erddap.variables = [griddap_erddap.variables[0]]
        
        # Should still work
        url = griddap_erddap.get_download_url()
        assert url is not None
