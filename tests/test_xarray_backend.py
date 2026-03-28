import pytest
from erddapy.xarray import open_erddap_dataset


def test_invalid_url():
    """Test that invalid URL raises ValueError."""
    with pytest.raises(ValueError):
        open_erddap_dataset("invalid_url")


def test_non_erddap_url():
    """Test that non-ERDDAP URL raises ValueError."""
    with pytest.raises(ValueError):
        open_erddap_dataset("https://google.com/data.nc")


def test_valid_structure_detection():
    """Test detection logic without actual network call."""
    url = "https://example.com/erddap/griddap/data.nc"

    try:
        open_erddap_dataset(url)
    except Exception as e:
        # Expected: network or parsing error, but NOT ValueError
        assert not isinstance(e, ValueError)