import pytest

from requests.exceptions import ReadTimeout

from erddapy import ERDDAP


@pytest.mark.web
@pytest.mark.vcr()
def test_erddap_requests_kwargs():
    """Test that an ERDDAP instance can have requests_kwargs attribute assigned
    and are passed to the underlying methods"""

    base_url = "http://www.neracoos.org/erddap"
    timeout_seconds = 1  # request timeout in seconds
    slowwly_milliseconds = (timeout_seconds + 1) * 1000
    slowwly_url = (
        f"https://flash.siwalik.in/delay/{slowwly_milliseconds}/url/{base_url}"
    )

    connection = ERDDAP(slowwly_url)
    connection.dataset_id = "M01_sbe37_all"
    connection.protocol = "tabledap"

    connection.requests_kwargs["timeout"] = timeout_seconds

    with pytest.raises(ReadTimeout):
        connection.to_xarray()
