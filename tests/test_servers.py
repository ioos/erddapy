"""Tests Servers List."""

import pytest

from erddapy import servers
from erddapy.url_handling import check_url_response


@pytest.mark.web
@pytest.mark.xfail
def test_servers():
    """Tests if listed servers are responding."""
    for server in servers.values():
        # Should raise HTTPError if broken, otherwise returns the URL.
        check_url_response(server.url) == server.url
