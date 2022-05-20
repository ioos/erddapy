"""Tests Servers List."""

import pytest

from erddapy import servers
from erddapy.url_handling import check_url_response


@pytest.mark.web
@pytest.mark.xfail
def test_servers():
    """
    Tests if listed servers are responding.

    We are OK with redirects here b/c we only want to update a URL if they are broken,
    most of the time a redirect is only adding '/index.html'.
    """
    for server in servers.values():
        # Should raise HTTPError if broken, otherwise returns the URL.
        check_url_response(server.url, follow_redirects=True) == server.url
