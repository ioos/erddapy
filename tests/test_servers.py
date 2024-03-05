"""Tests Servers List."""

import pytest

from erddapy import servers
from erddapy.core.url import check_url_response


@pytest.mark.web()
@pytest.mark.xfail()
def test_servers():
    """Tests if listed servers are responding.

    Redirects are fine here. We only want to update URLs if they are broken,
    most of the time a redirect is only adding '/index.html'.
    """
    for server in servers.values():
        # Should raise HTTPError if broken, otherwise returns the URL.
        assert (
            check_url_response(server.url, follow_redirects=True) == server.url
        )
