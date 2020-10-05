import pytest

from erddapy.servers import servers
from erddapy.utilities import check_url_response


@pytest.mark.web
@pytest.mark.xfail
def test_servers():
    for server in servers.values():
        # Should raise HTTPError if broken, otherwise returns the URL.
        check_url_response(server.url) == server.url
