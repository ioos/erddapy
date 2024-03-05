"""Servers."""

import functools
import io
from typing import NamedTuple

import httpx
import pandas as pd


class Server(NamedTuple):
    """Container for the server short description and URL."""

    description: str
    url: str


@functools.lru_cache(maxsize=128)
def servers_list() -> dict:
    """Download a new server list from awesome-erddap.

    First we try to load the latest list from GitHub.
    If that fails we fall back to the default one shipped with the package.

    """
    try:
        url = "https://raw.githubusercontent.com/IrishMarineInstitute/awesome-erddap/master/erddaps.json"
        r = httpx.get(url, timeout=10)
        df_servers = pd.read_json(io.StringIO(r.text))
    except httpx.HTTPError:
        from pathlib import Path

        path = Path(__file__).absolute().parent
        df_servers = pd.read_json(path.joinpath("erddaps.json"))
    # Drop non-public servers.
    df_servers = df_servers[df_servers["public"]]
    return {
        row["short_name"].lower(): Server(row["name"], row["url"])
        for k, row in df_servers.iterrows()
        if row["short_name"]
    }


servers = servers_list()
