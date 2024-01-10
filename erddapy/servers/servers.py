"""Servers."""

import functools
import io
from collections import namedtuple

import httpx
import pandas as pd


@functools.lru_cache(maxsize=128)
def servers_list():
    """
    Download a new server list from awesome-erddap.

    If loading the latest one fails it falls back to the default one shipped with the package.

    """
    try:
        url = "https://raw.githubusercontent.com/IrishMarineInstitute/awesome-erddap/master/erddaps.json"
        r = httpx.get(url, timeout=10)
        df = pd.read_json(io.StringIO(r.text))
    except httpx.HTTPError:
        from pathlib import Path

        path = Path(__file__).absolute().parent
        df = pd.read_json(path.joinpath("erddaps.json"))
    _server = namedtuple("server", ["description", "url"])
    # drop non-public servers
    df = df[df["public"]]
    return {
        row["short_name"].lower(): _server(row["name"], row["url"])
        for k, row in df.iterrows()
        if row["short_name"]
    }


servers = servers_list()
