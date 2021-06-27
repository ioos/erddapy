"""
servers

"""

import functools
from collections import namedtuple

import pandas as pd


@functools.lru_cache(maxsize=None)
def servers_list():
    """
    Download a new server list from awesome-erddap.
    If loading the latest one fails it falls back to the default one shipped with the package.

    """
    from urllib.error import URLError

    try:
        url = "https://raw.githubusercontent.com/IrishMarineInstitute/awesome-erddap/master/erddaps.json"
        df = pd.read_json(url)
    except URLError:
        from pathlib import Path

        path = Path(__file__).absolute().parent
        df = pd.read_json(path.joinpath("erddaps.json"))
    _server = namedtuple("server", ["description", "url"])
    return {
        row["short_name"]: _server(row["name"], row["url"])
        for k, row in df.iterrows()
        if row["short_name"]
    }


servers_upper_case = servers_list()
servers = {k.lower(): v for k, v in servers_upper_case.items()}
