"""Multiple Server Search."""

import multiprocessing
from typing import Dict
from typing.io import BinaryIO

import pandas as pd
import requests

try:
    joblib = True
    from joblib import Parallel, delayed
except ImportError:
    joblib = False

from erddapy.erddapy import _search_url
from erddapy.servers import servers
from erddapy.url_handling import urlopen


def _format_search_string(server: str, query: str) -> str:
    """Generate a search string for an erddap server with user defined query."""
    return f'{server}search/index.csv?page=1&itemsPerPage=100000&searchFor="{query}"'


def _format_results(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    # we return None for bad server, so we need to filter them here
    df_all = pd.concat([list(df.values())[0] for df in dfs if df is not None])
    return df_all.reset_index(drop=True)


def _multi_urlopen(url: str) -> BinaryIO:
    """Simpler url open to work with multiprocessing."""
    try:
        data = urlopen(url)
    except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError):
        return None
    return data


def fetch_results(
    url: str,
    key: str,
    protocol,
) -> Dict[str, pd.DataFrame]:
    """
    Fetch search results from multiple servers.

    If the server fails to response this function returns None
    and the failed search should be parsed downstream.
    """
    data = _multi_urlopen(url)
    if data is None:
        return None
    else:
        df = pd.read_csv(data)
    try:
        df.dropna(subset=[protocol], inplace=True)
    except KeyError:
        return None
    df["Server url"] = url.split("search")[0]
    return {key: df[["Title", "Institution", "Dataset ID", "Server url"]]}


def search_servers(
    query,
    servers_list=None,
    parallel=False,
    protocol="tabledap",
):
    """
    Search all servers for a query string.

    Returns a dataframe of details for all matching datasets
    Args:
        query: string to search for
        servers_list: optional list of servers. if None, will search all servers in erddapy.servers
        protocol: tabledap or griddap
        parallel: If True, uses joblib to parallelize the search
    """
    if protocol not in ["tabledap", "griddap"]:
        raise ValueError(
            f"Protocol must be tabledap or griddap, got {protocol}",
        )
    if servers_list:
        urls = {server: _format_search_string(server, query) for server in servers_list}
    else:
        urls = {
            key: _format_search_string(server.url, query)
            for key, server in servers.items()
        }
    if parallel:
        num_cores = multiprocessing.cpu_count()
        if not joblib:
            raise ImportError(
                "Missing joblib. Please install it to use parallel searches.",
            )
        returns = Parallel(n_jobs=num_cores)(
            delayed(fetch_results)(url, key, protocol=protocol)
            for key, url in urls.items()
        )
        dfs = [x for x in returns if x is not None]
    else:
        dfs = []
        for key, url in urls.items():
            dfs.append(fetch_results(url, key, protocol=protocol))
    df_all = _format_results(dfs)
    return df_all


def advanced_search_servers(
    servers_list=None, parallel=False, protocol="tabledap", **kwargs
):
    """
    Search multiple ERDDAP servers.

    Returns a dataframe of details for all matching datasets
    Args:
        servers_list: optional list of servers. if None, will search all servers in erddapy.servers
        protocol: tabledap or griddap
        parallel: If True, uses joblib to parallelize the search

    """
    if protocol not in ["tabledap", "griddap"]:
        raise ValueError(
            f"Protocol must be tabledap or griddap, got {protocol}",
        )
    response = "csv"
    if servers_list:
        urls = {
            server: _search_url(server, response=response, **kwargs)
            for server in servers_list
        }
    else:
        urls = {
            key: _search_url(server.url, response=response, **kwargs)
            for key, server in servers.items()
        }

    if parallel:
        num_cores = multiprocessing.cpu_count()
        if not joblib:
            raise ImportError(
                "Missing joblib. Please install it to use parallel searches.",
            )
        returns = Parallel(n_jobs=num_cores)(
            delayed(fetch_results)(url, key, protocol=protocol)
            for key, url in urls.items()
        )
        dfs = [x for x in returns if x is not None]
    else:
        dfs = []
        for key, url in urls.items():
            dfs.append(fetch_results(url, key, protocol=protocol))
    df_all = _format_results(dfs)
    return df_all
