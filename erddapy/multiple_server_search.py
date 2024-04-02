"""Multiple Server Search."""

from __future__ import annotations

import multiprocessing

import pandas as pd

try:
    joblib = True
    from joblib import Parallel, delayed
except ImportError:
    joblib = False

from erddapy.core.url import (
    _format_search_string,
    _multi_urlopen,
    get_search_url,
)
from erddapy.servers.servers import servers

OptionalList = list | None
OptionalBool = bool | None
OptionalStr = str | None


def _format_results(dfs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Format dictionary of results into a Pandas dataframe."""
    # we return None for bad server, so we need to filter them here
    df_all = pd.concat(
        [next(iter(df.values())) for df in dfs if df is not None],
    )
    return df_all.reset_index(drop=True)


def fetch_results(
    url: str,
    key: str,
    protocol: str,
) -> dict[str, pd.DataFrame]:
    """Fetch search results from multiple servers.

    If the server fails to response this function returns None
    and the failed search should be parsed downstream.
    """
    data = _multi_urlopen(url)
    if data is None:
        return None
    try:
        df_results = pd.read_csv(data)
    except pd.errors.ParserError:
        # Bad servers will return data that is not
        # a csv but with valid html code.
        return None
    try:
        df_results = df_results.dropna(subset=[protocol])
    except KeyError:
        return None
    df_results["Server url"] = url.split("search")[0]
    return {
        key: df_results[["Title", "Institution", "Dataset ID", "Server url"]],
    }


def search_servers(
    query: str,
    *,
    servers_list: OptionalList = None,
    parallel: OptionalBool = False,
    protocol: OptionalStr = "tabledap",
) -> pd.Dataframe:
    """Search all servers for a query string.

    Returns a dataframe of details for all matching datasets
    Args:
        query: string to search for
        servers_list: optional list of servers.
                      If None, will search all servers in erddapy.servers
        protocol: tabledap or griddap
        parallel: If True, uses joblib to parallelize the search
    """
    if protocol not in ["tabledap", "griddap"]:
        msg = f"Protocol must be tabledap or griddap, got {protocol}"
        raise ValueError(msg)
    if servers_list:
        urls = {
            server: _format_search_string(server, query)
            for server in servers_list
        }
    else:
        urls = {
            key: _format_search_string(server.url, query)
            for key, server in servers.items()
        }
    if parallel:
        num_cores = multiprocessing.cpu_count()
        if not joblib:
            msg = "Missing joblib. Please install it to use parallel searches."
            raise ImportError(msg)
        returns = Parallel(n_jobs=num_cores)(
            delayed(fetch_results)(url, key, protocol=protocol)
            for key, url in urls.items()
        )
        dfs = [x for x in returns if x is not None]
    else:
        dfs = []
        for key, url in urls.items():
            dfs.append(fetch_results(url, key, protocol=protocol))
    return _format_results(dfs)


def advanced_search_servers(
    servers_list: OptionalList = None,
    *,
    parallel: OptionalBool = False,
    protocol: OptionalStr = "tabledap",
    **kwargs: dict,
) -> pd.DataFrame:
    """Search multiple ERDDAP servers.

    Returns a dataframe of details for all matching datasets
    Args:
        servers_list: optional list of servers.
                      If None, will search all servers in erddapy.servers
        protocol: tabledap or griddap
        parallel: If True, uses joblib to parallelize the search

    """
    if protocol not in ["tabledap", "griddap"]:
        msg = f"Protocol must be tabledap or griddap, got {protocol}"
        raise ValueError(msg)
    response = "csv"
    if servers_list:
        urls = {
            server: get_search_url(server, response=response, **kwargs)
            for server in servers_list
        }
    else:
        urls = {
            key: get_search_url(server.url, response=response, **kwargs)
            for key, server in servers.items()
        }

    if parallel:
        num_cores = multiprocessing.cpu_count()
        if not joblib:
            msg = "Missing joblib. Please install it to use parallel searches."
            raise ImportError(msg)
        returns = Parallel(n_jobs=num_cores)(
            delayed(fetch_results)(url, key, protocol=protocol)
            for key, url in urls.items()
        )
        dfs = [x for x in returns if x is not None]
    else:
        dfs = []
        for key, url in urls.items():
            dfs.append(fetch_results(url, key, protocol=protocol))
    return _format_results(dfs)
