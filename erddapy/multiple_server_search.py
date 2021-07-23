import multiprocessing
from typing import Dict, Optional

import pandas as pd
from joblib import Parallel, delayed
from erddapy.url_handling import format_search_string, multi_urlopen

from erddapy.servers import servers


def parse_results(data: bytes, protocol, key, url) -> Dict[str, pd.DataFrame]:
    """
    Parse server search results into a pandas DataFrame
    """
    df = pd.read_csv(data)
    try:
        df.dropna(subset=[protocol], inplace=True)
    except KeyError:
        return None
    df["Server url"] = url.split("search")[0]
    return {key: df[["Title", "Institution", "Dataset ID", "Server url"]]}


def fetch_results(
    url: str,
    key: str,
    protocol="tabledap",
) -> Optional[Dict[str, pd.DataFrame]]:
    """
    Fetch search results from multiple servers
    """
    data = multi_urlopen(url)
    if data is None:
        return None
    return parse_results(data, protocol, key, url)


def format_results(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    df_all = pd.concat([list(df.values())[0] for df in dfs])
    return df_all.reset_index(drop=True)


def search_servers(
    query="glider",
    servers_list=None,
    parallel=True,
    protocol="tabledap",
):
    """
    Search all servers for a query string
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
        urls = {server: format_search_string(server, query) for server in servers_list}
    else:
        urls = {
            key: format_search_string(server, query) for key, server in servers.items()
        }
    if parallel:
        num_cores = multiprocessing.cpu_count()
        returns = Parallel(n_jobs=num_cores)(
            delayed(fetch_results)(url, key, protocol=protocol)
            for key, url in urls.items()
        )
        dfs = [x for x in returns if x is not None]
    else:
        dfs = []
        for key, url in urls.items():
            dfs.append(fetch_results(url, key, protocol=protocol))
    df_all = format_results(dfs)
    return df_all
