"""URL handling."""

from __future__ import annotations

import copy
import functools
import io
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime
from typing import BinaryIO

import httpx
import pytz
from pandas import to_datetime
from yarl import URL

OptionalStr = str | None
OptionalBool = bool | None
OptionalDict = dict | None
OptionalList = list[str] | tuple[str] | None


_BIG_NUMBER = int(1e6)
_DOWNLOAD_FORMATS = (
    "asc",
    "csv",
    "csvp",
    "csv0",
    "dataTable",
    "das",
    "dds",
    "dods",
    "esriCsv",
    "fgdc",
    "geoJson",
    "graph",
    "help",
    "html",
    "iso19115",
    "itx",
    "json",
    "jsonlCSV1",
    "jsonlCSV",
    "jsonlKVP",
    "mat",
    "nc",
    "ncHeader",
    "ncCF",
    "ncCFHeader",
    "ncCFMA",
    "ncCFMAHeader",
    "nccsv",
    "nccsvMetadata",
    "ncoJson",
    "odvTxt",
    "subset",
    "tsv",
    "tsvp",
    "tsv0",
    "wav",
    "xhtml",
    "kml",
    "smallPdf",
    "pdf",
    "largePdf",
    "smallPng",
    "png",
    "largePng",
    "transparentPng",
)


def quote_url(url: str) -> str:
    """Quote URL args for modern ERDDAP servers."""
    return str(URL(url))


def _sort_url(url: str) -> str:
    """Return a URL with sorted variables and constraints for hashing.

    We have a few hacks to handled variables variables,
    params without a value, and sort them.
    xref.: https://github.com/aio-libs/yarl/issues/307

    Other fixes:
    ERDDAP separates variables from constrantains,
    query without values from query with values, using &.
    That means we need a & before the constranints when there are no variables.

    We also strip = and ? from URLs ending. The first is due to yarl issue 307,
    the second is harmless but we want to be able to have the same hash
    for URLs that will give the same response, so we remove it from all URLs.

    """
    replace = ("?", "?&")
    sorted_variables = None
    url = URL(url)
    query = url.query

    variables = [k for k, v in query.items() if not v]
    sorted_constraints = {k: v for k, v in sorted(query.items()) if v}

    if variables:
        sorted_variables = ",".join(sorted(variables[0].split(",")))
        replace = ("=&", "&")

    return (
        url.with_query(sorted_variables)
        .update_query(sorted_constraints)
        .human_repr()
        .replace(*replace)
        .strip("=")
        .strip("?")
    )


@functools.lru_cache(maxsize=128)
def _urlopen(url: str, auth: tuple | None = None, **kwargs: dict) -> BinaryIO:
    if "timeout" not in kwargs:
        kwargs["timeout"] = 60
    response = httpx.get(
        quote_url(url),
        follow_redirects=True,
        auth=auth,
        **kwargs,
    )
    try:
        response.raise_for_status()
    except httpx.HTTPError as err:
        msg = f"{response.content.decode()}"
        raise httpx.HTTPError(msg) from err
    return io.BytesIO(response.content)


def urlopen(
    url: str,
    requests_kwargs: dict | None = None,
) -> BinaryIO:
    """Thin wrapper around httpx get content.

    See httpx.get docs for the `params` and `kwargs` options.

    """
    if requests_kwargs is None:
        requests_kwargs = {}
    data = _urlopen(url, **requests_kwargs)
    data.seek(0)
    return data


@functools.lru_cache(maxsize=128)
def check_url_response(url: str, **kwargs: dict) -> str:
    """Shortcut to `raise_for_status` instead of fetching the whole content.

    One should only use this is passing URLs that are known to work is
    necessary. Otherwise let it fail later and avoid fetching the head.

    """
    r = httpx.head(url, **kwargs)
    r.raise_for_status()
    return url


def _distinct(url: str, *, distinct: OptionalBool = False) -> str:
    """Sort all of the rows in the results table.

    Starting with the first requested variable,
    then using the second requested variable if the first variable has a tie,
    then remove all non-unique rows of data.

    For example, a query for the variables ["stationType", "stationID"]
    with `distinct=True` will return a sorted list of "stationIDs" associated
    with each "stationType".

    See http://erddap.ioos.us/erddap/tabledap/documentation.html#distinct

    """
    if distinct:
        url = URL(url)
        # yarl cannot handle query entry without values,
        # so we need to strip the empty `=`.
        return str(url.update_query("distinct()")).strip("=")
    return url


def _format_search_string(server: str, query: str) -> str:
    """Generate search string for an ERDDAP server with user defined query."""
    kw = {
        "page": 1,
        "itemsPerPage": _BIG_NUMBER,
        "searchFor": query,
    }
    url = (URL(server) / "search" / "index.csv").with_query(kw)
    return str(url)


def _multi_urlopen(url: str) -> BinaryIO:
    """Simpler url open to work with multiprocessing."""
    try:
        data = urlopen(url)
    except (httpx.HTTPError, httpx.ConnectError):
        return None
    return data


def _check_substrings(constraint: dict) -> bool:
    """Extend the OPeNDAP with extra strings."""
    substrings = ("now", "min", "max")
    return any(
        True for substring in substrings if substring in str(constraint)
    )


def parse_dates(
    date_time: datetime | str,
    *,
    dayfirst: OptionalBool = False,
    yearfirst: OptionalBool = False,
) -> float:
    """Parse dates to ERDDAP internal format.

    ERDDAP ReSTful API standardizes the representation of dates as either ISO
    strings or seconds since 1970, but internally ERDDAPY uses datetime-like
    objects. `timestamp` returns the expected strings in seconds since 1970.

    """
    if isinstance(date_time, str):
        parse_date_time = to_datetime(
            date_time,
            dayfirst=dayfirst,
            yearfirst=yearfirst,
        ).to_pydatetime()
    else:
        parse_date_time = date_time

    if not parse_date_time.tzinfo:
        parse_date_time = pytz.utc.localize(parse_date_time)
    else:
        parse_date_time = parse_date_time.astimezone(pytz.utc)

    return parse_date_time.timestamp()


def get_search_url(  # noqa: PLR0913
    server: str,
    response: str = "html",
    search_for: str | None = None,
    protocol: str = "tabledap",
    items_per_page: int = 1_000_000,
    page: int = 1,
    **kwargs: dict,
) -> str:
    """Build the search URL for the `server` endpoint provided.

    Args:
    ----
        search_for: "Google-like" search of the datasets' metadata.

            - Type the words you want to search for,
                with spaces between the words.
                ERDDAP will search for the words separately, not as a phrase.
            - To search for a phrase, put double quotes around the phrase
                (for example, `"wind speed"`).
            - To exclude datasets with a specific word use `-excludedWord`.
            - To exclude datasets with a specific phrase,
                use `-"excluded phrase"`
            - Searches are not case-sensitive.
            - You can search for any part of a word. For example,
                searching for `spee` will find datasets with `speed`
                and datasets with `WindSpeed`
            - The last word in a phrase may be a partial word. For example,
                to find datasets from a specific website
                (usually the start of the datasetID),
                include (for example) `"datasetID=erd"` in your search.

        server: data server endpoint.
        response: default is HTML.
        protocol: tabledap or griddap.
        items_per_page: how many items per page in the return,
            default is 1_000_000 for HTML,
            1e6 (hopefully all items) for CSV, JSON.
        page: which page to display, default is the first page (1).
        kwargs: extra search constraints based on metadata and/or
            coordinates key/value.
        metadata: `cdm_data_type`, `institution`, `ioos_category`,
            `keywords`, `long_name`, `standard_name`, and `variableName`.
            coordinates: `minLon`, `maxLon`, `minLat`, `maxLat`,
            `minTime`, and `maxTime`.

    Returns:
    -------
        url: the search URL.

    """
    # Convert dates from datetime to `seconds since 1970-01-01T00:00:00Z`.
    min_time = kwargs.pop("min_time", "")
    max_time = kwargs.pop("max_time", "")
    if min_time and not _check_substrings(min_time):
        kwargs.update({"min_time": parse_dates(min_time)})
    else:
        kwargs.update({"min_time": min_time})
    if max_time and not _check_substrings(max_time):
        kwargs.update({"max_time": parse_dates(max_time)})
    else:
        kwargs.update({"max_time": max_time})

    if protocol:
        kwargs.update({"protocol": protocol})

    lower_case_search_terms = (
        "cdm_data_type",
        "institution",
        "ioos_category",
        "keywords",
        "long_name",
        "standard_name",
        "variableName",
    )
    for search_term in lower_case_search_terms:
        if search_term in kwargs:
            lowercase = kwargs[search_term].lower()
            kwargs.update({search_term: lowercase})

    # These responses should not be paginated b/c that hinders the correct
    # amount of data silently and can surprise users when the number of items
    # is greater than ERDDAP's defaults (1_000_000 items).
    # Ideally there should be no pagination for this on the ERDDAP side but for
    # now we settled for a "really big" `items_per_page` number.
    non_paginated_responses = [
        "csv",
        "csvp",
        "csv0",
        "json",
        "jsonlCSV1",
        "jsonlCSV",
        "jsonlKVP",
        "tsv",
        "tsvp",
        "tsv0",
    ]
    if response in non_paginated_responses:
        items_per_page = _BIG_NUMBER

    default = "(ANY)"
    query = {
        "page": f"{page}",
        "itemsPerPage": f"{items_per_page}",
        "protocol": kwargs.get("protocol", default),
        "cdm_data_type": kwargs.get("cdm_data_type", default),
        "institution": kwargs.get("institution", default),
        "ioos_category": kwargs.get("ioos_category", default),
        "keywords": kwargs.get("keywords", default),
        "long_name": kwargs.get("long_name", default),
        "standard_name": kwargs.get("standard_name", default),
        "variableName": kwargs.get("variableName", default),
        "minLon": kwargs.get("min_lon", default),
        "maxLon": kwargs.get("max_lon", default),
        "minLat": kwargs.get("min_lat", default),
        "maxLat": kwargs.get("max_lat", default),
        # ERDDAP 2.10 no longer accepts strings placeholder for dates.
        # Removing them entirely should be OK for older versions too.
        "minTime": kwargs.get("min_time", ""),
        "maxTime": kwargs.get("max_time", ""),
    }
    if search_for:
        query.update({"searchFor": f"{search_for}"})

    url = URL(server)
    path = "search"
    name = f"advanced.{response}"
    url = (url / path / name).with_query(query)
    return str(url)


def get_info_url(
    server: str,
    dataset_id: OptionalStr = None,
    response: OptionalStr = "html",
) -> str:
    """Build the info URL for the `server` endpoint.

    Args:
    ----
        server: data server endpoint.
        dataset_id: a dataset unique id.
        If empty the full dataset listing will be returned.
        response: default is HTML.

    Returns:
    -------
        url: the info URL for the `response` chosen.

    """
    url = URL(server)
    if dataset_id is None:
        url = (url / "info" / f"index.{response}").with_query(
            {"itemsPerPage": _BIG_NUMBER},
        )
    url = url / "info" / f"{dataset_id}" / f"index.{response}"
    return str(url)


def get_categorize_url(
    server: str,
    categorize_by: str,
    value: OptionalStr = None,
    response: str = "html",
) -> str:
    """Build the categorize URL for the `server` endpoint.

    Args:
    ----
        server: data server endpoint.
        categorize_by: a valid attribute, e.g.: ioos_category or standard_name.
            Valid attributes are shown in
            http://erddap.ioos.us/erddap/categorize page.
        value: an attribute value.
        response: default is HTML.

    Returns:
    -------
        url: the categorized URL for the `response` chosen.

    """
    url = URL(server) / "categorize" / categorize_by

    if value:
        url = url / "value"
    url = url / f"index.{response}"
    return str(url)


def get_download_url(  # noqa: PLR0913, C901
    server: str,
    *,
    dataset_id: OptionalStr = None,
    protocol: OptionalStr = None,
    variables: OptionalList = None,
    dim_names: OptionalList = None,
    response: OptionalStr = "html",
    constraints: OptionalDict = None,
    distinct: OptionalBool = False,
) -> str:
    """Build the download URL.

    Args:
    ----
        server: data server endpoint.
        dataset_id: a dataset unique id.
        protocol: tabledap or griddap.
        variables (list/tuple): a list of the variables to download.
        dim_names (list/tuple): a list of the dimensions (griddap only).
        response (str): default is HTML.
        constraints (dict): download constraints, default None (opendap url)
        distinct (bool): if true, only unique values will be downloaded.

    Example:
    -------
            constraints = {
                'latitude<=': 41.0,
                'latitude>=': 38.0,
                'longitude<=': -69.0,
                'longitude>=': -72.0,
                'time<=': '2017-02-10T00:00:00+00:00',
                'time>=': '2016-07-10T00:00:00+00:00',
                }

        One can also use relative constraints like:
            constraints = {
                'time>': 'now-7days',
                'latitude<': 'min(longitude)+180',
                'depth>': 'max(depth)-23',
                }

    Returns:
    -------
        url (str): the download URL for the `response` chosen.

    """
    url = URL(server)

    if not dataset_id:
        msg = f"Please specify a valid `dataset_id`, got {dataset_id}"
        raise ValueError(msg)

    if not protocol:
        msg = f"Please specify a valid `protocol`, got {protocol}"
        raise ValueError(msg)

    name = f"{dataset_id}.{response}"
    download_url = url / protocol / name

    if (
        protocol == "griddap"
        and constraints is not None
        and variables is not None
        and dim_names is not None
    ):
        # NB: We should factor this out,
        # and try to make it easier to understand the griddap URLs.
        griddap = []
        for var in variables:
            sub_url = [var]
            sub_url.extend(
                f"[({constraints[dim + '>=']}):"
                f"{constraints[dim + '_step']}:"
                f"({constraints[dim + '<=']})]"
                for dim in dim_names
            )
            sub_url.append(",")
            griddap.append("".join(sub_url))

        # We need to remove the last , from the URL
        return f"{download_url}?{''.join(griddap)}".strip(",")

    # This is an unconstrained OPeNDAP response b/c
    # the integer based constrained version is just not worth supporting ;-p
    if response == "opendap":
        return str(download_url.with_name(dataset_id))

    replace = ("?", "?&")
    sorted_variables = None
    if variables:
        sorted_variables = ",".join(sorted(variables))
        replace = ("=&", "&")

    sorted_constraints = None
    if constraints:
        _constraints = copy.copy(constraints)
        for k, v in _constraints.items():
            if _check_substrings(v):
                continue
            # The valid operators are =, != (not equals),
            # =~ (a regular expression test), <, <=, >, and >=
            valid_time_constraints = (
                "time=",
                "time!=",
                "time=~",
                "time<",
                "time<=",
                "time>",
                "time>=",
            )
            if k.startswith(valid_time_constraints):
                _constraints.update({k: parse_dates(v)})
        # NB: This will create a wrong URL for inequalities that
        # are not `or =`. Yarl doesn't support that.
        sorted_constraints = {k.strip("="): v for k, v in _constraints.items()}

    download_url = download_url.with_query(sorted_variables)
    if sorted_constraints:
        download_url = download_url.update_query(sorted_constraints)

    download_url = (
        download_url.human_repr().replace(*replace).strip("=").strip("?")
    )

    download_url = str(download_url)
    return _distinct(download_url, distinct=distinct)
