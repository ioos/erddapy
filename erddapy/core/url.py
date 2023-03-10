"""URL handling."""

import copy
import functools
import io
from datetime import datetime
from typing import BinaryIO, Dict, List, Optional, Tuple, Union
from urllib.parse import quote_plus

import httpx
import pytz
from pandas._libs.tslibs.parsing import parse_time_string

ListLike = Union[List[str], Tuple[str]]
OptionalStr = Optional[str]


@functools.lru_cache(maxsize=256)
def _urlopen(url: str, auth: Optional[tuple] = None, **kwargs: Dict) -> BinaryIO:
    response = httpx.get(url, follow_redirects=True, auth=auth, **kwargs)
    try:
        response.raise_for_status()
    except httpx.HTTPError as err:
        raise httpx.HTTPError(f"{response.content.decode()}") from err
    return io.BytesIO(response.content)


def urlopen(
    url: str,
    requests_kwargs: Optional[Dict] = None,
) -> BinaryIO:
    """Thin wrapper around httpx get content.

    See httpx.get docs for the `params` and `kwargs` options.

    """
    # Ignoring type checks here b/c mypy does not support decorated functions.
    if requests_kwargs is None:
        requests_kwargs = {}
    data = _urlopen(url, **requests_kwargs)  # type: ignore
    data.seek(0)
    return data


@functools.lru_cache(maxsize=None)
def check_url_response(url: str, **kwargs: Dict) -> str:
    """
    Shortcut to `raise_for_status` instead of fetching the whole content.

    One should only use this is passing URLs that are known to work is necessary.
    Otherwise let it fail later and avoid fetching the head.

    """
    r = httpx.head(url, **kwargs)
    r.raise_for_status()
    return url


def _distinct(url: str, **kwargs: Dict) -> str:
    """
    Sort all of the rows in the results table.

    Starting with the first requested variable,
    then using the second requested variable if the first variable has a tie, ...,
    then remove all non-unique rows of data.

    For example, a query for the variables ["stationType", "stationID"] with `distinct=True`
    will return a sorted list of "stationIDs" associated with each "stationType".

    See http://erddap.ioos.us/erddap/tabledap/documentation.html#distinct

    """
    distinct = kwargs.pop("distinct", False)
    if distinct is True:
        return f"{url}&distinct()"
    return url


def _format_search_string(server: str, query: str) -> str:
    """Generate a search string for an erddap server with user defined query."""
    return f'{server}search/index.csv?page=1&itemsPerPage=100000&searchFor="{query}"'


def _multi_urlopen(url: str) -> BinaryIO:
    """Simpler url open to work with multiprocessing."""
    try:
        data = urlopen(url)
    except (httpx.HTTPError, httpx.ConnectError):
        return None
    return data


def _quote_string_constraints(kwargs: Dict) -> Dict:
    """
    Quote constraints of String variables.

    The right-hand-side value must be surrounded by double quotes if they are not relative constraints.
    """
    return {
        k: f'"{v}"' if isinstance(v, str) and not _check_substrings(v) else v
        for k, v in kwargs.items()
    }


def _format_constraints_url(kwargs: Dict) -> str:
    """Join the constraint variables with separator '&' to add to the download link."""
    return "".join([f"&{k}{v}" for k, v in kwargs.items()])


def _check_substrings(constraint):
    """Extend the OPeNDAP with extra strings."""
    substrings = ["now", "min", "max"]
    return any([True for substring in substrings if substring in str(constraint)])


def parse_dates(date_time: Union[datetime, str]) -> float:
    """
    Parse dates to ERDDAP internal format.

    ERDDAP ReSTful API standardizes the representation of dates as either ISO
    strings or seconds since 1970, but internally ERDDAPY uses datetime-like
    objects. `timestamp` returns the expected strings in seconds since 1970.

    """
    if isinstance(date_time, str):
        # pandas returns a tuple with datetime, dateutil, and string representation.
        # we want only the datetime obj.
        parse_date_time = parse_time_string(date_time)[0]
    else:
        parse_date_time = date_time

    if not parse_date_time.tzinfo:
        parse_date_time = pytz.utc.localize(parse_date_time)
    else:
        parse_date_time = parse_date_time.astimezone(pytz.utc)

    return parse_date_time.timestamp()


def get_search_url(
    server: str,
    response: str = "html",
    search_for: Optional[str] = None,
    protocol: str = "tabledap",
    items_per_page: int = 1000,
    page: int = 1,
    **kwargs,
):
    """
    Build the search URL for the `server` endpoint provided.

    Args:
        search_for: "Google-like" search of the datasets' metadata.

            - Type the words you want to search for, with spaces between the words.
                ERDDAP will search for the words separately, not as a phrase.
            - To search for a phrase, put double quotes around the phrase
                (for example, `"wind speed"`).
            - To exclude datasets with a specific word, use `-excludedWord`.
            - To exclude datasets with a specific phrase, use `-"excluded phrase"`
            - Searches are not case-sensitive.
            - You can search for any part of a word. For example,
                searching for `spee` will find datasets with `speed` and datasets with
                `WindSpeed`
            - The last word in a phrase may be a partial word. For example,
                to find datasets from a specific website (usually the start of the datasetID),
                include (for example) `"datasetID=erd"` in your search.

        response: default is HTML.
        items_per_page: how many items per page in the return,
            default is 1000 for HTML, 1e6 (hopefully all items) for CSV, JSON.
        page: which page to display, default is the first page (1).
        kwargs: extra search constraints based on metadata and/or coordinates ke/value.
            metadata: `cdm_data_type`, `institution`, `ioos_category`,
            `keywords`, `long_name`, `standard_name`, and `variableName`.
            coordinates: `minLon`, `maxLon`, `minLat`, `maxLat`, `minTime`, and `maxTime`.

    Returns:
        url: the search URL.

    """
    server = server.rstrip("/")
    base = (
        "{server}/search/advanced.{response}"
        "?page={page}"
        "&itemsPerPage={itemsPerPage}"
        "&protocol={protocol}"
        "&cdm_data_type={cdm_data_type}"
        "&institution={institution}"
        "&ioos_category={ioos_category}"
        "&keywords={keywords}"
        "&long_name={long_name}"
        "&standard_name={standard_name}"
        "&variableName={variableName}"
        "&minLon={minLon}"
        "&maxLon={maxLon}"
        "&minLat={minLat}"
        "&maxLat={maxLat}"
        "&minTime={minTime}"
        "&maxTime={maxTime}"
    )
    if search_for:
        search_for = quote_plus(search_for)
        base += "&searchFor={searchFor}"

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
        if search_term in kwargs.keys():
            lowercase = kwargs[search_term].lower()
            kwargs.update({search_term: lowercase})

    # These responses should not be paginated b/c that hinders the correct amount of data silently
    # and can surprise users when the number of items is greater than ERDDAP's defaults (1000 items).
    # Ideally there should be no pagination for this on the ERDDAP side but for now we settled for a
    # "really big" `items_per_page` number.
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
        items_per_page = int(1e6)

    default = "(ANY)"
    url = base.format(
        server=server,
        response=response,
        page=page,
        itemsPerPage=items_per_page,
        protocol=kwargs.get("protocol", default),
        cdm_data_type=kwargs.get("cdm_data_type", default),
        institution=kwargs.get("institution", default),
        ioos_category=kwargs.get("ioos_category", default),
        keywords=kwargs.get("keywords", default),
        long_name=kwargs.get("long_name", default),
        standard_name=kwargs.get("standard_name", default),
        variableName=kwargs.get("variableName", default),
        minLon=kwargs.get("min_lon", default),
        maxLon=kwargs.get("max_lon", default),
        minLat=kwargs.get("min_lat", default),
        maxLat=kwargs.get("max_lat", default),
        minTime=kwargs.get("min_time", default),
        maxTime=kwargs.get("max_time", default),
        searchFor=search_for,
    )
    # ERDDAP 2.10 no longer accepts strings placeholder for dates.
    # Removing them entirely should be OK for older versions too.
    url = url.replace("&minTime=(ANY)", "").replace("&maxTime=(ANY)", "")
    return url


def get_info_url(
    server: str,
    dataset_id: OptionalStr = None,
    response: OptionalStr = None,
) -> str:
    """
    Build the info URL for the `server` endpoint.

    Args:
        dataset_id: a dataset unique id.
        response: default is HTML.

    Returns:
        url: the info URL for the `response` chosen.

    """
    if not dataset_id:
        raise ValueError(f"You must specify a valid dataset_id, got {dataset_id}")

    url = f"{server}/info/{dataset_id}/index.{response}"
    return url


def get_categorize_url(
    server: str,
    categorize_by: str,
    value: OptionalStr = None,
    response: OptionalStr = None,
) -> str:
    """
    Build the categorize URL for the `server` endpoint.

    Args:
        categorize_by: a valid attribute, e.g.: ioos_category or standard_name.
            Valid attributes are shown in http://erddap.ioos.us/erddap/categorize page.
        value: an attribute value.
        response: default is HTML.

    Returns:
        url: the categorized URL for the `response` chosen.

    """
    if value:
        url = f"{server}/categorize/{categorize_by}/{value}/index.{response}"
    else:
        url = f"{server}/categorize/{categorize_by}/index.{response}"
    return url


def get_download_url(
    server: str,
    dataset_id: OptionalStr = None,
    protocol: OptionalStr = None,
    variables: Optional[ListLike] = None,
    dim_names: Optional[ListLike] = None,
    response=None,
    constraints=None,
    **kwargs,
) -> str:
    """
    Build the download URL for the `server` endpoint.

    Args:
        dataset_id: a dataset unique id.
        protocol: tabledap or griddap.
        variables (list/tuple): a list of the variables to download.
        response (str): default is HTML.
        constraints (dict): download constraints, default None (opendap-like url)
        example: constraints = {'latitude<=': 41.0,
                                'latitude>=': 38.0,
                                'longitude<=': -69.0,
                                'longitude>=': -72.0,
                                'time<=': '2017-02-10T00:00:00+00:00',
                                'time>=': '2016-07-10T00:00:00+00:00',}

        One can also use relative constraints like {'time>': 'now-7days',
                                                    'latitude<': 'min(longitude)+180',
                                                    'depth>': 'max(depth)-23',}

    Returns:
        url (str): the download URL for the `response` chosen.

    """
    if not dataset_id:
        raise ValueError(f"Please specify a valid `dataset_id`, got {dataset_id}")

    if not protocol:
        raise ValueError(f"Please specify a valid `protocol`, got {protocol}")

    if (
        protocol == "griddap"
        and constraints is not None
        and variables is not None
        and dim_names is not None
    ):
        download_url = [
            server,
            "/",
            protocol,
            "/",
            dataset_id,
            ".",
            response,
            "?",
        ]
        for var in variables:
            sub_url = [var]
            for dim in dim_names:
                sub_url.append(
                    f"[({constraints[dim + '>=']}):"
                    f"{constraints[dim + '_step']}:"
                    f"({constraints[dim + '<=']})]",
                )
            sub_url.append(",")
            download_url.append("".join(sub_url))
        url = "".join(download_url)[:-1]
        return url

    # This is an unconstrained OPeNDAP response b/c
    # the integer based constrained version is just not worth supporting ;-p
    if response == "opendap":
        return f"{server}/{protocol}/{dataset_id}"
    else:
        url = f"{server}/{protocol}/{dataset_id}.{response}?"

    if variables:
        url += ",".join(variables)

    if constraints:
        _constraints = copy.copy(constraints)
        for k, v in _constraints.items():
            if _check_substrings(v):
                continue
            # The valid operators are
            # =, != (not equals), =~ (a regular expression test), <, <=, >, and >=
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
        _constraints = _quote_string_constraints(_constraints)
        _constraints_url = _format_constraints_url(_constraints)

        url += f"{_constraints_url}"

    url = _distinct(url, **kwargs)
    return url
