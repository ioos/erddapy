"""Pythonic way to access ERDDAP data."""

from __future__ import annotations

import functools
import hashlib
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.request import urlretrieve

import pandas as pd

from erddapy.core.griddap import (
    _griddap_check_constraints,
    _griddap_check_variables,
    _griddap_get_constraints,
)
from erddapy.core.interfaces import to_iris, to_ncCF, to_pandas, to_xarray
from erddapy.core.url import (
    _check_substrings,
    _distinct,
    _format_constraints_url,
    _quote_string_constraints,
    _sort_url,
    download_formats,
    get_categorize_url,
    get_download_url,
    get_info_url,
    get_search_url,
    parse_dates,
    urlopen,
)
from erddapy.servers.servers import servers

# Objects used by downstream packages
__all__ = [
    "ERDDAP",
    "_check_substrings",
    "_distinct",
    "_format_constraints_url",
    "_quote_string_constraints",
    "parse_dates",
    "urlopen",
]

if TYPE_CHECKING:
    import iris.cube
    import netCDF4.Dataset
    import xarray as xr

OptionalBool = bool | None
OptionalDict = dict | None
OptionalList = list[str] | tuple[str] | None
OptionalStr = str | None


class ERDDAP:
    """Creates an ERDDAP instance for a specific server endpoint.

    Args:
    ----
        server: an ERDDAP server URL or acronym is using the builtin servers.
        protocol: tabledap or griddap.

    Attributes:
    ----------
        dataset_id: a dataset unique id.
        variables: a list variables to download.
        response: default is HTML.
        constraints: download constraints, default None (opendap-like url)
        params and requests_kwargs: `httpx.get` options

    Returns:
    -------
        instance: the ERDDAP URL builder.

    Examples:
    --------
        Specifying the server URL

        >>> e = ERDDAP(server="https://gliders.ioos.us/erddap")

        let's search for glider `ru29` and read the csv response with pandas.

        >>> import pandas as pd
        >>> url = e.get_search_url(search_for="ru29", response="csv")
        >>> pd.read_csv(url)["Dataset ID"]
        0    ru29-20150623T1046
        1    ru29-20161105T0131
        Name: Dataset ID, dtype: object

        there are "shortcuts" for some servers

        >>> e = ERDDAP(server="SECOORA")
        >>> e.server
        'http://erddap.secoora.org/erddap'

        to get a list of the shortcuts available servers:

        >>> from erddapy import servers
        >>> {k: v.url for k, v in servers.items()}
        {'MDA': 'https://bluehub.jrc.ec.europa.eu/erddap/',
         'MII': 'https://erddap.marine.ie/erddap/',
         'CSCGOM': 'http://cwcgom.aoml.noaa.gov/erddap/',
         'CSWC': 'https://coastwatch.pfeg.noaa.gov/erddap/',
         'CeNCOOS': 'http://erddap.axiomalaska.com/erddap/',
         'NERACOOS': 'http://www.neracoos.org/erddap/',
         'NGDAC': 'https://gliders.ioos.us/erddap/',
         'PacIOOS': 'http://oos.soest.hawaii.edu/erddap/',
         'SECOORA': 'http://erddap.secoora.org/erddap/',
         'NCEI': 'https://ecowatch.ncddc.noaa.gov/erddap/',
         'OSMC': 'http://osmc.noaa.gov/erddap/',
         'UAF': 'https://upwell.pfeg.noaa.gov/erddap/',
         'ONC': 'http://dap.onc.uvic.ca/erddap/',
         'BMLSC': 'http://bmlsc.ucdavis.edu:8080/erddap/',
         'RTECH': 'https://meteo.rtech.fr/erddap/',
         'IFREMER': 'http://www.ifremer.fr/erddap/',
         'UBC': 'https://salishsea.eos.ubc.ca/erddap/'}

    """

    def __init__(
        self: ERDDAP,
        server: str,
        protocol: OptionalStr = None,
        response: str = "html",
    ) -> None:
        """Instantiate main class attributes.

        Attributes
        ----------
          server: the server URL.
          protocol: ERDDAP's protocol (tabledap/griddap)
          response: default is HTML.

        """
        if server.lower() in servers:
            server = servers[server.lower()].url
        self.server = server.rstrip("/")
        self.protocol = protocol
        self.response = response

        # Initialized only via properties.
        self.server_functions: dict | None = None
        self.requests_kwargs: dict = {}
        self.auth: tuple | None = None

        self.constraints: dict | None = None
        self.variables: OptionalList | None = None
        self.dim_names: OptionalList | None = None

        self._get_variables = functools.lru_cache(maxsize=128)(
            self._get_variables_uncached,
        )
        # Caching the last dataset_id and variables list request for
        # quicker access, will be overridden when requesting a new dataset_id.
        self._dataset_id: OptionalStr = None
        self._variables: dict = {}

    @property
    def dataset_id(self) -> str:
        """dataset_id property."""
        return self._dataset_id

    @dataset_id.setter
    def dataset_id(self, value: str) -> None:
        self._dataset_id = value
        self.griddap_initialize(dataset_id=value)

    def griddap_initialize(
        self: ERDDAP,
        dataset_id: OptionalStr = None,
        step: int = 1,
    ) -> None:
        """Fetch metadata of dataset and initialize constraints and variables.

        Args:
        ----
        dataset_id: a dataset unique id.
        step: step used to subset dataset

        """
        dataset_id = dataset_id if dataset_id else self.dataset_id
        # Short-circuit for opendap and/or non-griddap datasets.
        if self.protocol != "griddap" or self.response == "opendap":
            return
        msg = f"Must set a valid dataset_id, got {self.dataset_id}"
        if dataset_id is None:
            raise ValueError(msg)

        metadata_url = f"{self.server}/griddap/{self.dataset_id}"
        (
            self.constraints,
            self.dim_names,
            self.variables,
        ) = _griddap_get_constraints(metadata_url, step)
        self._constraints_original = self.constraints.copy()
        self._variables_original = self.variables.copy()

    def get_search_url(
        self: ERDDAP,
        response: OptionalStr = None,
        search_for: OptionalStr = None,
        protocol: OptionalStr = None,
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
        protocol = protocol if protocol else self.protocol
        response = response if response else self.response

        return get_search_url(
            self.server,
            response=response,
            search_for=search_for,
            protocol=protocol,
            items_per_page=items_per_page,
            page=page,
            **kwargs,
        )

    def get_info_url(
        self: ERDDAP,
        dataset_id: OptionalStr = None,
        response: OptionalStr = None,
    ) -> str:
        """Build the info URL for the `server` endpoint.

        Args:
        ----
            dataset_id: a dataset unique id.
            If empty the full dataset listing will be returned.
            response: default is HTML.

        Returns:
        -------
            url: the info URL for the `response` chosen.

        """
        dataset_id = dataset_id if dataset_id else self.dataset_id
        response = response if response else self.response

        return get_info_url(
            self.server,
            dataset_id=dataset_id,
            response=response,
        )

    def get_categorize_url(
        self: ERDDAP,
        categorize_by: str,
        value: OptionalStr = None,
        response: OptionalStr = None,
    ) -> str:
        """Build the categorize URL for the `server` endpoint.

        Args:
        ----
            categorize_by: a valid attribute, e.g. ioos_category
                or standard_name. Valid attributes are shown in
                http://erddap.ioos.us/erddap/categorize page.
            value: an attribute value.
            response: default is HTML.

        Returns:
        -------
            url: the categorized URL for the `response` chosen.

        """
        response = response if response else self.response
        return get_categorize_url(self.server, categorize_by, value, response)

    def get_download_url(  # noqa: PLR0913
        self: ERDDAP,
        *,
        dataset_id: OptionalStr = None,
        protocol: OptionalStr = None,
        variables: OptionalList = None,
        dim_names: OptionalList = None,
        response: OptionalStr = None,
        constraints: OptionalDict = None,
        distinct: OptionalBool = False,
    ) -> str:
        """Build the download URL for the `server` endpoint.

        Args:
        ----
            dataset_id: a dataset unique id.
            protocol: tabledap or griddap.
            variables (list/tuple): a list of the variables to download.
            dim_names (list/tuple): a list of the dimensions (griddap only).
            response (str): default is HTML.
            constraints (dict): download constraints, default None (opendap).
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
        dataset_id = dataset_id if dataset_id else self.dataset_id
        protocol = protocol if protocol else self.protocol
        variables = variables if variables else self.variables
        dim_names = dim_names if dim_names else self.dim_names
        response = response if response else self.response
        constraints = constraints if constraints else self.constraints

        if not dataset_id:
            msg = f"Please specify a valid `dataset_id`, got {dataset_id}"
            raise ValueError(msg)

        if not protocol:
            msg = f"Please specify a valid `protocol`, got {protocol}"
            raise ValueError(msg)

        if (
            protocol == "griddap"
            and constraints is not None
            and variables is not None
            and dim_names is not None
        ):
            # Check that dimensions, constraints,
            # and variables are valid for this dataset.

            _griddap_check_constraints(constraints, self._constraints_original)
            _griddap_check_variables(variables, self._variables_original)

        return get_download_url(
            self.server,
            dataset_id=dataset_id,
            protocol=protocol,
            variables=variables,
            dim_names=dim_names,
            response=response,
            constraints=constraints,
            distinct=distinct,
        )

    def to_pandas(
        self: ERDDAP,
        requests_kwargs: dict | None = None,
        **kw: dict,
    ) -> pd.DataFrame:
        """Save a data request to a pandas.DataFrame.

        Accepts any `pandas.read_csv` keyword arguments,
        passed as a dictionary to pandas_kwargs.

        This method uses the .csvp [1] response as the default for simplicity,
        please check ERDDAP's docs for the other csv options available.

        [1] Download a ISO-8859-1 .csv file with line 1: name (units).
            Times are ISO 8601 strings.

        requests_kwargs: kwargs to be passed to urlopen method.
        **kw: kwargs to be passed to third-party library (pandas).
        """
        response = kw.pop("response", "csvp")
        distinct = kw.pop("distinct", False)
        url = self.get_download_url(response=response, distinct=distinct)
        return to_pandas(
            url,
            requests_kwargs=requests_kwargs,
            pandas_kwargs=dict(**kw),
        )

    def to_ncCF(  # noqa: N802
        self: ERDDAP,
        protocol: OptionalStr = None,
        **kw: dict,
    ) -> netCDF4.Dataset:
        """Load the data request into a CF compliant netCDF4-python object."""
        distinct = kw.pop("distinct", False)
        protocol = protocol if protocol else self.protocol
        url = self.get_download_url(response="ncCF", distinct=distinct)
        return to_ncCF(url, protocol=protocol, requests_kwargs=dict(**kw))

    def to_xarray(
        self: ERDDAP,
        requests_kwargs: dict | None = None,
        **kw: dict,
    ) -> xr.Dataset:
        """Load the data request into a xarray.Dataset.

        Accepts any `xr.open_dataset` keyword arguments.
        """
        if self.response == "opendap":
            response = "opendap"
        elif self.protocol == "griddap":
            response = "nc"
        else:
            response = "ncCF"
        distinct = kw.pop("distinct", False)
        url = self.get_download_url(response=response, distinct=distinct)
        if requests_kwargs:
            requests_kwargs = {"auth": self.auth, **requests_kwargs}
        else:
            requests_kwargs = {"auth": self.auth}
        return to_xarray(
            url,
            response,
            requests_kwargs,
            xarray_kwargs=dict(**kw),
        )

    def to_iris(self: ERDDAP, **kw: dict) -> iris.cube.CubeList:
        """Load the data request into an iris.cube.CubeList.

        Accepts any `iris.load_raw` keyword arguments.
        """
        response = "nc" if self.protocol == "griddap" else "ncCF"
        distinct = kw.pop("distinct", False)
        url = self.get_download_url(response=response, distinct=distinct)
        return to_iris(url, iris_kwargs=dict(**kw))

    def _get_variables_uncached(
        self: ERDDAP,
        dataset_id: OptionalStr = None,
    ) -> dict:
        if not dataset_id:
            dataset_id = self.dataset_id

        if dataset_id is None:
            msg = f"You must specify a valid dataset_id, got {dataset_id}"
            raise ValueError(msg)

        url = self.get_info_url(dataset_id=dataset_id, response="csv")

        variables = {}
        data = urlopen(url, self.requests_kwargs)
        _df = pd.read_csv(data)
        self._dataset_id = dataset_id
        for variable in set(_df["Variable Name"]):
            attributes = (
                _df.loc[
                    _df["Variable Name"] == variable,
                    ["Attribute Name", "Value"],
                ]
                .set_index("Attribute Name")
                .to_dict()["Value"]
            )
            variables.update({variable: attributes})
        return variables

    def get_var_by_attr(
        self: ERDDAP,
        dataset_id: OptionalStr = None,
        **kwargs: dict,
    ) -> list[str]:
        """Return a variable based on its attributes.

        The `get_var_by_attr` method will create an info `csv` return,
        for the `dataset_id`, and the variables attribute dictionary,
        similar to netCDF4-python `get_variables_by_attributes`.

        Examples
        --------
            >>> e = ERDDAP(server_url="https://gliders.ioos.us/erddap")
            >>> dataset_id = "whoi_406-20160902T1700"

            Get variables with x-axis attribute.

            >>> e.get_var_by_attr(dataset_id, axis="X")
            ['longitude']

            Get variables with matching "standard_name" attribute

            >>> e.get_var_by_attr(
            ...     dataset_id, standard_name="northward_sea_water_velocity"
            ... )
            ['v']

            Get Axis variables

            >>> axis = lambda v: v in ["X", "Y", "Z", "T"]
            >>> e.get_var_by_attr(dataset_id, axis=axis)
            ['latitude', 'longitude', 'time', 'depth']

        """
        variables = self._get_variables(dataset_id=dataset_id)
        # Virtually the same code as the netCDF4 counterpart.
        vs = []
        has_value_flag = False
        for vname in variables:
            var = variables[vname]
            for k, v in kwargs.items():
                if callable(v):
                    has_value_flag = v(var.get(k, None))
                    if has_value_flag is False:
                        break
                elif var.get(k) and var.get(k) == v:
                    has_value_flag = True
                else:
                    has_value_flag = False
                    break
            if has_value_flag is True:
                vs.append(vname)
        return vs

    def download_file(
        self: ERDDAP,
        file_type: str,
    ) -> str:
        """Download the dataset to a file in a user specified format."""
        file_type = file_type.lstrip(".")
        if file_type not in download_formats:
            msg = f"Requested filetype {file_type} not available on ERDDAP"
            raise ValueError(msg)
        url = _sort_url(self.get_download_url(response=file_type))
        fname_hash = hashlib.shake_256(url.encode()).hexdigest(5)
        file_name = Path(f"{self.dataset_id}_{fname_hash}.{file_type}")
        if not file_name.exists():
            urlretrieve(url, file_name)  # noqa: S310
        return file_name
