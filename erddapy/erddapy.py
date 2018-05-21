"""
erddapy

Pythonic way to access ERDDAP data

"""

from __future__ import (absolute_import, division, print_function)

from erddapy.url_builder import (
    download_url,
    info_url,
    search_url,
)
from erddapy.utilities import (
    _check_url_response,
    servers,
    urlopen,
    )

import pandas as pd


class ERDDAP(object):
    """Creates an ERDDAP instance for a specific server endpoint.

    Args:
        server (str): an ERDDAP server URL or an acronym for one of the builtin servers.
        dataset_id (str): a dataset unique id.
        protocol (str): tabledap or griddap.
        variables (:obj:`list`/`tuple`): a list variables to download.
        response (str): default is HTML.
        constraints (:obj:`dict`): download constraints, default None (opendap-like url)
        params and requests_kwargs: `request.get` options

    Returns:
        instance: the ERDDAP URL builder.

    Examples:
        Specifying the server URL

        >>> e = ERDDAP(server='https://data.ioos.us/gliders/erddap')

        let's search for glider `ru29` and read the csv response with pandas.

        >>> import pandas as pd
        >>> url = e.get_search_url(search_for='ru29', response='csv')
        >>> pd.read_csv(url)['Dataset ID']
        0    ru29-20150623T1046
        1    ru29-20161105T0131
        Name: Dataset ID, dtype: object

        there are "shortcuts" for some servers

        >>> e = ERDDAP(server='SECOORA')
        >>> e.server
        'http://erddap.secoora.org/erddap'

        to get a list of the shortcuts available servers:

        >>> from erddapy import servers
        >>> {k: v.url for k, v in servers.items()}
        {'BMLSC': 'http://bmlsc.ucdavis.edu:8080/erddap',
         'CSCGOM': 'http://cwcgom.aoml.noaa.gov/erddap',
         'CSWC': 'https://coastwatch.pfeg.noaa.gov/erddap',
         'CeNCOOS': 'http://erddap.axiomalaska.com/erddap',
         'IFREMER': 'http://www.ifremer.fr/erddap',
         'MDA': 'https://bluehub.jrc.ec.europa.eu/erddap',
         'MII': 'http://erddap.marine.ie/erddap',
         'NCEI': 'http://ecowatch.ncddc.noaa.gov/erddap',
         'NERACOOS': 'http://www.neracoos.org/erddap',
         'NGDAC': 'http://data.ioos.us/gliders/erddap',
         'ONC': 'http://dap.onc.uvic.ca/erddap',
         'OSMC': 'http://osmc.noaa.gov/erddap',
         'PacIOOS': 'http://oos.soest.hawaii.edu/erddap',
         'RTECH': 'http://meteo.rtech.fr/erddap',
         'SECOORA': 'http://erddap.secoora.org/erddap',
         'UAF': 'https://upwell.pfeg.noaa.gov/erddap'
         'UBC': 'https://salishsea.eos.ubc.ca/erddap'}

    """
    def __init__(self, server, dataset_id=None, protocol=None, variables='',
                 response='html', constraints=None, params=None, requests_kwargs=None):
        if server in servers.keys():
            server = servers[server].url
        self.server = _check_url_response(server)
        self.dataset_id = dataset_id
        self.protocol = protocol
        self.variables = variables
        self.response = response
        self.constraints = constraints
        self.params = params
        self.requests_kwargs = requests_kwargs if requests_kwargs else {}

        # Caching the last `dataset_id` request for quicker multiple accesses,
        # will be overridden when requesting a new `dataset_id`.
        self._dataset_id = None
        self._variables = {}

    def get_search_url(self, response=None, search_for=None, items_per_page=1000, page=1, **kwargs):
        response = response if response else self.response
        return search_url(
            server=self.server,
            response=response,
            search_for=search_for,
            items_per_page=items_per_page,
            page=page,
            **kwargs
            )

    def get_info_url(self, dataset_id=None, response=None):
        dataset_id = dataset_id if dataset_id else self.dataset_id
        response = response if response else self.response

        if not dataset_id:
            raise ValueError('You must specify a valid dataset_id, got {}'.format(self.dataset_id))

        return info_url(
            server=self.server,
            dataset_id=dataset_id,
            response=response
            )

    def get_download_url(self, dataset_id=None, protocol=None,
                         variables=None, response=None, constraints=None):
        dataset_id = dataset_id if dataset_id else self.dataset_id
        protocol = protocol if protocol else self.protocol
        variables = variables if variables else self.variables
        response = response if response else self.response
        constraints = constraints if constraints else self.constraints

        if not dataset_id:
            raise ValueError('Please specify a valid `dataset_id`, got {}'.format(self.dataset_id))

        if not protocol:
            raise ValueError('Please specify a valid `protocol`, got {}'.format(self.protocol))

        return download_url(
            server=self.server,
            dataset_id=dataset_id,
            protocol=protocol,
            variables=variables,
            response=response,
            constraints=constraints,
            )

    def to_pandas(self, **kw):
        """Save a data request to a pandas.DataFrame.

        Accepts any `pandas.read_csv` keyword arguments.

        """
        url = self.get_download_url(response='csv')
        return pd.read_csv(urlopen(url, params=self.params, **self.requests_kwargs), **kw)

    def to_xarray(self, **kw):
        """Save a data request to a xarray.Dataset.

        Accepts any `xr.open_dataset` keyword arguments.
        """
        import xarray as xr
        from tempfile import NamedTemporaryFile
        url = self.get_download_url(response='nc')
        data = urlopen(url, params=self.params, **self.requests_kwargs).read()
        with NamedTemporaryFile(suffix='.nc', prefix='erddapy_') as tmp:
            tmp.write(data)
            tmp.flush()
            return xr.open_dataset(tmp.name, **kw)

    def get_var_by_attr(self, dataset_id=None, **kwargs):
        """Similar to netCDF4-python `get_variables_by_attributes` for an ERDDAP
        `dataset_id`.

        The `get_var_by_attr` method will create an info `csv` return,
        for the `dataset_id`, and the variables attribute dictionary.

        Examples:
            >>> e = ERDDAP(server_url='https://data.ioos.us/gliders/erddap')
            >>> dataset_id = 'whoi_406-20160902T1700'

            Get variables with x-axis attribute.

            >>> e.get_var_by_attr(dataset_id, axis='X')
            ['longitude']

            Get variables with matching "standard_name" attribute

            >>> e.get_var_by_attr(dataset_id, standard_name='northward_sea_water_velocity')
            ['v']

            Get Axis variables

            >>> e.get_var_by_attr(dataset_id, axis=lambda v: v in ['X', 'Y', 'Z', 'T'])
            ['latitude', 'longitude', 'time', 'depth']

        """
        if not dataset_id:
            dataset_id = self.dataset_id
        url = info_url(self.server, dataset_id=dataset_id, response='csv')

        # Creates the variables dictionary for the `get_var_by_attr` lookup.
        if not self._variables or dataset_id != self._dataset_id:
            variables = {}
            _df = pd.read_csv(urlopen(url, params=self.params, **self.requests_kwargs))
            self._dataset_id = dataset_id
            for variable in set(_df['Variable Name']):
                attributes = _df.loc[
                    _df['Variable Name'] == variable, ['Attribute Name', 'Value']
                ].set_index('Attribute Name').to_dict()['Value']
                variables.update({variable: attributes})
                self._variables = variables
        # Virtually the same code as the netCDF4 counterpart.
        vs = []
        has_value_flag = False
        for vname in self._variables:
            var = self._variables[vname]
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
