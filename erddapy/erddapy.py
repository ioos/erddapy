# -*- coding: utf-8 -*-

from __future__ import (absolute_import, division, print_function)

import pandas as pd


def _clean_response(response):
    """Allow for extension or .extension format.

    The user can, for example,
    use either `.csv` or `csv` in the response kwarg.

    """
    return response.lstrip('.')


class ERDDAP(object):
    """Returns a ERDDAP instance for the user defined server endpoint.

    Different from the R version there are no hard-coded servers!
    The user must pass the endpoint explicitly!!

    Examples:
        >>> e = ERDDAP(
        ...     server_url='https://data.ioos.us/gliders/erddap',
        ...     server_type='tabledap'
        ... )

    """
    def __init__(self, server_url, server_type='tabledap'):
        self.server_url = server_url
        self.server_type = server_type
        # Caching the last request for quicker multiple access,
        # will be overridden when making a new requested.
        self._nc = None
        self._dataset_id = None
        self._variables = {}

    def search_url(self, items_per_page=1000, page=1, response='csv', **kwargs):
        """Compose the search URL for the `server_url` endpoint.

        Args:
            items_per_page (int): how many items per page in the return,
                default is 1000.
            page (int): which page to display, defatul is the first page (1).
            response (str): default is a Comma Separated Value ('csv').
                See ERDDAP docs for all the options,
                tabledap: http://coastwatch.pfeg.noaa.gov/erddap/tabledap/documentation.html
                griddap: http://coastwatch.pfeg.noaa.gov/erddap/griddap/documentation.html
        Returns:
            search_url (str): the search URL for the `response` chosen.
        """
        base = (
            '{server_url}/search/advanced.{response}'
            '?page={page}'
            '&itemsPerPage={itemsPerPage}'
            '&searchFor='
            '&protocol={protocol}'
            '&cdm_data_type={cdm_data_type}'
            '&institution={institution}'
            '&ioos_category={ioos_category}'
            '&keywords={keywords}'
            '&long_name={long_name}'
            '&standard_name={standard_name}'
            '&variableName={variableName}'
            '&minLon={minLon}'
            '&maxLon={maxLon}'
            '&minLat={minLat}'
            '&maxLat={maxLat}'
            '&minTime={minTime}'
            '&maxTime={maxTime}'
        ).format

        default = '(ANY)'
        response = _clean_response(response)
        search_options = {
            'server_url': self.server_url,
            'response': response,
            'page': page,
            'itemsPerPage': items_per_page,
            'protocol': kwargs.get('protocol', default),
            'cdm_data_type': kwargs.get('cdm_data_type', default),
            'institution': kwargs.get('institution', default),
            'ioos_category': kwargs.get('ioos_category', default),
            'keywords': kwargs.get('keywords', default),
            'long_name': kwargs.get('long_name', default),
            'standard_name': kwargs.get('standard_name', default),
            'variableName': kwargs.get('variableName', default),
            'minLon': kwargs.get('min_lon', default),
            'maxLon': kwargs.get('max_lon', default),
            'minLat': kwargs.get('min_lat', default),
            'maxLat': kwargs.get('max_lat', default),
            'minTime': kwargs.get('min_time', default),
            'maxTime': kwargs.get('max_time', default),
        }
        return base(**search_options)

    def info_url(self, dataset_id, response='csv'):
        """Compose the info URL for the `server_url` endpoint.

        Args:
            dataset_id (str): a dataset unique id.
            response (str): default is a Comma Separated Value ('csv').
                See ERDDAP docs for all the options,
                tabledap: http://coastwatch.pfeg.noaa.gov/erddap/tabledap/documentation.html
                griddap: http://coastwatch.pfeg.noaa.gov/erddap/griddap/documentation.html
        Returns:
            info_url (str): the info URL for the `response` chosen.
        """
        response = _clean_response(response)
        base = '{server_url}/info/{dataset_id}/index.{response}'.format
        info_options = {
            'server_url': self.server_url,
            'dataset_id': dataset_id,
            'response': response
        }
        return base(**info_options)

    def download_url(self, dataset_id, variables, response='csv', **kwargs):
        """Compose the download URL for the `server_url` endpoint.

        Args:
            dataset_id (str): a dataset unique id.
            variables (list/tuple): a list of the variables to download.
            response (str): default is a Comma Separated Value ('csv').
                See ERDDAP docs for all the options,
                tabledap: http://coastwatch.pfeg.noaa.gov/erddap/tabledap/documentation.html
                griddap: http://coastwatch.pfeg.noaa.gov/erddap/griddap/documentation.html
        Returns:
            download_url (str): the download URL for the `response` chosen.
        """
        variables = ','.join(variables)
        base = (
            '{server_url}/{server_type}/{dataset_id}.{response}'
            '?{variables}'
            '{kwargs}'
        ).format

        kwargs = ''.join(['&{}{}'.format(k, v) for k, v in kwargs.items()])
        return base(server_url=self.server_url, server_type=self.server_type,
                    dataset_id=dataset_id, response=response, variables=variables,
                    kwargs=kwargs)

    def opendap_url(self, dataset_id):
        """Compose the opendap URL for the `server_url` the endpoint.

        Args:
            dataset_id (str): a dataset unique id.
        Returns:
            download_url (str): the download URL for the `response` chosen.
        """
        base = '{server_url}/{server_type}/{dataset_id}'.format
        return base(
            server_url=self.server_url,
            server_type=self.server_type,
            dataset_id=dataset_id
            )

    def get_var_by_attr(self, dataset_id, **kwargs):
        """Similar to netCDF4-python `get_variables_by_attributes` for a ERDDAP
        `dataset_id`.

        The `get_var_by_attr` method will create an info `csv` return,
        for the `dataset_id`, and the variables attribute dictionary.

        Examples:
            >>> # Get variables with x-axis attribute.
            >>> vs = nc.get_variables_by_attributes(axis='X')

            >>> # Get variables with matching "standard_name" attribute
            >>> vs = nc.get_variables_by_attributes(standard_name='northward_sea_water_velocity')

            >>> # Get Axis variables
            >>> vs = nc.get_variables_by_attributes(axis=lambda v: v in ['X', 'Y', 'Z', 'T'])

            >>> # Get variables that don't have an "axis" attribute
            >>> vs = nc.get_variables_by_attributes(axis=lambda v: v is None)

            >>> # Get variables that have a "grid_mapping" attribute
            >>> vs = nc.get_variables_by_attributes(grid_mapping=lambda v: v is not None)
        """
        info_url = self.info_url(dataset_id, response='csv')

        # Creates the variables dictionary for the `get_var_by_attr` lookup.
        if not self._variables or self._dataset_id != dataset_id:
            _df = pd.read_csv(info_url)
            self._dataset_id = dataset_id
            variables = {}
            for variable in set(_df['Variable Name']):
                attributes = _df.loc[
                    _df['Variable Name'] == variable,
                    ['Attribute Name', 'Value']
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
