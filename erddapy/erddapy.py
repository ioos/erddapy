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
        >>> e = ERDDAP(server_url='https://data.ioos.us/gliders/erddap')

    """
    def __init__(self, server_url):
        self.server_url = server_url
        # Caching the last resquest for multiple access,
        # will be overriden when a new variable is requested.
        self._nc = None
        self._dataset_id = None
        self.search_url = None
        self._search_options = {}
        self.info_url = None
        self._info_options = {}
        self._variables = {}

    def search(self, items_per_page=1000, page=1, response='csv', **kwargs):
        """Compose the search URL for `server_url` endpoint.

        Args:
            items_per_page (int): how many items per page in the return,
                default is 1000.
            page (int): which page to display, defatul is the first page (1).
            response (str): check https://data.ioos.us/gliders/erddap/tabledap/documentation.html#fileType
                for all the options, default is a Comma Separated Value ('csv').
        Returns:
            search_url (str): the search URL for the `response` chosen.
                The `search_url` is cached as an instance property,
                but will be overriden when performing a new search.
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
        if not self.search_url or (self._search_options.items() != search_options):
            self.search_url = base(**search_options)
            self._search_options = search_options
        return self.search_url

    def info(self, dataset_id, response='csv'):
        """Compose the info URL for `server_url` endpoint.

        Args:
            dataset_id (str): Use the data id found using the the search.
            response (str): check https://data.ioos.us/gliders/erddap/tabledap/documentation.html#fileType
                for all the options, default is a Comma Separated Value ('csv').
        Returns:
            info_url (str): the info URL for the `response` chosen.
                The `info_url` is cached as an instance property,
                but will be overriden when performing a new info request.
        """
        response = _clean_response(response)
        base = '{server_url}/info/{dataset_id}/index.{response}'.format
        info_options = {
            'server_url': self.server_url,
            'dataset_id': dataset_id,
            'response': response
        }
        if not self.info_url or (self._info_options.items() != info_options):
            self.info_url = base(**info_options)
            self._info_options = info_options
        return self.info_url

    def opendap(self, dataset_id):
        base = '{server_url}/tabledap/{dataset_id}'.format
        self.opendap_url = base(server_url=self.server_url, dataset_id=dataset_id)
        return self.opendap_url

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
        info_url = self.info(dataset_id, response='csv')
        df = pd.read_csv(info_url)

        # Creates the variables dictionary for the `get_var_by_attr` lookup.
        if not self._variables or self._dataset_id != dataset_id:
            variables = {}
            # Virtually the same code as the netCDF4 counterpart.
            for variable in set(df['Variable Name']):
                attributes = df.loc[
                    df['Variable Name'] == variable,
                    ['Attribute Name', 'Value']
                ].set_index('Attribute Name').to_dict()['Value']
                variables.update({variable: attributes})
                self._variables = variables
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
