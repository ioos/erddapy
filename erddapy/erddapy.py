from __future__ import (absolute_import, division, print_function)

from urllib.parse import quote_plus

from erddapy.utilities import _check_url_response, _clean_response


class ERDDAP(object):
    """Returns a ERDDAP instance for the user defined server endpoint.

    Different from the `R` package there are no hard-coded servers in `erddapy`!
    The user must pass an endpoint explicitly!!

    Examples:
        >>> e = ERDDAP(server_url='https://data.ioos.us/gliders/erddap')

    """
    def __init__(self, server_url):
        _check_url_response(server_url)
        self.server_url = server_url
        self.search_options = {}
        self.download_options = {}
        # Caching the last request for quicker multiple access,
        # will be overridden when making a new requested.
        self._nc = None
        self._dataset_id = None
        self._variables = {}

    def get_search_url(self, response='csv', search_for=None, items_per_page=1000, page=1, **kwargs):
        """Compose the search URL for the `server_url` endpoint provided.

        Args:
            search_for (str): "Google-like" search
                - This is a Google-like search of the datasets' metadata:
                  Type the words you want to search for, with spaces between the words.
                  ERDDAP will search for the words separately, not as a phrase.
                - To search for a phrase, put double quotes around the phrase
                  (for example, `"wind speed"`).
                - To exclude datasets with a specific word, use `-excludedWord`.
                - To exclude datasets with a specific phrase, use `-"excluded phrase"`
                - Searches are not case-sensitive.
                - To find just grid or just table datasets, include `protocol=griddap`
                  or `protocol=tabledap` in your search.
                - You can search for any part of a word. For example,
                  searching for `spee` will find datasets with `speed` and datasets with
                  `WindSpeed`
                - The last word in a phrase may be a partial word. For example,
                  to find datasets from a specific website (usually the start of the datasetID),
                  include (for example) `"datasetID=erd"` in your search.
            response (str): default is a Comma Separated Value ('csv').
                See ERDDAP docs for all the options,
                griddap: http://coastwatch.pfeg.noaa.gov/erddap/griddap/documentation.html
                tabledap: http://coastwatch.pfeg.noaa.gov/erddap/tabledap/documentation.html
            items_per_page (int): how many items per page in the return,
                default is 1000.
            page (int): which page to display, defatul is the first page (1).
        Returns:
            search_url (str): the search URL for the `response` chosen.
        """
        base = (
            '{server_url}/search/advanced.{response}'
            '?page={page}'
            '&itemsPerPage={itemsPerPage}'
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
            )
        if search_for:
            search_for = quote_plus(search_for)
            base += '&searchFor={searchFor}'

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
            'searchFor': search_for,
        }
        self.search_options.update(search_options)
        search_url = base.format(**search_options)
        _check_url_response(search_url)
        return search_url

    def get_info_url(self, dataset_id, response='csv'):
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
        info_url = base(**info_options)
        _check_url_response(info_url)
        return info_url

    def get_download_url(self, dataset_id, variables, response='csv', protocol='tabledap', **kwargs):
        """Compose the download URL for the `server_url` endpoint.

        Args:
            dataset_id (str): a dataset unique id.
            variables (list/tuple): a list of the variables to download.
            response (str): default is a Comma Separated Value ('csv').
                See ERDDAP docs for all the options,
                griddap: http://coastwatch.pfeg.noaa.gov/erddap/griddap/documentation.html
                tabledap: http://coastwatch.pfeg.noaa.gov/erddap/tabledap/documentation.html
        Returns:
            download_url (str): the download URL for the `response` chosen.
        """
        self.download_options.update(kwargs)
        variables = ','.join(variables)
        base = (
            '{server_url}/{protocol}/{dataset_id}.{response}'
            '?{variables}'
            '{kwargs}'
        ).format

        kwargs = ''.join(['&{}{}'.format(k, v) for k, v in self.download_options.items()])
        download_url = base(
            server_url=self.server_url,
            protocol=protocol,
            dataset_id=dataset_id,
            response=response,
            variables=variables,
            kwargs=kwargs
        )
        _check_url_response(download_url)
        return download_url

    def get_opendap_url(self, dataset_id, protocol='tabledap'):
        """Compose the opendap URL for the `server_url` the endpoint.

        Args:
            dataset_id (str): a dataset unique id.
        Returns:
            download_url (str): the download URL for the `response` chosen.
        """
        base = '{server_url}/{protocol}/{dataset_id}'.format
        opendap_url = base(
            server_url=self.server_url,
            protocol=protocol,
            dataset_id=dataset_id
            )
        _check_url_response(opendap_url)
        return opendap_url

    def get_var_by_attr(self, dataset_id, **kwargs):
        """Similar to netCDF4-python `get_variables_by_attributes` for a ERDDAP
        `dataset_id`.

        The `get_var_by_attr` method will create an info `csv` return,
        for the `dataset_id`, and the variables attribute dictionary.

        Examples:
            >>> e = ERDDAP(server_url='https://data.ioos.us/gliders/erddap')
            >>> dataset_id = 'whoi_406-20160902T1700'

            >>> # Get variables with x-axis attribute.
            >>> vs = e.get_var_by_attr(dataset_id, axis='X')

            >>> # Get variables with matching "standard_name" attribute
            >>> vs = e.get_var_by_attr(dataset_id, standard_name='northward_sea_water_velocity')

            >>> # Get Axis variables
            >>> vs = e.get_var_by_attr(dataset_id, axis=lambda v: v in ['X', 'Y', 'Z', 'T'])

            >>> # Get variables that don't have an "axis" attribute
            >>> vs = e.get_var_by_attr(dataset_id, axis=lambda v: v is None)

            >>> # Get variables that have a "grid_mapping" attribute
            >>> vs = e.get_var_by_attr(dataset_id, grid_mapping=lambda v: v is not None)
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError('pandas is needed to use `get_var_by_attr`.')
        info_url = self.get_info_url(dataset_id, response='csv')

        # Creates the variables dictionary for the `get_var_by_attr` lookup.
        if not self._variables or self._dataset_id != dataset_id:
            _df = pd.read_csv(info_url)
            self._dataset_id = dataset_id
            variables = {}
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
