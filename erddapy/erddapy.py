# -*- coding: utf-8 -*-

from __future__ import (absolute_import, division, print_function)

from urllib.parse import quote_plus


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
        self.search_options = {}
        self.download_options = {}
        # Caching the last request for quicker multiple access,
        # will be overridden when making a new requested.
        self._nc = None
        self._dataset_id = None
        self._variables = {}

    def search_url(self, response='csv', search_for=None, items_per_page=1000, page=1, **kwargs):
        """Compose the search URL for the `server_url` endpoint.

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
                tabledap: http://coastwatch.pfeg.noaa.gov/erddap/tabledap/documentation.html
                griddap: http://coastwatch.pfeg.noaa.gov/erddap/griddap/documentation.html
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
        return base.format(**search_options)

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

    def download_url(self, dataset_id, variables, response='csv', protocol='tabledap', **kwargs):
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
        self.download_options.update(kwargs)
        variables = ','.join(variables)
        base = (
            '{server_url}/{protocol}/{dataset_id}.{response}'
            '?{variables}'
            '{kwargs}'
        ).format

        kwargs = ''.join(['&{}{}'.format(k, v) for k, v in kwargs.items()])
        return base(server_url=self.server_url, protocol=protocol,
                    dataset_id=dataset_id, response=response, variables=variables,
                    kwargs=self.download_options)

    def opendap_url(self, dataset_id, protocol='tabledap'):
        """Compose the opendap URL for the `server_url` the endpoint.

        Args:
            dataset_id (str): a dataset unique id.
        Returns:
            download_url (str): the download URL for the `response` chosen.
        """
        base = '{server_url}/{protocol}/{dataset_id}'.format
        return base(
            server_url=self.server_url,
            protocol=protocol,
            dataset_id=dataset_id
            )

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
        # FIXME: try to make the variables dict with the json response to make
        # erddapy run with no dependencyes.
        try:
            import pandas as pd
        except ImportError:
            raise ImportError('pandas is needed to use `get_var_by_attr`.')
        info_url = self.info_url(dataset_id, response='csv')

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



def urlopen(url):
    import io
    import requests
    return io.BytesIO(requests.get(url).content)


def open_dataset(url, **kwargs):
    import xarray as xr
    from tempfile import NamedTemporaryFile
    data = urlopen(url).read()
    with NamedTemporaryFile(suffix='.nc', prefix='erddapy_') as tmp:
        tmp.write(data)
        tmp.flush()
        return xr.open_dataset(tmp.name, **kwargs)
