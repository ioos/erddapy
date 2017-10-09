from __future__ import (absolute_import, division, print_function)


import requests


def _clean_response(response):
    """Allow for `ext` or `.ext` format.

    The user can, for example,
    use either `.csv` or `csv` in the response kwarg.

    """
    return response.lstrip('.')


def _check_url_response(url):
    r = requests.head(url)
    r.raise_for_status()


def _urlopen(url):
    import io
    import requests
    return io.BytesIO(requests.get(url).content)


def open_dataset(url, **kwargs):
    import xarray as xr
    from tempfile import NamedTemporaryFile
    data = _urlopen(url).read()
    with NamedTemporaryFile(suffix='.nc', prefix='erddapy_') as tmp:
        tmp.write(data)
        tmp.flush()
        return xr.open_dataset(tmp.name, **kwargs)
