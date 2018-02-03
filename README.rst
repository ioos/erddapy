.. image:: https://travis-ci.org/pyoceans/erddapy.svg?branch=master
   :target: https://travis-ci.org/pyoceans/erddapy

.. image:: https://anaconda.org/conda-forge/erddapy/badges/version.svg
   :target: https://anaconda.org/conda-forge/erddapy

.. image:: https://img.shields.io/pypi/v/erddapy.svg
   :target: https://pypi.python.org/pypi/erddapy/

.. image:: https://img.shields.io/pypi/l/erddapy.svg
   :target: https://pypi.python.org/pypi/erddapy/

.. image:: https://zenodo.org/badge/104919828.svg
   :target: https://zenodo.org/badge/latestdoi/104919828


erddapy: ERDDAP + Python
========================

Easier access to scientific data.

erddapy takes advantage of ERDDAP's RESTful web services and creates the ERDDAP URL for any request,
like searching for datasets, acquiring metadata, downloading the data, etc.


Example
-------

.. code:: python

    from erddapy import ERDDAP
    import pandas as pd


    constraints = {
        'time>=': '2016-07-10T00:00:00Z',
        'time<=': '2017-02-10T00:00:00Z',
        'latitude>=': 38.0,
        'latitude<=': 41.0,
        'longitude>=': -72.0,
        'longitude<=': -69.0,
    }

    variables = [
    'depth',
    'latitude',
    'longitude',
    'salinity',
    'temperature',
    'time',
    ]

    e = ERDDAP(
        server='https://data.ioos.us/gliders/erddap',
        protocol='tabledap',
        response='csv',
        dataset_id='blue-20160818T1448',
        constraints=constraints,
        variables=variables,
    )

    df = e.to_pandas()


What is ERDDAP?
---------------

ERDDAP unifies the different types of data servers and offers a consistent way to get the data in multiple the formats.
