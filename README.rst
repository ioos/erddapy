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

What is ERDDAP?
---------------

The Environmental Research Division's Data Access Program (ERDDAP)
is a data server that provides a consistent way to download subsets of scientific datasets.

There are many scientific data server available, like OPeNDAP, WCS, SOS, OBIS, etc.
The main advantages of using ERDDAP are:

- offers an easy-to-use, consistent way to request data via the OPeNDAP;
- returns data in the common file format of your choice,
  .html table, ESRI .asc and .csv, Google Earth .kml, OPeNDAP binary, .mat, .nc, ODV .txt, .csv, .tsv, .json, and .xhtml,
  and even some image formats (.png and .pdf);
- standardizes the datetimes where string times are always ISO 8601:2004(E) and
  numeric times are always uses "seconds since 1970-01-01T00:00:00Z";
- acts as a middleman by reformatting the request into the format required by the remote server,
  and reformats the data into the format that you requested.


TL;DR ERDDAP unifies the different types of data servers and offers a consistent way to get the data in multiple the formats.


See https://coastwatch.pfeg.noaa.gov/erddap/index.html for more information.
