erddapy: ERDDAP servers + Python data
=====================================

What is erddapy?
----------------

``erddapy`` takes advantage of ERDDAP's RESTful web services to create the ERDDAP URLs.
The users can create virtually any request like,
searching for datasets, acquiring metadata, downloading data, etc.

What is ERDDAP?
---------------

ERDDAP is a data server that provides a consistent way to download subsets of scientific datasets.

There are many scientific data server available,
like OPeNDAP, WCS, SOS, OBIS, etc.
They all have their advantages and disadvantages,
ERDDAP goal is to fill the gaps and unify most of the advantages in a single service.
The main advantages of ERDDAP are:

- offers an easy-to-use, consistent way to request data via the OPeNDAP;
- returns data in the common file format of your choice,
  .html table, ESRI .asc and .csv, Google Earth .kml, OPeNDAP binary, .mat, .nc, ODV .txt, .csv, .tsv, .json, and .xhtml,
  and even some image formats (.png and .pdf);
- standardizes the date-times where string times are always ISO 8601:2004(E) and
  numeric times are always "seconds since 1970-01-01T00:00:00Z";
- acts as a middleman by reformatting the request into the format required by the remote server,
  and reformats the data into the format that you requested.


See https://erddap.github.io/ for more information.


.. toctree::
   :maxdepth: 3
   :caption: Contents:

   00-quick_intro-output.ipynb
   01a-griddap-output.ipynb
   01b-tabledap-output.ipynb
   02-extras-output.ipynb
   03-advanced_search-output.ipynb
   erddapy

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
