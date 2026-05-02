xarray backend
==============

``erddapy`` registers an ``xarray`` **backend** so you can open ERDDAP
dataset URLs with ``xarray.open_dataset`` using ``engine="erddap"``.

ERDDAP can serve netCDF-like responses (``.nc``, ``.ncCF``, ``.ncCFMA``) and
classic OPeNDAP views of the same datasets. The backend normalizes that
difference: it validates the URL, picks the appropriate download path, and
delegates to :func:`erddapy.core.interfaces.to_xarray`.

Requirements
------------

You need ``xarray`` (and its IO stack, typically ``netCDF4`` or ``h5netcdf``)
installed in the same environment as ``erddapy``. The development environment
in ``requirements-dev.txt`` already includes these dependencies.

Quick start
-----------

Use any ERDDAP **GridDAP** or **TableDAP** URL that ends with a netCDF-style
suffix or that you would otherwise treat as an OPeNDAP endpoint:

.. code-block:: python

   import xarray as xr

   url = (
       "https://gliders.ioos.us/erddap/tabledap/whoi_406-20160902T1700.nc"
       "?time,latitude,longitude,temperature"
       "&time>=2016-07-10T00:00:00Z"
       "&time<=2016-07-15T00:00:00Z"
   )
   ds = xr.open_dataset(url, engine="erddap")

You can also call :func:`~erddapy.xarray_erddap.open_erddap_dataset` directly
if you prefer not to pass ``engine``:

.. code-block:: python

   from erddapy.xarray_erddap import open_erddap_dataset

   ds = open_erddap_dataset(url)

URLs must be HTTP or HTTPS and contain ``/erddap/`` in the path. If the URL is
not a netCDF-like response, the backend strips netCDF query fragments, converts
the path to the OPeNDAP form, and opens it with ``response="opendap"``.

Relationship to :class:`~erddapy.erddapy.ERDDAP`
------------------------------------------------

The :class:`~erddapy.erddapy.ERDDAP` class helps you build constrained URLs and
download data in several formats. The xarray backend is a thin layer on top of
that stack for the common case “I already have an ERDDAP URL and want an
``xarray.Dataset``”.

API reference
-------------

.. automodule:: erddapy.xarray_erddap
   :members:
   :show-inheritance:
