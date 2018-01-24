import io
from collections import namedtuple
from tempfile import NamedTemporaryFile

import requests


def _urlopen(url):
    """Thin wrapper around requests get content."""
    return io.BytesIO(requests.get(url).content)


def open_dataset(url, **kwargs):
    """
    Load data as a xarray dataset from the .nc response.

    Caveat: this downloads a temporary file! Be careful with the size of the request.

    """
    import xarray as xr
    data = _urlopen(url).read()
    with NamedTemporaryFile(suffix='.nc', prefix='erddapy_') as tmp:
        tmp.write(data)
        tmp.flush()
        return xr.open_dataset(tmp.name, **kwargs)


_server = namedtuple('server', ['description', 'url'])
servers = {
    'MDA': _server(
        'Marine Domain Awareness (MDA) - Italy',
        'https://bluehub.jrc.ec.europa.eu/erddap'
    ),
    'MII': _server(
        'Marine Institute - Ireland',
        'http://erddap.marine.ie/erddap'
    ),
    'CSCGOM': _server(
        'CoastWatch Caribbean/Gulf of Mexico Node',
        'http://cwcgom.aoml.noaa.gov/erddap'
    ),
    'CSWC': _server(
        'CoastWatch West Coast Node',
        'https://coastwatch.pfeg.noaa.gov/erddap'
    ),
    'CeNCOOS': _server(
        'NOAA IOOS CeNCOOS (Central and Northern California Ocean Observing System)',
        'http://erddap.axiomalaska.com/erddap'
    ),
    'NERACOOS': _server(
        'NOAA IOOS NERACOOS (Northeastern Regional Association of Coastal and Ocean Observing Systems)',
        'http://www.neracoos.org/erddap'
    ),
    'NGDAC': _server(
        'NOAA IOOS NGDAC (National Glider Data Assembly Center)',
        'http://data.ioos.us/gliders/erddap'
    ),
    'PacIOOS': _server(
        'NOAA IOOS PacIOOS (Pacific Islands Ocean Observing System) at the University of Hawaii (UH)',
        'http://oos.soest.hawaii.edu/erddap'
    ),
    'SECOORA': _server(
        'NOAA IOOS SECOORA (Southeast Coastal Ocean Observing Regional Association)',
        'http://erddap.secoora.org/erddap'
    ),
    'NCEI': _server(
        'NOAA NCEI (National Centers for Environmental Information) / NCDDC',
        'http://ecowatch.ncddc.noaa.gov/erddap'
    ),
    'OSMC': _server(
        'NOAA OSMC (Observing System Monitoring Center)',
        'http://osmc.noaa.gov/erddap'
    ),
    'UAF': _server(
        'NOAA UAF (Unified Access Framework)',
        'https://upwell.pfeg.noaa.gov/erddap'
    ),
    'ONC': _server(
        'ONC (Ocean Networks Canada)',
        'http://dap.onc.uvic.ca/erddap'
    ),
    'BMLSC': _server(
        'UC Davis BML (University of California at Davis, Bodega Marine Laboratory)',
        'http://bmlsc.ucdavis.edu:8080/erddap'
    ),
    'RTECH': _server(
        'R.Tech Engineering',
        'http://meteo.rtech.fr/erddap'
    ),
    'IFREMER': _server(
        'French Research Institute for the Exploitation of the Sea',
        'http://www.ifremer.fr/erddapindex.html'
    ),
}
