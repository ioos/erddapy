from __future__ import (absolute_import, division, print_function)

from erddapy.erddapy import ERDDAP
from erddapy.extras import open_dataset, servers

__all__ = [
    'ERDDAP',
    'open_dataset',
    'servers',
]

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
