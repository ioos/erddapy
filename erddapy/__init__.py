from __future__ import (absolute_import, division, print_function)

from erddapy.erddapy import ERDDAP
from erddapy.utilities import open_dataset

__all__ = [
    'ERDDAP',
    'open_dataset',
]

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
