# -*- coding: utf-8 -*-

from __future__ import (absolute_import, division, print_function)

from erddapy.erddapy import ERDDAP

__all__ = ['ERDDAP']

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
