from erddapy.erddapy import ERDDAP, servers

__all__ = ["ERDDAP", "servers"]

from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions
