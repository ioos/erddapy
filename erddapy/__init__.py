import pkg_resources

from erddapy.erddapy import ERDDAP, servers


__all__ = ["ERDDAP", "servers"]

try:
    __version__ = pkg_resources.get_distribution("erddapy").version
except Exception:
    __version__ = "unknown"
