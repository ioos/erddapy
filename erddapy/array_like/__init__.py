"""
This module contains opinionated, higher-level objects for searching servers and accessing datasets.

It is named 'objects' after object-relational mapping, which is the concept of having an object-oriented
layer between a database (in this case, ERDDAP), and the programming language.
"""


from .connection import ERDDAPConnection
from .datasets import ERDDAPDataset, GridDataset, TableDataset
from .server import ERDDAPServer

__all__ = [
    "ERDDAPDataset",
    "ERDDAPConnection",
    "ERDDAPServer",
    "TableDataset",
    "GridDataset",
]
