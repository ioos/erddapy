"""
This module contains opinionated, higher-level objects for searching servers and accessing datasets.

It is named 'objects' after object-relational mapping, which is the concept of having an object-oriented
layer between a database (in this case, ERDDAP), and the programming language.
"""


from .array_like import (
    ERDDAPConnection,
    ERDDAPDataset,
    ERDDAPServer,
    GridDataset,
    TableDataset,
)

__all__ = [
    "ERDDAPDataset",
    "ERDDAPConnection",
    "ERDDAPServer",
    "TableDataset",
    "GridDataset",
]
