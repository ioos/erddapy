from erddapy.doc_helpers import show_iframe
import copy
import functools
from datetime import datetime
from typing import Dict,List,Optional,Tuple,Union
from urllib.parse import quote_plus
import pandas as pd
import pytz
from erddapy.netcdf_handling import _nc_dataset,_tempnc
from erddapy.servers import servers
from erddapy.url_handling import _distinct,urlopen
try:
    from pandas.core.indexes.period import parse_time_string
except ImportError:
    from pandas._libs.tslibs.parsing import parse_time_string