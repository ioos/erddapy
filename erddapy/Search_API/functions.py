from imports import *

ListLike = Union[List[str],Tuple[str]]
OptionalStr = Optional[str]

def _quote_string_constraints(kwargs:Dict)->Dict:
    return {k:f'"{v}"' if isinstance(v,str) else v for k,v in kwargs.items()}

def _format_constraints_url(kwargs:Dict)->str:
    return "".join([f"&{k}{v}" for k,v in kwargs.items()])

def parse_dates(date_time: Union[datetime,str])->float:
    if isinstance(date_time,str):
        parse_date_time = parse_time_string(date_time)[0]
    else:
        parse_date_time = date_time

    if not parse_date_time.tzinfo:
        parse_date_time = pytz.utc.localize(parse_date_time)
    else:
        parse_date_time = parse_date_time.astimezone(pytz.utc)
    return parse_date_time.timestamp()

    


    