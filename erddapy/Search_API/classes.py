from imports import *
from functions import *

class ERDDAP:
    def __init__(
        self,
        server: str,
        protocol: OptionalStr = None,
        response: str = "html",
    ):
        if server in servers.keys():
            server = servers[server].url
        self.server = server.rstrip("/")
        self.protocol = protocol
        self.response = response

        self.constraints: Optional[Dict] = None
        self.relative_constraints: Optional[Dict] = None
        self.server_functions: Optional[Dict] = None
        self.dataset_id: OptionalStr = None
        self.requests_kwargs: Dict = {}
        self.auth: Optional[tuple] = None
        self.variables: Optional[ListLike] = None

        self._dataset_id: OptionalStr = None
        self._variables: Dict = {}

    def get_search_url(
        self,
        response: OptionalStr = None,
        search_for: OptionalStr = None,
        protocol: OptionalStr = None,
        items_per_page: int = 1000,
        page: int = 1,
        **kwargs,
    ) -> str:

        base = (
            "{server}/{protocol}/{dataset_id}.{response}"
            "?cdm_data_type={cdm_data_type}"
            "&institution={institution}"
            "&ioos_category={ioos_category}"
            "&keywords={keywords}"
            "&long_name={long_name}"
            "&standard_name={standard_name}"
            "&variableName={variableName}"
            "&Lat={Lat}"
            "&Lon={Lon}"
            "&Time={Time}"
        )
        if search_for:
            search_for = quote_plus(search_for)
            base += "&searchFor={searchFor}"

        # Convert dates from datetime to `seconds since 1970-01-01T00:00:00Z`.
        Time = kwargs.pop("Time", None)
        if Time:
            kwargs.update({"Time": parse_dates(Time)})

        protocol = protocol if protocol else self.protocol
        response = response if response else self.response
        if protocol:
            kwargs.update({"protocol": protocol})

        lower_case_search_terms = (
            "cdm_data_type",
            "institution",
            "ioos_category",
            "keywords",
            "long_name",
            "standard_name",
            "variableName",
        )
        for search_term in lower_case_search_terms:
            if search_term in kwargs.keys():
                lowercase = kwargs[search_term].lower()
                kwargs.update({search_term: lowercase})

        default = "(ANY)"

        url = base.format(
            server=self.server,
            protocol=kwargs.get("protocol",default),
            dataset_id=kwargs.get("dataset_id",default),
            response=response,
            page=page,
            cdm_data_type=kwargs.get("cdm_data_type", default),
            institution=kwargs.get("institution", default),
            ioos_category=kwargs.get("ioos_category", default),
            keywords=kwargs.get("keywords", default),
            long_name=kwargs.get("long_name", default),
            standard_name=kwargs.get("standard_name", default),
            variableName=kwargs.get("variableName", default),
            Lon=kwargs.get("Lon", default),
            Lat=kwargs.get("Lat", default),
            Time=kwargs.get("Time", default),
            searchFor=search_for,
        )
        url = url.replace("Time=(ANY)", "")
        return url 