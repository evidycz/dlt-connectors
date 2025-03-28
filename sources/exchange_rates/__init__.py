from typing import List, Optional, Iterator

import dlt
from dlt import resource
from dlt.common.typing import TDataItem
from dlt.extract import DltResource

from .helpers import fetch_exchange_rates, convert_timestamp_to_date


@dlt.source(name="open_exchange_rates")
def open_exchange_rates_source(
    date: str,
    app_id: str = dlt.secrets.value,
    base: str = "USD",
    symbols: Optional[List[str]] = None,
) -> DltResource:
    @resource(name="exchange_rates", table_name="exchange_rates", write_disposition="merge", merge_key="date")
    def exchange_rates() -> Iterator[TDataItem]:
        response = fetch_exchange_rates(app_id, date, base, symbols)
        yield {"date": convert_timestamp_to_date(response["timestamp"]), "base": response["base"]} | response["rates"]

    return exchange_rates
