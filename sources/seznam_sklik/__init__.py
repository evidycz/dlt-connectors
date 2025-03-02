from typing import Iterator, Dict, List, Sequence

import dlt
from dlt.common.typing import TDataItems
from dlt.sources import DltResource
from sklik.api import SklikApi
from sklik.object import Account
from sklik.report import create_report

from .helpers import validate_and_format_dates
from .settings import DEFAULT_STATS_FIELDS, DEFAULT_STATS_FILTER, STATS_PRIMARY_KEY


@dlt.source(name="seznam_sklik")
def sklik_stats_source(
    start_date: str,
    end_date: str,
    level: str = "campaigns",
    fields: Sequence[str] = DEFAULT_STATS_FIELDS,
    granularity: str = "daily",
    sklik_token: str = dlt.secrets.value,
    account_id: str = dlt.config.value,

) -> DltResource:
    start_date, end_date = validate_and_format_dates(start_date, end_date)
    SklikApi.init(sklik_token)
    account = Account(int(account_id))

    @dlt.resource(name="sklik_stats", write_disposition="merge", merge_key=STATS_PRIMARY_KEY)
    def sklik_stats() -> Iterator[TDataItems]:
        report = create_report(
            account=account,
            service=level,
            since=start_date,
            until=end_date,
            fields=fields,
            granularity=granularity,
        )

        for page in report:
            yield page

    return sklik_stats
