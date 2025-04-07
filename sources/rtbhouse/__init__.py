from typing import Iterator, Sequence, Optional

import dlt
from dlt.common import pendulum
from dlt.common.typing import TDataItem, TDataItems
from dlt.sources import DltResource
from rtbhouse_sdk.client import Client, BasicAuth
from rtbhouse_sdk.schema import StatsGroupBy, StatsMetric, CountConvention

from .helpers import get_start_date
from .settings import DEFAULT_STATS


@dlt.source(name="rtbhouse")
def rtbhouse_source(
    username: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    advertiser_hash: Optional[str] = dlt.config.value,
    initial_load_past_days: int = 28,
    attribution_window_days_lag: int = 7,
    # group_by: Sequence[StatsGroupBy] = (StatsGroupBy.DAY,),
    # metrics: Sequence[StatsMetric] = DEFAULT_STATS,
    # conv_attribution: CountConvention = CountConvention.ATTRIBUTED_POST_CLICK,
) -> DltResource:
    initial_load_start_date = pendulum.today().subtract(days=initial_load_past_days)
    initial_load_start_date_str = initial_load_start_date.isoformat()

    @dlt.resource(name="rtbhouse_advertisers", write_disposition="replace")
    def advertisers() -> Iterator[TDataItem]:
        with Client(auth=BasicAuth(username, password)) as api:
            if advertiser_hash:
                advs = [api.get_advertiser(advertiser_hash)]
            else:
                advs = api.get_advertisers()

        for adv in advs:
            yield adv.model_dump()

    @dlt.transformer(name="rtbhouse_stats", data_from=advertisers, merge_key=StatsGroupBy.DAY.value, write_disposition="merge")
    def stats(
        advertiser: TDataItem,
        refresh_start_date: dlt.sources.incremental[str] = dlt.sources.incremental(
            "date", initial_value=initial_load_start_date_str,
        ),
    ) -> Iterator[TDataItem]:
        start_date = get_start_date(refresh_start_date, attribution_window_days_lag)
        end_date = pendulum.now()

        with Client(auth=BasicAuth(username, password)) as api:
            rtb_stats = api.get_rtb_stats(
                adv_hash=advertiser["hash"],
                day_from=start_date,
                day_to=end_date,
                group_by=StatsGroupBy.DAY,
                metrics=DEFAULT_STATS,
                count_convention=CountConvention.ATTRIBUTED_POST_CLICK,
            )

        for stat in rtb_stats:
            stat = stat.model_dump()
            stat["date"] = pendulum.instance(stat["day"]).to_date_string()
            stat["advertiser_name"] = advertiser["name"]
            stat["advertiser_hash"] = advertiser["hash"]
            yield stat

    return advertisers, advertisers | stats
