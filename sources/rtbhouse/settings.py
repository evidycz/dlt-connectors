"""RTBHouse source settings and constants"""

from rtbhouse_sdk.schema import StatsMetric

DEFAULT_STATS = (
    StatsMetric.IMPS_COUNT,
    StatsMetric.CLICKS_COUNT,
    StatsMetric.CAMPAIGN_COST,
    StatsMetric.CONVERSIONS_COUNT,
    StatsMetric.CONVERSIONS_VALUE,
)
