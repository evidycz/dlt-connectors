"""Seznam Sklik source settings and constants"""

SKLIK_DATA_FORMAT = "YYYYMMDD"

DEFAULT_STATS_FIELDS = [
    "id",
    "name",
    "type",
    "status",
    "impressions",
    "clicks",
    "transactions",
    "conversions",
    "conversionValue",
    "totalMoney"
]

DEFAULT_STATS_FILTER = {}

STATS_PRIMARY_KEY = ["id", "date"]