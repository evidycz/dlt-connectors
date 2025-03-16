"""RTBHouse source helpers"""
import dlt
from dlt.common import pendulum
from dlt.common.time import ensure_pendulum_datetime


def get_start_date(
    incremental_start_date: dlt.sources.incremental[str],
    attribution_window_days_lag: int = 7,
) -> pendulum.DateTime:
    """
    Get the start date for incremental loading of Seznam Sklik stats data.
    """
    start_date: pendulum.DateTime = ensure_pendulum_datetime(
        incremental_start_date.start_value
    ).subtract(days=attribution_window_days_lag)

    # lag the incremental start date by attribution window lag
    incremental_start_date.start_value = start_date.isoformat()
    return start_date
