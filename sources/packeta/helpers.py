import dlt
import pandas as pd
from dlt.common import pendulum
from dlt.common.time import ensure_pendulum_datetime

from .settings import PACKETA_DATE_FORMAT


def validate_and_format_dates(
    start_date: str,
    end_date: str
) -> tuple[str, str]:
    try:
        start = pendulum.parse(start_date)
        end = pendulum.parse(end_date)

        if start >= end:
            start, end = end, start

        formatted_start = start.format(PACKETA_DATE_FORMAT)
        formatted_end = end.format(PACKETA_DATE_FORMAT)

        return formatted_start, formatted_end

    except Exception as e:
        raise ValueError(f"Error processing dates: {str(e)}")


def get_start_date(
    incremental_start_date: dlt.sources.incremental[str],
    attribution_window_days_lag: int = 28,
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


def download_csv_file(file_url: str, delimiter: str = ",") -> pd.DataFrame:
    return pd.read_csv(file_url, delimiter=delimiter, parse_dates=True)
