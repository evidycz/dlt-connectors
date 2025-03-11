from io import StringIO
from typing import Optional, Generator, Any

import dlt
import pandas as pd
from dlt.common import pendulum
from dlt.common.time import ensure_pendulum_datetime
from dlt.sources.helpers import requests

from .settings import SHOPTET_BASE_URL, REPORT_PARAMETERS, SHOPTET_DATE_FORMAT


def build_report_url(
    eshop_url: str,
    partner_id: str,
    report_type: str,
    report_id: str,
    report_hash: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> str:
    report_url = SHOPTET_BASE_URL.format(eshop_url, report_type)
    parameters = REPORT_PARAMETERS.format(report_hash, partner_id, report_id)
    final_url = f"{report_url}?{parameters}"

    if start_date and end_date:
        final_url = f"{final_url}&dateFrom={start_date}&dateUntil={end_date}"

    return final_url


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


def validate_and_format_dates(
    start_date: str,
    end_date: str
) -> tuple[str, str]:
    try:
        start = pendulum.parse(start_date)
        end = pendulum.parse(end_date)

        if start >= end:
            start, end = end, start

        formatted_start = start.format(SHOPTET_DATE_FORMAT)
        formatted_end = end.format(SHOPTET_DATE_FORMAT)

        return formatted_start, formatted_end

    except Exception as e:
        raise ValueError(f"Error processing dates: {str(e)}")


def stream_csv_file(report_url: str) -> Generator[Any, Any, None]:
    with requests.get(report_url, stream=True, ) as response:
        response.raise_for_status()

        chunks = pd.read_csv(
            StringIO(response.text), encoding="cp1250", delimiter=";", chunksize=1024, low_memory=False
        )

        for chunk in chunks:
            for record in chunk.to_dict(orient="records"):
                yield record
