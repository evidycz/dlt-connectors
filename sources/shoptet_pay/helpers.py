from io import StringIO
from typing import Generator

import pandas as pd
from dlt.common import pendulum
from dlt.sources.helpers import requests

from .settings import SPAY_API_URL, SPAY_DATE_FORMAT


def download_payouts(
    api_key: str,
    start_date: str,
    end_date: str,
    page_size: int = 50,
    sort: str = "id,-date"
) -> Generator[dict, None, None]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    offset = 0
    while True:
        api_url = SPAY_API_URL
        params = {
            "dateFrom": start_date,
            "dateTo": end_date,
            "types": "PAYOUT",
            "limit": page_size,
            "offset": offset,
            "sort": sort
        }

        response = requests.get(api_url, headers=headers, params=params)
        response.raise_for_status()
        result = response.json()

        reports = result.get("items", [])
        if not reports:
            break

        yield from reports

        offset += page_size


def download_csv_file(api_key: str, file_url: str, delimiter: str = ",", encoding: str = "utf-8") -> pd.DataFrame:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    response = requests.get(file_url, headers=headers)
    response.raise_for_status()

    report = pd.read_csv(StringIO(response.text), delimiter=delimiter, encoding=encoding, parse_dates=True)
    return report


def validate_and_format_dates(start_date, end_date) -> tuple[str, str]:
    try:
        start = pendulum.parse(start_date)
        end = pendulum.parse(end_date)

        if start >= end:
            start, end = end, start

        formatted_start = start.format(SPAY_DATE_FORMAT)
        formatted_end = end.format(SPAY_DATE_FORMAT)

        return formatted_start, formatted_end

    except Exception as e:
        raise ValueError(f"Error processing dates: {str(e)}")
