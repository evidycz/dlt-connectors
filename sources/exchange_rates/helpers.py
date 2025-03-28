"""Open Exchange Rates source helpers"""
from typing import Optional, List, Dict, Any

from dlt.common import pendulum
from dlt.sources.helpers import requests

from .settings import OPEN_EXCHANGE_RATES_API_URL


def fetch_exchange_rates(app_id: str, date: str, base_currency: str, symbols: Optional[List[str]]) -> Dict[str, Any]:
    url = f"{OPEN_EXCHANGE_RATES_API_URL}/{date}.json"
    params = {
        "app_id": app_id,
        "base": base_currency,
    }

    if symbols:
        params["symbols"] = ",".join(symbols)

    response = requests.get(url, params=params)
    response.raise_for_status()
    return response.json()


def convert_timestamp_to_date(timestamp: int) -> str:
    return pendulum.from_timestamp(timestamp).to_date_string()
