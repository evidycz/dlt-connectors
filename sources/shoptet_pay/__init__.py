from typing import Sequence, Iterator

import dlt
import pendulum
from dlt.common.typing import TDataItem, TDataItems
from dlt.sources import DltResource

from .helpers import download_payouts, download_csv_file, validate_and_format_dates, get_start_date
from .settings import SPAY_API_URL


@dlt.source(name="shoptet_pay")
def shoptet_pay_source(
    spay_api_key: str = dlt.secrets.value,
    initial_load_past_days: int = 28,
    attribution_window_days_lag: int = 28,
) -> Sequence[DltResource]:
    initial_load_start_date = pendulum.today().subtract(days=initial_load_past_days)
    initial_load_start_date_str = initial_load_start_date.isoformat()

    @dlt.resource(name="payouts", write_disposition="merge", primary_key="id")
    def payouts(
        refresh_start_date: dlt.sources.incremental[str] = dlt.sources.incremental(
            "createdAt",
            initial_value=initial_load_start_date_str,
            last_value_func=max,
        ),
    ) -> Iterator[TDataItem]:
        start_date = get_start_date(refresh_start_date, attribution_window_days_lag).to_date_string()
        end_date = pendulum.now().subtract(days=1).to_date_string()
        start_date, end_date = validate_and_format_dates(start_date, end_date)

        yield from download_payouts(spay_api_key, start_date, end_date)

    @dlt.transformer(name="payments", write_disposition="merge", primary_key="variableSymbol")
    def payments(payouts: TDataItem) -> TDataItems:
        payment_id = payouts["id"]
        report_url = f"{SPAY_API_URL}/{payment_id}/csv"

        report = download_csv_file(spay_api_key, report_url)
        report = report[(report["transactionType"] == "Payment") | (report["transactionType"] == "Refund")]

        return report.to_dict(orient="records")

    return payouts, payouts | payments
