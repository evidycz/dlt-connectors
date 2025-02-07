from typing import Sequence, Iterator

import dlt
from dlt.common.typing import TDataItem, TDataItems
from dlt.sources import DltResource

from .helpers import download_payouts, download_csv_file, validate_and_format_dates
from .settings import SPAY_API_URL


@dlt.source(name="shoptet_pay")
def shoptet_pay_source(
    start_date: str,
    end_date: str,
    spay_api_key: str = dlt.secrets.value
) -> Sequence[DltResource]:
    start_date, end_date = validate_and_format_dates(start_date, end_date)

    @dlt.resource(name="payouts")
    def payouts() -> Iterator[TDataItem]:
        yield from download_payouts(spay_api_key, start_date, end_date)

    @dlt.transformer(name="payments")
    def payments(payouts: TDataItem) -> TDataItems:
        payment_id = payouts["id"]
        report_url = f"{SPAY_API_URL}/{payment_id}/csv"

        report = download_csv_file(spay_api_key, report_url)
        report = report.astype(str)

        return report.to_dict(orient="records")

    return payouts, payouts | payments
