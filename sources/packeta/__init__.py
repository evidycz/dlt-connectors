from typing import Iterator, Sequence

import dlt
from dlt.common import pendulum
from dlt.common.typing import TDataItems, TDataItem
from dlt.sources import DltResource

from .helpers import download_csv_file, validate_and_format_dates, get_start_date
from .settings import PACKETA_LIST_API_URL, INVOICE_LIST_COLUMNS, PACKETA_INVOICE_API_URL, INVOICE_COLUMNS


@dlt.source(name="packeta")
def packeta_source(
    api_key: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    initial_load_past_days: int = 28,
    attribution_window_days_lag: int = 28,
) -> Sequence[DltResource]:
    initial_load_start_date = pendulum.today().subtract(days=initial_load_past_days)
    initial_load_start_date_str = initial_load_start_date.isoformat()

    def construct_url(base_url: str, **params) -> str:
        query_params = "&".join(f"{key}={value}" for key, value in params.items())
        return f"{base_url}?{query_params}"

    @dlt.resource(name="invoices", write_disposition="merge", primary_key="invoice_id")
    def invoices(
        refresh_start_date: dlt.sources.incremental[str] = dlt.sources.incremental(
            "issue_date",
            initial_value=initial_load_start_date_str,
            last_value_func=max,
        ),
    ) -> Iterator[TDataItem]:
        start_date = get_start_date(refresh_start_date, attribution_window_days_lag).to_date_string()
        end_date = pendulum.now().subtract(days=1).to_date_string()
        start_date, end_date = validate_and_format_dates(start_date, end_date)

        invoices_url = construct_url(
            PACKETA_LIST_API_URL,
            key=api_key,
            password=password,
            version="2"
        )

        invoice_list = download_csv_file(invoices_url, delimiter=";")
        invoice_list = invoice_list.rename(columns=INVOICE_LIST_COLUMNS)

        filtered_invoices = invoice_list[
            (invoice_list["issue_date"] >= start_date) &
            (invoice_list["issue_date"] <= end_date)
        ]

        yield from filtered_invoices.to_dict(orient="records")

    @dlt.transformer(name="packages", write_disposition="merge", merge_key="entry_date")
    def packages(invoices: TDataItem) -> TDataItems:
        invoice_url = construct_url(
            PACKETA_INVOICE_API_URL,
            key=api_key,
            password=password,
            number=invoices["invoice_id"],
            lang="en",
            version="4"
        )

        invoice_details = download_csv_file(invoice_url, delimiter=";")
        invoice_details = invoice_details.rename(columns=INVOICE_COLUMNS)

        return invoice_details.to_dict(orient="records")

    return invoices, invoices | packages
