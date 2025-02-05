from typing import Iterator, Sequence

import dlt
from dlt.common.typing import TDataItems, TDataItem
from dlt.sources import DltResource

from .helpers import download_csv_file, validate_and_format_dates
from .settings import PACKETA_LIST_API_URL, INVOICE_LIST_COLUMNS, PACKETA_INVOICE_API_URL, INVOICE_COLUMNS


@dlt.source(name="packeta")
def packeta_source(
    start_date: str,
    end_date: str,
    api_key: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
) -> Sequence[DltResource]:
    start_date, end_date = validate_and_format_dates(start_date, end_date)

    def construct_url(base_url: str, **params) -> str:
        query_params = "&".join(f"{key}={value}" for key, value in params.items())
        return f"{base_url}?{query_params}"

    @dlt.resource(name="invoices", write_disposition="merge", merge_key="invoice_id")
    def invoices() -> Iterator[TDataItem]:
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

        filtered_invoices = filtered_invoices.astype(str)

        yield from filtered_invoices.to_dict(orient="records")

    @dlt.transformer(name="packages", write_disposition="merge", merge_key="entry_date")
    def packages(invoices: TDataItem) -> TDataItems:
        invoice_url = construct_url(
            PACKETA_INVOICE_API_URL,
            key=api_key,
            password=password,
            number=invoices["invoice_id"],
            lang="en",
            version=4
        )

        invoice_details = download_csv_file(invoice_url, delimiter=";")
        invoice_details = invoice_details.rename(columns=INVOICE_COLUMNS)
        invoice_details = invoice_details.astype(str)

        return invoice_details.to_dict(orient="records")

    return invoices, invoices | packages
