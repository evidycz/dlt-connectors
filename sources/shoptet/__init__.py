from typing import Iterator

import dlt
from dlt.common.typing import TDataItem
from dlt.sources import DltResource
from .helpers import build_report_url, stream_csv_file, validate_and_format_dates


@dlt.source(name="shoptet")
def shoptet_products_source(
    eshop_url: str = dlt.config.value,
    partner_id: str = dlt.config.value,
    products_report_id: str = dlt.config.value,
    products_report_hash: str = dlt.secrets.value,
) -> DltResource:
    @dlt.resource(name="products", write_disposition="replace")
    def products() -> Iterator[TDataItem]:
        report_url = build_report_url(
            eshop_url, partner_id, "products", products_report_id, products_report_hash
        )
        yield from stream_csv_file(report_url)

    return products


@dlt.source(name="shoptet")
def shoptet_orders_source(
    start_date: str,
    end_date: str,
    eshop_url: str = dlt.config.value,
    partner_id: str = dlt.config.value,
    orders_report_id: str = dlt.config.value,
    orders_report_hash: str = dlt.secrets.value
) -> DltResource:
    start_date, end_date = validate_and_format_dates(start_date, end_date)

    @dlt.resource(name="orders", write_disposition="merge", merge_key="order_id")
    def orders() -> Iterator[TDataItem]:
        report_url = build_report_url(
            eshop_url, partner_id, "orders", orders_report_id, orders_report_hash, start_date, end_date
        )
        yield from stream_csv_file(report_url)

    return orders
