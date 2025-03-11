from typing import Iterator

import dlt
from dlt.common import pendulum
from dlt.common.typing import TDataItem
from dlt.sources import DltResource

from .helpers import build_report_url, stream_csv_file, validate_and_format_dates, get_start_date
from .settings import ORDER_COLUMNS


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
    eshop_url: str = dlt.config.value,
    partner_id: str = dlt.config.value,
    orders_report_id: str = dlt.config.value,
    orders_report_hash: str = dlt.secrets.value,
    initial_load_past_days: int = 28,
    attribution_window_days_lag: int = 28,
) -> DltResource:
    initial_load_start_date = pendulum.today().subtract(days=initial_load_past_days)
    initial_load_start_date_str = initial_load_start_date.isoformat()

    @dlt.resource(
        name="orders",
        table_name=lambda row: f"order_{row['item_type']}s",
        write_disposition="merge",
        merge_key="order_id",
        columns=ORDER_COLUMNS,
    )
    def orders(
        refresh_start_date: dlt.sources.incremental[str] = dlt.sources.incremental(
            "order_timestamp",
            initial_value=initial_load_start_date_str,
            last_value_func=max,
        )
    ) -> Iterator[TDataItem]:
        start_date = get_start_date(refresh_start_date, attribution_window_days_lag).to_date_string()
        end_date = pendulum.now().subtract(days=1).to_date_string()
        start_date, end_date = validate_and_format_dates(start_date, end_date)

        report_url = build_report_url(
            eshop_url, partner_id, "orders", orders_report_id, orders_report_hash, start_date, end_date
        )
        yield from stream_csv_file(report_url)

    return orders
