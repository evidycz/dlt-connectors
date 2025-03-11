"""Shoptet source settings and constants"""

SHOPTET_BASE_URL = "https://{}/export/{}.csv"
REPORT_PARAMETERS = "hash={}&partnerId={}&patternId={}"
SHOPTET_DATE_FORMAT = "YYYY-M-D"

ORDER_COLUMNS = {
    "order_id": {"data_type": "text"},
    "order_timestamp": {"data_type": "timestamp"},
    "order_status": {"data_type": "text"},
    "order_price": {"data_type": "double"},
    "order_price_w_vat": {"data_type": "double"},
    "order_tax": {"data_type": "double"},
    "order_price_rounding": {"data_type": "double"},
    "order_price_to_pay": {"data_type": "double"},
    "customer_id": {"data_type": "text"},
    "customer_email": {"data_type": "text"},
    "customer_type": {"data_type": "text"},
    "customer_group": {"data_type": "text"},
    "company_id": {"data_type": "text"},
    "company_vat": {"data_type": "text"},
    "delivery_country": {"data_type": "text"},
    "delivery_city": {"data_type": "text"},
    "exchange_rate": {"data_type": "double"},
    "currency_code": {"data_type": "text"},
    "is_paid": {"data_type": "bool"},
    "item_amount": {"data_type": "text"},
    "item_unit": {"data_type": "text"},
}

PRODUCTS_COLUMNS = {
    "code": {"data_type": "text"},
}