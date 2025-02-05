"""Packeta source settings and constants"""

PACKETA_LIST_API_URL = "https://www.zasilkovna.cz/api/invoice.csv"
PACKETA_INVOICE_API_URL = "https://www.zasilkovna.cz/api/invoice-packet.csv"
PACKETA_DATE_FORMAT = "YYYY-MM-DD"

INVOICE_LIST_COLUMNS = {
    "Mena": "currency",
    "Datum vystaveni": "issue_date",
    "Datum splatnosti": "due_date",
    "Cislo dokladu": "invoice_id",
    "Uctovane sluzby": "billed_services",
    "Sluzby s DPH": "billed_services_w_vat",
    "Vybrane dobirky": "cash_on_delivery",
    "Nahrada skody": "damage_compensation",
}

INVOICE_COLUMNS = {
    "Your e-shop": "e_shop",
    "Entry Date": "entry_date",
    "Date of posting": "posting_date",
    "Date of Delivery or Return": "delivery_date",
    "Order Number": "order_id",
    "Name": "first_name",
    "Surname": "last_name",
    "Barcode": "barcode",
    "Billed service": "billed_service",
    "VAT %": "vat",
    "Service Incl VAT": "billed_service_w_vat",
    "Currency of billed services": "currency",
    "COD amount": "cod_amount",
    "COD currency": "cod_currency",
    "Specific symbol": "specific_symbol",
    "Note": "note",
    "Status": "status",
    "Weight": "weight",
    "Base price": "base_price",
    "COD surcharge": "cod_surcharge",
    "Insurance": "insurance_surcharge",
    "Fuel surcharge": "fuel_surcharge",
    "Toll surcharge": "toll_surcharge",
    "Card payment surcharge": "card_payment_surcharge",
    "Non depot posting": "non_depot_posting",
    "Returned package fee": "returned_package_fee",
    "Other": "other",
    "External tracking code": "external_tracking_code"
}
