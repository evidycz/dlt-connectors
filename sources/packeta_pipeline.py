import dlt
from packeta import packeta_source


def load_packeta_invoices(start_date: str, end_date: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="packeta_pipeline", destination="duckdb", dataset_name="packeta_invoices",
    )
    load_info = pipeline.run(
        packeta_source(start_date, end_date)
    )
    print(load_info)

if __name__ == '__main__':
    start_date = "2025-01-27"
    end_date = "2025-02-02"
    load_packeta_invoices(start_date, end_date)