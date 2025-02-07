import dlt
from shoptet_pay import shoptet_pay_source


def load_shoptet_pay_data(start_date: str, end_date: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="shoptet_pay_pipeline",
        destination="duckdb",
        dataset_name="shoptet_pay",
    )
    load_info = pipeline.run(
        shoptet_pay_source(start_date, end_date)
    )
    print(load_info)


if __name__ == "__main__":
    start_date = "2025-01-01"
    end_date = "2025-01-31"
    load_shoptet_pay_data(start_date, end_date)
