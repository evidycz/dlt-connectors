import dlt

from shoptet_pay import shoptet_pay_source


def load_shoptet_pay_data() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="shoptet_pay_pipeline",
        destination="duckdb",
        dataset_name="shoptet_pay",
    )
    load_info = pipeline.run(
        shoptet_pay_source(
            initial_load_past_days=365
        )
    )
    print(load_info)


if __name__ == "__main__":
    load_shoptet_pay_data()
