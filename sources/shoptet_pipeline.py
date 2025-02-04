import dlt
from shoptet import shoptet_orders_source


def load_shoptet_orders_data(start_date: str, end_date: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="shoptet_orders",
        destination="duckdb",
        dataset_name="shoptet_orders",
    )
    load_info = pipeline.run(
        shoptet_orders_source(start_date, end_date)
    )
    print(load_info)


if __name__ == "__main__":
    # Set start and end dates
    # If you set a longer time period, the connection may be interrupted.
    start_date = "2024-01-01"
    end_date = "2024-01-31"

    load_shoptet_orders_data(start_date, end_date)
    
