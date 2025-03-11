import dlt
from shoptet import shoptet_orders_source
from shoptet import shoptet_products_source


def load_shoptet_orders_data() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="shoptet_pipeline",
        destination="duckdb",
        dataset_name="shoptet_orders",
    )
    load_info = pipeline.run(
        shoptet_orders_source()
    )
    print(load_info)

def load_shoptet_products_data() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="shoptet_pipeline",
        destination="duckdb",
        dataset_name="shoptet_products",
    )
    load_info = pipeline.run(
        shoptet_products_source()
    )
    print(load_info)


if __name__ == "__main__":
    # load_shoptet_orders_data()
    load_shoptet_products_data()

