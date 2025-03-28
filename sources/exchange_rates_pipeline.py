import dlt
from dlt.common import pendulum

from exchange_rates import open_exchange_rates_source


def load_exchange_rates(date: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="open_exchange_rates",
        destination='duckdb',
        dataset_name="exchange_rates",
    )
    load_info = pipeline.run(
        open_exchange_rates_source(date=date)
    )
    print(load_info)


if __name__ == "__main__":
    yesterday = pendulum.yesterday().to_date_string()

    load_exchange_rates(yesterday)
