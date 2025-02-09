import dlt
from seznam_sklik import sklik_stats_source


def load_sklik_stats(start_date: str, end_date: str) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="seznam_sklik_pipeline",
        destination="duckdb",
        dataset_name="seznam_sklik_stats",
        dev_mode=False,
    )
    load_info = pipeline.run(
        sklik_stats_source(start_date=start_date, end_date=end_date)
    )
    print(load_info)


if __name__ == "__main__":
    start_date = "2025-01-01"
    end_date = "2025-01-15"

    load_sklik_stats(start_date, end_date)
