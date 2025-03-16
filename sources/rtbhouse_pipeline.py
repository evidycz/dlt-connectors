import dlt

from rtbhouse import rtbhouse_source


def load_rtbhouse_stats() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rtbhouse_pipeline",
        destination="duckdb",
        dataset_name="rtbhouse_data"
    )
    load_info = pipeline.run(rtbhouse_source())
    print(load_info)

if __name__ == "__main__":
    load_rtbhouse_stats()
