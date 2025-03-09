import dlt

from packeta import packeta_source


def load_packeta_invoices() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="packeta_pipelin",
        destination="duckdb",
        dataset_name="packeta_invoices",
    )
    load_info = pipeline.run(
        packeta_source()
    )
    print(load_info)


if __name__ == '__main__':
    load_packeta_invoices()
