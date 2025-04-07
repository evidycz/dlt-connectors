import dlt

from ppl import ppl_source


def load_ppl_shipments() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_ppl_pipeline",
        destination='duckdb',
        dataset_name="ppl_packages",
    )
    load_info = pipeline.run(ppl_source())
    print(load_info)


if __name__ == "__main__":
    load_ppl_shipments()
