import dlt

from csv_file import csv_file_source


def load_csv_file(csv_url: str, encoding: str = "utf-8", delimiter: str = ",", chunk_size: int = 1024) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="csv_file_pipeline", destination="duckdb", dataset_name="csv_file_data"
    )
    load_info = pipeline.run(
        csv_file_source(csv_url, encoding, delimiter, chunk_size)
    )
    print(load_info)


if __name__ == "__main__":
    # Reading COVID-19 data from GitHub
    url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/01-01-2021.csv"
    load_csv_file(url, chunk_size=100)
