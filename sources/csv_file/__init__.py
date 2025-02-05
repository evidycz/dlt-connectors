from io import StringIO
from typing import Iterator

import dlt
import pandas as pd
from dlt.common.typing import TDataItem
from dlt.extract import DltResource
from dlt.sources.helpers import requests


@dlt.source(name="csv_source")
def csv_file_source(
    url: str,
    encoding: str = "utf-8",
    delimiter: str = ",",
    chunk_size: int = 1024,
) -> DltResource:
    @dlt.resource(name="csv_data", write_disposition="replace")
    def csv_data() -> Iterator[TDataItem]:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()

            chunks = pd.read_csv(
                StringIO(response.text), encoding=encoding, delimiter=delimiter, chunksize=chunk_size, low_memory=False
            )

            for chunk in chunks:
                for record in chunk.to_dict(orient="records"):
                    yield record

    return csv_data
