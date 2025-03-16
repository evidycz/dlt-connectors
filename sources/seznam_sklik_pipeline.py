from typing import Sequence

import dlt
from sklik.api import SklikApi

from seznam_sklik import sklik_accounts, sklik_object, sklik_stats, sklik_stats_source, sklik_mcc_objects_source, sklik_mcc_stats_source
from seznam_sklik import DEFAULT_SETTINGS_FIELDS, DEFAULT_STATS_FIELDS, DEFAULT_GROUP_SETTINGS_FIELDS, DEFAULT_GROUP_STATS_FIELDS


def load_sklik_stats() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="seznam_sklik_pipeline",
        destination="duckdb",
        dataset_name="seznam_sklik_stats",
    )
    load_info = pipeline.run(
        sklik_stats_source()
    )
    print(load_info)


def load_sklik_custom(
    api: SklikApi,
    object_name: str = "campaigns",
    initial_load_start_date: str = "2025-01-01",
    object_fields: Sequence = DEFAULT_SETTINGS_FIELDS,
    stats_fields: Sequence = DEFAULT_STATS_FIELDS
) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="seznam_sklik_pipeline",
        destination="duckdb",
        dataset_name="seznam_sklik_custom",
    )
    load_info = pipeline.run([
        sklik_accounts(api),
        sklik_accounts(api) | sklik_object(sklik_api=api, object_name=object_name, fields=object_fields),
        sklik_accounts(api) | sklik_stats(
            sklik_api=api,
            refresh_start_date=dlt.sources.incremental("date", initial_load_start_date),
            object_name=object_name,
            fields=stats_fields
        ),
    ])
    print(load_info)


def load_sklik_mcc_objects(object_name: str = "campaigns") -> None:
    pipeline = dlt.pipeline(
        pipeline_name="seznam_sklik_pipeline",
        destination="duckdb",
        dataset_name="seznam_sklik_settings",
    )
    load_info = pipeline.run(
        sklik_mcc_objects_source(load_sklik_mcc_objects(object_name=object_name))
    )
    print(load_info)


def load_sklik_mcc_stats(object_name: str = "campaigns") -> None:
    pipeline = dlt.pipeline(
        pipeline_name="seznam_sklik_pipeline",
        destination="duckdb",
        dataset_name="seznam_sklik_mcc_stats",
    )
    load_info = pipeline.run(
        sklik_mcc_stats_source(object_name=object_name)
    )
    print(load_info)


def load_sklik_mcc_full(object_name: str = "campaigns") -> None:
    pipeline = dlt.pipeline(
        pipeline_name="seznam_sklik_pipeline",
        destination="duckdb",
        dataset_name="seznam_sklik_full",
    )
    load_info = pipeline.run(
        [sklik_mcc_stats_source(object_name=object_name), sklik_mcc_objects_source(object_name=object_name)]
    )
    print(load_info)


if __name__ == "__main__":
    api = SklikApi.init(dlt.secrets.get("sources.seznam_sklik.sklik_token"))
    load_sklik_custom(
        api=api,
        object_name="groups",
        object_fields=DEFAULT_GROUP_SETTINGS_FIELDS,
        stats_fields=DEFAULT_GROUP_STATS_FIELDS,
    )

    # load_sklik_stats()

    # load_sklik_mcc_objects()
    # load_sklik_mcc_stats()

    # load_sklik_mcc_full()
