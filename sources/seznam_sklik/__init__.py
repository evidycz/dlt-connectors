from typing import Iterator, Sequence

import dlt
import pendulum
from dlt.common.typing import TDataItems, TDataItem
from dlt.sources import DltResource
from sklik.api import SklikApi
from sklik.object import Account
from sklik.report import create_report

from .helpers import validate_and_format_dates, get_start_date
from .settings import (
    DEFAULT_SETTINGS_FIELDS,
    DEFAULT_CAMPAIGN_SETTINGS_FIELDS,
    DEFAULT_GROUP_SETTINGS_FIELDS,
    DEFAULT_AD_SETTINGS_FIELDS,
    DEFAULT_BANNER_SETTINGS_FIELDS,
    DEFAULT_STATS_FIELDS,
    DEFAULT_CAMPAIGN_STATS_FIELDS,
    DEFAULT_GROUP_STATS_FIELDS,
    DEFAULT_AD_STATS_FIELDS,
    DEFAULT_BANNER_STATS_FIELDS,
    DEFAULT_SITELINKS_STATS_FIELDS,
    DEFAULT_RETARGETING_STATS_FIELDS,
    DEFAULT_QUERIES_STATS_FIELDS,
    STATS_PRIMARY_KEY,
    OBJECT_SETTINGS_COLUMNS,
    OBJECT_STATS_COLUMNS,
)


@dlt.source(name="seznam_sklik")
def sklik_stats_source(
    sklik_token: str = dlt.secrets.value,
    account_id: str = dlt.config.value,
    initial_load_past_days: int = 28,
    attribution_window_days_lag: int = 7,
    object_name: str = "campaigns",
    granularity: str = "daily",
    fields: Sequence[str] = DEFAULT_STATS_FIELDS,
) -> DltResource:
    initial_load_start_date = pendulum.today().subtract(days=initial_load_past_days)
    initial_load_start_date_str = initial_load_start_date.isoformat()

    SklikApi.init(sklik_token)
    account = Account(int(account_id))

    @dlt.resource(name="sklik_stats", write_disposition="merge", primary_key=STATS_PRIMARY_KEY)
    def sklik_stats(
        refresh_start_date: dlt.sources.incremental[str] = dlt.sources.incremental(
            "date", initial_value=initial_load_start_date_str
        )
    ) -> Iterator[TDataItems]:
        start_date = get_start_date(refresh_start_date, attribution_window_days_lag).to_date_string()
        end_date = pendulum.now().subtract(days=1).to_date_string()
        start_date, end_date = validate_and_format_dates(start_date, end_date)

        report = create_report(
            account=account,
            service=object_name,
            since=start_date,
            until=end_date,
            fields=fields,
            granularity=granularity,
        )

        for stats in report:
            if stats["date"]:
                stats["date"] = pendulum.parse(str(stats["date"])).to_date_string()
            yield stats

    return sklik_stats


@dlt.source(name="seznam_sklik")
def sklik_mcc_objects_source(
    sklik_token: str = dlt.secrets.value,
    object_name: str = "campaigns",
    fields: Sequence[str] = DEFAULT_CAMPAIGN_SETTINGS_FIELDS,
) -> Sequence[DltResource]:
    api = SklikApi.init(sklik_token)
    object_settings = sklik_object(api, object_name, fields)

    return sklik_accounts(api) | object_settings


@dlt.source(name="seznam_sklik")
def sklik_mcc_stats_source(
    sklik_token: str = dlt.secrets.value,
    initial_load_past_days: int = 28,
    attribution_window_days_lag: int = 7,
    object_name: str = "campaigns",
    granularity: str = "daily",
    fields: Sequence[str] = DEFAULT_CAMPAIGN_STATS_FIELDS,
) -> Sequence[DltResource]:
    api = SklikApi.init(sklik_token)

    initial_load_start_date = pendulum.today().subtract(days=initial_load_past_days)
    initial_load_start_date_str = initial_load_start_date.to_date_string()

    object_stats = sklik_stats(
        api,
        object_name,
        refresh_start_date=dlt.sources.incremental("date", initial_value=initial_load_start_date_str),
        attribution_window_days_lag=attribution_window_days_lag,
        granularity=granularity,
        fields=fields,
    )

    return sklik_accounts(api) | object_stats


@dlt.resource(name="accounts", write_disposition="replace")
def sklik_accounts(sklik_api: SklikApi) -> Iterator[TDataItem]:
    response = sklik_api.call("client", "get")

    main_account = response["user"]
    foreign_accounts = response["foreignAccounts"]
    all_accounts = [main_account] + foreign_accounts

    for account in all_accounts:
        yield account


@dlt.transformer(
    name=lambda args: f"{args['object_name']}_settings",
    data_from=sklik_accounts,
    write_disposition="replace",
    columns=OBJECT_SETTINGS_COLUMNS,
    standalone=True,
)
def sklik_object(
    account_detail: TDataItem,
    sklik_api: SklikApi,
    object_name: str,
    fields: Sequence[str] = DEFAULT_SETTINGS_FIELDS,
) -> Iterator[TDataItem]:
    account_id = account_detail["userId"]
    account_name = account_detail["username"]
    response = sklik_api.call(
        object_name,
        "list",
        [{"userId": account_id}, {}, {"limit": 1000, "offset": 0, "displayColumns": fields}])

    for obj in response[object_name]:
        obj["account_id"] = account_id
        obj["account_name"] = account_name
        yield obj


@dlt.transformer(
    name=lambda args: f"{args['object_name']}_stats",
    data_from=sklik_accounts,
    write_disposition="merge",
    primary_key=STATS_PRIMARY_KEY,
    columns=OBJECT_STATS_COLUMNS,
    standalone=True,
)
def sklik_stats(
    account_detail: TDataItem,
    sklik_api: SklikApi,
    object_name: str,
    refresh_start_date: dlt.sources.incremental[str] = dlt.sources.incremental("date"),
    attribution_window_days_lag: int = 7,
    granularity: str = "daily",
    fields: Sequence[str] = DEFAULT_STATS_FIELDS,
) -> Iterator[TDataItems]:
    start_date = get_start_date(refresh_start_date, attribution_window_days_lag).to_date_string()
    end_date = pendulum.now().subtract(days=1).to_date_string()
    start_date, end_date = validate_and_format_dates(start_date, end_date)

    account = Account(account_detail["userId"], api=sklik_api)
    report = create_report(
        account=account,
        service=object_name,
        since=start_date,
        until=end_date,
        fields=fields,
        granularity=granularity,
    )

    for stats in report:
        if stats["date"]:
            stats["date"] = pendulum.parse(str(stats["date"])).to_date_string()
        stats["account_id"] = account_detail["userId"]
        stats["account_name"] = account_detail["username"]
        yield stats
