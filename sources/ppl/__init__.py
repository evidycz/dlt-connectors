from typing import Any

import dlt
from dlt.common.pendulum import pendulum
from dlt.sources.helpers.rest_client.auth import OAuth2ClientCredentials
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.sources.rest_api import (
    RESTAPIConfig,
    rest_api_resources,
)

from settings import PPL_BASE_URL
from sources.ppl.settings import PPL_AUTH_URL


@dlt.source(name="ppl")
def ppl_source(
    client_id: str = dlt.secrets.value,
    client_secret: str = dlt.secrets.value,
    initial_load_past_days: int = 28,
) -> Any:
    initial_load_start_date = pendulum.yesterday().subtract(days=initial_load_past_days)
    initial_load_start_date_str = initial_load_start_date.isoformat()

    config: RESTAPIConfig = {
        "client": {
            "base_url": PPL_BASE_URL,
            "auth": OAuth2ClientCredentials(
                client_id=client_id,
                client_secret=client_secret,
                access_token_url=PPL_AUTH_URL,
                default_token_expiration=1800,
            )
        },
        "resource_defaults": {
            "write_disposition": "merge",
        },
        "resources": [
            {
                "name": "shipment",
                "primary_key": "shipmentNumber",
                "endpoint": {
                    "path": "shipment",
                    "method": "GET",
                    "paginator": OffsetPaginator(
                        limit=100,
                        limit_param="Limit",
                        offset=0,
                        offset_param="Offset",
                        total_path=None,
                        stop_after_empty_page=True,
                    ),
                    "incremental": {
                        "start_param": "DateFrom",
                        "end_param": "DateTo",
                        "cursor_path": "lastUpdateDate",
                        "initial_value": initial_load_start_date_str,
                        "end_value": pendulum.yesterday().to_date_string(),
                    }
                },
            }
        ],
    }

    yield from rest_api_resources(config)
