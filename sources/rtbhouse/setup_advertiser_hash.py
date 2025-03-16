import dlt
from dlt.common.configuration import with_config, ConfigFieldMissingException
from rtbhouse_sdk.client import Client, BasicAuth


@with_config(sections=("sources", "rtbhouse"))
def print_advertisers_hash(
    username: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
) -> None:
    with Client(auth=BasicAuth(username, password)) as client:
        advertisers = client.get_advertisers()
        for advertiser in advertisers:
            print(f"{advertiser.name} - {advertiser.hash}")


if __name__ == "__main__":
    try:
        print_advertisers_hash()
    except ConfigFieldMissingException:
        print(
            "*****\nMissing secrets! Make sure you added username and passowrd to secrets.toml or environment variables.\n*****"
        )
        raise
