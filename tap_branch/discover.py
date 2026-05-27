import pendulum
import singer
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_branch.branch_api_contract import BranchExportConfig, EndpointConfig
from tap_branch.exceptions import (
    BranchBadRequestError,
    BranchForbiddenError,
    BranchUnauthorizedError,
    BranchError,
)
from tap_branch.schema import get_schemas

LOGGER = singer.get_logger()

# Shared endpoint config for the data-readiness probe
_DATA_READY_PATH = "v2/data/ready/"
_DATA_READY_ENDPOINT_CONFIG = EndpointConfig(
    required_query_params={"app_id"},
    required_headers={"Access-Token"},
)


def _check_stream_access(client, stream_name: str) -> bool:
    """Probe the Branch data-readiness endpoint to verify the account has
    access to `stream_name`'s report type.

    Yesterday's date is used as the probe date so the request always falls
    within Branch's data-retention window, avoiding unrelated 400 errors
    caused by stale start dates.

    Returns True if the stream is accessible, False if the account lacks
    access or the topic is unavailable.
    """
    probe_date = pendulum.yesterday("UTC").to_datetime_string()
    headers = client.build_headers(
        _DATA_READY_ENDPOINT_CONFIG,
        {"Access-Token": client.config["branch_access_token"]},
    )
    params = client.build_query_params(
        _DATA_READY_ENDPOINT_CONFIG,
        {"app_id": client.config["branch_app_id"]},
    )
    api_config = BranchExportConfig(
        method="POST",
        path=_DATA_READY_PATH,
        headers_data=headers,
        query_params_data=params,
    )
    try:
        client.check_data_readiness(
            export_start=probe_date,
            report_type=stream_name,
            api_config=api_config,
        )
        return True
    except BranchUnauthorizedError as e:
        # 401 means the credentials themselves are invalid — fail immediately
        # rather than silently excluding every stream.
        LOGGER.error(
            "Authentication failed during discovery. Verify that 'branch_access_token' "
            "and 'branch_app_id' are correct. API response: %s",
            e.response.text if e.response is not None else str(e),
        )
        raise
    except BranchForbiddenError:
        LOGGER.warning(
            "Stream '%s' is not accessible (permission denied); excluding from catalog.",
            stream_name,
        )
        return False
    except BranchBadRequestError:
        LOGGER.warning(
            "Stream '%s' is not accessible (topic not available for this account); "
            "excluding from catalog.",
            stream_name,
        )
        return False


def discover(client) -> Catalog:
    """
    Run the discovery mode, prepare the catalog file and return the catalog.
    Each stream is probed via the data-readiness endpoint before being added
    to the catalog:
      - 401 (Unauthorized): aborts discovery immediately — credentials are invalid.
      - 403 (Forbidden) / 400 (Bad Request): stream is excluded from the catalog.
      - Success: stream is included in the catalog.
    Raises BranchUnauthorizedError if credentials are invalid.
    Raises BranchError if no streams are accessible after all probes.
    """
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        if not _check_stream_access(client, stream_name):
            continue

        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[stream_name]
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error("stream_name: {}".format(stream_name))
            LOGGER.error("type schema_dict: {}".format(type(schema_dict)))
            raise err

        key_properties = metadata.to_map(mdata).get((), {}).get("table-key-properties")

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=key_properties,
                schema=schema,
                metadata=mdata,
            )
        )

    if not catalog.streams:
        LOGGER.error(
            "Discovery returned an empty catalog: no streams are accessible with the "
            "provided credentials. Verify that the Branch app ID and access token "
            "have the required permissions."
        )
        raise BranchError(
            "No streams are accessible. Check that the configured credentials have "
            "the required permissions."
        )

    return catalog
