from typing import Dict

from singer import get_logger, metrics, write_record

from tap_branch.branch_api_contract import EndpointConfig
from tap_branch.streams.abstracts import FullTableStream

LOGGER = get_logger()


class AppConfig(FullTableStream):
    tap_stream_id = "app_config"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "v1/app/{api_key}"
    http_method = "GET"
    endpoint_config = EndpointConfig(
        path=path,
        method=http_method,
        required_headers={"Accept"},
        required_query_params={"branch_secret"}
    )

    def get_records(self, headers: Dict, query_params: Dict):

        response = self.client.make_request(
                method=self.http_method,
                endpoint=None,
                path=self.path.format(api_key=self.client.config["branch_key"]),
                params=query_params,
                headers=headers
            )

        if isinstance(response, dict):
            raw_records = [response]
        else:
            raw_records = response

        yield from raw_records

    def sync(self, state, transformer, parent_obj=None):

        # Set the required headers
        headers = self.client.build_headers(endpoint_config=self.endpoint_config, headers_data={"Accept": "application/json"})

        # Iterate on all the deeplink urls and extract information
        with metrics.record_counter(self.tap_stream_id) as counter:
            # Set the required query_params
            query_params = self.client.build_query_params(endpoint_config=self.endpoint_config,
                                                          query_params_data={"branch_secret": self.client.config["branch_secret"]})

            for record in self.get_records(headers=headers, query_params=query_params):
                self.modify_object(record=record)

                transformed_record = transformer.transform(
                    record, self.schema, self.metadata
                )
                if self.is_selected():
                    write_record(self.tap_stream_id, transformed_record)
                    counter.increment()

            return counter.value
