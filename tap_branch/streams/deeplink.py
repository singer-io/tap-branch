from typing import Dict

from singer import metrics, write_record

from tap_branch.branch_api_contract import EndpointConfig
from tap_branch.streams.abstracts import FullTableStream


class DeepLink(FullTableStream):
    tap_stream_id = "deeplink"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "v1/url/"
    http_method = "GET"
    endpoint_config = EndpointConfig(
        path=path,
        method=http_method,
        required_headers={"Accept"},
        required_query_params={"url", "branch_key"}
    )

    def modify_object(self, record, parent_record=None):
        deeplink_id = record["data"]["~id"]
        record["id"] = deeplink_id

    def get_records(self, headers: Dict, query_params: Dict):

        response = self.client.make_request(
                method=self.http_method,
                endpoint=None,
                path=self.path,
                params=query_params,
                headers=headers
            )

        if isinstance(response, dict):
            raw_records = [response]
        else:
            raw_records = response

        yield from raw_records

    def sync(self, state, transformer, parent_obj=None):

        # Get all the deeplink urls from the config
        deeplink_urls = self.client.config["deeplink_urls"]

        # Set the required headers
        headers = self.client.build_headers(endpoint_config=self.endpoint_config, headers_data={"Accept": "application/json"})

        # Iterate on all the deeplink urls and extract information
        with metrics.record_counter(self.tap_stream_id) as counter:
            for url in deeplink_urls:
                url = url.strip()
                # Set the required query_params
                query_params = self.client.build_query_params(endpoint_config=self.endpoint_config,
                                                              query_params_data={
                                                                "url": url,
                                                                "branch_key": self.client.config["branch_key"]
                                                                })

                for record in self.get_records(headers=headers, query_params=query_params):
                    self.modify_object(record=record)

                    transformed_record = transformer.transform(
                        record, self.schema, self.metadata
                    )
                    if self.is_selected():
                        write_record(self.tap_stream_id, transformed_record)
                        counter.increment()

            return counter.value
