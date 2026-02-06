
import gzip
import io
import json
from typing import Dict

import pendulum
import requests
import singer
from singer import bookmarks, metrics, write_record
from singer.transform import Transformer

from tap_branch.branch_api_contract import BranchExportConfig, EndpointConfig
from tap_branch.branch_constants import (BRANCH_EVENTS_SCHEMA,
                                         BRANCH_MAX_DATE_WINDOW, JOB_TIMEOUT)
from tap_branch.streams.abstracts import IncrementalStream

LOGGER = singer.get_logger()


class BranchEventsBaseStream(IncrementalStream):
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    schema_path = "shared/branch_events"

    # Export Job paths
    export_data_readiness_path = "v2/data/ready/"
    create_export_job_path = "v2/logs/"
    poll_export_job_path = "v2/logs/{request_handle}/"

    endpoint_config = EndpointConfig(
        required_query_params={"app_id"},
        required_headers={"Access-Token"}
    )

    @staticmethod
    def extract_data(job_response: Dict):

        data_url = job_response["response_url"]

        with requests.get(data_url, stream=True) as r:
            r.raise_for_status()

            with gzip.GzipFile(fileobj=r.raw) as gz:
                reader = io.TextIOWrapper(gz, encoding="utf-8")
                for line in reader:
                    yield json.loads(line)

    def get_window_configurations(self, export_start: pendulum.DateTime):
        window_size = self.client.config.get("branch_window_size", BRANCH_MAX_DATE_WINDOW)
        if int(window_size) > 60:
            LOGGER.info(f"Window size {window_size} is greater than 60, setting to max {BRANCH_MAX_DATE_WINDOW}")
            window_size = BRANCH_MAX_DATE_WINDOW

        export_end = export_start.add(days=window_size)
        if export_end >= pendulum.now("UTC"):
            export_end = pendulum.now("UTC")

        export_end = export_end.replace(microsecond=0)

        return export_end

    def sync(self, state: Dict, transformer: Transformer, parent_obj: Dict = None):

        # Build up the required headers
        self.required_headers = self.client.build_headers(endpoint_config=self.endpoint_config,
                                                          headers_data={"Access-Token": self.client.config["branch_access_token"]})

        # Build up the required query_params
        self.required_query_params = self.client.build_query_params(endpoint_config=self.endpoint_config,
                                                                    query_params_data={"app_id": self.client.config["branch_app_id"]})

        # NOTE: We have added a log-interval to the counter to make sure that
        # it is not reset in case of long-running export jobs
        with metrics.record_counter(self.tap_stream_id, log_interval=JOB_TIMEOUT) as counter:
            report_type = self.tap_stream_id
            replication_key = self.replication_keys[0]

            # Initiate date-windowing workflow
            initial_bookmark = export_start = pendulum.parse(bookmarks.get_bookmark(state=state, tap_stream_id=self.tap_stream_id,
                                                                                    key=replication_key, default=self.client.config["start_date"]))

            # Check for data readiness for that specific report_type
            data_ready_api_config = BranchExportConfig(
                method="POST",
                path=self.export_data_readiness_path,
                headers_data=self.required_headers,
                query_params_data=self.required_query_params
            )
            data_ready = self.client.check_data_readiness(export_start=export_start.to_datetime_string(),
                                                          report_type=report_type,
                                                          api_config=data_ready_api_config)
            if data_ready is False:
                LOGGER.info(f"Data is not ready for the time period {export_start} againt the report_type {report_type}")
                return 0

            job_start = pendulum.now("UTC")
            LOGGER.info(f"JOB Start {job_start.to_iso8601_string()} \n")
            max_bookmark = initial_bookmark
            # If data is ready, initiate export job.
            while export_start < job_start:

                window_end = self.get_window_configurations(export_start=export_start)

                create_export_api_config = BranchExportConfig(
                                            method="POST",
                                            path=self.create_export_job_path,
                                            headers_data=self.required_headers,
                                            query_params_data=self.required_query_params,
                                            additional_data={
                                                "start_date": export_start.to_iso8601_string(),
                                                "end_date": window_end.to_iso8601_string(),
                                                "schema_path": BRANCH_EVENTS_SCHEMA
                                            }
                                        )
                request_handle = self.client.create_export_job(report_type=report_type, api_config=create_export_api_config)

                # Poll for export job status
                poll_export_api_config = BranchExportConfig(
                                            method="GET",
                                            path=self.poll_export_job_path.format(request_handle=request_handle),
                                            headers_data=self.required_headers,
                                            query_params_data=self.required_query_params
                                        )
                is_export_ready, export_job_response = self.client.check_export_job_status(request_handle=request_handle,
                                                                                           api_config=poll_export_api_config)

                # Finally get the export job response and yield records
                if is_export_ready:
                    for record in self.extract_data(job_response=export_job_response):
                        transformed_record = transformer.transform(
                            record, self.schema, self.metadata
                        )
                        record_bookmark = pendulum.parse(transformed_record[replication_key])
                        if record_bookmark >= initial_bookmark:
                            if self.is_selected():
                                write_record(self.tap_stream_id, transformed_record)
                                counter.increment()

                            max_bookmark = max(max_bookmark, record_bookmark)

                    # Once done with the extraction of the current batch, update the bookmark
                    state = bookmarks.write_bookmark(state=state, tap_stream_id=self.tap_stream_id,
                                                     key=replication_key, val=max_bookmark.to_iso8601_string())
                    # Write the state file
                    singer.write_state(state)

                export_start = window_end

            return counter.value
