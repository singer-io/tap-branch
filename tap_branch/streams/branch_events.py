import gzip
import io
import json
from typing import Dict

import backoff
import pendulum
import requests
import singer
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout
from singer import bookmarks, metrics, write_record
from singer.transform import Transformer

from tap_branch.branch_api_contract import BranchExportConfig, EndpointConfig
from tap_branch.branch_constants import (BRANCH_EVENTS_SCHEMA, JOB_TIMEOUT,
                                         MAX_BRANCH_DATE_WINDOW)
from tap_branch.exceptions import BranchError
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
    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(
            ConnectionResetError,
            ConnectionError,
            ChunkedEncodingError,
            Timeout
        ),
        max_tries=5,
        factor=2,
    )
    def _fetch_export_data(data_url: str):
        """Fetch gzipped export data from URL with retry logic.

        This is a separate function so backoff decorator can properly retry
        network failures, since extract_data is a generator.
        """
        response = requests.get(data_url, stream=True, timeout=JOB_TIMEOUT)
        response.raise_for_status()
        return response

    @staticmethod
    def extract_data(job_response: Dict):

        data_url = job_response["response_url"]

        try:
            # Use helper function with backoff for network request
            r = BranchEventsBaseStream._fetch_export_data(data_url)

            with r:
                with gzip.GzipFile(fileobj=r.raw) as gz:
                    reader = io.TextIOWrapper(gz, encoding="utf-8")
                    line_num = 0
                    for line in reader:
                        line_num += 1
                        try:
                            yield json.loads(line)
                        except json.JSONDecodeError as e:
                            LOGGER.warning("Skipping malformed JSON at line %s: %s", line_num, e)
                            continue

        except (ConnectionResetError, ConnectionError, ChunkedEncodingError, Timeout):
            # Re-raise network errors (already handled by backoff in _fetch_export_data)
            raise

        except Exception as e:
            LOGGER.error("Failed to extract data from %s: %s", data_url, e)
            raise BranchError(f"Data extraction failed: {e}") from e

    def get_window_configurations(self, export_start: pendulum.DateTime):
        now = pendulum.now("UTC")  # Call once
        window_size = int(self.client.config.get("branch_window_size", MAX_BRANCH_DATE_WINDOW))

        if window_size > MAX_BRANCH_DATE_WINDOW:
            LOGGER.warning("Window size %s exceeds max %s, capping", window_size, MAX_BRANCH_DATE_WINDOW)
            window_size = MAX_BRANCH_DATE_WINDOW

        export_end = export_start.add(days=window_size)
        export_end = min(export_end, now)
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
                LOGGER.info("Data is not ready for the time period %s against the report_type %s", export_start, report_type)
                return 0

            job_start = pendulum.now("UTC")
            max_bookmark = initial_bookmark
            # If data is ready, initiate export job.
            while export_start < job_start:

                window_end = self.get_window_configurations(export_start=export_start)
                LOGGER.info("Initiating export job for the time period %s to %s against the report_type %s", export_start, window_end, report_type)

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
                    batch_record_counter = 0
                    for record in self.extract_data(job_response=export_job_response):
                        transformed_record = transformer.transform(
                            record, self.schema, self.metadata
                        )
                        record_bookmark = pendulum.parse(transformed_record[replication_key])
                        if record_bookmark >= initial_bookmark:
                            if self.is_selected():
                                write_record(self.tap_stream_id, transformed_record)
                                counter.increment()
                                batch_record_counter += 1

                            max_bookmark = max(max_bookmark, record_bookmark)

                    # Once done with the extraction of the current batch, update the bookmark
                    state = bookmarks.write_bookmark(state=state, tap_stream_id=self.tap_stream_id,
                                                     key=replication_key, val=max_bookmark.to_iso8601_string())
                    LOGGER.info("Processed %s records for the time period %s to %s against the report_type %s", batch_record_counter, export_start, window_end, report_type)
                    # Write the state file
                    singer.write_state(state)

                export_start = window_end

            return counter.value
