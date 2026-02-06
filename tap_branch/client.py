import time
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple

import backoff
import pendulum
import requests
from requests import session
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout
from singer import get_logger, metrics

from tap_branch.branch_api_contract import (BranchDataReadyPayload,
                                            BranchExportConfig,
                                            BranchExportJobPayload,
                                            EndpointConfig)
from tap_branch.branch_constants import (JOB_TIMEOUT, MAX_RECORDS_TO_FETCH,
                                         POLL_INTERVAL)
from tap_branch.branch_utils import (check_branch_rate_limit,
                                     handle_branch_validation_error)
from tap_branch.exceptions import (ERROR_CODE_EXCEPTION_MAPPING,
                                   BranchBackoffError, BranchError,
                                   BranchExportFailed, BranchRateLimitError,
                                   BranchUnsupportedFieldsError)

LOGGER = get_logger()
REQUEST_TIMEOUT = 300


def raise_for_error(response: requests.Response) -> None:
    """Raises the associated response exception. Takes in a response object,
    checks the status code, and throws the associated exception based on the
    status code.

    :param resp: requests.Response object
    """
    # Check for branch fatal-rate-limit error first
    check_branch_rate_limit(response=response)

    try:
        response_json = response.json()
    except Exception:
        response_json = {}
    if response.status_code not in [200, 201, 204]:
        if response_json.get("error"):
            message = f"HTTP-error-code: {response.status_code}, Error: {response_json.get('error')}"
        else:
            error_message = ERROR_CODE_EXCEPTION_MAPPING.get(
                response.status_code, {}
            ).get("message", "Unknown Error")
            message = f"HTTP-error-code: {response.status_code}, Error: {response_json.get('message', error_message)}"
        exc = ERROR_CODE_EXCEPTION_MAPPING.get(response.status_code, {}).get(
            "raise_exception", BranchError
        )
        raise exc(message, response) from None


class Client:
    """
    A Wrapper class.
    ~~~
    Performs:
     - Authentication
     - Response parsing
     - HTTP Error handling and retry
    """

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config
        self._session = session()
        self.base_url = "https://api2.branch.io"
        config_request_timeout = config.get("request_timeout")
        self.request_timeout = float(config_request_timeout) if config_request_timeout else REQUEST_TIMEOUT

    def __enter__(self):
        self.check_api_credentials()
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self._session.close()

    def check_api_credentials(self) -> None:
        pass

    def build_headers(self, endpoint_config: EndpointConfig, headers_data: Dict) -> Dict[str, str]:
        """ Function to build headers from the Endpoint specific config

        Args:
            endpoint_config (EndpointConfig): Stream specific Endpoint config
            headers_data (Dict): Headers data that needs to be added

        Returns:
            Dict[str, str]: Formatted final headers
        """

        headers = {}
        required_headers = endpoint_config.required_headers

        for header in required_headers:
            headers[header] = headers_data[header]

        return headers

    def build_query_params(self, endpoint_config: EndpointConfig, query_params_data: Dict) -> Dict[str, str]:
        """ Function to build query params from the Endpoint specific config

        Args:
            endpoint_config (EndpointConfig): Stream specific Endpoint config
            query_params_data (Dict): Query Params data that needs to be added

        Returns:
            Dict[str, str]: Formatted final query_params
        """

        params = {}
        required_query_params = endpoint_config.required_query_params

        for query_param in required_query_params:
            params[query_param] = query_params_data[query_param]

        return params

    def authenticate(self, headers: Dict, params: Dict) -> Tuple[Dict, Dict]:
        """Authenticates the request with the token"""

        params.update({
            "app_id": self.config["branch_app_id"]
        })

        headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json"
        })

        return headers, params

    def make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        path: Optional[str] = None
    ) -> Any:
        """
        Sends an HTTP request to the specified API endpoint.
        """
        params = params or {}
        headers = headers or {}
        body = body or {}
        endpoint = endpoint or f"{self.base_url}/{path}"
        headers, params = self.authenticate(headers, params)
        return self.__make_request(
            method, endpoint,
            headers=headers,
            params=params,
            json=body,
            timeout=self.request_timeout
        )

    @backoff.on_exception(
        wait_gen=backoff.expo,
        exception=(
            ConnectionResetError,
            ConnectionError,
            ChunkedEncodingError,
            Timeout,
            BranchBackoffError,
            BranchRateLimitError
        ),
        max_tries=6,
        factor=2,
    )
    def __make_request(
        self, method: str, endpoint: str, **kwargs
    ) -> Optional[Mapping[Any, Any]]:
        """Performs HTTP Operations."""
        method = method.upper()
        with metrics.http_request_timer(endpoint):
            if method in ("GET", "POST"):
                if method == "GET":
                    kwargs.pop("json", None)
                response = self._session.request(method, endpoint, **kwargs)
                if response.status_code == 400:
                    # Specific condition to handle validation error for export Job
                    handle_branch_validation_error(response)
                raise_for_error(response)
            else:
                raise ValueError(f"Unsupported method: {method}")

        return response.json()

    def check_data_readiness(self, export_start: str,
                             report_type: str, api_config: BranchExportConfig):
        """ Function to check readiness of data for the specific export start

        Args:
            export_start (str): Datetime when the export job is about to start
            report_type (str): Report type
            api_config (BranchExportConfig): API endoint specific config

        """

        data_ready_payload = BranchDataReadyPayload(
                                date=export_start,
                                warehouse_meta_type="EVENT",
                                topic=report_type,
                                app_id=self.config["branch_app_id"]
                            )
        payload_data = data_ready_payload.to_payload()

        data_ready_response = self.make_request(
                                method=api_config.method,
                                endpoint=None,
                                path=api_config.path,
                                params=api_config.query_params_data,
                                headers=api_config.headers_data,
                                body=payload_data
                            )

        is_data_ready = data_ready_response["data_ready"]

        return is_data_ready

    def poll_export_job(self, api_config: BranchExportConfig):
        """ Function to check and poll for the status of export job

        Args:
            api_config (BranchExportConfig): Endpoint specific config

        Returns:
            Tuple[str, dict]: Tuple of status and response json
        """

        response = self.make_request(
                        method=api_config.method,
                        endpoint=None,
                        path=api_config.path,
                        params=api_config.query_params_data,
                        headers=api_config.headers_data
                    )

        status = response.get("status", "NA")

        return status, response

    def check_export_job_status(self, request_handle, api_config: BranchExportConfig):
        """ Function to check the export job

        Args:
            request_handle (str): Request handle for the Export Job
            api_config (BranchExportConfig): Endpoint specific config

        Raises:
            BranchExportFailed: In case if the export job fails or exceeds the set timeout
        """

        timeout_time = pendulum.now("UTC").add(seconds=JOB_TIMEOUT)
        while pendulum.now("UTC") < timeout_time:
            status, export_job_response = self.poll_export_job(api_config=api_config)
            LOGGER.info("Current export status of handle %s is %s", request_handle, status)

            if status == "complete":
                # If the status is finished, then the data is ready to be consumed
                return True, export_job_response

            elif status in ["cancelled", "fail"]:
                raise BranchExportFailed("Export job failed with status: {}".format(status))

            time.sleep(POLL_INTERVAL)

        raise BranchExportFailed("Export timed out after {} minutes".format(JOB_TIMEOUT / 60))

    def create_export_job(self, report_type: str, api_config: BranchExportConfig):
        """ Function to create a export Job for the specified report_type

        Args:
            report_type (str): Report type
            api_config (BranchExportConfig): Endpoint specific config

        """

        start_date = api_config.additional_data["start_date"]
        end_date = api_config.additional_data["end_date"]

        export_job_payload = BranchExportJobPayload(
                                start_date=start_date,
                                end_date=end_date,
                                report_type=report_type,
                                schema_path=Path(api_config.additional_data["schema_path"]),
                                limit=MAX_RECORDS_TO_FETCH,
                                filter=[],
                                response_format="json",
                                response_format_compression="gz",
                                allow_multiple_files=True
                            )
        payload_data = export_job_payload.to_payload()

        # NOTE: We don't have any mapping of report_type specific fields. Hence we pass all fields to the payload
        # The API returns Bad request and we catch the same, exclude the fields and re-raise request
        try:
            export_job_response = self.make_request(
                                    method=api_config.method,
                                    endpoint=None,
                                    path=api_config.path,
                                    params=api_config.query_params_data,
                                    headers=api_config.headers_data,
                                    body=payload_data
                                )
        except BranchUnsupportedFieldsError as e:
            rejected_fields = e.fields
            new_payload_data = export_job_payload.to_payload(rejected_fields=rejected_fields)
            export_job_response = self.make_request(
                                    method=api_config.method,
                                    endpoint=None,
                                    path=api_config.path,
                                    params=api_config.query_params_data,
                                    headers=api_config.headers_data,
                                    body=new_payload_data
                                )

        request_handle = export_job_response["handle"]
        LOGGER.info(f"Received request_handle {request_handle} for export report_type {report_type}")

        return request_handle
