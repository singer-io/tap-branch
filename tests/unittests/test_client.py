import unittest
from unittest.mock import MagicMock, patch

import pendulum
import requests
from parameterized import parameterized
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout

from tap_branch.client import Client
from tap_branch.exceptions import *

default_config = {
    "base_url": "https://api.example.com",
    "request_timeout": 30,
    "auth_token": "dummy_token",
    "branch_app_id": "1234",
}

DEFAULT_REQUEST_TIMEOUT = 300


class MockResponse:
    """Mocked standard HTTPResponse to test error handling."""

    def __init__(
        self, status_code, resp="", content=[""], headers=None, raise_error=True, text={}
    ):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error
        self.text = text
        self.reason = "error"

    def raise_for_status(self):
        """If an error occur, this method returns a HTTPError object.

        Raises:
            requests.HTTPError: Mock http error.

        Returns:
            int: Returns status code if not error occurred.
        """
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("mock sample message")

    def json(self):
        """Returns a JSON object of the result."""
        return self.text


class TestClient(unittest.TestCase):

    def setUp(self):
        """Set up the client with default configuration."""
        self.client = Client(default_config)

    @parameterized.expand([    
        ["empty value", "", DEFAULT_REQUEST_TIMEOUT],
        ["string value", "12", 12.0],
        ["integer value", 10, 10.0],
        ["float value", 20.0, 20.0],
        ["zero value", 0, DEFAULT_REQUEST_TIMEOUT]
    ])
    @patch("tap_branch.client.session")
    def test_client_initialization(self, test_name, input_value, expected_value, mock_session):
        default_config["request_timeout"] = input_value
        client = Client(default_config)
        assert client.request_timeout == expected_value
        assert isinstance(client._session, mock_session().__class__)

    @patch("tap_branch.client.Client._Client__make_request")
    def test_client_get(self, mock_make_request):
        mock_make_request.return_value = {"data": "ok"}
        result = self.client.make_request("GET", "https://api.example.com/resource")
        assert result == {"data": "ok"}
        mock_make_request.assert_called_once()

    @patch("tap_branch.client.Client._Client__make_request")
    def test_client_post(self, mock_make_request):
        mock_make_request.return_value = {"created": True}
        result = self.client.make_request("POST", "https://api.example.com/resource", body={"key": "value"})
        assert result == {"created": True}
        mock_make_request.assert_called_once()

    @parameterized.expand([
        ["400 error", 400, MockResponse(400), BranchBadRequestError, "A validation exception has occurred."],
        ["401 error", 401, MockResponse(401), BranchUnauthorizedError, "The access token provided is expired, revoked, malformed or invalid for other reasons."],
        ["403 error", 403, MockResponse(403), BranchForbiddenError, "You are missing the following required scopes: read"],
        ["404 error", 404, MockResponse(404), BranchNotFoundError, "The resource you have specified cannot be found."],
        ["409 error", 409, MockResponse(409), BranchConflictError, "The API request cannot be completed because the requested operation would conflict with an existing item."],
        ["422 error", 422, MockResponse(422), BranchUnprocessableEntityError, "The request content itself is not processable by the server."],
    ])
    def test_make_request_http_failure_without_retry(self, test_name, error_code, mock_response, error, error_message):

        with patch.object(self.client._session, "request", return_value=mock_response):
            with self.assertRaises(error) as e:
                self.client._Client__make_request("GET", "https://api.example.com/resource")

        expected_error_message = (f"HTTP-error-code: {error_code}, Error: {error_message}")
        self.assertEqual(str(e.exception), expected_error_message)

    @parameterized.expand([
        ["429 error", 429, MockResponse(429), BranchRateLimitError, "The API rate limit for your organisation/application pairing has been exceeded."],
        ["500 error", 500, MockResponse(500), BranchInternalServerError, "The server encountered an unexpected condition which prevented it from fulfilling the request."],
        ["501 error", 501, MockResponse(501), BranchNotImplementedError, "The server does not support the functionality required to fulfill the request."],
        ["502 error", 502, MockResponse(502), BranchBadGatewayError, "Server received an invalid response."],
        ["503 error", 503, MockResponse(503), BranchServiceUnavailableError, "API service is currently unavailable."],
    ])
    @patch("time.sleep")
    def test_make_request_http_failure_with_retry(self, test_name, error_code, mock_response, error, error_message, mock_sleep):

        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            with self.assertRaises(error) as e:
                self.client._Client__make_request("GET", "https://api.example.com/resource")

            expected_error_message = (f"HTTP-error-code: {error_code}, Error: {error_message}")
            self.assertEqual(str(e.exception), expected_error_message)
            self.assertEqual(mock_request.call_count, 6)

    @parameterized.expand([
        ["ConnectionResetError", ConnectionResetError],
        ["ConnectionError", ConnectionError],
        ["ChunkedEncodingError", ChunkedEncodingError],
        ["Timeout", Timeout],
    ])
    @patch("time.sleep")
    def test_make_request_other_failure_with_retry(self, test_name, error, mock_sleep):

        with patch.object(self.client._session, "request", side_effect=error) as mock_request:
            with self.assertRaises(error) as e:
                self.client._Client__make_request("GET", "https://api.example.com/resource")

            self.assertEqual(mock_request.call_count, 6)

    @patch("tap_branch.branch_constants.JOB_TIMEOUT", 10)
    @patch("tap_branch.branch_constants.POLL_INTERVAL", 2)
    @patch("tap_branch.client.Client.poll_export_job", return_value=("complete", {"status": "complete"}))
    def test_check_export_job_status_success(self, mock_poll_export_job):
        """ Test to validate export job is successfully created """

        client = Client(default_config)
        result = client.check_export_job_status(request_handle="dummy_handle", api_config=None)

        # Check the result is as expected
        self.assertEqual(result, (True, {"status": "complete"}))

        # Assert that poll_export_job was called once
        mock_poll_export_job.assert_called_once()

    @patch("tap_branch.client.Client.poll_export_job")
    def test_export_fail(self, mock_poll):
        """ Test to validate export job failure scenario """

        client = Client(default_config)

        # Simulate export job status polling returning a failure status
        mock_poll.side_effect = [
            ("fail", {"status": "fail"})
        ]

        with self.assertRaises(BranchExportFailed) as ctx:
            client.check_export_job_status("dummy", None)

        self.assertEqual(
            str(ctx.exception),
            "Export job failed with status: fail"
        )

        self.assertEqual(mock_poll.call_count, 1)

    @patch("tap_branch.client.JOB_TIMEOUT", 10)
    @patch("tap_branch.client.POLL_INTERVAL", 2)
    @patch("tap_branch.client.time.sleep", return_value=None)
    @patch("tap_branch.client.pendulum.now")
    @patch("tap_branch.client.Client.poll_export_job")
    def test_export_timeout(self, mock_poll, mock_now, mock_sleep):
        """ Test to validate export job time-out scenario """

        client = Client(default_config)

        start = pendulum.datetime(2026, 1, 1, tz="UTC")

        # Gradually increase the time returned by pendulum.now() to simulate the passage of time during polling
        mock_now.side_effect = [
            start,
            start.add(seconds=2),
            start.add(seconds=4),
            start.add(seconds=6),
            start.add(seconds=8),
            start.add(seconds=11),
        ]

        # Every time the export job status is polled, return "in_progress" to simulate a long-running job that never completes
        mock_poll.return_value = ("in_progress", {"status": "in_progress"})

        with self.assertRaises(BranchExportTimeout):
            client.check_export_job_status("dummy", None)

        self.assertEqual(mock_poll.call_count, 4)

    @patch("tap_branch.branch_api_contract.BranchExportJobPayload.to_payload", return_value={"key": "value"})
    @patch("tap_branch.client.Client.make_request", return_value={"handle": "dummy_handle"})
    def test_create_export_job(self, mock_make_request, mock_payload):
        """ Test to validate successful export job creation """

        client = Client(default_config)

        result = client.create_export_job(report_type="dummy_report", api_config=MagicMock())

        self.assertEqual(result, "dummy_handle")
        mock_make_request.assert_called_once()

    @patch("tap_branch.branch_api_contract.BranchExportJobPayload.to_payload", return_value={"key": "value"})
    @patch("tap_branch.client.Client.make_request", side_effect=BranchUnsupportedFieldsError(fields=["field1", "field2"], raw_response={}))
    def test_create_export_job_exception(self, mock_make_request, mock_payload):
        """ Test to validate export job API returns Bad-Request with Unsupported Fields """

        client = Client(default_config)

        with self.assertRaises(BranchUnsupportedFieldsError) as ctx:
            client.create_export_job(report_type="dummy_report", api_config=MagicMock())
        self.assertEqual(ctx.exception.fields, ["field1", "field2"])

        self.assertEqual(mock_make_request.call_count, 2)
