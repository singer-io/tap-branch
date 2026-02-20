import gzip
import io
import json
import unittest
from unittest.mock import MagicMock, patch

import pendulum
from parameterized import parameterized
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout

from tap_branch.branch_api_contract import EndpointConfig
from tap_branch.branch_constants import MAX_BRANCH_DATE_WINDOW
from tap_branch.exceptions import BranchError
from tap_branch.streams.branch_events import BranchEventsBaseStream


# Concrete implementation for testing
class ConcreteBranchEventsStream(BranchEventsBaseStream):
    """Concrete implementation of BranchEventsBaseStream for testing."""
    tap_stream_id = "eo_click"
    replication_keys = ["timestamp"]


class TestBranchEventsBaseStream(unittest.TestCase):
    """Test suite for BranchEventsBaseStream class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_client = MagicMock()
        self.mock_client.config = {
            "branch_app_id": "test_app_id",
            "branch_access_token": "test_token",
            "branch_key": "test_key",
            "branch_secret": "test_secret",
            "branch_window_size": 30,
            "deeplink_urls": "test-url",
            "start_date": "2024-01-01T00:00:00Z",
        }
        self.mock_catalog = MagicMock()
        self.mock_catalog.schema.to_dict.return_value = {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "timestamp": {"type": "string", "format": "date-time"}
            }
        }
        self.mock_catalog.metadata = []

        self.stream = ConcreteBranchEventsStream(
            client=self.mock_client,
            catalog=self.mock_catalog
        )

    def test_stream_initialization(self):
        """Test that stream initializes with correct attributes."""
        self.assertEqual(self.stream.key_properties, ["id"])
        self.assertEqual(self.stream.replication_method, "INCREMENTAL")
        self.assertEqual(self.stream.schema_path, "shared/branch_events")
        self.assertEqual(self.stream.export_data_readiness_path, "v2/data/ready/")
        self.assertEqual(self.stream.create_export_job_path, "v2/logs/")
        self.assertIn("{request_handle}", self.stream.poll_export_job_path)

    def test_endpoint_config(self):
        """Test that endpoint configuration is set correctly."""
        self.assertEqual(self.stream.endpoint_config.required_query_params, {"app_id"})
        self.assertEqual(self.stream.endpoint_config.required_headers, {"Access-Token"})

    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream._fetch_export_data")
    def test_extract_data_success(self, mock_fetch):
        """Test successful data extraction from gzipped response."""
        # Create mock gzipped data
        test_data = [
            {"id": "event1", "timestamp": "2024-01-01T00:00:00Z"},
            {"id": "event2", "timestamp": "2024-01-02T00:00:00Z"}
        ]

        # Create gzipped content
        gzipped_content = io.BytesIO()
        with gzip.GzipFile(fileobj=gzipped_content, mode='wb') as gz:
            for record in test_data:
                gz.write((json.dumps(record) + '\n').encode('utf-8'))
        gzipped_content.seek(0)

        # Mock the response
        mock_response = MagicMock()
        mock_response.raw = gzipped_content
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_fetch.return_value = mock_response

        # Test extraction
        job_response = {"response_url": "https://test.url/data.gz"}
        extracted_records = list(self.stream.extract_data(job_response))

        self.assertEqual(len(extracted_records), 2)
        self.assertEqual(extracted_records[0]["id"], "event1")
        self.assertEqual(extracted_records[1]["id"], "event2")
        mock_fetch.assert_called_once_with("https://test.url/data.gz")

    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream._fetch_export_data")
    def test_extract_data_raises_http_error(self, mock_fetch):
        """Test that extract_data raises an error when HTTP request fails."""
        mock_fetch.side_effect = BranchError("Malformed response")

        job_response = {"response_url": "https://test.url/data.gz"}

        with self.assertRaises(BranchError):
            list(self.stream.extract_data(job_response))

    @parameterized.expand([
        ["default_window_30_days", 30, "2024-01-01T00:00:00Z", "2024-01-31T00:00:00Z"],
        ["window_45_days", 45, "2024-01-01T00:00:00Z", "2024-02-15T00:00:00Z"],
        ["window_60_days", 60, "2024-01-01T00:00:00Z", "2024-03-01T00:00:00Z"],
    ])
    @patch("pendulum.now")
    def test_get_window_configurations_valid_windows(self, test_name, window_size, 
                                                     start_date_str, expected_end_str,
                                                     mock_now):
        """Test window configuration with valid window sizes."""
        mock_now.return_value = pendulum.parse("2024-06-01T00:00:00Z")
        self.mock_client.config["branch_window_size"] = window_size

        export_start = pendulum.parse(start_date_str)
        export_end = self.stream.get_window_configurations(export_start)

        expected_end = pendulum.parse(expected_end_str)
        self.assertEqual(export_end, expected_end)

    @patch("pendulum.now")
    def test_get_window_configurations_exceeds_max(self, mock_now):
        """Test that window size greater than 60 days is capped at max."""
        mock_now.return_value = pendulum.parse("2024-06-01T00:00:00Z")
        self.mock_client.config["branch_window_size"] = 90  # Exceeds max

        export_start = pendulum.parse("2024-01-01T00:00:00Z")
        export_end = self.stream.get_window_configurations(export_start)

        # Should be capped at 60 days
        expected_end = export_start.add(days=MAX_BRANCH_DATE_WINDOW)
        self.assertEqual(export_end, expected_end)

    @patch("pendulum.now")
    def test_get_window_configurations_near_current_time(self, mock_now):
        """Test that window end is capped at current time."""
        current_time = pendulum.parse("2024-01-15T12:00:00Z")
        mock_now.return_value = current_time
        self.mock_client.config["branch_window_size"] = 30

        export_start = pendulum.parse("2024-01-01T00:00:00Z")
        export_end = self.stream.get_window_configurations(export_start)

        # Should be capped at current time (with microsecond = 0)
        expected_end = current_time.replace(microsecond=0)
        self.assertEqual(export_end, expected_end)

    @patch("pendulum.now")
    def test_get_window_configurations_microseconds_removed(self, mock_now):
        """Test that microseconds are removed from export_end."""
        mock_now.return_value = pendulum.parse("2024-06-01T12:34:56.123456Z")
        self.mock_client.config["branch_window_size"] = 10

        export_start = pendulum.parse("2024-01-01T00:00:00Z")
        export_end = self.stream.get_window_configurations(export_start)

        self.assertEqual(export_end.microsecond, 0)

    @patch("singer.write_state")
    @patch("tap_branch.streams.branch_events.write_record")
    @patch("singer.bookmarks.write_bookmark")
    @patch("singer.bookmarks.get_bookmark")
    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream.extract_data")
    def test_sync_success_with_data_ready(self, mock_extract_data, mock_get_bookmark,
                                          mock_write_bookmark, mock_write_record,
                                          mock_write_state):
        """Test successful sync when data is ready."""
        # Setup mocks
        self.stream.metadata = {}

        start_date = "2024-01-01T00:00:00Z"
        mock_get_bookmark.return_value = start_date

        self.mock_client.build_headers.return_value = {"Access-Token": "test_token"}
        self.mock_client.build_query_params.return_value = {"app_id": "test_app_id"}
        self.mock_client.check_data_readiness.return_value = True
        self.mock_client.create_export_job.return_value = "test_request_handle"
        self.mock_client.check_export_job_status.return_value = (
            True,
            {"response_url": "https://test.url/data.gz"}
        )

        # Mock extracted data
        mock_extract_data.return_value = [
            {"id": "event1", "timestamp": "2024-01-01T10:00:00Z"},
            {"id": "event2", "timestamp": "2024-01-01T11:00:00Z"}
        ]

        mock_write_bookmark.return_value = {"bookmarks": {}}

        # Mock transformer
        mock_transformer = MagicMock()
        mock_transformer.transform.side_effect = lambda record, schema, metadata: record

        # Mock is_selected to return True
        self.stream.is_selected = MagicMock(return_value=True)

        state = {}

        with patch("pendulum.now", return_value=pendulum.parse("2024-01-02T00:00:00Z")):
            result = self.stream.sync(state, mock_transformer)

        # Verify data was processed
        self.assertEqual(mock_write_record.call_count, 2)
        self.mock_client.check_data_readiness.assert_called_once()
        self.mock_client.create_export_job.assert_called_once()
        self.mock_client.check_export_job_status.assert_called_once()

    @patch("singer.bookmarks.get_bookmark")
    def test_sync_data_not_ready(self, mock_get_bookmark):
        """Test sync returns 0 when data is not ready."""
        start_date = "2024-01-01T00:00:00Z"
        mock_get_bookmark.return_value = start_date

        self.mock_client.build_headers.return_value = {"Access-Token": "test_token"}
        self.mock_client.build_query_params.return_value = {"app_id": "test_app_id"}
        self.mock_client.check_data_readiness.return_value = False

        mock_transformer = MagicMock()
        state = {}

        result = self.stream.sync(state, mock_transformer)

        self.assertEqual(result, 0)
        self.mock_client.check_data_readiness.assert_called_once()
        self.mock_client.create_export_job.assert_not_called()

    @patch("singer.write_state")
    @patch("tap_branch.streams.branch_events.write_record")
    @patch("singer.bookmarks.write_bookmark")
    @patch("singer.bookmarks.get_bookmark")
    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream.extract_data")
    def test_sync_filters_records_before_bookmark(self, mock_extract_data, mock_get_bookmark,
                                                  mock_write_bookmark, mock_write_record,
                                                  mock_write_state):
        """Test that sync filters out records before the bookmark date."""
        self.stream.metadata = {}

        start_date = "2024-01-01T12:00:00Z"
        mock_get_bookmark.return_value = start_date

        self.mock_client.build_headers.return_value = {"Access-Token": "test_token"}
        self.mock_client.build_query_params.return_value = {"app_id": "test_app_id"}
        self.mock_client.check_data_readiness.return_value = True
        self.mock_client.create_export_job.return_value = "test_request_handle"
        self.mock_client.check_export_job_status.return_value = (
            True,
            {"response_url": "https://test.url/data.gz"}
        )

        # Mock extracted data with records before and after bookmark
        mock_extract_data.return_value = [
            {"id": "event1", "timestamp": "2024-01-01T10:00:00Z"},  # Before bookmark
            {"id": "event2", "timestamp": "2024-01-01T13:00:00Z"},  # After bookmark
            {"id": "event3", "timestamp": "2024-01-01T14:00:00Z"}   # After bookmark
        ]

        mock_write_bookmark.return_value = {"bookmarks": {}}

        mock_transformer = MagicMock()
        mock_transformer.transform.side_effect = lambda record, schema, metadata: record

        self.stream.is_selected = MagicMock(return_value=True)

        state = {}

        with patch("pendulum.now", return_value=pendulum.parse("2024-01-02T00:00:00Z")):
            result = self.stream.sync(state, mock_transformer)

        # Only 2 records should be written (those after bookmark)
        self.assertEqual(mock_write_record.call_count, 2)

    @patch("singer.write_state")
    @patch("tap_branch.streams.branch_events.write_record")
    @patch("singer.bookmarks.write_bookmark")
    @patch("singer.bookmarks.get_bookmark")
    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream.extract_data")
    def test_sync_not_selected_stream(self, mock_extract_data, mock_get_bookmark,
                                      mock_write_bookmark, mock_write_record,
                                      mock_write_state):
        """Test that sync does not write records when stream is not selected."""
        self.stream.metadata = {}

        start_date = "2024-01-01T00:00:00Z"
        mock_get_bookmark.return_value = start_date

        self.mock_client.build_headers.return_value = {"Access-Token": "test_token"}
        self.mock_client.build_query_params.return_value = {"app_id": "test_app_id"}
        self.mock_client.check_data_readiness.return_value = True
        self.mock_client.create_export_job.return_value = "test_request_handle"
        self.mock_client.check_export_job_status.return_value = (
            True,
            {"response_url": "https://test.url/data.gz"}
        )

        mock_extract_data.return_value = [
            {"id": "event1", "timestamp": "2024-01-01T10:00:00Z"}
        ]

        mock_write_bookmark.return_value = {"bookmarks": {}}

        mock_transformer = MagicMock()
        mock_transformer.transform.side_effect = lambda record, schema, metadata: record

        # Stream is not selected
        self.stream.is_selected = MagicMock(return_value=False)

        state = {}

        with patch("pendulum.now", return_value=pendulum.parse("2024-01-02T00:00:00Z")):
            result = self.stream.sync(state, mock_transformer)

        # No records should be written
        mock_write_record.assert_not_called()
        # But bookmark should still be updated
        mock_write_bookmark.assert_called()

    @patch("singer.write_state")
    @patch("tap_branch.streams.branch_events.write_record")
    @patch("singer.bookmarks.write_bookmark")
    @patch("singer.bookmarks.get_bookmark")
    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream.extract_data")
    def test_sync_updates_max_bookmark(self, mock_extract_data, mock_get_bookmark,
                                       mock_write_bookmark, mock_write_record,
                                       mock_write_state):
        """Test that sync correctly tracks the maximum bookmark value."""
        self.stream.metadata = {}

        start_date = "2024-01-01T00:00:00Z"
        mock_get_bookmark.return_value = start_date

        self.mock_client.build_headers.return_value = {"Access-Token": "test_token"}
        self.mock_client.build_query_params.return_value = {"app_id": "test_app_id"}
        self.mock_client.check_data_readiness.return_value = True
        self.mock_client.create_export_job.return_value = "test_request_handle"
        self.mock_client.check_export_job_status.return_value = (
            True,
            {"response_url": "https://test.url/data.gz"}
        )

        # Records in non-chronological order
        mock_extract_data.return_value = [
            {"id": "event1", "timestamp": "2024-01-01T15:00:00Z"},
            {"id": "event2", "timestamp": "2024-01-01T10:00:00Z"},
            {"id": "event3", "timestamp": "2024-01-01T20:00:00Z"},  # Latest
        ]

        mock_write_bookmark.return_value = {"bookmarks": {}}

        mock_transformer = MagicMock()
        mock_transformer.transform.side_effect = lambda record, schema, metadata: record

        self.stream.is_selected = MagicMock(return_value=True)

        state = {}

        with patch("pendulum.now", return_value=pendulum.parse("2024-01-02T00:00:00Z")):
            result = self.stream.sync(state, mock_transformer)

        # Verify bookmark was written with the latest timestamp
        calls = mock_write_bookmark.call_args_list
        self.assertTrue(len(calls) > 0)
        # The last call should have the max bookmark
        last_call_val = calls[-1][1]['val']
        self.assertIn("2024-01-01T20:00:00", last_call_val)

    @patch("singer.write_state")
    @patch("singer.bookmarks.write_bookmark")
    @patch("singer.bookmarks.get_bookmark")
    def test_sync_multiple_windows(self, mock_get_bookmark, mock_write_bookmark, mock_write_state):
        """Test sync with multiple date windows."""
        self.stream.metadata = {}

        # Start date that requires multiple windows
        start_date = "2024-01-01T00:00:00Z"
        mock_get_bookmark.return_value = start_date

        self.mock_client.config["branch_window_size"] = 10  # Small window to force multiple iterations
        self.mock_client.build_headers.return_value = {"Access-Token": "test_token"}
        self.mock_client.build_query_params.return_value = {"app_id": "test_app_id"}
        self.mock_client.check_data_readiness.return_value = True
        self.mock_client.create_export_job.return_value = "test_request_handle"
        self.mock_client.check_export_job_status.return_value = (
            True,
            {"response_url": "https://test.url/data.gz"}
        )

        mock_write_bookmark.return_value = {"bookmarks": {}}

        mock_transformer = MagicMock()
        mock_transformer.transform.side_effect = lambda record, schema, metadata: record

        self.stream.is_selected = MagicMock(return_value=True)

        state = {}

        with patch("pendulum.now", return_value=pendulum.parse("2024-01-25T00:00:00Z")):
            with patch.object(self.stream, "extract_data", return_value=[]):
                result = self.stream.sync(state, mock_transformer)

        # Should create multiple export jobs (at least 2 for 10-day windows over 24 days)
        self.assertGreaterEqual(self.mock_client.create_export_job.call_count, 2)

    def test_sync_builds_correct_headers(self):
        """Test that sync builds headers correctly."""
        self.mock_client.build_headers.return_value = {"Access-Token": "test_token"}
        self.mock_client.build_query_params.return_value = {"app_id": "test_app_id"}
        self.mock_client.check_data_readiness.return_value = False

        mock_transformer = MagicMock()
        state = {}

        with patch("singer.bookmarks.get_bookmark", return_value="2024-01-01T00:00:00Z"):
            self.stream.sync(state, mock_transformer)

        # Verify build_headers was called with correct parameters
        self.mock_client.build_headers.assert_called_once()
        call_args = self.mock_client.build_headers.call_args
        self.assertIsInstance(call_args[1]["endpoint_config"], EndpointConfig)
        self.assertEqual(
            call_args[1]["headers_data"]["Access-Token"],
            self.mock_client.config["branch_access_token"]
        )

    def test_sync_builds_correct_query_params(self):
        """Test that sync builds query parameters correctly."""
        self.mock_client.build_headers.return_value = {"Access-Token": "test_token"}
        self.mock_client.build_query_params.return_value = {"app_id": "test_app_id"}
        self.mock_client.check_data_readiness.return_value = False

        mock_transformer = MagicMock()
        state = {}

        with patch("singer.bookmarks.get_bookmark", return_value="2024-01-01T00:00:00Z"):
            self.stream.sync(state, mock_transformer)

        # Verify build_query_params was called with correct parameters
        self.mock_client.build_query_params.assert_called_once()
        call_args = self.mock_client.build_query_params.call_args
        self.assertIsInstance(call_args[1]["endpoint_config"], EndpointConfig)
        self.assertEqual(
            call_args[1]["query_params_data"]["app_id"],
            self.mock_client.config["branch_app_id"]
        )

    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream._fetch_export_data")
    def test_extract_data_empty_response(self, mock_fetch):
        """Test extract_data with empty gzipped response."""
        # Create empty gzipped content
        gzipped_content = io.BytesIO()
        with gzip.GzipFile(fileobj=gzipped_content, mode='wb') as gz:
            pass  # Empty file
        gzipped_content.seek(0)

        mock_response = MagicMock()
        mock_response.raw = gzipped_content
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_fetch.return_value = mock_response

        job_response = {"response_url": "https://test.url/data.gz"}
        extracted_records = list(self.stream.extract_data(job_response))

        self.assertEqual(len(extracted_records), 0)


class TestExtractDataRetryLogic(unittest.TestCase):
    """Test suite for extract_data retry logic with backoff decorator."""

    @parameterized.expand([
        ["connection_reset_error", ConnectionResetError, "Connection reset"],
        ["connection_error", ConnectionError, "Connection failed"],
        ["timeout", Timeout, "Request timeout"],
        ["chunked_encoding_error", ChunkedEncodingError, "Chunked encoding error"],
    ])
    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream._fetch_export_data")
    def test_fetch_export_data_reraises_network_errors(self, test_name, exception_class, 
                                                        error_message, mock_fetch):
        """Test that _fetch_export_data re-raises network exceptions for backoff to handle."""
        # Disable backoff for this unit test
        with patch("tap_branch.streams.branch_events.backoff.on_exception", lambda *args, **kwargs: lambda f: f):
            mock_fetch.side_effect = exception_class(error_message)

            job_response = {"response_url": "https://test.url/data.gz"}

            # Network errors should be re-raised
            with self.assertRaises(exception_class):
                list(BranchEventsBaseStream.extract_data(job_response))

    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream._fetch_export_data")
    def test_extract_data_wraps_non_network_errors_in_branch_error(self, mock_fetch):
        """Test that non-network exceptions are wrapped in BranchError."""
        mock_response = MagicMock()
        mock_response.__enter__ = MagicMock(side_effect=ValueError("Unexpected error"))
        mock_fetch.return_value = mock_response

        job_response = {"response_url": "https://test.url/data.gz"}

        with self.assertRaises(BranchError) as cm:
            list(BranchEventsBaseStream.extract_data(job_response))

        self.assertIn("Data extraction failed", str(cm.exception))

    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream._fetch_export_data")
    def test_extract_data_successful_with_valid_data(self, mock_fetch):
        """Test successful data extraction with valid gzipped data."""
        # Create mock gzipped data
        test_data = [
            {"id": "event1", "timestamp": "2024-01-01T00:00:00Z"},
            {"id": "event2", "timestamp": "2024-01-02T00:00:00Z"}
        ]
        gzipped_content = io.BytesIO()
        with gzip.GzipFile(fileobj=gzipped_content, mode='wb') as gz:
            for record in test_data:
                gz.write((json.dumps(record) + '\n').encode('utf-8'))
        gzipped_content.seek(0)

        mock_response = MagicMock()
        mock_response.raw = gzipped_content
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_fetch.return_value = mock_response

        job_response = {"response_url": "https://test.url/data.gz"}
        extracted_records = list(BranchEventsBaseStream.extract_data(job_response))

        self.assertEqual(len(extracted_records), 2)
        self.assertEqual(extracted_records[0]["id"], "event1")
        self.assertEqual(extracted_records[1]["id"], "event2")
