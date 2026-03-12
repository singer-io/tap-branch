"""Verify that start_date config controls which eo_click records are replicated.

All tests use a fixed 'now' and a 60-day window so the date-windowing loop
terminates in a single iteration regardless of when the suite is executed.
"""
import unittest
from unittest.mock import patch

import pendulum
from singer import Transformer

from tap_branch.client import Client
from tap_branch.streams import STREAMS

from base import BranchBaseTest

STREAM = "eo_click"
REPLICATION_KEY = "timestamp"

# Fixed 'now' keeps the windowing loop to exactly 1 iteration for every
# start-date used in this file (all are within 60 days of FIXED_NOW).
FIXED_NOW = pendulum.datetime(2026, 3, 11, tz="UTC")


class TestStartDate(BranchBaseTest, unittest.TestCase):
    """start_date config determines the earliest record that is replicated."""

    START_DATE_1 = "2026-01-12T11:03:00Z"
    START_DATE_2 = "2026-02-01T00:00:00Z"

    # Timestamps relative to the two start dates
    BEFORE_START_DATE_1 = "2026-01-01T00:00:00Z"   # before both
    AT_START_DATE_1 = "2026-01-12T11:03:00Z"        # == START_DATE_1
    BETWEEN_DATES = "2026-01-20T00:00:00Z"          # between the two
    AFTER_START_DATE_2 = "2026-02-15T00:00:00Z"     # after both

    def _run_sync(self, start_date, record_date,
                  mock_extract, mock_write_record, mock_write_state):
        """Configure injected mocks, run sync, return list of written records."""
        record = self._generate_stream_record(STREAM, date_value=record_date)
        written = []

        mock_extract.side_effect = lambda job_response: iter([record])
        mock_write_record.side_effect = lambda sid, rec: written.append(rec)
        mock_write_state.side_effect = lambda s: None

        config = self.make_config(start_date=start_date)
        config["branch_window_size"] = "60"
        client = Client(config)
        stream_entry = self._get_selected_stream(STREAM)
        stream = STREAMS[STREAM](client, stream_entry)
        stream.sync(state={}, transformer=Transformer())
        return written

    # ------------------------------------------------------------------
    # Test methods — all share identical @patch stacks
    # ------------------------------------------------------------------

    @patch("tap_branch.streams.branch_events.singer.write_state")
    @patch("tap_branch.streams.branch_events.write_record")
    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream.extract_data")
    @patch("tap_branch.client.Client.check_export_job_status", return_value=(True, {"response_url": "http://mock"}))
    @patch("tap_branch.client.Client.create_export_job", return_value="mock_handle")
    @patch("tap_branch.client.Client.check_data_readiness", return_value=True)
    @patch("tap_branch.streams.branch_events.pendulum.now", return_value=FIXED_NOW)
    def test_record_at_start_date_is_replicated(
        self,
        _mock_now,
        _mock_ready,
        _mock_create,
        _mock_status,
        mock_extract,
        mock_write_record,
        mock_write_state,
    ):
        """A record whose timestamp equals start_date must be replicated."""
        written = self._run_sync(
            self.START_DATE_1, self.AT_START_DATE_1,
            mock_extract, mock_write_record, mock_write_state,
        )
        self.assertEqual(len(written), 1,
                         "Record at start_date should be replicated")

    @patch("tap_branch.streams.branch_events.singer.write_state")
    @patch("tap_branch.streams.branch_events.write_record")
    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream.extract_data")
    @patch("tap_branch.client.Client.check_export_job_status", return_value=(True, {"response_url": "http://mock"}))
    @patch("tap_branch.client.Client.create_export_job", return_value="mock_handle")
    @patch("tap_branch.client.Client.check_data_readiness", return_value=True)
    @patch("tap_branch.streams.branch_events.pendulum.now", return_value=FIXED_NOW)
    def test_record_before_start_date_is_not_replicated(
        self,
        _mock_now,
        _mock_ready,
        _mock_create,
        _mock_status,
        mock_extract,
        mock_write_record,
        mock_write_state,
    ):
        """A record whose timestamp is before start_date must be skipped."""
        written = self._run_sync(
            self.START_DATE_1, self.BEFORE_START_DATE_1,
            mock_extract, mock_write_record, mock_write_state,
        )
        self.assertEqual(len(written), 0,
                         "Record before start_date should not be replicated")

    @patch("tap_branch.streams.branch_events.singer.write_state")
    @patch("tap_branch.streams.branch_events.write_record")
    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream.extract_data")
    @patch("tap_branch.client.Client.check_export_job_status", return_value=(True, {"response_url": "http://mock"}))
    @patch("tap_branch.client.Client.create_export_job", return_value="mock_handle")
    @patch("tap_branch.client.Client.check_data_readiness", return_value=True)
    @patch("tap_branch.streams.branch_events.pendulum.now", return_value=FIXED_NOW)
    def test_record_after_start_date_is_replicated(
        self,
        _mock_now,
        _mock_ready,
        _mock_create,
        _mock_status,
        mock_extract,
        mock_write_record,
        mock_write_state,
    ):
        """A record after start_date must be replicated."""
        written = self._run_sync(
            self.START_DATE_1, self.AFTER_START_DATE_2,
            mock_extract, mock_write_record, mock_write_state,
        )
        self.assertEqual(len(written), 1,
                         "Record after start_date should be replicated")

    @patch("tap_branch.streams.branch_events.singer.write_state")
    @patch("tap_branch.streams.branch_events.write_record")
    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream.extract_data")
    @patch("tap_branch.client.Client.check_export_job_status", return_value=(True, {"response_url": "http://mock"}))
    @patch("tap_branch.client.Client.create_export_job", return_value="mock_handle")
    @patch("tap_branch.client.Client.check_data_readiness", return_value=True)
    @patch("tap_branch.streams.branch_events.pendulum.now", return_value=FIXED_NOW)
    def test_later_start_date_excludes_earlier_records(
        self,
        _mock_now,
        _mock_ready,
        _mock_create,
        _mock_status,
        mock_extract,
        mock_write_record,
        mock_write_state,
    ):
        """A record between START_DATE_1 and START_DATE_2 must be skipped when
        start_date is set to START_DATE_2."""
        written = self._run_sync(
            self.START_DATE_2, self.BETWEEN_DATES,
            mock_extract, mock_write_record, mock_write_state,
        )
        self.assertEqual(len(written), 0,
                         "Record before START_DATE_2 should be excluded")

    @patch("tap_branch.streams.branch_events.singer.write_state")
    @patch("tap_branch.streams.branch_events.write_record")
    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream.extract_data")
    @patch("tap_branch.client.Client.check_export_job_status", return_value=(True, {"response_url": "http://mock"}))
    @patch("tap_branch.client.Client.create_export_job", return_value="mock_handle")
    @patch("tap_branch.client.Client.check_data_readiness", return_value=True)
    @patch("tap_branch.streams.branch_events.pendulum.now", return_value=FIXED_NOW)
    def test_more_records_with_earlier_start_date(
        self,
        _mock_now,
        _mock_ready,
        _mock_create,
        _mock_status,
        mock_extract,
        mock_write_record,
        mock_write_state,
    ):
        """Earlier start_date includes a record that the later one excludes."""
        # Sync with START_DATE_1 → BETWEEN_DATES record qualifies
        written_1 = self._run_sync(
            self.START_DATE_1, self.BETWEEN_DATES,
            mock_extract, mock_write_record, mock_write_state,
        )

        # Reset mocks, then sync with START_DATE_2 → same record is excluded
        mock_extract.reset_mock()
        mock_write_record.reset_mock()
        mock_write_state.reset_mock()

        written_2 = self._run_sync(
            self.START_DATE_2, self.BETWEEN_DATES,
            mock_extract, mock_write_record, mock_write_state,
        )

        self.assertGreater(
            len(written_1), len(written_2),
            msg="Earlier start_date should yield more records",
        )
