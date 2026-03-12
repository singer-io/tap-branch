"""Verify that eo_click resumes correctly from an interrupted-sync state.

An "interrupted" state contains both a ``currently_syncing`` key and an
existing bookmark.  The stream must:
  - Continue from the existing bookmark (not from start_date).
  - Write records whose timestamp is >= the bookmark.
  - Skip records whose timestamp is < the bookmark.
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

# Fixed 'now' → 1 window iteration regardless of when the suite runs.
FIXED_NOW = pendulum.datetime(2026, 3, 11, tz="UTC")


class TestInterruptedSync(BranchBaseTest, unittest.TestCase):
    """eo_click correctly handles a state with an existing bookmark."""

    START_DATE = "2026-01-01T00:00:00Z"
    EXISTING_BOOKMARK = "2026-02-01T00:00:00Z"
    NEWER_RECORD_DATE = "2026-02-15T00:00:00Z"   # after bookmark
    OLDER_RECORD_DATE = "2026-01-15T00:00:00Z"   # before bookmark

    def _build_interrupted_state(self):
        return {
            "currently_syncing": STREAM,
            "bookmarks": {STREAM: {REPLICATION_KEY: self.EXISTING_BOOKMARK}},
        }

    def _run_sync(self, state, record_date,
                  mock_extract, mock_write_record, mock_write_state):
        """Configure injected mocks, run sync, return list of written records."""
        record = self._generate_stream_record(STREAM, date_value=record_date)
        written = []

        mock_extract.side_effect = lambda job_response: iter([record])
        mock_write_record.side_effect = lambda sid, rec: written.append(rec)
        mock_write_state.side_effect = lambda s: None

        config = self.make_config(start_date=self.START_DATE)
        config["branch_window_size"] = "60"
        client = Client(config)
        stream_entry = self._get_selected_stream(STREAM)
        stream = STREAMS[STREAM](client, stream_entry)
        stream.sync(state=state, transformer=Transformer())
        return written

    # ------------------------------------------------------------------
    # Test methods
    # ------------------------------------------------------------------

    @patch("tap_branch.streams.branch_events.singer.write_state")
    @patch("tap_branch.streams.branch_events.write_record")
    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream.extract_data")
    @patch("tap_branch.client.Client.check_export_job_status", return_value=(True, {"response_url": "http://mock"}))
    @patch("tap_branch.client.Client.create_export_job", return_value="mock_handle")
    @patch("tap_branch.client.Client.check_data_readiness", return_value=True)
    @patch("tap_branch.streams.branch_events.pendulum.now", return_value=FIXED_NOW)
    def test_resumes_from_existing_bookmark(
        self,
        _mock_now,
        _mock_ready,
        _mock_create,
        _mock_status,
        mock_extract,
        mock_write_record,
        mock_write_state,
    ):
        """A record newer than the existing bookmark is written after resume."""
        state = self._build_interrupted_state()
        written = self._run_sync(
            state, self.NEWER_RECORD_DATE,
            mock_extract, mock_write_record, mock_write_state,
        )
        self.assertEqual(len(written), 1,
                         "Record newer than bookmark should be replicated on resume")

    @patch("tap_branch.streams.branch_events.singer.write_state")
    @patch("tap_branch.streams.branch_events.write_record")
    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream.extract_data")
    @patch("tap_branch.client.Client.check_export_job_status", return_value=(True, {"response_url": "http://mock"}))
    @patch("tap_branch.client.Client.create_export_job", return_value="mock_handle")
    @patch("tap_branch.client.Client.check_data_readiness", return_value=True)
    @patch("tap_branch.streams.branch_events.pendulum.now", return_value=FIXED_NOW)
    def test_old_record_skipped_when_resuming(
        self,
        _mock_now,
        _mock_ready,
        _mock_create,
        _mock_status,
        mock_extract,
        mock_write_record,
        mock_write_state,
    ):
        """A record older than the existing bookmark is skipped on resume."""
        state = self._build_interrupted_state()
        written = self._run_sync(
            state, self.OLDER_RECORD_DATE,
            mock_extract, mock_write_record, mock_write_state,
        )
        self.assertEqual(len(written), 0,
                         "Record before existing bookmark should not be replicated")

    @patch("tap_branch.streams.branch_events.singer.write_state")
    @patch("tap_branch.streams.branch_events.write_record")
    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream.extract_data")
    @patch("tap_branch.client.Client.check_export_job_status", return_value=(True, {"response_url": "http://mock"}))
    @patch("tap_branch.client.Client.create_export_job", return_value="mock_handle")
    @patch("tap_branch.client.Client.check_data_readiness", return_value=True)
    @patch("tap_branch.streams.branch_events.pendulum.now", return_value=FIXED_NOW)
    def test_sync_with_currently_syncing_in_state(
        self,
        _mock_now,
        _mock_ready,
        _mock_create,
        _mock_status,
        mock_extract,
        mock_write_record,
        mock_write_state,
    ):
        """eo_click syncs successfully even when currently_syncing is present."""
        state = self._build_interrupted_state()
        written = self._run_sync(
            state, self.NEWER_RECORD_DATE,
            mock_extract, mock_write_record, mock_write_state,
        )
        self.assertEqual(len(written), 1,
                         "eo_click should run normally with currently_syncing in state")
