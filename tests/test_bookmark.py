"""Test that eo_click (incremental) correctly writes and advances its bookmark.

Two sync runs are performed with mock data:
  - Sync 1 uses a record at start_date → bookmark advances to that timestamp.
  - Sync 2 uses a record at a later timestamp → bookmark advances further.
"""
import copy
import unittest
from unittest.mock import patch

import pendulum
from singer import Transformer

from tap_branch.client import Client
from tap_branch.streams import STREAMS

from base import BranchBaseTest

STREAM = "eo_click"
REPLICATION_KEY = "timestamp"

# Fixed 'now' so the date-windowing loop terminates in exactly 1-2 windows
# regardless of when the test suite is executed.
FIXED_NOW = pendulum.datetime(2026, 3, 11, tz="UTC")


class TestBookmark(BranchBaseTest, unittest.TestCase):
    """Verify eo_click bookmark is written and advances across syncs."""

    START_DATE = "2026-01-01T00:00:00Z"
    RECORD_DATE_1 = "2026-01-12T11:03:00Z"
    RECORD_DATE_2 = "2026-02-01T00:00:00Z"

    def _build_stream(self, start_date):
        # Use a 60-day window (API maximum) so the loop runs at most twice
        # between START_DATE and FIXED_NOW instead of ~69 single-day iterations.
        config = self.make_config(start_date=start_date)
        config["branch_window_size"] = "60"
        client = Client(config)
        stream_entry = self._get_selected_stream(STREAM)
        return STREAMS[STREAM](client, stream_entry)

    def _configure_and_run(self, stream, state, record_date,
                           mock_extract, mock_write_record, mock_write_state):
        """Set side_effects on injected mocks, run sync; return (written, last_state).

        ``last_state`` is a deep-copy of the state captured on the last
        ``write_state`` call, so it is not affected by later mutations.
        """
        record = self._generate_stream_record(STREAM, date_value=record_date)
        written = []
        last_state = [None]

        mock_extract.side_effect = lambda job_response: iter([record])
        mock_write_record.side_effect = lambda sid, rec: written.append(rec)
        mock_write_state.side_effect = (
            lambda s: last_state.__setitem__(0, copy.deepcopy(s))
        )

        stream.sync(state=state, transformer=Transformer())
        return written, last_state[0] or state

    @patch("tap_branch.streams.branch_events.singer.write_state")
    @patch("tap_branch.streams.branch_events.write_record")
    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream.extract_data")
    @patch("tap_branch.client.Client.check_export_job_status", return_value=(True, {"response_url": "http://mock"}))
    @patch("tap_branch.client.Client.create_export_job", return_value="mock_handle")
    @patch("tap_branch.client.Client.check_data_readiness", return_value=True)
    @patch("tap_branch.streams.branch_events.pendulum.now", return_value=FIXED_NOW)
    def test_bookmark_is_written_after_sync(
        self,
        _mock_now,
        _mock_ready,
        _mock_create,
        _mock_status,
        mock_extract,
        mock_write_record,
        mock_write_state,
    ):
        """A bookmark is persisted into state after the first sync."""
        stream = self._build_stream(self.START_DATE)
        _, state_after = self._configure_and_run(
            stream, {}, self.RECORD_DATE_1,
            mock_extract, mock_write_record, mock_write_state,
        )
        bookmark = (
            state_after.get("bookmarks", {})
            .get(STREAM, {})
            .get(REPLICATION_KEY)
        )
        self.assertIsNotNone(bookmark, "No bookmark found after first sync")

    @patch("tap_branch.streams.branch_events.singer.write_state")
    @patch("tap_branch.streams.branch_events.write_record")
    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream.extract_data")
    @patch("tap_branch.client.Client.check_export_job_status", return_value=(True, {"response_url": "http://mock"}))
    @patch("tap_branch.client.Client.create_export_job", return_value="mock_handle")
    @patch("tap_branch.client.Client.check_data_readiness", return_value=True)
    @patch("tap_branch.streams.branch_events.pendulum.now", return_value=FIXED_NOW)
    def test_bookmark_advances_with_newer_record(
        self,
        _mock_now,
        _mock_ready,
        _mock_create,
        _mock_status,
        mock_extract,
        mock_write_record,
        mock_write_state,
    ):
        """After a second sync with a newer record, the bookmark advances."""
        # Sync 1 — record at RECORD_DATE_1
        stream1 = self._build_stream(self.START_DATE)
        _, state_after_1 = self._configure_and_run(
            stream1, {}, self.RECORD_DATE_1,
            mock_extract, mock_write_record, mock_write_state,
        )
        bm1 = state_after_1["bookmarks"][STREAM][REPLICATION_KEY]

        # Sync 2 — re-configure mocks for RECORD_DATE_2; pass deep-copy of
        # state_after_1 so sync 1's snapshot is not mutated in-place.
        mock_extract.reset_mock()
        mock_write_record.reset_mock()
        mock_write_state.reset_mock()

        stream2 = self._build_stream(self.START_DATE)
        _, state_after_2 = self._configure_and_run(
            stream2, copy.deepcopy(state_after_1), self.RECORD_DATE_2,
            mock_extract, mock_write_record, mock_write_state,
        )
        bm2 = state_after_2["bookmarks"][STREAM][REPLICATION_KEY]

        self.assertGreater(bm2, bm1,
                           msg="Bookmark should advance after newer record")

    @patch("tap_branch.streams.branch_events.singer.write_state")
    @patch("tap_branch.streams.branch_events.write_record")
    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream.extract_data")
    @patch("tap_branch.client.Client.check_export_job_status", return_value=(True, {"response_url": "http://mock"}))
    @patch("tap_branch.client.Client.create_export_job", return_value="mock_handle")
    @patch("tap_branch.client.Client.check_data_readiness", return_value=True)
    @patch("tap_branch.streams.branch_events.pendulum.now", return_value=FIXED_NOW)
    def test_records_before_bookmark_are_skipped(
        self,
        _mock_now,
        _mock_ready,
        _mock_create,
        _mock_status,
        mock_extract,
        mock_write_record,
        mock_write_state,
    ):
        """Records whose timestamp is before the current bookmark are not written."""
        state = {
            "bookmarks": {STREAM: {REPLICATION_KEY: self.RECORD_DATE_2}}
        }
        stream = self._build_stream(self.START_DATE)
        # Feed an old record (earlier than existing bookmark)
        written, _ = self._configure_and_run(
            stream, state, self.RECORD_DATE_1,
            mock_extract, mock_write_record, mock_write_state,
        )
        self.assertEqual(len(written), 0,
                         msg="Record older than bookmark should not be written")
