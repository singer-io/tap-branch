"""Verify that every expected field is present in synced records.

Full-table streams (deeplink, app_config) mock Client.make_request.
The incremental stream (eo_click) mocks the job-based export pipeline.
"""
import unittest
from unittest.mock import patch

import pendulum
from singer import Transformer

from tap_branch.client import Client
from tap_branch.streams import STREAMS

from base import BranchBaseTest

# Fixed 'now' keeps the eo_click windowing loop to 1 iteration.
_FIXED_NOW = pendulum.datetime(2026, 3, 11, tz="UTC")
# Start date close to _FIXED_NOW → exactly 1 window (10 days < 60-day max).
_EO_CLICK_START = "2026-03-01T00:00:00Z"


class TestAllFields(BranchBaseTest, unittest.TestCase):
    """Every field returned by the API must appear in the synced record."""

    # Fields not covered by the mock-data generator (rare / nested paths).
    MISSING_FIELDS = {
        "deeplink": set(),
        "app_config": set(),
        "eo_click": set()
    }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _run_full_table_sync(self, stream_name, mock_record,
                              mock_make_request, mock_write_record):
        """Configure injected full-table mocks, run sync, return written list."""
        config = self.make_config()
        client = Client(config)
        stream_entry = self._get_selected_stream(stream_name)
        stream = STREAMS[stream_name](client, stream_entry)

        mock_make_request.return_value = mock_record
        written = []
        mock_write_record.side_effect = lambda sid, rec: written.append(rec)

        stream.sync(state={}, transformer=Transformer())
        return written

    def _check_fields(self, stream_name, record):
        """Assert every schema field (minus known gaps) appears in ``record``."""
        schemas, _ = get_schemas_for(stream_name)
        schema_properties = schemas[stream_name]["properties"].keys()
        missing_allowed = self.MISSING_FIELDS.get(stream_name, set())
        for field in schema_properties:
            if field in missing_allowed:
                continue
            self.assertIn(
                field,
                record,
                msg=f"Expected field '{field}' missing from {stream_name} record",
            )

    # ------------------------------------------------------------------
    # Full-table streams
    # ------------------------------------------------------------------

    @patch("tap_branch.streams.deeplink.write_record")
    @patch("tap_branch.client.Client.make_request")
    def test_deeplink_all_expected_fields_present(
        self, mock_make_request, mock_write_record
    ):
        record = self._generate_stream_record("deeplink")
        written = self._run_full_table_sync(
            "deeplink", record, mock_make_request, mock_write_record
        )
        self.assertTrue(written, "No records written for deeplink")
        self._check_fields("deeplink", written[0])

    @patch("tap_branch.streams.app_config.write_record")
    @patch("tap_branch.client.Client.make_request")
    def test_app_config_all_expected_fields_present(
        self, mock_make_request, mock_write_record
    ):
        record = self._generate_stream_record("app_config")
        written = self._run_full_table_sync(
            "app_config", record, mock_make_request, mock_write_record
        )
        self.assertTrue(written, "No records written for app_config")
        self._check_fields("app_config", written[0])

    # ------------------------------------------------------------------
    # Incremental stream (job-based export pipeline)
    # ------------------------------------------------------------------

    @patch("tap_branch.streams.branch_events.singer.write_state")
    @patch("tap_branch.streams.branch_events.write_record")
    @patch("tap_branch.streams.branch_events.BranchEventsBaseStream.extract_data")
    @patch("tap_branch.client.Client.check_export_job_status", return_value=(True, {"response_url": "http://mock"}))
    @patch("tap_branch.client.Client.create_export_job", return_value="mock_handle")
    @patch("tap_branch.client.Client.check_data_readiness", return_value=True)
    @patch("tap_branch.streams.branch_events.pendulum.now", return_value=_FIXED_NOW)
    def test_eo_click_all_expected_fields_present(
        self,
        _mock_now,
        _mock_ready,
        _mock_create,
        _mock_status,
        mock_extract,
        mock_write_record,
        _mock_write_state,
    ):
        record = self._generate_stream_record("eo_click",
                                              date_value=_EO_CLICK_START)
        mock_extract.side_effect = lambda job_response: iter([record])
        written = []
        mock_write_record.side_effect = lambda sid, rec: written.append(rec)

        config = self.make_config(start_date=_EO_CLICK_START)
        config["branch_window_size"] = "60"
        client = Client(config)
        stream_entry = self._get_selected_stream("eo_click")
        stream = STREAMS["eo_click"](client, stream_entry)
        stream.sync(state={}, transformer=Transformer())

        self.assertTrue(written, "No records written for eo_click")
        self._check_fields("eo_click", written[0])


# ---------------------------------------------------------------------------
# Utility — thin wrapper so _check_fields can call the schema loader
# ---------------------------------------------------------------------------
from tap_branch.schema import get_schemas as _get_schemas  # noqa: E402


def get_schemas_for(stream_name):
    """Return (schemas, field_metadata) scoped to a single stream."""
    schemas, field_metadata = _get_schemas()
    return (
        {stream_name: schemas[stream_name]},
        {stream_name: field_metadata[stream_name]},
    )
