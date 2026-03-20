"""Test tap discovery mode and metadata."""
import unittest

import singer.metadata as singer_metadata
from tap_branch.discover import discover

from base import BranchBaseTest


class TestDiscovery(BranchBaseTest, unittest.TestCase):
    """Verify that discover() returns the expected streams and catalog metadata."""

    def test_expected_streams_in_catalog(self):
        """All 3 representative streams appear in the catalog."""
        catalog = discover()
        stream_ids = {entry.tap_stream_id for entry in catalog.streams}
        for stream_name in self.STREAMS_TO_TEST:
            with self.subTest(stream=stream_name):
                self.assertIn(stream_name, stream_ids)

    def test_primary_keys(self):
        """Primary keys match expected metadata for each stream."""
        catalog = discover()
        stream_map = {e.tap_stream_id: e for e in catalog.streams}
        expected = self.expected_metadata()

        for stream_name in self.STREAMS_TO_TEST:
            with self.subTest(stream=stream_name):
                root_meta = singer_metadata.to_map(stream_map[stream_name].metadata)[()]
                actual_pks = set(root_meta.get("table-key-properties", []))
                self.assertEqual(actual_pks, expected[stream_name][self.PRIMARY_KEYS])

    def test_replication_methods(self):
        """Replication methods match expected metadata for each stream."""
        catalog = discover()
        stream_map = {e.tap_stream_id: e for e in catalog.streams}
        expected = self.expected_metadata()

        for stream_name in self.STREAMS_TO_TEST:
            with self.subTest(stream=stream_name):
                root_meta = singer_metadata.to_map(stream_map[stream_name].metadata)[()]
                actual_method = root_meta.get("forced-replication-method")
                self.assertEqual(actual_method, expected[stream_name][self.REPLICATION_METHOD])

    def test_replication_keys(self):
        """Replication keys match expected metadata for each stream."""
        catalog = discover()
        stream_map = {e.tap_stream_id: e for e in catalog.streams}
        expected = self.expected_metadata()

        for stream_name in self.STREAMS_TO_TEST:
            with self.subTest(stream=stream_name):
                root_meta = singer_metadata.to_map(stream_map[stream_name].metadata)[()]
                actual_rep_keys = set(root_meta.get("valid-replication-keys", []))
                self.assertEqual(actual_rep_keys, expected[stream_name][self.REPLICATION_KEYS])

    def test_eo_click_replication_key_is_automatic(self):
        """eo_click's 'timestamp' replication key has inclusion=automatic."""
        catalog = discover()
        stream_entry = catalog.get_stream("eo_click")
        meta_map = singer_metadata.to_map(stream_entry.metadata)
        inclusion = meta_map.get(("properties", "timestamp"), {}).get("inclusion")
        self.assertEqual(inclusion, "automatic")
