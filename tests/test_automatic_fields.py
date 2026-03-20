"""Test that automatic fields (primary keys, replication keys) have
inclusion=automatic in the catalog metadata."""
import unittest

import singer.metadata as singer_metadata
from tap_branch.discover import discover

from base import BranchBaseTest


class TestAutomaticFields(BranchBaseTest, unittest.TestCase):
    """Primary keys and replication keys must have inclusion=automatic."""

    def test_primary_keys_are_automatic(self):
        """Each stream's primary key(s) have inclusion=automatic."""
        catalog = discover()
        stream_map = {e.tap_stream_id: e for e in catalog.streams}
        expected = self.expected_metadata()

        for stream_name in self.STREAMS_TO_TEST:
            with self.subTest(stream=stream_name):
                meta_map = singer_metadata.to_map(stream_map[stream_name].metadata)
                for pk in expected[stream_name][self.PRIMARY_KEYS]:
                    inclusion = meta_map.get(("properties", pk), {}).get("inclusion")
                    self.assertEqual(
                        inclusion, "automatic",
                        msg=f"{stream_name}.{pk} should have inclusion=automatic",
                    )

    def test_replication_keys_are_automatic(self):
        """Each incremental stream's replication key has inclusion=automatic."""
        catalog = discover()
        stream_map = {e.tap_stream_id: e for e in catalog.streams}
        expected = self.expected_metadata()

        for stream_name in self.STREAMS_TO_TEST:
            rep_keys = expected[stream_name][self.REPLICATION_KEYS]
            if not rep_keys:
                continue  # FULL_TABLE streams have no replication key
            with self.subTest(stream=stream_name):
                meta_map = singer_metadata.to_map(stream_map[stream_name].metadata)
                for rk in rep_keys:
                    inclusion = meta_map.get(("properties", rk), {}).get("inclusion")
                    self.assertEqual(
                        inclusion, "automatic",
                        msg=f"{stream_name}.{rk} should have inclusion=automatic",
                    )
