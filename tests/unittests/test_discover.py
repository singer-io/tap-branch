import unittest
from unittest.mock import patch

from tap_branch.discover import discover


class TestDiscover(unittest.TestCase):
    test_stream_name = "test"

    dummy_schema = {
        test_stream_name: {
            "type": "object",
            "properties": {
                "id": {
                    "type": [
                        "null",
                        "string"
                    ]
                },
                "name": {
                    "type": [
                        "null",
                        "string"
                    ]
                },
                "email": {
                    "type": [
                        "null",
                        "string"
                    ]
                }
            }
        }
    }

    dummy_metadata = {
        test_stream_name: {
            (): {
                "breadcrumb": (),
                "table-key-properties": ["id"],
                "forced-replication-method": "FULL_TABLE",
                "valid-replication-keys": [],
            }
        }
    }

    @patch("tap_branch.discover.get_schemas")
    @patch("singer.metadata.to_map")
    def test_discover(self, mock_to_map, mock_get_schemas):
        """ Test the discover function """

        mock_get_schemas.return_value = (self.dummy_schema, self.dummy_metadata)
        mock_to_map.return_value = self.dummy_metadata[self.test_stream_name]

        catalog_obj = discover()

        self.assertIsNotNone(catalog_obj)

        self.assertEqual(len(catalog_obj.streams), 1)
        self.assertEqual(catalog_obj.streams[0].stream, self.test_stream_name)

    def test_discovery_error(self):
        """ Test the discover function error handling """

        with patch("tap_branch.discover.get_schemas") as mock_get_schemas:
            mock_get_schemas.return_value = ({"invalid_stream": "invalid_schema"}, {})

            with self.assertRaises(Exception):
                discover()
