import unittest
from unittest.mock import MagicMock, patch

from tap_branch.discover import _check_stream_access, discover
from tap_branch.exceptions import (
    BranchBadRequestError,
    BranchError,
    BranchForbiddenError,
    BranchUnauthorizedError,
)


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

    @patch("tap_branch.discover._check_stream_access")
    @patch("tap_branch.discover.get_schemas")
    @patch("singer.metadata.to_map")
    def test_discover(self, mock_to_map, mock_get_schemas, mock_check_access):
        """ Test the discover function """

        mock_check_access.return_value = True
        mock_get_schemas.return_value = (self.dummy_schema, self.dummy_metadata)
        mock_to_map.return_value = self.dummy_metadata[self.test_stream_name]

        mock_client = MagicMock()
        catalog_obj = discover(mock_client)

        self.assertIsNotNone(catalog_obj)

        self.assertEqual(len(catalog_obj.streams), 1)
        self.assertEqual(catalog_obj.streams[0].stream, self.test_stream_name)

    @patch("tap_branch.discover._check_stream_access")
    def test_discovery_error(self, mock_check_access):
        """ Test the discover function error handling """

        mock_check_access.return_value = True
        mock_client = MagicMock()

        with patch("tap_branch.discover.get_schemas") as mock_get_schemas:
            mock_get_schemas.return_value = ({"invalid_stream": "invalid_schema"}, {})

            with self.assertRaises(Exception):
                discover(mock_client)

    @patch("tap_branch.discover._check_stream_access")
    @patch("tap_branch.discover.get_schemas")
    @patch("singer.metadata.to_map")
    def test_discover_excludes_inaccessible_streams(self, mock_to_map, mock_get_schemas, mock_check_access):
        """Streams for which _check_stream_access returns False must be
        excluded from the returned catalog."""

        two_stream_schema = {
            "accessible_stream": self.dummy_schema[self.test_stream_name],
            "blocked_stream": self.dummy_schema[self.test_stream_name],
        }
        two_stream_metadata = {
            "accessible_stream": self.dummy_metadata[self.test_stream_name],
            "blocked_stream": self.dummy_metadata[self.test_stream_name],
        }
        # accessible_stream passes, blocked_stream is denied
        mock_check_access.side_effect = lambda client, name: name == "accessible_stream"
        mock_get_schemas.return_value = (two_stream_schema, two_stream_metadata)
        mock_to_map.return_value = self.dummy_metadata[self.test_stream_name]

        catalog_obj = discover(MagicMock())

        self.assertEqual(len(catalog_obj.streams), 1)
        self.assertEqual(catalog_obj.streams[0].stream, "accessible_stream")

    @patch("tap_branch.discover._check_stream_access")
    @patch("tap_branch.discover.get_schemas")
    def test_discover_raises_when_no_streams_accessible(self, mock_get_schemas, mock_check_access):
        """discover() must raise BranchError when all streams are excluded."""

        mock_check_access.return_value = False
        mock_get_schemas.return_value = (self.dummy_schema, self.dummy_metadata)

        with self.assertRaises(BranchError):
            discover(MagicMock())


class TestCheckStreamAccess(unittest.TestCase):

    def _make_client(self):
        client = MagicMock()
        client.config = {
            "branch_access_token": "test_token",
            "branch_app_id": "test_app_id",
        }
        client.build_headers.return_value = {"Access-Token": "test_token"}
        client.build_query_params.return_value = {"app_id": "test_app_id"}
        return client

    def test_returns_true_when_accessible(self):
        """Returns True when check_data_readiness succeeds."""
        client = self._make_client()
        result = _check_stream_access(client, "eo_impression")
        self.assertTrue(result)

    def test_returns_false_on_forbidden(self):
        """Returns False (stream excluded) when API responds with 403."""
        client = self._make_client()
        client.check_data_readiness.side_effect = BranchForbiddenError("403")
        result = _check_stream_access(client, "eo_impression")
        self.assertFalse(result)

    def test_returns_false_on_bad_request(self):
        """Returns False (stream excluded) when API responds with 400."""
        client = self._make_client()
        client.check_data_readiness.side_effect = BranchBadRequestError("400")
        result = _check_stream_access(client, "eo_impression")
        self.assertFalse(result)

    def test_raises_on_unauthorized(self):
        """Re-raises BranchUnauthorizedError immediately (fail fast on bad credentials)."""
        client = self._make_client()
        client.check_data_readiness.side_effect = BranchUnauthorizedError("401")
        with self.assertRaises(BranchUnauthorizedError):
            _check_stream_access(client, "eo_impression")
