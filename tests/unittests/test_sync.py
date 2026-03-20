import unittest
from unittest.mock import MagicMock, patch

from tap_branch.exceptions import BranchFatalRateLimitError
from tap_branch.sync import sync, update_currently_syncing, write_schema


class TestSync(unittest.TestCase):

    def test_write_schema_only_parent_selected(self):
        mock_stream = MagicMock()
        mock_stream.is_selected.return_value = True
        mock_stream.children = ["app_config"]
        mock_stream.child_to_sync = []

        client = MagicMock()
        catalog = MagicMock()
        catalog.get_stream.return_value = MagicMock()

        write_schema(mock_stream, client, [], catalog)

        mock_stream.write_schema.assert_called_once()
        self.assertEqual(len(mock_stream.child_to_sync), 0)

    @patch("singer.write_schema")
    @patch("singer.get_currently_syncing")
    @patch("singer.Transformer")
    @patch("singer.write_state")
    @patch("tap_branch.streams.BranchEventsBaseStream.sync")
    def test_sync_stream1_called(self, mock_sync, mock_write_state, mock_transformer, mock_get_currently_syncing, mock_write_schema):
        mock_catalog = MagicMock()
        templates_stream = MagicMock()
        templates_stream.stream = "eo_click"
        snippets_stream = MagicMock()
        snippets_stream.stream = "eo_install"

        mock_catalog.get_selected_streams.return_value = [
            templates_stream,
            snippets_stream
        ]
        state = {}

        client = MagicMock()
        config = {}

        sync(client, config, mock_catalog, state)

        self.assertEqual(mock_sync.call_count, 2)

    @patch("singer.get_currently_syncing")
    @patch("singer.set_currently_syncing")
    @patch("singer.write_state")
    def test_remove_currently_syncing(self, mock_write_state, mock_set_currently_syncing, mock_get_currently_syncing):
        mock_get_currently_syncing.return_value = "some_stream"
        state = {"currently_syncing": "some_stream"}

        update_currently_syncing(state, None)

        mock_get_currently_syncing.assert_called_once_with(state)
        mock_set_currently_syncing.assert_not_called()
        mock_write_state.assert_called_once_with(state)
        self.assertNotIn("currently_syncing", state)

    @patch("singer.get_currently_syncing")
    @patch("singer.set_currently_syncing")
    @patch("singer.write_state")
    def test_set_currently_syncing(self, mock_write_state, mock_set_currently_syncing, mock_get_currently_syncing):
        mock_get_currently_syncing.return_value = None
        state = {}

        update_currently_syncing(state, "new_stream")

        mock_get_currently_syncing.assert_not_called()
        mock_set_currently_syncing.assert_called_once_with(state, "new_stream")
        mock_write_state.assert_called_once_with(state)
        self.assertNotIn("currently_syncing", state)

    def test_sync_raises_exception_on_rate_limit(self):
        mock_catalog = MagicMock()
        templates_stream = MagicMock()
        templates_stream.stream = "eo_click"
        snippets_stream = MagicMock()
        snippets_stream.stream = "eo_install"

        mock_catalog.get_selected_streams.return_value = [
            templates_stream,
            snippets_stream
        ]
        state = {}

        client = MagicMock()
        config = {}

        with patch("tap_branch.streams.BranchEventsBaseStream.sync", side_effect=BranchFatalRateLimitError("Rate limit exceeded")):
            with self.assertRaises(Exception) as context:
                sync(client, config, mock_catalog, state)

            self.assertEqual("Rate limit exceeded", str(context.exception))
