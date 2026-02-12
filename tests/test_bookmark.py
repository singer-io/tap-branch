from base import BranchBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class SendwithusBookMarkTest(BookmarkTest, BranchBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a
    stream."""
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            "eo_click": {"timestamp": "2026-01-12T11:03:00.451000Z"}
        }
    }

    @staticmethod
    def name():
        return "tap_tester_sendwithus_bookmark_test"

    def streams_to_test(self):
        streams_to_exclude = self.streams_with_no_data() | {"deeplink", "app_config"}  # Union of FULL_TABLE streams
        return self.expected_stream_names().difference(streams_to_exclude)

    def calculate_new_bookmarks(self):
        """Calculates new bookmarks by looking through sync 1 data to determine
        a bookmark that will sync 2 records in sync 2 (plus any necessary look
        back data)"""

        new_bookmarks = {
            "eo_click": {"timestamp": "2026-02-01T00:00:00.000000Z"},
        }

        return new_bookmarks
