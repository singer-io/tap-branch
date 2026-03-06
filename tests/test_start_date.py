from base import BranchBaseTest
from tap_tester.base_suite_tests.start_date_test import StartDateTest


class BranchStartDateTest(StartDateTest, BranchBaseTest):
    """Instantiate start date according to the desired data set and run the
    test."""

    @staticmethod
    def name():
        return "tap_tester_branch_start_date_test"

    def streams_to_test(self):
        streams_to_exclude = self.streams_with_no_data() | {"deeplink", "app_config"} # Union of FULL_TABLE streams
        return self.expected_stream_names().difference(streams_to_exclude)

    @property
    def start_date_1(self):
        return "2026-01-12T11:03:00.451000Z"

    @property
    def start_date_2(self):

        return "2026-02-01T00:00:00.000000Z"
