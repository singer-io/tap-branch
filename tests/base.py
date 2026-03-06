import os

from tap_tester.base_suite_tests.base_case import BaseCase


class BranchBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """
    start_date = "2026-01-01T00:00:00Z"
    PARENT_TAP_STREAM_ID = "parent-tap-stream-id"

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-branch"

    @staticmethod
    def get_type():
        """The name of the tap."""
        return "platform.branch"

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        return {
            "deeplink": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "app_config": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_impression": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_click": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_web_to_app_auto_redirect": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_branch_cta_view": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_open": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_install": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_reinstall": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_web_session_start": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_pageview": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_commerce_event": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_custom_event": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_content_event": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_dismissal": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_user_lifecycle_event": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "cost": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "skadnetwork_valid_messages": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_san_touch": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_click_blocked": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_impression_blocked": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_install_blocked": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_reinstall_blocked": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_open_blocked": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_web_session_start_blocked": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_pageview_blocked": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_custom_event_blocked": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_content_event_blocked": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_commerce_event_blocked": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_user_lifecycle_event_blocked": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_branch_cta_view_blocked": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            },
            "eo_web_to_app_auto_redirect_blocked": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100,
                cls.PARENT_TAP_STREAM_ID: None
            }
        }

    @staticmethod
    def get_credentials():
        """Authentication information for the test account."""
        credentials_dict = {}
        creds = {
            "branch_app_id": "TAP_BRANCH_APP_ID",
            "branch_access_token": "TAP_BRANCH_ACCESS_TOKEN",
            "branch_key": "TAP_BRANCH_KEY",
            "branch_secret": "TAP_BRANCH_SECRET",
            "branch_window_size": "TAP_BRANCH_WINDOW_SIZE",
            "deeplink_urls": "TAP_BRANCH_DEEPLINK_URLS"
        }
        for cred in creds:
            credentials_dict[cred] = os.getenv(creds[cred])

        return credentials_dict

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return_value = {
            "start_date": self.start_date,
        }

        return return_value

    def expected_parent_tap_stream(self, stream=None):
        """return a dictionary with key of table name and value of parent stream"""
        parent_stream = {
            table: properties.get(self.PARENT_TAP_STREAM_ID, None)
            for table, properties in self.expected_metadata().items()}
        if not stream:
            return parent_stream
        return parent_stream[stream]

    def streams_with_no_data(self):
        """Return a set of streams that have no data."""
        return {
            'eo_impression', 'eo_web_to_app_auto_redirect', 'eo_branch_cta_view', 'eo_open',
            'eo_install', 'eo_reinstall', 'eo_web_session_start',
            'eo_pageview', 'eo_commerce_event', 'eo_custom_event', 'eo_content_event',
            'eo_dismissal', 'eo_user_lifecycle_event', 'cost', 'skadnetwork_valid_messages',
            'eo_san_touch', 'eo_click_blocked', 'eo_impression_blocked', 'eo_install_blocked',
            'eo_reinstall_blocked', 'eo_open_blocked', 'eo_web_session_start_blocked',
            'eo_pageview_blocked', 'eo_custom_event_blocked', 'eo_content_event_blocked',
            'eo_commerce_event_blocked', 'eo_user_lifecycle_event_blocked', 'eo_branch_cta_view_blocked',
            'eo_web_to_app_auto_redirect_blocked'
        }
