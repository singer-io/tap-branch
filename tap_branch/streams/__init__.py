from tap_branch.branch_utils import generate_custom_event_streams
from tap_branch.streams.app_config import AppConfig
from tap_branch.streams.deeplink import DeepLink

STREAMS = {
    "deeplink": DeepLink,
    "app_config": AppConfig
}

# Add dynamically generated each report_type stream to STREAMS
STREAMS.update(generate_custom_event_streams())
