from tap_branch.streams.app_config import AppConfig
from tap_branch.streams.branch_events import BranchEvents
from tap_branch.streams.deeplink import DeepLink

STREAMS = {
    "deeplink": DeepLink,
    "app_config": AppConfig,
    "branch_events": BranchEvents,
}
