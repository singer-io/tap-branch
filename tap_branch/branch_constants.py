""" Branch specific constants used across the tap """

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List

BRANCH_MAX_DATE_WINDOW = 60

# We will wait for 1 hour for an export job to be completed and it will be polled every 2 mins
JOB_TIMEOUT = 60 * 60
POLL_INTERVAL = 60 * 2

MAX_RETRY_WAIT_SECONDS = 60
MAX_RECORDS_TO_FETCH = 1_000_000

BASE_DIR = Path(__file__).resolve().parent

SCHEMAS_DIR = BASE_DIR / "schemas"

BRANCH_EVENTS_SCHEMA = SCHEMAS_DIR / "shared/branch_events.json"


# NOTE: Add new report types here as and when Branch adds them to their API
class BranchReportType(str, Enum):
    # Core event exports
    EO_IMPRESSION = "eo_impression"
    EO_CLICK = "eo_click"
    EO_WEB_TO_APP_AUTO_REDIRECT = "eo_web_to_app_auto_redirect"
    EO_BRANCH_CTA_VIEW = "eo_branch_cta_view"
    EO_OPEN = "eo_open"
    EO_INSTALL = "eo_install"
    EO_REINSTALL = "eo_reinstall"
    EO_WEB_SESSION_START = "eo_web_session_start"
    EO_PAGEVIEW = "eo_pageview"
    EO_COMMERCE_EVENT = "eo_commerce_event"
    EO_CUSTOM_EVENT = "eo_custom_event"
    EO_CONTENT_EVENT = "eo_content_event"
    EO_DISMISSAL = "eo_dismissal"
    EO_USER_LIFECYCLE_EVENT = "eo_user_lifecycle_event"

    # Non-event exports
    COST = "cost"
    SKADNETWORK_VALID_MESSAGES = "skadnetwork_valid_messages"
    EO_SAN_TOUCH = "eo_san_touch"

    # Blocked variants
    EO_CLICK_BLOCKED = "eo_click_blocked"
    EO_IMPRESSION_BLOCKED = "eo_impression_blocked"
    EO_INSTALL_BLOCKED = "eo_install_blocked"
    EO_REINSTALL_BLOCKED = "eo_reinstall_blocked"
    EO_OPEN_BLOCKED = "eo_open_blocked"
    EO_WEB_SESSION_START_BLOCKED = "eo_web_session_start_blocked"
    EO_PAGEVIEW_BLOCKED = "eo_pageview_blocked"
    EO_CUSTOM_EVENT_BLOCKED = "eo_custom_event_blocked"
    EO_CONTENT_EVENT_BLOCKED = "eo_content_event_blocked"
    EO_COMMERCE_EVENT_BLOCKED = "eo_commerce_event_blocked"
    EO_USER_LIFECYCLE_EVENT_BLOCKED = "eo_user_lifecycle_event_blocked"
    EO_BRANCH_CTA_VIEW_BLOCKED = "eo_branch_cta_view_blocked"
    EO_WEB_TO_APP_AUTO_REDIRECT_BLOCKED = "eo_web_to_app_auto_redirect_blocked"


@dataclass(frozen=True)
class BranchExportEventDefinition:
    report_type: BranchReportType
    tap_stream_id: str
    replication_keys: List[str]

    # Behavior flags
    is_event: bool
    is_blocked: bool


# Final mapping of report types to their definitions
# NOTE: Add new mappings here as well when Branch adds new report types
BRANCH_EXPORT_EVENT_DEFINITIONS = {
    BranchReportType.EO_IMPRESSION: BranchExportEventDefinition(
        report_type=BranchReportType.EO_IMPRESSION,
        tap_stream_id=BranchReportType.EO_IMPRESSION.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=False
    ),

    BranchReportType.EO_CLICK: BranchExportEventDefinition(
        report_type=BranchReportType.EO_CLICK,
        tap_stream_id=BranchReportType.EO_CLICK.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=False
    ),

    BranchReportType.EO_WEB_TO_APP_AUTO_REDIRECT: BranchExportEventDefinition(
        report_type=BranchReportType.EO_WEB_TO_APP_AUTO_REDIRECT,
        tap_stream_id=BranchReportType.EO_WEB_TO_APP_AUTO_REDIRECT.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=False
    ),

    BranchReportType.EO_BRANCH_CTA_VIEW: BranchExportEventDefinition(
        report_type=BranchReportType.EO_BRANCH_CTA_VIEW,
        tap_stream_id=BranchReportType.EO_BRANCH_CTA_VIEW.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=False
    ),

    BranchReportType.EO_OPEN: BranchExportEventDefinition(
        report_type=BranchReportType.EO_OPEN,
        tap_stream_id=BranchReportType.EO_OPEN.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=False
    ),

    BranchReportType.EO_INSTALL: BranchExportEventDefinition(
        report_type=BranchReportType.EO_INSTALL,
        tap_stream_id=BranchReportType.EO_INSTALL.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=False
    ),

    BranchReportType.EO_REINSTALL: BranchExportEventDefinition(
        report_type=BranchReportType.EO_REINSTALL,
        tap_stream_id=BranchReportType.EO_REINSTALL.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=False
    ),

    BranchReportType.EO_WEB_SESSION_START: BranchExportEventDefinition(
        report_type=BranchReportType.EO_WEB_SESSION_START,
        tap_stream_id=BranchReportType.EO_WEB_SESSION_START.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=False
    ),

    BranchReportType.EO_PAGEVIEW: BranchExportEventDefinition(
        report_type=BranchReportType.EO_PAGEVIEW,
        tap_stream_id=BranchReportType.EO_PAGEVIEW.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=False
    ),

    BranchReportType.EO_COMMERCE_EVENT: BranchExportEventDefinition(
        report_type=BranchReportType.EO_COMMERCE_EVENT,
        tap_stream_id=BranchReportType.EO_COMMERCE_EVENT.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=False
    ),

    BranchReportType.EO_CUSTOM_EVENT: BranchExportEventDefinition(
        report_type=BranchReportType.EO_CUSTOM_EVENT,
        tap_stream_id=BranchReportType.EO_CUSTOM_EVENT.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=False
    ),

    BranchReportType.EO_CONTENT_EVENT: BranchExportEventDefinition(
        report_type=BranchReportType.EO_CONTENT_EVENT,
        tap_stream_id=BranchReportType.EO_CONTENT_EVENT.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=False
    ),

    BranchReportType.EO_DISMISSAL: BranchExportEventDefinition(
        report_type=BranchReportType.EO_DISMISSAL,
        tap_stream_id=BranchReportType.EO_DISMISSAL.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=False
    ),

    BranchReportType.EO_USER_LIFECYCLE_EVENT: BranchExportEventDefinition(
        report_type=BranchReportType.EO_USER_LIFECYCLE_EVENT,
        tap_stream_id=BranchReportType.EO_USER_LIFECYCLE_EVENT.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=False
    ),

    BranchReportType.COST: BranchExportEventDefinition(
        report_type=BranchReportType.COST,
        tap_stream_id=BranchReportType.COST.value,
        replication_keys=["timestamp"],
        is_event=False,
        is_blocked=False
    ),

    BranchReportType.SKADNETWORK_VALID_MESSAGES: BranchExportEventDefinition(
        report_type=BranchReportType.SKADNETWORK_VALID_MESSAGES,
        tap_stream_id=BranchReportType.SKADNETWORK_VALID_MESSAGES.value,
        replication_keys=["timestamp"],
        is_event=False,
        is_blocked=False
    ),

    BranchReportType.EO_SAN_TOUCH: BranchExportEventDefinition(
        report_type=BranchReportType.EO_SAN_TOUCH,
        tap_stream_id=BranchReportType.EO_SAN_TOUCH.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=False
    ),

    BranchReportType.EO_CLICK_BLOCKED: BranchExportEventDefinition(
        report_type=BranchReportType.EO_CLICK_BLOCKED,
        tap_stream_id=BranchReportType.EO_CLICK_BLOCKED.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=True
    ),

    BranchReportType.EO_IMPRESSION_BLOCKED: BranchExportEventDefinition(
        report_type=BranchReportType.EO_IMPRESSION_BLOCKED,
        tap_stream_id=BranchReportType.EO_IMPRESSION_BLOCKED.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=True
    ),

    BranchReportType.EO_INSTALL_BLOCKED: BranchExportEventDefinition(
        report_type=BranchReportType.EO_INSTALL_BLOCKED,
        tap_stream_id=BranchReportType.EO_INSTALL_BLOCKED.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=True
    ),

    BranchReportType.EO_REINSTALL_BLOCKED: BranchExportEventDefinition(
        report_type=BranchReportType.EO_REINSTALL_BLOCKED,
        tap_stream_id=BranchReportType.EO_REINSTALL_BLOCKED.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=True
    ),

    BranchReportType.EO_OPEN_BLOCKED: BranchExportEventDefinition(
        report_type=BranchReportType.EO_OPEN_BLOCKED,
        tap_stream_id=BranchReportType.EO_OPEN_BLOCKED.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=True
    ),

    BranchReportType.EO_WEB_SESSION_START_BLOCKED: BranchExportEventDefinition(
        report_type=BranchReportType.EO_WEB_SESSION_START_BLOCKED,
        tap_stream_id=BranchReportType.EO_WEB_SESSION_START_BLOCKED.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=True
    ),

    BranchReportType.EO_PAGEVIEW_BLOCKED: BranchExportEventDefinition(
        report_type=BranchReportType.EO_PAGEVIEW_BLOCKED,
        tap_stream_id=BranchReportType.EO_PAGEVIEW_BLOCKED.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=True
    ),

    BranchReportType.EO_CUSTOM_EVENT_BLOCKED: BranchExportEventDefinition(
        report_type=BranchReportType.EO_CUSTOM_EVENT_BLOCKED,
        tap_stream_id=BranchReportType.EO_CUSTOM_EVENT_BLOCKED.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=True
    ),

    BranchReportType.EO_CONTENT_EVENT_BLOCKED: BranchExportEventDefinition(
        report_type=BranchReportType.EO_CONTENT_EVENT_BLOCKED,
        tap_stream_id=BranchReportType.EO_CONTENT_EVENT_BLOCKED.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=True
    ),

    BranchReportType.EO_COMMERCE_EVENT_BLOCKED: BranchExportEventDefinition(
        report_type=BranchReportType.EO_COMMERCE_EVENT_BLOCKED,
        tap_stream_id=BranchReportType.EO_COMMERCE_EVENT_BLOCKED.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=True
    ),

    BranchReportType.EO_USER_LIFECYCLE_EVENT_BLOCKED: BranchExportEventDefinition(
        report_type=BranchReportType.EO_USER_LIFECYCLE_EVENT_BLOCKED,
        tap_stream_id=BranchReportType.EO_USER_LIFECYCLE_EVENT_BLOCKED.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=True
    ),

    BranchReportType.EO_BRANCH_CTA_VIEW_BLOCKED: BranchExportEventDefinition(
        report_type=BranchReportType.EO_BRANCH_CTA_VIEW_BLOCKED,
        tap_stream_id=BranchReportType.EO_BRANCH_CTA_VIEW_BLOCKED.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=True
    ),

    BranchReportType.EO_WEB_TO_APP_AUTO_REDIRECT_BLOCKED: BranchExportEventDefinition(
        report_type=BranchReportType.EO_WEB_TO_APP_AUTO_REDIRECT_BLOCKED,
        tap_stream_id=BranchReportType.EO_WEB_TO_APP_AUTO_REDIRECT_BLOCKED.value,
        replication_keys=["timestamp"],
        is_event=True,
        is_blocked=True
    ),
}
