from typing import Dict

from tap_branch.branch_constants import (BRANCH_EXPORT_EVENT_DEFINITIONS,
                                         BranchExportEventDefinition)
from tap_branch.streams.app_config import AppConfig
from tap_branch.streams.branch_events import BranchEventsBaseStream
from tap_branch.streams.deeplink import DeepLink


def create_branch_event_stream(definition: BranchExportEventDefinition):
    """ Function to generate dynamic event class stream which extends the BranchEventsBaseStream

    Args:
        definition (BranchExportEventDefinition): Export event definition

    """

    class _BranchEventStream(BranchEventsBaseStream):
        tap_stream_id = definition.tap_stream_id
        replication_keys = definition.replication_keys

    return _BranchEventStream


def generate_custom_event_streams() -> Dict:
    """ Generate all branch event streams on runtime with defined configuration

    Returns:
        Dict: Dictionary having branch event stream classes
    """

    custom_event_streams = {}

    for stream_name, stream_definition in BRANCH_EXPORT_EVENT_DEFINITIONS.items():
        custom_event_streams[stream_name] = create_branch_event_stream(stream_definition)

    return custom_event_streams


STREAMS = {
    "deeplink": DeepLink,
    "app_config": AppConfig
}

# Add dynamically generated each report_type stream to STREAMS
STREAMS.update(generate_custom_event_streams())
