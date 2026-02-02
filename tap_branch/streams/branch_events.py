from tap_branch.streams.abstracts import IncrementalStream


class BranchEvents(IncrementalStream):
    tap_stream_id = "branch_events"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["timestamp"]
