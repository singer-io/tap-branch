from tap_branch.streams.abstracts import IncrementalStream


class BranchEventsBaseStream(IncrementalStream):
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    schema_path = "shared/branch_events"

    # Export Job paths
    export_data_readiness_path = "v2/data/ready/"
    create_export_job_path = "v2/logs/"
    poll_export_job_path = "v2/logs/{request_handle}/"
