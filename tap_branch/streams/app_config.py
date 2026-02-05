from tap_branch.streams.abstracts import FullTableStream


class AppConfig(FullTableStream):
    tap_stream_id = "app_config"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "v1/app/{api_key}"
    http_method = "GET"
