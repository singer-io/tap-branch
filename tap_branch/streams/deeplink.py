from tap_branch.streams.abstracts import FullTableStream


class DeepLink(FullTableStream):
    tap_stream_id = "deeplink"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "v1/url/"
    http_method = "GET"
