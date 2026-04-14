from tap_branch.schema import get_schemas


class BranchBaseTest:
    """Base test mixin providing shared metadata, config helpers, and mock-data
    generators for tap-branch unit tests.

    Covers the Custom Export API streams:
      - eo_click      (INCREMENTAL)
    """

    DEFAULT_START_DATE = "2024-01-01T00:00:00Z"

    # Metadata key constants
    PRIMARY_KEYS = "primary_keys"
    REPLICATION_METHOD = "replication_method"
    REPLICATION_KEYS = "replication_keys"
    FULL_TABLE = "FULL_TABLE"
    INCREMENTAL = "INCREMENTAL"

    # The streams exercised by these tests
    STREAMS_TO_TEST = {"eo_click"}

    @classmethod
    def expected_metadata(cls):
        """The expected stream metadata for the representative streams under test."""
        return {
            "eo_click": {
                cls.PRIMARY_KEYS: {"id"},
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: {"timestamp"},
            },
        }

    @staticmethod
    def make_config(start_date=None):
        """Return a minimal tap config dict suitable for unit tests."""
        return {
            "start_date": start_date or BranchBaseTest.DEFAULT_START_DATE,
            "branch_app_id": "test-app-id",
            "branch_access_token": "test-access-token",
            "branch_key": "test-key",
            "branch_secret": "test-secret",
            "branch_window_size": "1",
        }

    @staticmethod
    def _get_selected_stream(stream_name):
        """Return a CatalogEntry for *stream_name* with ``selected=True``."""
        import singer.metadata as singer_metadata
        from tap_branch.discover import discover

        catalog = discover()
        stream_entry = catalog.get_stream(stream_name)
        meta_map = singer_metadata.to_map(stream_entry.metadata)
        meta_map[()]["selected"] = True
        stream_entry.metadata = singer_metadata.to_list(meta_map)
        return stream_entry

    # -------------------------------------------------------------------------
    # Schema / mock-data helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _schema_type(schema):
        """Return the first non-null type from a JSON-Schema type definition."""
        prop_type = schema.get("type")
        if isinstance(prop_type, list):
            non_null = [t for t in prop_type if t != "null"]
            return non_null[0] if non_null else "null"
        return prop_type

    @classmethod
    def _generate_value(cls, schema, date_value=None):
        """Recursively generate a placeholder value that conforms to *schema*."""
        date_value = date_value or cls.DEFAULT_START_DATE
        prop_type = cls._schema_type(schema)
        fmt = schema.get("format")

        if fmt == "date-time":
            return date_value
        if prop_type == "string":
            return "mock_value"
        if prop_type in ("integer", "number"):
            return 1
        if prop_type == "boolean":
            return True
        if prop_type == "array":
            items_schema = schema.get("items", {})
            return [cls._generate_value(items_schema, date_value)]
        if prop_type == "object":
            properties = schema.get("properties", {})
            return {k: cls._generate_value(v, date_value) for k, v in properties.items()}
        return None

    @classmethod
    def _generate_stream_record(cls, stream_name, date_value=None):
        """Return a synthetic record dict for *stream_name* built from its schema."""
        date_value = date_value or cls.DEFAULT_START_DATE
        schemas, _ = get_schemas()
        schema = schemas[stream_name]
        return cls._generate_value(schema, date_value)
