""" Branch API interaction related module """

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Literal, Optional, Set

# NOTE: These fields (from branch_events schema) are not supported while generating export reports
# Hence having them listed here which will be excluded from payload
GLOBAL_EXPORT_FIELD_DENYLIST = {
    "datasource",
    "persona_identifiers",
    "user_data_referral_source",
    "last_attributed_touch_data_dollar_fb_data_terms_not_signed",

    "last_cta_view_data_tilde_advertising_partner_id",
    "last_cta_view_data_tilde_agency_id",
    "last_cta_view_data_tilde_customer_ad_name",
    "last_cta_view_data_tilde_customer_ad_set_name",
    "last_cta_view_data_tilde_customer_campaign",
    "last_cta_view_data_tilde_customer_keyword",
    "last_cta_view_data_tilde_customer_placement",
    "last_cta_view_data_tilde_customer_secondary_publisher",
    "last_cta_view_data_tilde_customer_sub_site_name",

    "last_cta_view_data_tilde_keyword",
    "last_cta_view_data_tilde_placement_id",
    "last_cta_view_data_tilde_secondary_publisher_id",
    "last_cta_view_data_tilde_sub_site_name",

    "last_cta_view_data_tilde_tune_publisher_id",
    "last_cta_view_data_tilde_tune_publisher_name",
    "last_cta_view_data_tilde_tune_publisher_sub1",
    "last_cta_view_data_tilde_tune_publisher_sub2",
    "last_cta_view_data_tilde_tune_publisher_sub3",
    "last_cta_view_data_tilde_tune_publisher_sub4",
    "last_cta_view_data_tilde_tune_publisher_sub5",

    "event_data_custom_param_4",
    "event_data_custom_param_5",
    "event_data_custom_param_6",
    "event_data_custom_param_7",
    "event_data_custom_param_8",
    "event_data_custom_param_9",
    "event_data_custom_param_10",
}


@dataclass(frozen=True)
class EndpointConfig:
    required_headers: Set[str]
    required_query_params: Set[str]
    path: Optional[str] = None
    method: Optional[str] = None


@dataclass(frozen=True)
class BranchExportConfig:
    path: str
    method: str
    headers_data: Dict
    query_params_data: Dict
    additional_data: Optional[Dict] = None


@dataclass(frozen=True)
class BranchDataReadyPayload:
    date: str
    warehouse_meta_type: Literal["EVENT"]
    topic: str
    app_id: str

    def to_payload(self) -> dict:
        """ Function to generate payload from the instantiated class

        Returns:
            dict: Payload for Export Job ready
        """

        return {
            "date": self.date,
            "warehouse_meta_type": self.warehouse_meta_type,
            "topic": self.topic,
            "app_id": self.app_id,
        }


@dataclass(frozen=True)
class BranchExportJobPayload:
    start_date: str
    end_date: str
    report_type: str
    limit: int
    response_format: Literal["json"]
    allow_multiple_files: bool
    response_format_compression: Literal["gz"]

    # Schema file path to export all the fields from
    schema_path: Path

    filter: List[str] = field(default_factory=list)
    timezone: str = "UTC"

    def _load_fields(self) -> List[str]:
        """ Function to load the schema file and extract keys

        Returns:
            List[str]: Keys to be used in branch export job payload
        """

        with open(self.schema_path, "r") as f:
            schema = json.load(f)

        return list(schema["properties"].keys())

    def to_payload(self, rejected_fields: List[str] = None) -> dict:
        """ Function to generate payload from the instantiated class

        Args:
            rejected_fields (List[str], optional): List of fields that are not supported for export. Defaults to None.

        Returns:
            dict: Payload for export job
        """

        rejected = set(rejected_fields or []) | GLOBAL_EXPORT_FIELD_DENYLIST

        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "report_type": self.report_type,
            "fields": list(set(self._load_fields()) - rejected),
            "limit": self.limit,
            "timezone": self.timezone,
            "filter": self.filter,
            "response_format": self.response_format,
            "allow_multiple_files": self.allow_multiple_files,
            "response_format_compression": self.response_format_compression
        }
