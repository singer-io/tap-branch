import re

import requests
import singer

from tap_branch.branch_constants import MAX_RETRY_WAIT_SECONDS
from tap_branch.exceptions import (BranchFatalRateLimitError,
                                   BranchRateLimitError,
                                   BranchUnsupportedFieldsError)

LOGGER = singer.get_logger()


def extract_field_from_message(message: str) -> list[str]:
    # Take the first token before ' field is not available'
    field = message.split(" field is not available")[0].strip()
    return [field]


def extract_retry_seconds(message):
    match = re.search(r"retry after (\d+)", message.lower())
    return int(match.group(1)) if match else None


def handle_branch_validation_error(response: requests.Response):
    """ Function to check and extract unsupported fields in branch export request

    Args:
        response (requests.Response): Response object

    Raises:
        BranchUnsupportedFieldsError: Raised with proper fields data
    """

    try:
        payload = response.json()
    except ValueError:
        return  # not JSON, let raise_for_error handle it

    errors = payload.get("errors") or []
    unsupported_fields = set()

    for error in errors:
        message = error.get("message", "").lower()

        if "field" in message and "not available for exports" in message:
            fields = extract_field_from_message(error["message"])
            unsupported_fields.update(fields)

    if unsupported_fields:
        raise BranchUnsupportedFieldsError(
            fields=sorted(unsupported_fields),
            raw_response=payload,
        )


def raise_for_branch_rate_limit(response: requests.Response):
    """ Function to detect and raise appropriate branch rate-limit error

    Args:
        response (requests.Response): Response object

    Raises:
        BranchFatalRateLimitError: Raised when wait time exceeds configured limit
        BranchRateLimitError: Raised when wait time is under the configured limit
    """

    try:
        payload = response.json()
    except Exception:
        return

    errors = payload.get("errors", [])

    for err in errors:
        message = err.get("message", "")
        code = err.get("error_code")

        if code == 7 and "retry after" in message.lower():
            retry_seconds = extract_retry_seconds(message)

            LOGGER.info(
                "Branch rate limit encountered. Retry after %s seconds",
                retry_seconds
            )

            if retry_seconds and retry_seconds > MAX_RETRY_WAIT_SECONDS:
                raise BranchFatalRateLimitError(
                    f"Retry time {retry_seconds}s exceeds allowed limit of {MAX_RETRY_WAIT_SECONDS}s"
                )
            else:
                raise BranchRateLimitError(message)
