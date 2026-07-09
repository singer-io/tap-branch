import unittest
from unittest.mock import MagicMock

from parameterized import parameterized

from tap_branch.branch_utils import (raise_for_branch_rate_limit,
                                     handle_branch_validation_error)
from tap_branch.exceptions import (BranchRateLimitError,
                                   BranchUnsupportedFieldsError)


class TestUtils(unittest.TestCase):

    def test_handle_branch_validation_error(self):
        """ Test to validate that unsupported fields are correctly extracted and BranchUnsupportedFieldsError is raised"""

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "errors": [
                {"message": "test_field field is not available for exports"}
            ]
        }

        with self.assertRaises(BranchUnsupportedFieldsError) as context:
            handle_branch_validation_error(mock_response)
        self.assertIn("test_field", context.exception.fields)

    @parameterized.expand([
        ["small retry seconds", 10, "Rate limit exceeded retry after 10 seconds."],
        ["large retry seconds", 3600, "Rate limit exceeded retry after 3600 seconds."]
    ])
    def test_branch_rate_limit_error(self, test_name, retry_seconds, expected_message):
        """ Test to validate that rate limit errors always raise BranchRateLimitError
        (retryable), regardless of how long Branch asked us to wait. The actual wait
        time is honored exactly (no capping) by rate_limit_wait_gen in client.py."""

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "errors": [
                {"message": f"Rate limit exceeded retry after {retry_seconds} seconds.", "error_code": 7}
            ]
        }

        with self.assertRaises(BranchRateLimitError) as context:
            raise_for_branch_rate_limit(mock_response)

        self.assertEqual(str(context.exception), expected_message)
