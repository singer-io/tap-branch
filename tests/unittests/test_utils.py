import unittest
from unittest.mock import MagicMock

from parameterized import parameterized

from tap_branch.branch_constants import MAX_RETRY_WAIT_SECONDS
from tap_branch.branch_utils import (check_branch_rate_limit,
                                     handle_branch_validation_error)
from tap_branch.exceptions import (BranchFatalRateLimitError,
                                   BranchRateLimitError,
                                   BranchUnsupportedFieldsError)


class TestUtils(unittest.TestCase):

    def test_handle_branch_validation_error(self):
        """ Test to validate that unsupported fields are correctly extracted and BranchUnsupportedFieldsError is raised"""

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "errors": [
                {"message": "field 'test_field' is not available for exports"}
            ]
        }

        with self.assertRaises(BranchUnsupportedFieldsError) as context:
            handle_branch_validation_error(mock_response)
            self.assertIn("test_field", context.exception.fields)

    @parameterized.expand([
        ["small retry seconds", 10, BranchRateLimitError, "Rate limit exceeded retry after 10 seconds."],
        ["large retry seconds", 3600, BranchFatalRateLimitError, f"Retry time 3600s exceeds allowed limit of {MAX_RETRY_WAIT_SECONDS}s"]
    ])
    def test_branch_rate_limit_error(self, test_name, retry_seconds, expected_exception, expected_message):
        """ Test to validate that rate limit errors are correctly handled and appropriate exceptions are raised based on retry seconds"""

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "errors": [
                {"message": f"Rate limit exceeded retry after {retry_seconds} seconds.", "error_code": 7}
            ]
        }

        with self.assertRaises(expected_exception) as context:
            check_branch_rate_limit(mock_response)

        self.assertEqual(str(context.exception), expected_message)
