class BranchError(Exception):
    """class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class BranchBackoffError(BranchError):
    """class representing backoff error handling."""
    pass


class BranchBadRequestError(BranchError):
    """class representing 400 status code."""
    pass


class BranchUnauthorizedError(BranchError):
    """class representing 401 status code."""
    pass


class BranchForbiddenError(BranchError):
    """class representing 403 status code."""
    pass


class BranchNotFoundError(BranchError):
    """class representing 404 status code."""
    pass


class BranchConflictError(BranchError):
    """class representing 409 status code."""
    pass


class BranchUnprocessableEntityError(BranchError):
    """class representing 422 status code."""
    pass


class BranchRateLimitError(BranchBackoffError):
    """class representing 429 status code."""
    pass


class BranchInternalServerError(BranchBackoffError):
    """class representing 500 status code."""
    pass


class BranchNotImplementedError(BranchBackoffError):
    """class representing 501 status code."""
    pass


class BranchBadGatewayError(BranchBackoffError):
    """class representing 502 status code."""
    pass


class BranchServiceUnavailableError(BranchBackoffError):
    """class representing 503 status code."""
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": BranchBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": BranchUnauthorizedError,
        "message": "The access token provided is expired, revoked, malformed or invalid for other reasons."
    },
    403: {
        "raise_exception": BranchForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": BranchNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": BranchConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item."
    },
    422: {
        "raise_exception": BranchUnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": BranchRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": BranchInternalServerError,
        "message": "The server encountered an unexpected condition which prevented it from fulfilling the request."
    },
    501: {
        "raise_exception": BranchNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": BranchBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": BranchServiceUnavailableError,
        "message": "API service is currently unavailable."
    }
}
