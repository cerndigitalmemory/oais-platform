from rest_framework.exceptions import APIException


class BadRequest(APIException):
    status_code = 400
    default_detail = "Bad Request"
    default_code = "bad_request"


class DoesNotExist(APIException):
    status_code = 404
    default_detail = "Does Not Exist"
    default_code = "does_not_exist"


class PayloadTooLarge(APIException):
    status_code = 413
    default_detail = "Payload Too Large"
    default_code = "payload_too_large"


class InternalServerError(APIException):
    status_code = 500
    default_detail = "Internal server error"
    default_code = "internal_server_error"


class ServiceUnavailable(APIException):
    status_code = 503
    default_detail = "Service temporarily unavailable"
    default_code = "service_unavailable"


class ConfigFileUnavailable(Exception):
    pass


class InvalidSource(Exception):
    pass


class RetryableException(Exception):
    pass


class MaxRetriesExceeded(Exception):
    pass
