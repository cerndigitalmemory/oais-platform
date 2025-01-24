from rest_framework.exceptions import APIException


class BadRequest(APIException):
    status_code = 400
    default_detail = "Bad Request"
    default_code = "bad_request"


class ServiceUnavailable(APIException):
    status_code = 503
    default_detail = "Service temporarily unavailable"
    default_code = "service_unavailable"


class DoesNotExist(APIException):
    status_code = 404
    default_detail = "DoesNotExist"
    default_code = "does_not_exist"


class ConfigFileUnavailable(Exception):
    pass


class InvalidSource(Exception):
    pass


class RetryableException(Exception):
    pass
