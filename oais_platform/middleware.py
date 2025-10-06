from django.conf import settings

"""
Middleware to add the API version to every HTTP response header.

This middleware retrieves the application's version from Django settings (APP_VERSION)
and sets it in the 'X-API-Version' header of each response. This allows clients to
determine which version of the API they are interacting with.
"""


class api_version_middleware:

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        response["X-API-Version"] = settings.APP_VERSION
        return response
