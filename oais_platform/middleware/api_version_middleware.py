from oais_platform import settings


class api_version_middleware(get_response):

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        response["X-API-Version"] = settings.APP_VERSION
        return response
