from oais_platform import settings


class api_version_middleware(get_response):

    def middleware(request):
        response = get_response(request)
        response["X-API-Version"] = settings.APP_VERSION
        return response