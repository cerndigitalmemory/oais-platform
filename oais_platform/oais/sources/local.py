from oais_platform.oais.sources.abstract_source import AbstractSource


class Local(AbstractSource):
    def __init__(self, source=None, baseURL=None, token=None):
        pass

    def get_record_url(self, recid):
        return ""

    def search(self, query, page=1, size=20):
        pass

    def search_by_id(self, recid):
        pass

    def notify_source(self, archive, notification_endpoint, api_key=None):
        pass


class LocalNotifyNotImpl(AbstractSource):
    def __init__(self, source=None, baseURL=None, token=None):
        pass

    def get_record_url(self, recid):
        return ""

    def search(self, query, page=1, size=20):
        pass

    def search_by_id(self, recid):
        pass
