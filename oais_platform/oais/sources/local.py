from oais_platform.oais.sources.abstract_source import AbstractSource


class Local(AbstractSource):
    def __init__(self):
        pass

    def get_record_url(self, recid):
        return ""

    def search(self, query, page=1, size=20):
        pass

    def search_by_id(self, recid):
        pass
