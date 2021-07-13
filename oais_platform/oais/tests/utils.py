from oais_platform.oais.exceptions import ServiceUnavailable
from oais_platform.oais.sources.source import Source


class TestSource(Source):
    def get_record_url(self, recid):
        return f"https://example.com/record/{recid}"

    def search(self, query):
        return [{
            "url": self.get_record_url("1"),
            "recid": "1",
            "title": query,
            "authors": [],
            "source": "test"
        }]
