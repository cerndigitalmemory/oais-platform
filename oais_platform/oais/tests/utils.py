from oais_platform.oais.exceptions import ServiceUnavailable
from oais_platform.oais.sources.source import Source


class TestSource(Source):
    def get_record_url(self, recid):
        return f"https://example.com/record/{recid}"

    def search(self, query, page=1, size=20):
        return {
            "total_num_hits": 1,
            "results": [
                {
                    "url": self.get_record_url("1"),
                    "recid": "1",
                    "title": query,
                    "authors": [],
                    "source": "test",
                }
            ],
        }

    def search_by_id(self, recid):
        return {
            "result": [
                {
                    "url": self.get_record_url("1"),
                    "recid": "1",
                    "title": "test",
                    "authors": [],
                    "source": "test",
                }
            ]
        }
