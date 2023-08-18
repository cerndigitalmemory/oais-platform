import json

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
                    "source_url": self.get_record_url("1"),
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
                    "source_url": self.get_record_url("1"),
                    "recid": "1",
                    "title": "test",
                    "authors": [],
                    "source": "test",
                }
            ]
        }

`   # TODO: Check if this should be static. Maybe create another class for this?
    @staticmethod
    def get_public_links_for_download(file_count):
        FILE_PUBLIC_LINK = "https://gitlab.cern.ch/digitalmemory/oais-platform/-/raw/develop/README.md"
        files = {}
        
        for i in range(file_count):
            file_name = f"file{i}.md"
            files[file_name] = FILE_PUBLIC_LINK

        return json.dumps(files)    

