from datetime import datetime, timezone

from oais_platform.oais.models import FilterType
from oais_platform.oais.sources.abstract_source import AbstractSource


class TestSource(AbstractSource):
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

    def get_records_to_harvest(
        self, start=None, end=None, size=500, filter_type=FilterType.UPDATED
    ):
        yield [
            {
                "source_url": self.get_record_url("1"),
                "recid": "1",
                "title": "test",
                "authors": [],
                "source": "test",
            }
        ], datetime.now(timezone.utc)


def verify_archives_pipeline(self, archives, expected_steps):
    for archive in archives:
        archive.refresh_from_db()
        pipeline_step_names = [step[0] for step in archive.pipeline_steps]
        self.assertEqual(pipeline_step_names, expected_steps)
