import json
from operator import itemgetter

import requests
from oais_platform.oais.exceptions import ServiceUnavailable
from oais_platform.oais.sources.source import Source


class Gitlab(Source):
    def __init__(self, source, baseURL, api_key):
        self.source = source
        self.baseURL = baseURL
        self.api_key = api_key

    def get_record_url(self, recid):
        """
        To get the actual record url, a request using the Gitlab API must be made using the authentication token.
        """
        return f"{self.baseURL}/api/v4/projects/{recid}"

    def get_records(self):
        try:
            req = requests.get(
                f"{self.baseURL}/api/v4/projects",
                params={"membership": True},
                headers={"Authorization": f"Bearer {self.api_key}"},
            )
        except Exception:
            raise ServiceUnavailable("Cannot perform search")
        records = req.json()

        if not req.ok:
            raise ServiceUnavailable(f"Search failed with error code {req.status_code}")

        return records

    def search(self, query, page=1, size=20):
        """
        Look for all notes on CodiMD using the /history/ API endpoint
        Returns a list of all the notes from the user
        """

        # Get the integer of size and page to make calculations
        size = int(size)
        page = int(page)

        records = self.get_records()
        results = []

        # add all records if query is empty, filter otherwise
        if not query:
            results = list(map(self.parse_record, records))
        else:
            query = query.lower().split()
            sorted_records = []

            for record in records:
                title = record["name_with_namespace"]

                record["score"] = 0
                for word in query:
                    if word in title:
                        record["score"] += 1

                if record["score"] > 0:
                    sorted_records.append(record)

            sorted_records.sort(key=itemgetter("score"), reverse=True)
            results = list(map(self.parse_record, sorted_records))

        total_num_hits = len(results)
        idx = 10 * (page - 1)
        return {"total_num_hits": total_num_hits, "results": results[idx : idx + size]}

    def search_by_id(self, recid):
        """
        Look for a record on CodiMD given a record ID.
        Returns the resulting record if it exists, None otherwise
        """
        records = self.get_records()

        for record in records:
            if record["id"] == recid:
                return {"result": [self.parse_record(record)]}

        return {"result": None}

    def parse_record(self, record):
        """
        Parses each note returned from the API and returns the necessary values
        """

        recid = record["id"]
        url = record["web_url"]

        return {
            "source_url": url,
            "recid": recid,
            "title": record["name_with_namespace"],
            "authors": record["creator_id"],
            "source": self.source,
        }
