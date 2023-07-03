import json
from operator import itemgetter

import requests

from oais_platform.oais.exceptions import ServiceUnavailable
from oais_platform.oais.sources.source import Source


class CodiMD(Source):
    def __init__(self, source, baseURL, session_id):
        self.source = source
        self.baseURL = baseURL
        self.session_id = session_id

    def get_record_url(self, recid):
        return f"{self.baseURL}/{recid}#"

    def get_records(self):
        try:
            req = requests.get(
                "https://codimd.web.cern.ch/history",
                stream=True,
                cookies={"connect.sid": self.session_id},
            )
        except Exception:
            raise ServiceUnavailable("Cannot perform search")
        data = json.loads(req.text)
        records = data["history"]

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
                title = record["text"].lower()

                record["score"] = 0
                for word in query:
                    if word in title:
                        record["score"] += 1

                if record["score"] > 0:
                    sorted_records.append(record)

            sorted_records.sort(key=itemgetter("score"), reverse=True)
            results = list(map(self.parse_record, sorted_records))

        total_num_hits = len(results)

        # paginate the resulting query
        # number of elements to skip (in the previous pages than the requested)
        skip = size * (page - 1)
        if skip + size > total_num_hits:
            # if there are not enough results to fill a page, return everything that is remaining
            return {
                "total_num_hits": total_num_hits,
                "results": results[skip:],
            }
        else:
            # else, return exactly as many elements to fill a page after the skipped results
            return {
                "total_num_hits": total_num_hits,
                "results": results[skip : skip + size],
            }

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
        url = self.get_record_url(recid)

        return {
            "source_url": url,
            "recid": recid,
            "title": record["text"],
            "authors": "",
            "source": self.source,
        }
