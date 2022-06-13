import json

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
            raise ServiceUnavailable(
                f"Search failed with error code {req.status_code}"
            )

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
        total_num_hits = len(records)

        results = []
        for record in records:
            results.append(self.parse_record(record))

        idx = 10 * (page - 1)
        return {"total_num_hits": total_num_hits, "results": results[idx:idx+size]}


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
            "source": self.source
        }