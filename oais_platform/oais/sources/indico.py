import configparser
import json
import os

import requests
from oais_platform.oais.exceptions import ServiceUnavailable
from oais_platform.oais.sources.source import Source


class ConfigFileUnavailable(Exception):
    pass


class Indico(Source):
    def __init__(self, source, baseURL):
        self.source = source
        self.baseURL = baseURL

        self.config_file = configparser.ConfigParser()
        self.config_file.read(os.path.join(os.path.dirname(__file__), "indico.ini"))
        self.config = None

        if len(self.config_file.sections()) == 0:
            raise ConfigFileUnavailable(
                f"Could not read config file for Indico instance: {source}"
            )

    def get_record_url(self, recid):
        """
        Returns the API endpoint of the event with the given ID
        """
        return f"{self.baseURL}/event/{recid}"

    def get_record_by_id(self, recid):
        """
        Returns the export API endpoint of the event with the given ID
        """
        return f"{self.baseURL}/export/event/{recid}.json"

    def search(self, query, page=1, size=20):
        """
        Look for a record on Indico using the /export/event/ API endpoint
        given a query.
        Returns a list of results and a tentatively total numer of results
        """

        # Get the integer of size and page to make calculations
        size = int(size)
        page = int(page)

        """
        Indico search api always returns 10 results per call so in order to 
        display 10,20 or 50 results we need to make 1, 2 or 5 api calls
        """
        number_of_api_calls = int(size) // 10

        """
        In order for pagination to work we need to skip the number of pages for which
        we did the extra api calls on the step above.
        """
        actual_page = page + (number_of_api_calls - 1) * (page - 1)
        results = []

        # Makes the api calls to get the results
        for api_page in range(number_of_api_calls):
            try:
                req = requests.get(
                    self.baseURL
                    + "/search/api/search?q="
                    + query
                    + "&type=event"
                    + f"&page={api_page + actual_page}"
                )
            except Exception:
                raise ServiceUnavailable("Cannot perform search")
            data = json.loads(req.text)
            total_num_hits = int(data["total"])

            if not req.ok:
                raise ServiceUnavailable(
                    f"Search failed with error code {req.status_code}"
                )

            # Parse JSON response

            # Gets the results from the parsed JSON
            records = data["results"]

            # for each record get the recid, the url, the title and the source
            for record in records:
                results.append(self.parse_record(record))

        return {"total_num_hits": total_num_hits, "results": results}

    def search_by_id(self, recid):
        """
        Look for a record on Indico given a record ID.
        Returns the resulting record if exists
        """
        result = []

        try:
            req = requests.get(self.get_record_by_id(recid))
        except Exception:
            raise ServiceUnavailable("Cannot perform searching", recid)

        if req.ok:
            record = json.loads(req.text)
            record_list = record["results"]
            result.append(self.parse_record(record_list[0]))

        return {"result": result}

    def parse_record(self, record):
        """
        Parses each record returned from the API and returns the necessairy values
        """
        recid = record["event_id"]
        if not isinstance(recid, str):
            recid = str(recid)

        url = self.get_record_url(recid)

        return {
            "source_url": url,
            "recid": recid,
            "title": record["title"],
            "authors": record["persons"],
            "source": self.source,
        }
