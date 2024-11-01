import configparser
import json
import os

import requests

from oais_platform.oais.exceptions import ServiceUnavailable
from oais_platform.oais.models import Status, Steps
from oais_platform.oais.sources.abstract_source import AbstractSource


def get_dict_value(dct, keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
    return dct


class ConfigFileUnavailable(Exception):
    pass


class Invenio(AbstractSource):
    def __init__(self, source, baseURL, token=None):
        self.source = source
        self.baseURL = baseURL

        self.config_file = configparser.ConfigParser()
        self.config_file.read(os.path.join(os.path.dirname(__file__), "invenio.ini"))
        self.config = None

        if len(self.config_file.sections()) == 0:
            raise ConfigFileUnavailable(
                f"Could not read config file for Invenio instance: {source}"
            )

        for instance in self.config_file.sections():
            if instance == source:
                self.config = self.config_file[instance]

        if not self.config:
            raise ValueError("No configuration found")

        self.headers = {
            "Content-Type": "application/json",
        }

        if token:
            self.headers["Authorization"] = f"Bearer {token}"

    def get_record_url(self, recid):
        return f"{self.baseURL}/records/{recid}"

    def search(self, query, page=1, size=20):
        try:
            req = requests.get(
                f"{self.baseURL}/records?q={query}&size={str(size)}&page={str(page)}",
                headers=self.headers,
                verify=False,
            )
        except Exception:
            raise ServiceUnavailable("Cannot perform search")

        if not req.ok:
            raise ServiceUnavailable(f"Search failed with error code {req.status_code}")

        # Parse JSON response
        data = json.loads(req.text)
        records_key_list = self.config["records"].split(",")
        records = get_dict_value(data, records_key_list)

        results = []
        for record in records:
            results.append(self.parse_record(record))

        # Get total number of hits
        total_num_hits = data["hits"]["total"]

        if self.source == "zenodo" and total_num_hits > 10000:
            total_num_hits = 10000

        return {"total_num_hits": total_num_hits, "results": results}

    def search_by_id(self, recid):
        result = []

        try:
            req = requests.get(self.get_record_url(recid), headers=self.headers)
        except Exception:
            raise ServiceUnavailable("Cannot perform search")

        if req.ok:
            record = json.loads(req.text)
            result.append(self.parse_record(record))

        return {"result": result}

    def parse_record(self, record):
        recid_key_list = self.config["recid"].split(",")
        recid = get_dict_value(record, recid_key_list)
        if not isinstance(recid, str):
            recid = str(recid)

        authors_key_list = self.config["authors"].split(",")
        authors_list = get_dict_value(record, authors_key_list)
        authors = []
        if authors_list:
            for author in authors_list:
                author_name_key_list = self.config["author_name"].split(",")
                authors.append(get_dict_value(author, author_name_key_list))

        url_key_list = self.config["url"].split(",")
        title_key_list = self.config["title"].split(",")

        status = None
        if self.config["status"]:
            status = get_dict_value(record, self.config["status"].split(","))

        return {
            "source_url": get_dict_value(record, url_key_list),
            "recid": recid,
            "title": get_dict_value(record, title_key_list),
            "authors": authors,
            "source": self.source,
            "status": status,
        }

    def notify_source(self, archive, notification_endpoint, api_key=None):
        headers = {
            "Content-Type": "application/json",
        }

        # Set up the authentication headers for the requests to the Source
        if not api_key:
            raise Exception(
                f"User has no API key set for the upstream source ({archive.source})."
            )
        else:
            headers["Authorization"] = f"Bearer {api_key}"

        harvest_time = (
            archive.steps.all()
            .filter(name=Steps.HARVEST, status=Status.COMPLETED)
            .first()
            .start_date
        )

        archive_time = (
            archive.steps.all()
            .filter(name=Steps.ARCHIVE, status=Status.COMPLETED)
            .order_by("-start_date")
            .first()
            .start_date
        )

        registry_link = archive.resource.invenio_parent_url

        payload = {
            "pid": archive.recid,
            "status": "P",  # Preserved
            "path": archive.path_to_aip,
            "uri": registry_link,
            "harvest_timestamp": str(harvest_time),
            "archive_timestamp": str(archive_time),
            "description": {"sender": "CERN Digital Memory", "compliance": "OAIS"},
        }

        req = requests.post(
            notification_endpoint,
            headers=headers,
            data=json.dumps(payload),
            verify=False,
        )

        if req.status_code == 202:
            return True
        else:
            raise Exception(
                f"Notifying the upstream source failed with status code {req.status_code}, message: {req.text}"
            )
