import configparser
import datetime
import json
import logging
import os
import urllib.parse

import requests

from oais_platform.oais.exceptions import (
    ConfigFileUnavailable,
    MaxRetriesExceeded,
    RetryableException,
    ServiceUnavailable,
)
from oais_platform.oais.models import Status, StepName
from oais_platform.oais.sources.abstract_source import AbstractSource


def get_dict_value(dct, keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            return None
    return dct


class Invenio(AbstractSource):
    def __init__(self, source, baseURL, token=None):
        self.source = source
        self.baseURL = baseURL
        self.max_results = 10000
        self.max_retries = 5

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
            "User-Agent": "cern-digital-memory-bot",
            "Accept": "application/vnd.inveniordm.v1+json",  # Needed for Zenodo compatibility
        }

        if token:
            self.headers["Authorization"] = f"Bearer {token}"

    def get_record_url(self, recid):
        return f"{self.baseURL}/records/{recid}"

    def search(self, query, page=1, size=20, sort=None):
        url = f"{self.baseURL}/records?q={query}&size={str(size)}&page={str(page)}"
        if sort:
            url += f"&sort={sort}"
        try:
            req = requests.get(url, headers=self.headers)
        except Exception as e:
            logging.exception(f"Error while performing search: {str(e)}")
            raise ServiceUnavailable("Cannot perform search")

        if not req.ok:
            logging.exception(f"Error while performing search: {str(req.text)}")
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

        return {"total_num_hits": total_num_hits, "results": results}

    def search_by_id(self, recid):
        result = []

        try:
            req = requests.get(self.get_record_url(recid), headers=self.headers)
        except Exception as e:
            logging.exception(f"Error while performing search: {str(e)}")
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

        return {
            "source_url": self._get_config_value(record, "url", mandatory=True),
            "recid": recid,
            "title": self._get_config_value(record, "title", mandatory=True),
            "authors": authors,
            "source": self.source,
            "status": self._get_config_value(record, "status"),
            "file_size": self._get_config_value(record, "file_size"),
            "updated": self._get_config_value(record, "updated"),
        }

    def _get_config_value(self, record, config_key, mandatory=False):
        path = self.config.get(config_key)
        if mandatory and not path:
            raise ValueError(f"Mandatory config key '{config_key}' is missing.")
        return get_dict_value(record, path.split(",")) if path else None

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

        if not archive.path_to_aip:
            raise Exception(f"Archive {archive.id} has no path_to_aip set.")

        harvest_time = (
            archive.steps.all()
            .filter(step_name=StepName.HARVEST, status=Status.COMPLETED)
            .first()
            .start_date
        )

        archive_time = (
            archive.steps.all()
            .filter(step_name=StepName.ARCHIVE, status=Status.COMPLETED)
            .order_by("-start_date")
            .first()
            .start_date
        )

        payload = {
            "pid": archive.recid,
            "status": "P",  # Preserved
            "path": archive.path_to_aip,
            "harvest_timestamp": str(harvest_time),
            "archive_timestamp": str(archive_time),
            "description": {"sender": "CERN Digital Memory", "compliance": "OAIS"},
        }

        registry_link = archive.resource.invenio_parent_url
        if registry_link:
            payload["uri"] = registry_link

        req = requests.post(
            notification_endpoint,
            headers=headers,
            data=json.dumps(payload),
            verify=False,
        )

        if req.status_code == 202:
            return 0
        elif req.status_code in [408, 429, 502, 503, 504]:
            raise RetryableException(f"Request returned status code {req.status_code}.")
        else:
            raise Exception(
                f"Notifying the upstream source failed with status code {req.status_code}, message: {req.text}"
            )

    def get_records_to_harvest(
        self, start=None, end=None, size=500, created_filter=False
    ):
        if not end:
            end = datetime.datetime.now(datetime.timezone.utc)
        logging.info(f"Starting fetching records from {start} to {end}.")
        yield from self.fetch_records_in_chunks(start, end, size, created_filter)

    def get_records_in_range(self, start, end, page, size, created_filter=False):
        filter_word = "created" if created_filter else "updated"

        if start:
            query = f"{filter_word}:[{start.strftime('%Y-%m-%dT%H:%M:%S')} TO {end.strftime('%Y-%m-%dT%H:%M:%S')}}}"
        else:
            query = f"{filter_word}:[* TO {end.strftime('%Y-%m-%dT%H:%M:%S')}}}"
        query = urllib.parse.quote_plus(query)
        return self.search(query, page, size, sort=f"{filter_word}-asc")

    def fetch_records_in_chunks(self, start, end, size, created_filter=False):
        result = self.get_records_in_range(start, end, 1, 1, created_filter)
        total = result["total_num_hits"]
        if total <= 0:
            yield [], end
        elif total <= self.max_results:
            logging.info(
                f"Fetching records for {start.strftime('%Y-%m-%dT%H:%M:%S') if start else '*'}–{end.strftime('%Y-%m-%dT%H:%M:%S')} with {total} results."
            )
            initial_total = total
            current_total = 0
            retry_count = 0
            while retry_count < self.max_retries:
                records_to_add = []
                page = 0
                while len(records_to_add) < initial_total:
                    page += 1
                    result = self.get_records_in_range(
                        start, end, page, size, created_filter
                    )
                    current_total = result["total_num_hits"]
                    if current_total != initial_total:
                        logging.warning(
                            f"Query result changed: {current_total} != {initial_total}, retrying ..."
                        )
                        break
                    records_to_add += result["results"]
                if (
                    initial_total == current_total
                    and len(records_to_add) == current_total
                ):
                    logging.info(f"Adding {current_total} records for {start}–{end}.")
                    yield records_to_add, end
                    break
                initial_total = current_total
                retry_count += 1
            if retry_count == self.max_retries:
                logging.exception(f"Max retries reached for {start}–{end}...")
                raise MaxRetriesExceeded(
                    f"Cannot get consistent ids for {start}–{end}..."
                )
        else:
            result = self.get_records_in_range(
                start, end, self.max_results, 1, created_filter
            )
            last_record = result["results"][0]
            last_record_update_time = datetime.datetime.fromisoformat(
                last_record["updated"]
            )
            yield from self.fetch_records_in_chunks(
                start, last_record_update_time, size, created_filter
            )
            yield from self.fetch_records_in_chunks(
                last_record_update_time, end, size, created_filter
            )
