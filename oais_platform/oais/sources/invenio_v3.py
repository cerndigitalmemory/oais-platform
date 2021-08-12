import json
import requests
from oais_platform.oais.exceptions import ServiceUnavailable
from oais_platform.oais.sources.source import Source

import configparser, os

def get_dict_value(dct, keys):
    for key in keys:
        try:
            dct = dct[key]
        except KeyError:
            raise InvalidJSONKey("Key:" + key + " not found in dict: " + str(dct))
    return dct

class InvalidJSONKey(Exception):
    pass

class ConfigFileUnavailable(Exception):
    pass

class InvenioV3(Source):

    def __init__(self, source, baseURL):
        self.source = source
        self.baseURL = baseURL

        self.config_file = configparser.ConfigParser()
        self.config_file.read(os.path.join(os.path.dirname(__file__), "invenio_v3.ini"))
        self.config = None

        if len(self.config_file.sections()) == 0:
            raise ConfigFileUnavailable(f"Could not read config file for InvenioV3 instance: {source}")

        for instance in self.config_file.sections():
            if instance == source:
                self.config = self.config_file[instance]
        
        if not self.config:
            raise ValueError("No configuration found")

    def get_record_url(self, recid):
        return f"{self.baseURL}/record/{recid}"

    def search(self, query):
        try:
            req = requests.get(self.baseURL + "/records?q=" + query)
        except:
            raise ServiceUnavailable("Cannot perform search")

        if not req.ok:
            raise ServiceUnavailable(
                f"Search failed with error code {req.status_code}")

        # Parse JSON response
        data = json.loads(req.text)
        records_key_list = self.config["records"].split(",")
        records = get_dict_value(data, records_key_list)
        
        results = []
        for record in records:
            recid_key_list = self.config["recid"].split(",")
            recid = get_dict_value(record, recid_key_list)
            if not isinstance(recid,str):
                recid = str(recid)

            authors_key_list = self.config["authors"].split(",")
            authors = []
            for author in get_dict_value(record, authors_key_list):
                author_name_key_list = self.config["author_name"].split(",")
                authors.append(get_dict_value(author,author_name_key_list))

            url_key_list = self.config["url"].split(",")
            title_key_list = self.config["title"].split(",")
            
            results.append({
                "url": get_dict_value(record, url_key_list),
                "recid": recid,
                "title": get_dict_value(record, title_key_list),
                "authors": authors,
                "source": self.source
            })

        return results
