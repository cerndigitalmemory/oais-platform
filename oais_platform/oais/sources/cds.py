import io

import pymarc
import requests
from oais_platform.oais.exceptions import ServiceUnavailable
from oais_platform.oais.sources.source import Source


class CDS(Source):

    def __init__(self, source, baseURL):
        self.source = source
        self.baseURL = baseURL

    def get_record_url(self, recid):
        return f"{self.baseURL}/record/{recid}"

    def search(self, query):
        try:
            req = requests.get(self.baseURL + "/search",
                               params={"p": query, "of": "xm"})
        except:
            raise ServiceUnavailable("Cannot perform search")

        if not req.ok:
            raise ServiceUnavailable(
                f"Search failed with error code {req.status_code}")

        # Parse MARC XML
        records = pymarc.parse_xml_to_array(io.BytesIO(req.content))
        results = []
        for record in records:
            recid = record["001"].value()

            authors = []
            for author in record.get_fields("100", "700"):
                authors.append(author["a"])

            results.append({
                "url": self.get_record_url(recid),
                "recid": recid,
                "title": record.title(),
                "authors": authors,
                "source": self.source
            })

        return results
