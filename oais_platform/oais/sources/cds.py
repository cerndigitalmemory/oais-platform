import io

import pymarc
import requests
from oais_platform.oais.exceptions import ServiceUnavailable
from oais_platform.oais.sources.source import Source

import re

class CDS(Source):

    def __init__(self, source, baseURL):
        self.source = source
        self.baseURL = baseURL

    def get_record_url(self, recid):
        return f"{self.baseURL}/record/{recid}"

    def search(self, query, page=1, size=20):   
        try:
            # The "sc" parameter (split by collection) is used to provide
            # search results consistent with the ones from the CDS website
            req = requests.get(self.baseURL + "/search",
                               params={"p": query, "of": "xm", "rg": size, "jrec": int(size)*(int(page)-1)+1})
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

            title = record.title()
            # If the title is not present, show the meeting name
            meeting_name = record["111"]
            if not title and meeting_name:
                title = meeting_name["a"]

            results.append({
                "url": self.get_record_url(recid),
                "recid": recid,
                "title": title,
                "authors": authors,
                "source": self.source
            })

        if(len(records) > 0):
            # Get total number of hits
            pattern = "<!-- Search-Engine-Total-Number-Of-Results:(.*?)-->"

            total_num_hits = int(re.search(pattern, req.text).group(1))
        else:
            total_num_hits = 0

        return {"total_num_hits" : total_num_hits, "results": results}
    
