from random import sample
from oais_platform.oais.exceptions import ServiceUnavailable
from oais_platform.oais.sources.source import Source
import json


class TestSource(Source):
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

def get_sample_sip_json():
    sample_json = {
    "$schema": "https://gitlab.cern.ch/digitalmemory/sip-spec/-/blob/master/sip-schema-d1.json",
    "created_by": "bagit-create",
    "audit": [
        {
            "tool": {
                "name": "CERN BagIt Create",
                "version": "0",
                "website": "https://gitlab.cern.ch/digitalmemory/bagit-create",
                "params": {
                    "recid": "test",
                    "url": None,
                    "source": "cds",
                    "loglevel": 2,
                    "target": None,
                    "source_path": None,
                    "author": None,
                    "source_base_path": None,
                    "dry_run": False,
                    "bibdoc": False,
                    "bd_ssh_host": None,
                    "timestamp": 0,
                    "cert": None
                }
            },
            "action": "sip_create",
            "timestamp": 0,
            "message": ""
        }
    ],
    "source": "cds",
    "recid": "2280000",
    "metadataFile_upstream": "https://cds.cern.ch/record/2280000?of=xm",
    "contentFiles": [
        {
            "origin": {
                "url": "https://www.worldscientific.com/toc/ijmpcs/46",
                "filename": "46",
                "path": ""
            },
            "size": 0,
            "bagpath": "data/content/46",
            "metadata": False,
            "downloaded": True,
            "checksum": [
                "md5:03fcfdd203f271fa2776f0e886e10c7e"
            ]
        },
        {
            "origin": {
                "filename": "metadata-cds-2280000.xml",
                "path": "",
                "url": "https://cds.cern.ch/record/2280000?of=xm"
            },
            "metadata": False,
            "downloaded": True,
            "bagpath": "data/content/metadata-cds-2280000.xml",
            "size": "807",
            "checksum": [
                "md5:8d468e05868b47e740b35abed89706d2"
            ]
        },
        {
            "origin": {
                "filename": "bagitcreate.log",
                "path": ""
            },
            "metadata": False,
            "downloaded": True,
            "bagpath": "data/meta/bagitcreate.log"
        },
        {
            "origin": {
                "filename": "sip.json",
                "path": ""
            },
            "metadata": False,
            "downloaded": True,
            "bagpath": "data/meta/sip.json"
        }
    ],
    "sip_creation_timestamp": 1646040977
}
    return json.dumps(sample_json)
