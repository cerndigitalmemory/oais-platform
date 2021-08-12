from oais_platform.oais.sources.cds import CDS
from oais_platform.oais.sources.invenio_v3 import InvenioV3

sources = {
    "cds": CDS("cds", "https://cds.cern.ch"),
    "cds-test": CDS("cds-test", "https://cds-test.cern.ch"),
    "zenodo": InvenioV3("zenodo", "https://zenodo.org/api"),
    "inveniordm": InvenioV3("inveniordm", "https://inveniordm.web.cern.ch/api")
}


class InvalidSource(Exception):
    pass


def get_source(source):
    if source not in sources:
        raise InvalidSource(f"Invalid source: {source}")
    return sources[source]


__all__ = [get_source]
