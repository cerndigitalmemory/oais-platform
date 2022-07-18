from oais_platform.oais.sources.cds import CDS
from oais_platform.oais.sources.invenio import Invenio
from oais_platform.oais.sources.indico import Indico
from oais_platform.oais.sources.codimd import CodiMD
from oais_platform.oais.sources.local import Local

sources = {
    "cds": CDS("cds", "https://cds.cern.ch"),
    "cds-test": CDS("cds-test", "https://cds-test.cern.ch"),
    "zenodo": Invenio("zenodo", "https://zenodo.org/api"),
    "inveniordm": Invenio("inveniordm", "https://inveniordm.web.cern.ch/api"),
    "cod": Invenio("cod", "https://opendata.cern.ch/api"),
    "local": Local()
    # "indico": Indico("indico", "https://indico.cern.ch"),
    # "codimd": CodiMD("codimd", "https://codimd.web.cern.ch")
}


class InvalidSource(Exception):
    pass


def get_source(source, api_token=None):
    if source == "indico":
        return Indico("indico", "https://indico.cern.ch", api_token)
    if source == "codimd":
        return CodiMD("codimd", "https://codimd.web.cern.ch", api_token)

    if source not in sources:
        raise InvalidSource(f"Invalid source: {source}")

    return sources[source]


__all__ = [get_source]
