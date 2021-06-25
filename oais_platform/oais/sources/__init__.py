from oais_platform.oais.sources.cds import CDS

sources = {
    "cds": CDS("cds", "https://cds.cern.ch"),
    "cds-test": CDS("cds-test", "https://cds-test.cern.ch")
}


class InvalidSource(Exception):
    pass


def get_source(source):
    if source not in sources:
        raise InvalidSource(f"Invalid source: {source}")
    return sources[source]


__all__ = [get_source]
