import oais_platform.oais.sources
import oais_platform.oais.sources.cds
import oais_platform.oais.sources.codimd
import oais_platform.oais.sources.indico
import oais_platform.oais.sources.invenio
import oais_platform.oais.sources.local
from oais_platform.oais.models import Source


class InvalidSource(Exception):
    pass


def get_source(source_name, api_token=None):
    try:
        source = Source.objects.get(name=source_name)
        module = getattr(oais_platform.oais.sources, source.classname.lower())
        class_ = getattr(module, source.classname)
        return class_(source.name, source.api_url, api_token)
    except Source.DoesNotExist:
        raise InvalidSource(f"Invalid source: {source_name}")


__all__ = [get_source]
