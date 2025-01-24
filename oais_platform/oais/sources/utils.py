import importlib
import inspect

import oais_platform.oais.sources.cds
import oais_platform.oais.sources.codimd
import oais_platform.oais.sources.indico
import oais_platform.oais.sources.invenio
import oais_platform.oais.sources.local
from oais_platform.oais.exceptions import InvalidSource
from oais_platform.oais.models import Source


def get_source(source_name, api_token=None):
    folder = [
        "oais_platform.oais.sources.cds",
        "oais_platform.oais.sources.codimd",
        "oais_platform.oais.sources.indico",
        "oais_platform.oais.sources.invenio",
        "oais_platform.oais.sources.local",
    ]
    try:
        source = Source.objects.get(name=source_name)
    except Source.DoesNotExist:
        raise InvalidSource(f"Invalid source: {source_name}")
    classname = source.classname
    for module_name in folder:
        module = importlib.import_module(module_name)
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if name == classname:
                return obj(source.name, source.api_url, api_token)
    raise InvalidSource(f"Invalid source: {source_name}")


__all__ = [get_source]
