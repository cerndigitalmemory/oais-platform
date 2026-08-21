AM_INSTANCES = [
    {
        "AM_INSTANCE": "AM1",
        "AM_URL": "http://host.docker.internal:62080",
        "AM_USERNAME": "test",
        "AM_API_KEY": "test",
        "AM_SS_URL": "http://host.docker.internal:62081",
        "AM_SS_USERNAME": "test",
        "AM_SS_API_KEY": "test",
        "SIP_UPSTREAM_BASEPATH": "/oais_platform/oais-data/sips/am1",
        "AIP_UPSTREAM_BASEPATH": "/oais_platform/oais-data/aips/am1",
        "AM_TRANSFER_SOURCE": None,
        "AM_RETRY_LIMIT": 2,
    }
]


def create_archivematica_instance(config=None):
    from oais_platform.oais.models import ArchivematicaInstance

    config = config or AM_INSTANCES[0]
    return ArchivematicaInstance.objects.create(
        name=config["AM_INSTANCE"],
        url=config["AM_URL"],
        username=config["AM_USERNAME"],
        api_key=config["AM_API_KEY"],
        storage_service_url=config["AM_SS_URL"],
        storage_service_username=config["AM_SS_USERNAME"],
        storage_service_api_key=config["AM_SS_API_KEY"],
        sip_upstream_basepath=config["SIP_UPSTREAM_BASEPATH"],
        aip_upstream_basepath=config["AIP_UPSTREAM_BASEPATH"],
        transfer_source=config["AM_TRANSFER_SOURCE"],
        retry_limit=config["AM_RETRY_LIMIT"],
    )
