# importing all tasks so they can be registered with Celery
from . import (
    announce,
    archivematica,
    create_sip,
    cta,
    extract_title,
    integrity_checks,
    notify_source,
    pipeline_actions,
    registry,
    remove_orphan_packages,
    scheduled_harvest,
)
