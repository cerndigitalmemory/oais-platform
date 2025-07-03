# importing all tasks so they can be registered with Celery
from . import (
    announce,
    archivematica,
    cta,
    extract_title,
    harvest,
    integrity_checks,
    notify_source,
    pipeline_actions,
    registry,
)
