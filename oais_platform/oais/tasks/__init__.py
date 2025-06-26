# importing all tasks so they can be registered with Celery
from . import (
    announce,
    archivematica,
    extract_title,
    fts,
    harvest,
    notify_source,
    pipeline_action,
    registry,
)
