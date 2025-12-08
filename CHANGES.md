# Changes

## Version 2.5.0 (released 2025-12-08)
- Moved step concurrency limits to the database
- Archivematica: added step manager to pick up waiting tasks

## Version 2.4.1 (released 2025-12-03)
- Dependencies: pin kombu 5.6.1, bump redis to 5.x for compatibility

## Version 2.4.0 (released 2025-12-03)
- Archivematica: add callback endpoint
- Added Personal Access Token
- Invenio scheduled harvest updates (created, updated)
- Archive: added version timestamp
- Fix: FTS job PeriodicTask deletion idempotent

## Version 2.3.3 (released 2025-11-18)
- Harvest batch: fix batch numbering

## Version 2.3.2 (released 2025-11-07)
- Local upload: keep file name, add file size limit

## Version 2.3.1 (released 2025-11-04)
- Step: set missing initiated_by_user
- Pipeline: fix auto tag creation

## Version 2.3.0 (released 2025-10-31)
- Pipeline: create GitLab tag for new version
- Adding version to response header
- Local upload: refactored, handling errors
- Upgrade dependencies: amclient, oais-utils, bagit-create
- Handling harvest redirect error
- Admin panel optimization
