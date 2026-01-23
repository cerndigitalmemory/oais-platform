# Changes

## Version 2.8.0 (released 2026-01-23)
- SIP: path structure change
- Management: script added to move SIP
- CTA: modify path, fix jobs not picked up
- AM: fix PeriodicTask disabled after callback
- views: add missing serializers, update config endpoint response key
- Local dev: add Django static content
- Unstage: add title option, remove single unstage
- Pipeline: add option to continue after completed with warnings
- Sanitize filename moved to oais_utils
- Job summary: add undefined for staged archives

## Version 2.7.2 (released 2026-01-15)
- AM callback: correct package_name suffix handling

## Version 2.7.1 (released 2026-01-15)
- AM: time out if stuck in processing
- AM callback: fix regex when package_name has suffix

## Version 2.7.0 (released 2026-01-14)
- CTA: improved error messages, check if file exists before submit
- AM: fix duplicate error, halt pipeline after completed with warnings
- Jobs: return summary
- Steps: improve checksum comparison function, merge checksum and validation
- Local dev: add files to serve by nginx

## Version 2.6.0 (released 2025-12-16)
- Archivematica: add more failure details, automatically retry on some errors
- Local dev: updated Archivematica local setup
- Fix: Archivematica step manager concurrency

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
