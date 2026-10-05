import os
import shutil
import time

from celery import shared_task
from celery.utils.log import get_task_logger

from oais_platform.oais.models import Archive, Step
from oais_platform.settings import ENVIRONMENT, REMOVE_ORPHAN_PACKAGES_DRY_RUN

logger = get_task_logger(__name__)

BASE_PATH = f"/eos/project-p/preserve-{ENVIRONMENT}/platform/{ENVIRONMENT}"
AIP_ROOT = os.path.join(BASE_PATH, "aips")
SIP_ROOT = os.path.join(BASE_PATH, "sips")

AIP_EXTENSION = ".7z"
SIP_DIR_PREFIX = "sip::"

MIN_PACKAGE_AGE_SECONDS = 7 * 24 * 3600  # 7 days


@shared_task(name="remove_orphan_packages")
def remove_orphan_packages():
    current_time = time.time()
    dry_run = REMOVE_ORPHAN_PACKAGES_DRY_RUN

    known_sips_path, known_aips_path = _known_packages_paths()

    orphan_sips = _find_orphan_sips(known_sips_path, current_time)
    orphan_aips = _find_orphan_aips(known_aips_path, current_time)

    sips_deleted, sip_errors = _delete_packages(orphan_sips, SIP_ROOT, dry_run)
    aips_deleted, aip_errors = _delete_packages(orphan_aips, AIP_ROOT, dry_run)

    report = {
        "dry_run": dry_run,
        "aips": {"orphans found": len(orphan_aips), "deleted": aips_deleted},
        "sips": {"orphans found": len(orphan_sips), "deleted": sips_deleted},
        "errors": aip_errors + sip_errors,
    }

    logger.info(f"Orphan packages report {report}")
    return report


def _known_packages_paths():
    sips_paths = _normalize_paths(Archive.objects.values_list("path_to_sip", flat=True))
    aips_paths = _normalize_paths(
        Archive.objects.exclude(path_to_aip="").values_list("path_to_aip", flat=True)
    )

    output_data_json_list = Step.objects.exclude(
        output_data_json__isnull=True
    ).values_list("output_data_json", flat=True)

    for current_output_data_json in output_data_json_list:
        artifact = current_output_data_json.get("artifact") or {}
        path = artifact.get("artifact_path")

        if not path:
            continue

        if artifact.get("artifact_name") == "SIP":
            sips_paths.add(_normalize_path(path))
        elif artifact.get("artifact_name") == "AIP":
            aips_paths.add(_normalize_path(path))

    return sips_paths, aips_paths


def _normalize_path(path):
    return os.path.normpath(path) if path else None


def _normalize_paths(paths):
    return {os.path.normpath(path) for path in paths if path}


def _is_package_old_enough(path, current_time):
    """
    True if the file hasn't been modified for at least MIN_PACKAGE_AGE_SECONDS.

    Protects against race conditions: a package might be on disk but not
    yet referenced in the DB because the application hasn't finished
    writing the Archive/Step record. Returns False (not old enough) if the
    file has already disappeared, instead of raising.
    """
    try:
        return current_time - os.path.getmtime(path) >= MIN_PACKAGE_AGE_SECONDS
    except FileNotFoundError:
        return False


def _find_orphan_sips(known_sips_path, current_time):
    orphans = []
    for root, dirs, _files in os.walk(SIP_ROOT):
        dirs_to_skip = []

        for current_dir in dirs:
            if not current_dir.startswith(SIP_DIR_PREFIX):
                continue
            sip_path = _normalize_path(os.path.join(root, current_dir))
            dirs_to_skip.append(current_dir)

            if sip_path not in known_sips_path and _is_package_old_enough(
                sip_path, current_time
            ):
                orphans.append(sip_path)

        # A SIP folder is the package itself, not a directory to walk into
        # Remove it from `dirs` so os.walk() doesn't descend into it.
        for subdir in dirs_to_skip:
            dirs.remove(subdir)

    return orphans


def _find_orphan_aips(known_aips_path, current_time):
    orphans = []

    for root, _dirs, files in os.walk(AIP_ROOT):
        for file_name in files:
            if not file_name.endswith(AIP_EXTENSION):
                continue
            path = _normalize_path(os.path.join(root, file_name))

            if path not in known_aips_path and _is_package_old_enough(
                path, current_time
            ):
                orphans.append(path)
    return orphans


def _remove_empty_parents(directory, stop_at):
    """
    Walk up from `directory` (the folder the deleted package was in) toward
    `stop_at` (AIP_ROOT or SIP_ROOT), deleting each folder that is now empty.
    Stops as soon as a folder still has something in it (meaning it might
    still be useful), or at `stop_at` itself (never deleted).
    """
    while directory.startswith(stop_at + os.sep):
        try:
            if os.listdir(directory):
                return
            os.rmdir(directory)
        except OSError:
            return
        directory = os.path.dirname(directory)


def _delete_packages(paths, root, dry_run):
    """
    Delete each path (file or directory), then clean up now-empty parent
    folders. In dry_run mode, nothing is actually deleted - just logged.

    The success counter is incremented right after the path itself is
    deleted, BEFORE attempting the parent cleanup - a failure in
    _remove_empty_parents (just a best-effort tidy-up) must not turn an
    already-successful deletion into a reported error.
    """
    deleted = 0
    errors = []

    for path in paths:
        logger.info(f"{'[DRY RUN] would delete' if dry_run else 'Deleting'} {path}")

        if dry_run:
            continue

        try:
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)
        except OSError as e:
            errors.append({"path": path, "error": str(e)})
            continue

        deleted += 1
        _remove_empty_parents(os.path.dirname(path), root)

    return deleted, errors
