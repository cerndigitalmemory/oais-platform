import os
from datetime import datetime, timedelta, timezone

import gitlab

# === CONFIG ===
GITLAB_URL = "https://gitlab.cern.ch"
GITLAB_TOKEN = os.environ.get("PRIVATE_TOKEN", os.environ.get("GITLAB_TOKEN"))
GROUP_NAME = "digitalmemory"
LABEL_NAME = "recently-updated"
DAYS = 3


# === LOGIC ===
def round_to_hour(dt: datetime) -> datetime:
    """
    Round a datetime to the nearest hour.
    """
    if dt.minute >= 30:
        # round up
        dt = dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        # round down
        dt = dt.replace(minute=0, second=0, microsecond=0)
    return dt


def was_label_removed_recently(issue, cutoff):
    """
    Return datetime of the last automated label removal after cutoff, or None.
    Only checks the most recent note.
    """
    notes = issue.notes.list(
        get_all=False,
        order_by="created_at",
        sort="desc",
        created_after=cutoff.isoformat(),  # only fetch notes after cutoff
        per_page=1,  # only the most recent note
    )

    if not notes:
        return False

    note = notes[0]
    if not note.system and f"[automation] {LABEL_NAME} removed" in note.body:
        ts_str = note.body.split("on")[-1].strip()
        return round_to_hour(datetime.fromisoformat(ts_str)) == round_to_hour(
            datetime.fromisoformat(issue.updated_at.replace("Z", "+00:00"))
        )
    return False


def update_label(gl, issue, cutoff, create=True):
    project = gl.projects.get(issue.project_id)
    full_issue = project.issues.get(issue.iid)
    if create:
        if was_label_removed_recently(full_issue, cutoff):
            print(f"The last action of {issue.iid} was removing the label...")
            return
        full_issue.labels.append(LABEL_NAME)
        message = "created on"
    else:
        full_issue.labels.remove(LABEL_NAME)
        message = "deleted from"
        full_issue.notes.create(
            {
                "body": f"[automation] {LABEL_NAME} removed on {datetime.now(timezone.utc).isoformat()}"
            }
        )
    full_issue.save()
    print(f"Label '{LABEL_NAME}' {message} {full_issue.web_url}")


def main():
    cutoff = datetime.now(timezone.utc) - timedelta(days=DAYS)

    gl = gitlab.Gitlab(GITLAB_URL, private_token=GITLAB_TOKEN)
    group = gl.groups.get(GROUP_NAME)

    for issue in group.issues.list(all=True, labels=[LABEL_NAME], scope="all"):
        updated_at = datetime.fromisoformat(issue.updated_at.replace("Z", "+00:00"))
        if updated_at < cutoff:
            update_label(gl, issue, cutoff, False)

    for issue in group.issues.list(
        all=True, updated_after=cutoff.isoformat(), scope="all"
    ):
        if LABEL_NAME not in issue.labels:
            update_label(gl, issue, cutoff, True)


if __name__ == "__main__":
    main()
