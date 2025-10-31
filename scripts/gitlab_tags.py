"""
Automated Git Tag Creation Script

This script automates the creation and pushing of semantic version tags for GitLab projects.
It provides a standalone Python solution for release tagging that can be integrated into CI/CD pipelines
or run manually for version management.

PURPOSE:
--------
- Automatically creates git tags based on version numbers provided via environment variables
- Prevents duplicate tag creation by checking existing tags in the remote repository
- Integrates with GitLab API for secure tag creation and management

WORKFLOW:
---------
1. Validates required environment variables (PRIVATE_TOKEN, PROJECT_ID, VERSION, BRANCH)
2. Validates that VERSION follows semantic versioning format (X.Y.Z(-rcD)?)
3. Constructs a git tag name by prefixing version with 'v' (e.g., v1.2.3, or v1.2.3-rc1 for feature branches)s
4. Connects to GitLab instance using API token authentication
5. Fetches project details and checks if tag already exists
6. Creates and pushes new tag to specified branch if it doesn't exist

USAGE:
------
Set required environment variables:
- PRIVATE_TOKEN: GitLab group token https://gitlab.cern.ch/groups/digitalmemory/-/settings/access_tokens?page=1
- PROJECT_ID: GitLab project ID (numeric)
- VERSION: Semantic version number (e.g., "1.2.3")
- BRANCH: Target branch for tag creation (e.g., "main", "master")

Then run: python scripts/gitlab_tags.py
"""

import os
import re
import sys

import gitlab
from gitlab.exceptions import GitlabGetError

GITLAB_URL = "https://gitlab.cern.ch"
TOKEN = os.environ.get("PRIVATE_TOKEN", None)
PROJECT_ID = os.environ.get("PROJECT_ID", None)
VERSION = os.environ.get("VERSION", None)
BRANCH = os.environ.get("BRANCH", None)


def validate_environment_vars():
    """Validate required environment variables."""
    missing_vars = []
    if not TOKEN or not TOKEN.strip():
        missing_vars.append("PRIVATE_TOKEN")
    if not PROJECT_ID or not PROJECT_ID.strip():
        missing_vars.append("PROJECT_ID")
    if not VERSION or not VERSION.strip():
        missing_vars.append("VERSION")
    if not BRANCH or not BRANCH.strip():
        missing_vars.append("BRANCH")

    return missing_vars


def validate_version(version, branch):
    """
    Validate that version follows semantic versioning format (X.Y.Z).

    Args:
        version: Version string to validate

    Returns:
        True if valid, False otherwise
    """
    if branch != "main":
        pattern = r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)-RC(0|[1-9]\d*)$"
    else:
        pattern = r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$"
    return bool(re.match(pattern, version))


def fetch_project(gitlab, project_id):
    """Fetch project from GitLab."""
    try:
        project = gitlab.projects.get(project_id)
        return project
    except GitlabGetError as e:
        print(f"❌ Failed to fetch project {project_id}: {e}")
        return False


def tag_exist(project, tag):
    """Fetch all tags from remote repository."""
    try:
        print("Fetching tags from remote repository...")
        tag = project.tags.get(tag)
        print("Tag does already exist.")
        return True
    except GitlabGetError:
        return False


def create_and_push_tag(project, tag, branch):
    """Create a git tag and push it to the remote repository."""
    try:
        tag = project.tags.create({"tag_name": tag, "ref": branch})
        # Configure git to use the token for authentication
        print(f"✅ Successfully created and pushed tag {tag}")
        return True
    except:
        print(f"❌ Failed to create tag {tag}: {e}")
        return False


def main():

    # check required environment variables
    invalid_vars = validate_environment_vars()
    if invalid_vars:
        print(f"❌ Invalid environment variables: {', '.join(invalid_vars)}")
        sys.exit(1)
    # Validate version format
    if not validate_version(VERSION, BRANCH):
        print(
            f"❌ Invalid VERSION '{VERSION}' for branch '{BRANCH}' (expected X.Y.Z for 'main' branch and 'X.Y.Z-RC[0-9]' for feature branches)"
        )
        if BRANCH != "main":
            sys.exit(0)
        sys.exit(1)

    gl = gitlab.Gitlab(GITLAB_URL, private_token=TOKEN)

    project = fetch_project(gl, PROJECT_ID)
    if not project:
        sys.exit(1)

    tag = f"v{VERSION}"
    print(f"🚀 Starting release tag creation for {tag}")

    # Check if tag already exists
    if tag_exist(project, tag):
        print(f"⚠️  Tag {tag} already exists, skipping release creation")
        sys.exit(0)

    print(f"✨ Tag {tag} does not exist, creating tag...")

    # Create and push the tag
    if not create_and_push_tag(project, tag, BRANCH):
        sys.exit(1)


if __name__ == "__main__":
    main()
