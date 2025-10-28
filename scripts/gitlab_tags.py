"""
Automated Git Tag Creation Script

This script automates the creation and pushing of semantic version tags for the OAIS platform projects.
It was converted from a GitLab CI pipeline job that handles release tagging across multiple repositories.

PURPOSE:
--------
- Automatically creates git tags based on version numbers extracted from project files
- Ensures consistent release tagging across oais-platform (Django backend) and oais-web (React frontend)
- Prevents duplicate tag creation and maintains semantic versioning standards
- Integrates with GitLab's release management workflow

WORKFLOW:
---------
1. Validates that the VERSION follows semantic versioning format (X.Y.Z)
2. Constructs a git tag name by prefixing version with 'v' (e.g., v1.2.3)
3. Checks if the tag already exists to prevent duplicates
4. Fetches latest tags from remote repository for accurate comparison
5. Creates and pushes the new tag using authenticated GitLab access

BUSINESS CONTEXT:
-----------------
This script is part of the CERN Digital Memory platform's CI/CD pipeline. It ensures that:
- Each version bump in the codebase automatically creates a corresponding git tag
- Release management is consistent between backend (Python/Django) and frontend (React) projects
- Tags serve as reference points for deployments and release notes
- Version history is properly tracked in git for audit and rollback purposes
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


def validate_version(version):
    """
    Validate that version follows semantic versioning format (X.Y.Z).

    Args:
        version: Version string to validate

    Returns:
        True if valid, False otherwise
    """
    pattern = r"^[0-9]+\.[0-9]+\.[0-9]+.*$"
    return bool(re.match(pattern, version))


def fetch_project(gitlab, project_id):
    """Fetch project from GitLab."""
    try:
        project = gitlab.projects.get(project_id)
        return project
    except GitlabGetError as e:
        print(f"❌ Failed to fetch project {project_id}: {e}")
        sys.exit(1)


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
    except:
        print(f"❌ Failed to create tag {tag}: {e}")
        sys.exit(1)


def main():

    # check required environment variables
    invalid_vars = validate_environment_vars()
    if invalid_vars:
        print(f"❌ Invalid environment variables: {', '.join(invalid_vars)}")
        sys.exit(1)
    # Validate version format
    if not validate_version(VERSION):
        print(f"❌ Invalid VERSION '{VERSION}' (expected X.Y.Z format)")
        sys.exit(1)

    gl = gitlab.Gitlab(GITLAB_URL, private_token=TOKEN)

    project = fetch_project(gl, PROJECT_ID)

    tag = f"v{VERSION}"
    print(f"🚀 Starting release tag creation for {tag}")

    # Check if tag already exists
    if tag_exist(project, tag):
        print(f"⚠️  Tag {tag} already exists, skipping release creation")
        sys.exit(0)

    print(f"✨ Tag {tag} does not exist, creating tag...")

    # Create and push the tag
    create_and_push_tag(project, tag, BRANCH)


if __name__ == "__main__":
    main()
