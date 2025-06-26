import json

import requests
from celery import shared_task
from celery.utils.log import get_task_logger

from oais_platform.oais.models import Archive, Status, Step
from oais_platform.oais.tasks.pipeline_actions import finalize
from oais_platform.settings import BASE_URL, INVENIO_API_TOKEN, INVENIO_SERVER_URL

logger = get_task_logger(__name__)
logger.setLevel("DEBUG")


@shared_task(
    name="processInvenio", bind=True, ignore_result=True, after_return=finalize
)
def invenio(self, archive_id, step_id, input_data=None, api_key=None):
    """
    Publish an Archive on the configured InvenioRDM instance
    If the Archive was already published, create a new version of the Record.
    If another Archive referring to the same Resource (Source, Record ID)
    was already published, create a new version of the Record.
    """
    logger.info(f"Starting the publishing to InvenioRDM of Archive {archive_id}")

    # Get the Archive and Step we're running for
    archive = Archive.objects.get(pk=archive_id)
    step = Step.objects.get(pk=step_id)
    # And set the step as in progress
    step.set_status(Status.IN_PROGRESS)

    # The InvenioRDM API endpoint
    invenio_records_endpoint = f"{INVENIO_SERVER_URL}/api/records"

    # Set up the authentication headers for the requests to the InvenioRDM API
    headers = {
        "Authorization": "Bearer " + INVENIO_API_TOKEN,
        "Content-type": "application/json",
    }

    # If this Archive was never published before to InvenioRDM
    # and no similar Archive was published before

    if (archive.resource.invenio_parent_id) is None:
        # We create a brand new Record in InvenioRDM
        archive.invenio_version = 1
        data = prepare_invenio_payload(archive)

        try:
            # Create a record as a InvenioRDM draft
            req = requests.post(
                invenio_records_endpoint,
                headers=headers,
                data=json.dumps(data),
                verify=False,
            )
            req.raise_for_status()
        except Exception as err:
            logger.error(f"The request didn't succeed:{err}")
            step.set_status(Status.FAILED)
            return {"status": 1, "errormsg": err}

        # Parse the response and get our new record ID so we can link it
        data_loaded = json.loads(req.text)
        invenio_id = data_loaded["id"]
        relative_path = f"/records/{invenio_id}"

        # Create a path artifact with a link to the InvenioRDM Record we just created
        # FIXME: Use a single method to create artifacts
        output_invenio_artifact = {
            "artifact_name": "Registry",
            "artifact_path": relative_path,
            "artifact_url": f"{INVENIO_SERVER_URL}{relative_path}",
        }

        # Publish the InvenioRDM draft so it's accessible publicly
        req_publish_invenio = requests.post(
            f"{invenio_records_endpoint}/{invenio_id}/draft/actions/publish",
            headers=headers,
            verify=False,
        )

        # An InvenioRDM parent ID groups every published version reffering to the same Resource
        data_published = json.loads(req_publish_invenio.text)
        invenio_parent_id = data_published["parent"]["id"]

        # Save the Invenio parent ID on the Resource
        resource = archive.resource
        resource.set_invenio_id(invenio_id)
        resource.set_invenio_parent_fields(invenio_parent_id)

        # Save the resource and the archive
        resource.save()
        archive.save()

    # Create a new InvenioRDM version of an already published Record
    else:
        # Let's get the Parent ID for which we will create a new version
        invenio_id = archive.resource.invenio_id

        # Create new version as draft
        req_invenio_draft_new_version = requests.post(
            f"{INVENIO_SERVER_URL}/api/records/{invenio_id}/versions",
            headers=headers,
            verify=False,
        )

        # Get the ID of the draft we just created
        new_version_invenio_id = json.loads(req_invenio_draft_new_version.text)["id"]

        # Increment the version
        archive.invenio_version += 1

        # Initialize the archive data that is going to be sent on the request
        new_version_data = prepare_invenio_payload(archive)

        # Update draft with the new adata
        requests.put(
            f"{invenio_records_endpoint}/{new_version_invenio_id}/draft",
            headers=headers,
            data=json.dumps(new_version_data),
            verify=False,
        )

        # Publish the new Invenio RDM version draft
        requests.post(
            f"{invenio_records_endpoint}/{new_version_invenio_id}/draft/actions/publish",
            headers=headers,
            verify=False,
        )

        archive.save()

        # Create a InvenioRDM path artifact with a link to the new version
        # FIXME: Use a single method to create artifacts
        relative_path = f"/records/{new_version_invenio_id}"
        output_invenio_artifact = {
            "artifact_name": "Invenio Link",
            "artifact_path": "test",
            "artifact_url": f"{INVENIO_SERVER_URL}{relative_path}",
        }

    return {"status": 0, "id": invenio_id, "artifact": output_invenio_artifact}


def prepare_invenio_payload(archive):
    """
    From the Archive data and metadata, prepare the payload to create an Invenio Record,
    ready to be POSTed to the Invenio RDM API.
    """

    # If there's no title, put the source and the record ID
    if archive.title == "":
        title = f"{archive.source} : {archive.recid}"
    else:
        title = archive.title

    if archive.restricted is True:
        access = "restricted"
    else:
        access = "public"

    # We don't have reliable information about the authors of the upstream resource here,
    # so let's put a placeholder
    last_name = "N/A"
    first_name = "N/A"

    # Prepare the artifacts to publish
    # Get all the completed (status = 4) steps of the Archive
    steps = archive.steps.all().order_by("start_date").filter(status=4)

    invenio_artifacts = []

    for step in steps:
        if "artifact" in step.output_data:
            out_data = json.loads(step.output_data)
            artifact_name = out_data["artifact"]["artifact_name"]
            if artifact_name in ["SIP", "AIP"]:
                invenio_artifacts.append(
                    {
                        "type": artifact_name,
                        "link": f"{BASE_URL}/api/steps/{step.id}/download-artifact",
                        "path": out_data["artifact"]["artifact_path"],
                        "add_details": {
                            "SIP": "Submission Information Package as harvested by the platform from the upstream digital repository.",
                            "AIP": "Archival Information Package, as processed by Archivematica.",
                        }[artifact_name],
                        "timestamp": step.finish_date.strftime("%m/%d/%Y, %H:%M:%S"),
                    }
                )
            elif artifact_name == "CTA":
                invenio_artifacts.append(
                    {
                        "type": artifact_name,
                        "link": None,
                        "path": out_data["artifact"]["artifact_url"],
                        "add_details": "Archival Information Package pushed to the CERN Tape Archive.",
                        "timestamp": step.finish_date.strftime("%m/%d/%Y, %H:%M:%S"),
                    }
                )

    # Prepare the final payload
    data = {
        "access": {
            "record": access,
            "files": access,
        },
        # Set it as Metadata only
        "files": {"enabled": False},
        "metadata": {
            "creators": [
                {
                    "person_or_org": {
                        "family_name": last_name,
                        "given_name": first_name,
                        "type": "personal",
                    }
                }
            ],
            # Set publication_date to the moment we trigger a publish
            "publication_date": archive.timestamp.date().isoformat(),
            "resource_type": {"id": "publication"},
            "title": title,
            "description": f"<b>Source:</b> {archive.source}<br><b>Link:</b> <a href={archive.source_url}>{archive.source_url}<br></a>",
            # The version "name" we give on invenio is the Nth time we publish to invenio + the Archive ID from the platform
            # (there can be different Archive IDs going as a version to the same Invenio record: when two Archives are about the same Resource)
            "version": f"{archive.invenio_version}, Archive {archive.id}",
        },
        "custom_fields": {"artifacts": invenio_artifacts},
    }

    return data
