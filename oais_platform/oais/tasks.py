from logging import log
from bagit_create import main as bic
from celery import states
from celery.decorators import task
from celery.utils.log import get_task_logger
from oais_platform.oais.models import Archive, Step, Status, Steps
from django.utils import timezone
from amclient import AMClient

from oais_utils.validate import validate_sip

import json, os, uuid, shutil, ntpath

logger = get_task_logger(__name__)


def process_after_return(self, status, retval, task_id, args, kwargs, einfo):
    # id is the first parameter passed to the task
    id = args[0]
    archive = Archive.objects.get(pk=id)

    id = args[1]
    step = Step.objects.get(pk=id)

    step.set_task(self.request.id)

    if status == states.SUCCESS:
        print(retval)
        if retval["status"] == 0:
            try:
                filename = retval["foldername"]
            except:
                step.set_status(Status.FAILED)
                logger.error(
                    f"Error while harvesting archive {id}: Update bagit-create version"
                )

            # Previous job
            step.set_status(Status.COMPLETED)
            step.set_finish_date()
            step.set_output_data(os.path.join(os.getcwd(), filename))

            # Next step
            next_step = Step.objects.create(
                archive=step.archive,
                name=Steps.VALIDATION,
                input_step=step,
                input_data=step.output_data,
                status=Status.WAITING_APPROVAL,
            )

            # New Celery task will start
            archive.set_step(step.id)
            validate.delay(next_step.archive.id, next_step.input_data, next_step.id)

        else:
            # bagit_create returned an error
            errormsg = retval["errormsg"]
            logger.error(f"Error while harvesting archive {id}: {errormsg}")
            step.set_status(Status.FAILED)
    else:
        step.set_status(Status.FAILED)


@task(name="process", bind=True, ignore_result=True, after_return=process_after_return)
def process(self, archive_id, step_id):
    logger.info(f"Starting harvest of archive {archive_id}")

    archive = Archive.objects.get(pk=archive_id)

    step = Step.objects.get(pk=step_id)
    step.set_status(Status.IN_PROGRESS)

    bagit_result = bic.process(
        recid=archive.recid,
        source=archive.source,
        loglevel=2,
    )

    return bagit_result


def validate_after_return(self, status, retval, task_id, args, kwargs, einfo):
    # id is the first parameter passed to the task
    archive_id = args[0]
    archive = Archive.objects.get(pk=archive_id)

    path_to_sip = args[1]
    # Could be failed registry_check/validation or successful validation
    step_id = args[2]
    step = Step.objects.get(pk=step_id)

    if status == states.SUCCESS:
        if retval:
            step.set_status(Status.COMPLETED)

            # Next step
            next_step = Step.objects.create(
                archive=step.archive,
                name=Steps.CHECKSUM,
                input_step=step,
                input_data=step.output_data,
                status=Status.WAITING_APPROVAL,
            )

            archive = step.archive
            archive.set_step(next_step.id)

            checksum.delay(next_step.id, path_to_sip)
        else:
            logger.error(f"Error while validating sip {id}")
            step.set_status(Status.FAILED)
    else:
        step.set_status(Status.FAILED)


@task(
    name="validate", bind=True, ignore_result=True, after_return=validate_after_return
)
def validate(self, archive_id, path_to_sip, step_id):
    logger.info(f"Starting SIP validation {path_to_sip}")

    current_step = Step.objects.get(pk=step_id)
    current_step.set_status(Status.IN_PROGRESS)

    # Set task id
    current_step.set_task(self.request.id)

    # Checking registry = checking if the folder exists
    sip_exists = os.path.exists(path_to_sip)

    if not sip_exists:
        return False

    # Runs validate_sip from oais_utils
    valid = validate_sip(path_to_sip)

    return valid


def checksum_after_return(self, status, retval, task_id, args, kwargs, einfo):

    path_to_sip = args[1]
    step_id = args[0]
    step = Step.objects.get(pk=step_id)

    if status == states.SUCCESS:
        if retval:
            step.set_status(Status.COMPLETED)

            # Next step
            next_step = Step.objects.create(
                archive=step.archive,
                name=Steps.ARCHIVE,
                input_step=step,
                input_data=step.output_data,
                status=Status.WAITING_APPROVAL,
            )

            archive = step.archive
            archive.set_step(next_step.id)

            archivematica.delay(next_step.id, path_to_sip)
        else:
            logger.error(f"Error while validating sip {id}")
            step.set_status(Status.FAILED)
    else:
        step.set_status(Status.FAILED)


@task(
    name="checksum", bind=True, ignore_result=True, after_return=checksum_after_return
)
def checksum(self, step_id, path_to_sip):
    logger.info(f"Starting checksum validation {path_to_sip}")

    current_step = Step.objects.get(pk=step_id)
    current_step.set_status(Status.IN_PROGRESS)

    # Set task id
    current_step.set_task(self.request.id)

    sip_exists = os.path.exists(path_to_sip)
    if not sip_exists:
        return False

    sip_json = os.path.join(path_to_sip, "data/meta/sip.json")

    with open(sip_json) as json_file:
        data = json.load(json_file)
        for file in data["contentFiles"]:
            try:
                checksum_list = []
                for checksum in file["checksum"]:
                    splited = checksum.split(":")
                    checksum = splited[0] + ":" + "0"
                    checksum_list.append(checksum)
            except:
                if (
                    file["origin"]["filename"] == "bagitcreate.log"
                    or file["origin"]["filename"] == "sip.json"
                ):
                    pass
                else:
                    return False

    tempfile = os.path.join(os.path.dirname(path_to_sip), str(uuid.uuid4()))
    with open(tempfile, "w") as f:
        json.dump(data, f, indent=4)

    # rename temporary file to sip2 json
    new_sip_json = os.path.join(path_to_sip, "data/meta/sip2.json")
    os.rename(tempfile, new_sip_json)

    logger.info(f"Checksum completed!")
    checksumed = True

    return checksumed


def archive_after_return(self, status, retval, task_id, args, kwargs, einfo):

    path_to_sip = args[1]
    step_id = args[0]
    step = Step.objects.get(pk=step_id)

    if status == states.SUCCESS:
        if retval:
            step.set_status(Status.COMPLETED)
        else:
            logger.error(f"Error while archiving {id}")
            step.set_status(Status.FAILED)
    else:
        step.set_status(Status.FAILED)


@task(
    name="archivematica",
    bind=True,
    ignore_result=True,
    after_return=archive_after_return,
)
def archivematica(self, step_id, path_to_sip):
    logger.info(f"Starting archiving {path_to_sip}")

    current_step = Step.objects.get(pk=step_id)
    current_step.set_status(Status.IN_PROGRESS)

    archive_id = current_step.archive

    # Set task id
    current_step.set_task(self.request.id)

    # This is the absolute directory of the archivematica-sampledata folder in the system
    # [NEEDS TO BE CHANGED]
    a3m_abs_directory = "/home/kchelakis/a3m/archivematica/hack/submodules/archivematica-sampledata/oais-data"
    # This is the directory Archivematica "sees" on the local system
    a3m_rel_directory = "/home/archivematica/archivematica-sampledata/oais-data"

    system_dst = os.path.join(
        a3m_abs_directory,
        ntpath.basename(path_to_sip),
    )

    archivematica_dst = os.path.join(
        a3m_rel_directory,
        ntpath.basename(path_to_sip),
    )

    shutil.copytree(path_to_sip, system_dst)

    am = AMClient()
    am.am_url = "http://127.0.0.1:62080"
    am.am_user_name = "test"
    am.am_api_key = "test"
    am.transfer_source = "0f409b5d-7925-4c8d-b476-1932ab51402c"
    am.transfer_directory = archivematica_dst
    am.transfer_name = ntpath.basename(path_to_sip) + "::Archive " + str(archive_id.id)
    am.processing_config = "automated"

    package = am.create_package()

    return True
