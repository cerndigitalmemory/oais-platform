import json
import logging
import os
from datetime import timedelta

import bagit_create
from celery import shared_task
from celery.utils.log import get_task_logger
from django.contrib.auth.models import User
from django.db import transaction
from django.db.models import Sum
from django.utils import timezone
from django_celery_beat.models import IntervalSchedule, PeriodicTask

from oais_platform.oais.models import (
    ApiKey,
    Archive,
    Collection,
    Source,
    Status,
    Step,
    Steps,
)
from oais_platform.oais.sources.utils import get_source
from oais_platform.oais.tasks.pipeline_actions import execute_pipeline, finalize
from oais_platform.oais.tasks.utils import create_path_artifact, set_and_return_error
from oais_platform.settings import (
    AGGREGATED_FILE_SIZE_LIMIT,
    AUTOMATIC_HARVEST_BATCH_DELAY,
    AUTOMATIC_HARVEST_BATCH_SIZE,
    BIC_UPLOAD_PATH,
    BIC_WORKDIR,
    SIP_UPSTREAM_BASEPATH,
)

logger = get_task_logger(__name__)


@shared_task(
    name="harvest", bind=True, ignore_result=True, after_return=finalize, max_retries=5
)
def harvest(self, archive_id, step_id, input_data=None, api_key=None):
    """
    Run BagIt-Create to harvest data from upstream, preparing a
    Submission Package (SIP)
    """
    bic_version = bagit_create.version.get_version()
    logger.info(
        f"Starting harvest of Archive {archive_id} using BagIt Create {bic_version}"
    )

    archive = Archive.objects.get(pk=archive_id)
    step = Step.objects.get(pk=step_id)
    retry_interval_minutes = 2

    with transaction.atomic():
        size = archive.original_file_size
        if size:
            if size > AGGREGATED_FILE_SIZE_LIMIT:
                logger.warning(
                    f"Archive {archive.id} exceeds file size limit ({AGGREGATED_FILE_SIZE_LIMIT // (1024**3)}GB)."
                )
                return set_and_return_error(
                    step, "Record is too large to be harvested."
                )

            total_size = (
                Step.objects.select_for_update()
                .filter(name=Steps.HARVEST, status=Status.IN_PROGRESS)
                .aggregate(total_original_size=Sum("archive__original_file_size"))[
                    "total_original_size"
                ]
                or 0
            )

            if size + total_size > AGGREGATED_FILE_SIZE_LIMIT:
                logger.warning(
                    f"Archive {archive.id} exceeds aggregated file size limit "
                    f"({AGGREGATED_FILE_SIZE_LIMIT // (1024**3)}GB)."
                )
                if self.request.retries >= self.max_retries:
                    return {"status": 1, "errormsg": "Max retries exceeded."}
                with transaction.atomic():  # Separate transaction to update step before the exception
                    step.set_status(Status.WAITING)
                    step.set_output_data(
                        {
                            "message": f"Retrying in {retry_interval_minutes} minutes (aggregated file size limit exceeded)",
                        }
                    )
                    step.save()
                raise self.retry(
                    exc=Exception("Record is too large to be harvested at the moment"),
                    countdown=retry_interval_minutes * 60,
                )
        else:
            logger.warning(
                f"Archive {archive.id} does not have file size set, skipping size checks."
            )

        step.set_status(Status.IN_PROGRESS)

    if not api_key:
        logger.info(
            f"The given source({archive.source}) might requires an API key which was not provided."
        )

    try:
        bagit_result = bagit_create.main.process(
            recid=archive.recid,
            source=archive.source,
            loglevel=logging.WARNING,
            target=BIC_UPLOAD_PATH,
            token=api_key,
            workdir=BIC_WORKDIR,
        )
    except Exception as e:
        return {"status": 1, "errormsg": str(e)}

    logger.info(bagit_result)

    # If bagit returns an error return the error message
    if bagit_result["status"] == 1:
        error_msg = str(bagit_result["errormsg"])
        retry = False
        retry_codes = {
            "429": "Rate limit exceeded.",
            "408": "Request timeout.",
            "502": "Bad gateway",
            "503": "Service unavailable",
            "504": "Gateway timeout",
        }
        if any(key in error_msg for key in retry_codes):
            logger.error(
                next(retry_codes[key] for key in retry_codes if key in error_msg)
            )
            retry = True

        if retry:
            if self.request.retries >= self.max_retries:
                return {"status": 1, "errormsg": "Max retries exceeded."}
            step.set_status(Status.WAITING)
            step.set_output_data(
                {
                    "status": 0,
                    "errormsg": f"Retrying in {retry_interval_minutes} minutes (bagit-create error)",
                }
            )
            raise self.retry(
                exc=Exception(error_msg), countdown=retry_interval_minutes * 60
            )
        return {"status": 1, "errormsg": error_msg}

    sip_folder_name = bagit_result["foldername"]

    if BIC_UPLOAD_PATH:
        sip_folder_name = os.path.join(BIC_UPLOAD_PATH, sip_folder_name)

    archive.set_path(sip_folder_name)
    archive.update_sip_size()

    # Create a SIP path artifact
    output_artifact = create_path_artifact(
        "SIP", os.path.join(SIP_UPSTREAM_BASEPATH, sip_folder_name), sip_folder_name
    )

    bagit_result["artifact"] = output_artifact

    return bagit_result


@shared_task(name="periodic_harvest", bind=True, ignore_result=True)
def periodic_harvest(self, source_name, username, pipeline):
    logger.info(f"Starting the periodic harvest for {source_name}.")

    try:
        source = Source.objects.get(name=source_name)
    except Source.DoesNotExist:
        logger.error(f"Source with name {source_name} does not exist.")
        return

    try:
        user = User.objects.get(username=username)
    except User.DoesNotExist:
        logger.error(f"User with name {username} does not exist.")
        return

    api_key = None
    try:
        api_key = ApiKey.objects.get(source=source, user=user).key
    except ApiKey.DoesNotExist:
        logger.warning(
            f"User with name {username} does not have API key set for the given source, only public records will be available."
        )

    collection_name = f"{source_name} - automatic harvest"
    last_harvest = (
        Collection.objects.filter(title__contains=collection_name, internal=True)
        .order_by("-timestamp")
        .first()
    )
    last_harvest_time = None
    if not last_harvest:
        logger.info(f"First harvest for source {source_name}.")
    else:
        last_harvest_time = last_harvest.timestamp
        logger.info(
            f"Last harvest for source {source_name} was at {last_harvest_time}."
        )

    new_harvest = None
    schedule_time = None
    records_count = 0
    try:
        for records_to_harvest, new_harvest_time in get_source(
            source_name, api_key
        ).get_records_to_harvest(last_harvest_time):
            logger.info(
                f"Number of IDs to harvest for source {source_name}: {len(records_to_harvest)} until {new_harvest_time.strftime('%Y-%m-%dT%H:%M:%S')}."
            )
            if len(records_to_harvest) < 1:
                logger.info(f"There are no new records to harvest for {source_name}.")
                continue
            if not new_harvest:
                new_harvest = Collection.objects.create(
                    internal=True,
                    creator=user,
                    description=f"Starting automatic harvests for {source_name} is in progress.",
                )
                new_harvest.title = f"{collection_name} ({new_harvest.id})"
            new_harvest.timestamp = new_harvest_time
            new_harvest.save()

            batch_size = AUTOMATIC_HARVEST_BATCH_SIZE
            schedule, _ = IntervalSchedule.objects.get_or_create(
                every=10, period=IntervalSchedule.DAYS
            )
            for i in range(0, len(records_to_harvest), batch_size):
                batch = records_to_harvest[i : i + batch_size]
                batch_upper_limit = (
                    i + batch_size
                    if i + batch_size < len(records_to_harvest)
                    else len(records_to_harvest)
                )

                if not schedule_time:
                    schedule_time = timezone.now()
                else:
                    schedule_time = schedule_time + timedelta(
                        minutes=AUTOMATIC_HARVEST_BATCH_DELAY
                    )
                PeriodicTask.objects.create(
                    interval=schedule,  # need to set but one off is true
                    name=f"{new_harvest.title}, batch {i + 1 + records_count} to {batch_upper_limit + records_count}",
                    task="batch_harvest",
                    args=json.dumps(
                        [batch, user.id, source_name, pipeline, new_harvest.id, api_key]
                    ),
                    start_time=schedule_time,
                    enabled=True,
                    one_off=True,
                )

            records_count += len(records_to_harvest)
    except Exception as e:
        logger.error(f"Error while querying {source_name}: {str(e)}")
        return

    if records_count > 0:
        new_harvest.set_description(
            f"All automatic harvests were scheduled for source {source_name}."
        )
        logger.info(
            f"All ({records_count}) harvests were scheduled for source {source_name}."
        )
    else:
        logger.info("No records were harvested during this run.")


@shared_task(name="batch_harvest", bind=True, ignore_result=True)
def batch_harvest(
    self, records_to_harvest, user_id, source_name, pipeline, collection_id, api_key
):
    harvest_tag = Collection.objects.get(id=collection_id)
    for record in records_to_harvest:
        try:
            archive = Archive.objects.create(
                recid=record["recid"],
                title=record["title"],
                source=source_name,
                source_url=record["source_url"],
                requester_id=user_id,
                approver_id=user_id,
                original_file_size=record.get("file_size", 0),
            )
            harvest_tag.add_archive(archive.id)

            for step in pipeline:
                archive.add_step_to_pipeline(step)

            execute_pipeline(archive.id, api_key)
        except Exception as e:
            logger.error(
                f"Error while processing {record['recid']} from {source_name}: {str(e)}"
            )
    logger.info(f"A batch of automatic harvests has been started for {source_name}.")
