from datetime import datetime, timedelta, timezone

from celery import chord, shared_task
from celery.utils.log import get_task_logger
from django.db.models import Q

from oais_platform.oais.models import (
    ApiKey,
    Archive,
    ArchiveState,
    BatchStatus,
    Collection,
    HarvestBatch,
    HarvestRun,
    Profile,
    ScheduledHarvest,
)
from oais_platform.oais.sources.utils import get_source
from oais_platform.oais.tasks.pipeline_actions import execute_pipeline

logger = get_task_logger(__name__)


@shared_task(name="scheduled_harvest", bind=True, ignore_result=True)
def scheduled_harvest(self, scheduled_harvest_id):
    """
    This task can be triggered periodically (by a PeriodicTask) to start an automatic harvest.
    ScheduledHarvest object has to be created in the admin interface with all the parameters.
    The PeriodicTask needs the ID of the ScheduledHarvest object as argument to trigger it.
    """
    try:
        scheduled_harvest = ScheduledHarvest.objects.get(id=scheduled_harvest_id)
    except ScheduledHarvest.DoesNotExist:
        logger.error(f"ScheduledHarvest with id {scheduled_harvest_id} does not exist.")
        return

    if scheduled_harvest.enabled is False:
        logger.warning(f"ScheduledHarvest with id {scheduled_harvest_id} is disabled.")
        return

    source = scheduled_harvest.source
    logger.info(
        f"Starting the periodic harvest(id: {scheduled_harvest.id}) for {source.name}."
    )

    try:
        user = Profile.objects.get(system=True).user
    except Profile.DoesNotExist:
        logger.error("System user does not exist - cannot execute scheduled harvest.")
        return

    api_key = None
    try:
        api_key = ApiKey.objects.get(source=source, user=user).key
    except ApiKey.DoesNotExist:
        logger.warning(
            f"System user({user.username}) does not have API key set for the given source, only public records will be available."
        )

    last_run = scheduled_harvest.harvest_runs.all().order_by("-created_at").first()
    if last_run is None:
        logger.info(f"First harvest for source {source.name}.")
        last_harvest_time = None
    else:
        logger.info(
            f"Last harvest run for source {source.name} was until {last_run.query_end_time}."
        )
        last_harvest_time = last_run.query_end_time

    end = datetime.now(timezone.utc) - timedelta(
        days=scheduled_harvest.condition_unmodified_for_days
    )

    harvest_run = HarvestRun.objects.create(
        source=source,
        scheduled_harvest=scheduled_harvest,
        pipeline=scheduled_harvest.pipeline,
        query_start_time=last_harvest_time,
        query_end_time=end,
        condition_unmodified_for_days=scheduled_harvest.condition_unmodified_for_days,
        batch_size=scheduled_harvest.batch_size,
        batch_delay_minutes=scheduled_harvest.batch_delay_minutes,
    )
    records_count = 0
    batch_number = 1
    try:
        for records_to_harvest, new_harvest_time in get_source(
            source.name, api_key
        ).get_records_to_harvest(start=last_harvest_time, end=end):
            logger.info(
                f"Number of IDs to harvest for source {source.name}: {len(records_to_harvest)} until {new_harvest_time.strftime('%Y-%m-%dT%H:%M:%S')}."
            )
            if len(records_to_harvest) < 1:
                logger.info(f"There are no new records to harvest for {source.name}.")
                continue

            if records_count == 0:
                harvest_collection = Collection.objects.create(
                    internal=True,
                    creator=user,
                    description=f"Starting automatic harvests for {source.name} is in progress.",
                )
                harvest_collection.set_title(
                    f"{source.name} - automatic harvest({harvest_collection.id})"
                )
                harvest_run.set_collection(harvest_collection)

            batch_size = scheduled_harvest.batch_size
            for i in range(0, len(records_to_harvest), batch_size):
                batch = records_to_harvest[i : i + batch_size]

                HarvestBatch.objects.create(
                    batch_number=batch_number,
                    status=BatchStatus.PENDING,
                    records=batch,
                    harvest_run=harvest_run,
                )
                batch_number += 1

            records_count += len(records_to_harvest)
    except Exception as e:
        logger.error(f"Error while querying {source.name}: {str(e)}")
        return

    if records_count > 0:
        harvest_collection.set_description(
            f"All batches have been created for source {source.name}."
        )
        logger.info(f"All batches have been created for source {source.name}.")
        first_batch = harvest_run.get_next_pending_batch()
        batch_harvest.delay(first_batch.id)
    else:
        logger.info("No records were harvested during this run.")


@shared_task(name="batch_harvest", bind=True, ignore_result=True)
def batch_harvest(self, batch_id):
    """
    The ScheduledHarvest task creates a HarvestRun object and splits the records to be harvested into batches.
    This function processes one batch at a time, creating Archive objects and triggering the pipeline for each.
    """
    try:
        user = Profile.objects.get(system=True).user
    except Profile.DoesNotExist:
        logger.error("System user does not exist - cannot execute batch harvest.")
        return

    api_key = None
    try:
        batch = HarvestBatch.objects.get(id=batch_id)
        api_key = ApiKey.objects.get(source=batch.harvest_run.source, user=user).key
    except HarvestBatch.DoesNotExist:
        logger.error(f"HarvestBatch with id {batch_id} does not exist.")
        return
    except ApiKey.DoesNotExist:
        logger.warning(
            f"System user({user.username}) does not have API key set for the given source."
        )

    if batch.harvest_run.batches.filter(
        status__in=[
            BatchStatus.FAILED,
            BatchStatus.BLOCKED,
        ]  # Failed in case all last steps failed, blocked by manual intervention
    ).exists():
        logger.error(
            f"Harvest run {batch.harvest_run.id} has a blocked/failed batch, further batches will not be processed."
        )
        return

    sigs = []
    batch.set_status(BatchStatus.IN_PROGRESS)
    for record in batch.records:
        try:
            archive = Archive.objects.create(
                recid=record["recid"],
                title=record["title"],
                source=batch.harvest_run.source.name,
                source_url=record["source_url"],
                requester=user,
                approver=user,
                original_file_size=record.get("file_size") or 0,
            )
            batch.harvest_run.collection.add_archive(archive.id)

            for step_name in batch.harvest_run.pipeline:
                archive.add_step_to_pipeline(step_name, harvest_batch=batch)

            step, sig = execute_pipeline(archive.id, api_key, return_signature=True)
            sigs.append(sig)
        except Exception as e:
            logger.error(
                f"Error while processing {record['recid']} from {batch.harvest_run.source.name}: {str(e)}"
            )
    chord(sigs)(finalize_batch.s(batch_id=batch_id))
    logger.info(
        f"Batch {batch_id} of harvest run({batch.harvest_run.id}) has been started for {batch.harvest_run.source.name}."
    )


@shared_task(name="finalize_batch", bind=True, ignore_result=True)
def finalize_batch(self, results, batch_id):
    try:
        batch = HarvestBatch.objects.get(id=batch_id)
    except HarvestBatch.DoesNotExist:
        logger.error(f"HarvestBatch with id {batch_id} does not exist.")
        return

    if batch.status in [BatchStatus.BLOCKED, BatchStatus.FAILED]:
        logger.error(
            f"Batch {batch_id} had a blocking error, further batches will not be processed."
        )
        return
    else:
        none_state_archive = batch.archives.filter(
            Q(state=ArchiveState.NONE)
        )  # Count archives that did not progress, first step should always create an SIP
        if none_state_archive.count() == batch.size:
            logger.error(
                f"Batch {batch_id} had all archives failed the SIP creation. Halting further batches."
            )
            batch.set_status(BatchStatus.FAILED)
            return
        if none_state_archive.count() > 0:
            logger.warning(
                f"Batch {batch_id}: {none_state_archive.count()} archives had no SIP: {list(none_state_archive.values_list('recid', flat=True))}."
            )
        if batch.size != batch.archives.count():
            recid_list = [
                record["recid"]
                for record in batch.records
                if record.get("recid") is not None
            ]
            archived_recid_list = batch.archives.all().values_list("recid", flat=True)
            missing_recid_list = set(recid_list) - set(archived_recid_list)
            logger.warning(
                f"Batch {batch_id} has {len(missing_recid_list)} missing archives: {missing_recid_list}"
            )

        next_batch = batch.harvest_run.get_next_pending_batch()
        if next_batch:
            logger.info(
                f"Scheduling the next batch {next_batch.id} for {batch.harvest_run.source.name} in {batch.harvest_run.batch_delay_minutes} minutes."
            )
            batch_harvest.apply_async(
                (next_batch.id,), countdown=batch.harvest_run.batch_delay_minutes * 60
            )
        else:
            logger.info(
                f"All batches of harvest run({batch.harvest_run.id}) have been completed for {batch.harvest_run.source.name}."
            )
