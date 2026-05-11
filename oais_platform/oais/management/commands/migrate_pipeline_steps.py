from django.core.management.base import BaseCommand
from django.db import transaction

from oais_platform.oais.models import Archive, Step


class Command(BaseCommand):
    help = "A command-line tool to migrate Archive pipeline steps from [Step IDs, ...] to [(StepType ID, User ID, Batch ID), ...] format."

    def handle(self, *args, **options):
        archives = Archive.objects.filter(pipeline_steps__isnull=False).distinct()

        for archive in archives:
            if len(archive.pipeline_steps) == 0:
                continue
            self.stdout.write(f"Processing archive: {archive.id}")

            with transaction.atomic():
                new_pipeline_steps = []
                for step_id in archive.pipeline_steps:
                    if isinstance(step_id, list) or isinstance(step_id, tuple):
                        new_pipeline_steps = archive.pipeline_steps
                        break
                    try:
                        step = Step.objects.get(id=step_id)
                        user_id = (
                            step.initiated_by_user.id
                            if step.initiated_by_user
                            else None
                        )
                        batch_id = (
                            step.initiated_by_harvest_batch
                            if step.initiated_by_harvest_batch
                            else None
                        )
                        new_pipeline_steps.append(
                            (step.step_type.id, user_id, batch_id)
                        )
                        step.delete()
                    except Step.DoesNotExist:
                        self.stdout.write(
                            self.style.ERROR(
                                f"Step with ID {step_id} does not exist for archive {archive.id}. Skipping this step."
                            )
                        )
                        continue
                    except Exception as e:
                        self.stdout.write(
                            self.style.ERROR(
                                f"Error processing step with ID {step_id} for archive {archive.id}: {str(e)}. Skipping this step."
                            )
                        )
                        continue

                archive.pipeline_steps = new_pipeline_steps
                archive.save()
