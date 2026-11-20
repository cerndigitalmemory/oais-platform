from rest_framework.test import APITestCase

from oais_platform.oais.enums import StepFailureType
from oais_platform.oais.models import Archive, Status, Step, StepName
from oais_platform.oais.tasks.warning_analysis import warning_analysis


class WarningAnalysisTests(APITestCase):
    def setUp(self):
        self.archive = Archive.objects.create(recid="1", source="test", source_url="")
        self.archive.add_step_to_pipeline(StepName.PUSH_TO_CTA)

    def create_analysis_step(self, filename):
        archive_step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.ARCHIVE,
            status=Status.COMPLETED_WITH_WARNINGS,
            failure_type=StepFailureType.FORMAT_NOT_IDENTIFIED,
        )
        step = Step.objects.create(
            archive=self.archive,
            step_name=StepName.WARNING_ANALYSIS,
            input_step=archive_step,
            input_data_json={
                "errormsg": [
                    {
                        "task": "Identify file format",
                        "microservice": "Identify file format",
                        "link": "http://am/tasks/1",
                        "filenames": [filename],
                    }
                ]
            },
        )
        self.archive.set_last_step(step.id)
        return step

    def test_warning_analysis_recognized_format_continues(self):
        step = self.create_analysis_step("data.ROOT")

        warning_analysis.apply(args=[self.archive.id, step.id])

        step.refresh_from_db()
        self.assertEqual(step.status, Status.COMPLETED)
        self.assertIsNone(step.output_data_json["errormsg"])
        self.assertEqual(len(step.output_data_json["non_blocking_warnings"]), 1)
        self.assertTrue(
            Step.objects.filter(
                archive=self.archive,
                step_type__name=StepName.PUSH_TO_CTA,
                input_step=step,
            ).exists()
        )

    def test_warning_analysis_unknown_format_fails(self):
        step = self.create_analysis_step("data.xyz")

        warning_analysis.apply(args=[self.archive.id, step.id])

        step.refresh_from_db()
        self.assertEqual(step.status, Status.FAILED)
        self.assertEqual(step.failure_type, StepFailureType.FORMAT_NOT_IDENTIFIED)
        self.assertEqual(len(step.output_data_json["errormsg"]), 1)
        self.assertFalse(
            Step.objects.filter(
                archive=self.archive, step_type__name=StepName.PUSH_TO_CTA
            ).exists()
        )
