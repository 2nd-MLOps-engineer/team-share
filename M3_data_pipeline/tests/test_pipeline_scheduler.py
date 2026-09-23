import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


PIPELINE_DIR = Path(__file__).resolve().parents[1]
TEST_DEPENDENCIES = PIPELINE_DIR / ".test_deps"
if TEST_DEPENDENCIES.is_dir():
    sys.path.insert(0, str(TEST_DEPENDENCIES))
sys.path.insert(0, str(PIPELINE_DIR))

import pipeline_scheduler as scheduler  # noqa: E402


class PipelineSchedulerRetryTest(unittest.TestCase):
    def setUp(self):
        self.output = scheduler.OutputSpec("unused.csv", "facility")
        self.outputs = [(self.output, Path("facility.csv"))]
        self.spec = scheduler.JobSpec(
            "collector/facility_api_v2.py",
            "0 4 1 * *",
            (self.output,),
        )

    def environment(self):
        return patch.dict(
            os.environ,
            {
                "FACILITY_MAX_ATTEMPTS": "3",
                "PIPELINE_JOB_RETRY_SECONDS": "0",
            },
        )

    def test_database_failure_does_not_repeat_collection(self):
        with (
            self.environment(),
            patch.dict(scheduler.JOB_SPECS, {"facility": self.spec}, clear=True),
            patch.object(scheduler, "_load_resume_state", return_value=None),
            patch.object(scheduler, "_run_script") as run_script,
            patch.object(scheduler, "_resolve_outputs", return_value=self.outputs),
            patch.object(scheduler, "_save_resume_state") as save_state,
            patch.object(
                scheduler,
                "_load_outputs_to_database",
                side_effect=ValueError("year 0 is out of range"),
            ) as load_database,
            patch.object(scheduler, "_clear_resume_state") as clear_state,
        ):
            with self.assertRaisesRegex(ValueError, "year 0"):
                scheduler.run_job("facility")

        run_script.assert_called_once_with(self.spec)
        load_database.assert_called_once_with(self.outputs)
        save_state.assert_called_once_with("facility", self.outputs)
        clear_state.assert_not_called()

    def test_transient_collection_failure_is_retried(self):
        with (
            self.environment(),
            patch.dict(scheduler.JOB_SPECS, {"facility": self.spec}, clear=True),
            patch.object(scheduler, "_load_resume_state", return_value=None),
            patch.object(
                scheduler,
                "_run_script",
                side_effect=[scheduler.TransientCollectionError("timeout"), None],
            ) as run_script,
            patch.object(scheduler, "_resolve_outputs", return_value=self.outputs),
            patch.object(scheduler, "_save_resume_state"),
            patch.object(scheduler, "_load_outputs_to_database"),
            patch.object(scheduler, "_clear_resume_state"),
        ):
            scheduler.run_job("facility")

        self.assertEqual(run_script.call_count, 2)

    def test_deterministic_collection_failure_is_not_retried(self):
        with (
            self.environment(),
            patch.dict(scheduler.JOB_SPECS, {"facility": self.spec}, clear=True),
            patch.object(scheduler, "_load_resume_state", return_value=None),
            patch.object(
                scheduler,
                "_run_script",
                side_effect=scheduler.CollectionStageError("invalid configuration"),
            ) as run_script,
            patch.object(scheduler, "_load_outputs_to_database") as load_database,
        ):
            with self.assertRaises(scheduler.CollectionStageError):
                scheduler.run_job("facility")

        run_script.assert_called_once_with(self.spec)
        load_database.assert_not_called()

    def test_csv_ready_state_resumes_without_collector(self):
        with (
            patch.dict(scheduler.JOB_SPECS, {"facility": self.spec}, clear=True),
            patch.object(
                scheduler, "_load_resume_state", return_value=self.outputs
            ),
            patch.object(scheduler, "_run_script") as run_script,
            patch.object(scheduler, "_load_outputs_to_database") as load_database,
            patch.object(scheduler, "_clear_resume_state") as clear_state,
        ):
            scheduler.run_job("facility")

        run_script.assert_not_called()
        load_database.assert_called_once_with(self.outputs)
        clear_state.assert_called_once_with("facility")


if __name__ == "__main__":
    unittest.main()
