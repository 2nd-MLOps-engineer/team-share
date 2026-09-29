import logging
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch


PIPELINE_DIR = Path(__file__).resolve().parents[1]
TEST_DEPENDENCIES = PIPELINE_DIR / ".test_deps"
if TEST_DEPENDENCIES.is_dir():
    sys.path.insert(0, str(TEST_DEPENDENCIES))
sys.path.insert(0, str(PIPELINE_DIR))

import pipeline_scheduler as scheduler  # noqa: E402
import webhook_notifier as notifier  # noqa: E402
from pipeline_metadata import DatasetSpec, SourceKind, TableSpec  # noqa: E402


class DQAuditLoggingFilterTest(unittest.TestCase):
    @staticmethod
    def record(message):
        return logging.LogRecord(
            name="pipeline_elt",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg=message,
            args=(),
            exc_info=None,
        )

    def test_dq_json_is_hidden_from_console_but_kept_for_dq_file(self):
        dq_record = self.record('dq_audit={"dataset":"facility"}')
        normal_record = self.record("job=facility status=SUCCESS")

        self.assertFalse(scheduler.ExcludeDQAuditFilter().filter(dq_record))
        self.assertTrue(scheduler.DQAuditFilter().filter(dq_record))
        self.assertTrue(scheduler.ExcludeDQAuditFilter().filter(normal_record))
        self.assertFalse(scheduler.DQAuditFilter().filter(normal_record))

    def test_console_exclusion_does_not_filter_file_handlers(self):
        root_logger = MagicMock()
        console_handler = MagicMock()
        pipeline_handler = MagicMock()
        error_handler = MagicMock()
        dq_handler = MagicMock()

        with (
            patch.object(
                scheduler.logging,
                "getLogger",
                return_value=root_logger,
            ),
            patch.object(
                scheduler.logging,
                "StreamHandler",
                return_value=console_handler,
            ),
            patch.object(
                scheduler,
                "RotatingFileHandler",
                side_effect=[pipeline_handler, error_handler, dq_handler],
            ),
        ):
            scheduler.configure_logging()

        console_filter = console_handler.addFilter.call_args.args[0]
        dq_filter = dq_handler.addFilter.call_args.args[0]
        self.assertIsInstance(console_filter, scheduler.ExcludeDQAuditFilter)
        self.assertIsInstance(dq_filter, scheduler.DQAuditFilter)
        pipeline_handler.addFilter.assert_not_called()
        error_handler.addFilter.assert_not_called()


class PipelineSchedulerRetryTest(unittest.TestCase):
    def setUp(self):
        self.output = TableSpec(
            table="facility",
            processor="facility",
            source_kind=SourceKind.RAW_DATABASE,
        )
        self.spec = DatasetSpec(
            name="facility",
            collector_script="collector/facility_api_v2.py",
            default_cron="0 4 1 * *",
            tables=(self.output,),
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
        engine = MagicMock()
        with (
            self.environment(),
            patch.dict(scheduler.JOB_SPECS, {"facility": self.spec}, clear=True),
            patch.object(scheduler, "_run_script") as run_script,
            patch.object(scheduler, "create_db_engine", return_value=engine),
            patch.object(
                scheduler,
                "_process_and_load_dataset",
                side_effect=ValueError("year 0 is out of range"),
            ) as load_database,
            patch.object(scheduler, "write_pipeline_run_history"),
            patch.object(scheduler, "send_pipeline_failure_alert"),
        ):
            with self.assertRaisesRegex(ValueError, "year 0"):
                scheduler.run_job("facility")

        run_script.assert_called_once_with(self.spec)
        load_database.assert_called_once_with(self.spec, engine)

    def test_transient_collection_failure_is_retried(self):
        engine = MagicMock()
        with (
            self.environment(),
            patch.dict(scheduler.JOB_SPECS, {"facility": self.spec}, clear=True),
            patch.object(
                scheduler,
                "_run_script",
                side_effect=[scheduler.TransientCollectionError("timeout"), None],
            ) as run_script,
            patch.object(scheduler, "create_db_engine", return_value=engine),
            patch.object(
                scheduler,
                "_process_and_load_dataset",
                return_value=({}, {}),
            ),
            patch.object(scheduler, "write_pipeline_run_history"),
        ):
            scheduler.run_job("facility")

        self.assertEqual(run_script.call_count, 2)

    def test_deterministic_collection_failure_is_not_retried(self):
        engine = MagicMock()
        with (
            self.environment(),
            patch.dict(scheduler.JOB_SPECS, {"facility": self.spec}, clear=True),
            patch.object(
                scheduler,
                "_run_script",
                side_effect=scheduler.CollectionStageError("invalid configuration"),
            ) as run_script,
            patch.object(scheduler, "_process_and_load_dataset") as load_database,
            patch.object(scheduler, "create_db_engine", return_value=engine),
            patch.object(scheduler, "write_pipeline_run_history"),
            patch.object(scheduler, "send_pipeline_failure_alert"),
        ):
            with self.assertRaises(scheduler.CollectionStageError):
                scheduler.run_job("facility")

        run_script.assert_called_once_with(self.spec)
        load_database.assert_not_called()

    def test_load_existing_uses_raw_without_collector(self):
        engine = MagicMock()
        with (
            patch.dict(scheduler.JOB_SPECS, {"facility": self.spec}, clear=True),
            patch.object(scheduler, "_run_script") as run_script,
            patch.object(scheduler, "create_db_engine", return_value=engine),
            patch.object(
                scheduler,
                "_process_and_load_dataset",
                return_value=({}, {}),
            ) as load_database,
            patch.object(scheduler, "write_pipeline_run_history"),
        ):
            scheduler.run_job("facility", load_existing=True)

        run_script.assert_not_called()
        load_database.assert_called_once_with(self.spec, engine)

    def test_raw_database_dataset_uses_same_generic_loader(self):
        table = TableSpec(
            table="durunubi_trails",
            processor="durunubi_trails",
            source_kind=SourceKind.RAW_DATABASE,
        )
        spec = DatasetSpec(
            name="raw_dataset",
            collector_script="collector/durunubi_api.py",
            default_cron="0 4 * * 1",
            tables=(table,),
        )

        engine = MagicMock()
        with (
            patch.dict(scheduler.JOB_SPECS, {"raw_dataset": spec}, clear=True),
            patch.object(scheduler, "_run_script") as run_script,
            patch.object(scheduler, "create_db_engine", return_value=engine),
            patch.object(
                scheduler,
                "_process_and_load_dataset",
                return_value=({}, {}),
            ) as load_database,
            patch.object(scheduler, "write_pipeline_run_history"),
        ):
            scheduler.run_job("raw_dataset")

        run_script.assert_called_once_with(spec)
        load_database.assert_called_once_with(spec, engine)

    def test_check_failed_records_warning_then_sends_dq_alert(self):
        engine = MagicMock()
        dq_result = MagicMock()
        dq_result.as_dict.return_value = {"status": "CHECK_FAILED"}
        events = []

        with (
            patch.dict(scheduler.JOB_SPECS, {"facility": self.spec}, clear=True),
            patch.object(scheduler, "_run_script"),
            patch.object(scheduler, "create_db_engine", return_value=engine),
            patch.object(
                scheduler,
                "_process_and_load_dataset",
                return_value=(
                    {"facility": (1, 0)},
                    {"facility": dq_result},
                ),
            ),
            patch.object(
                scheduler,
                "write_pipeline_run_history",
                side_effect=lambda *args, **kwargs: events.append("history"),
            ) as write_history,
            patch.object(
                notifier,
                "send_discord_message",
                side_effect=lambda *args, **kwargs: events.append("alert") or True,
            ) as send_discord,
            patch.object(scheduler, "send_pipeline_failure_alert") as failure_alert,
        ):
            scheduler.run_job("facility")

        self.assertEqual(events, ["history", "alert"])
        self.assertEqual(write_history.call_args.kwargs["run_status"], "WARNING")
        self.assertEqual(
            write_history.call_args.kwargs["dq_results"],
            {"facility": dq_result},
        )
        send_discord.assert_called_once()
        self.assertIn(
            "우심운까 데이터 품질 이상 감지",
            send_discord.call_args.args[0],
        )
        failure_alert.assert_not_called()


class RunAllOrderingTest(unittest.TestCase):
    @staticmethod
    def spec(name, run_order, *, run_last=False):
        table = TableSpec(
            table=name,
            processor=name,
            source_kind=SourceKind.RAW_DATABASE,
        )
        return DatasetSpec(
            name=name,
            collector_script=f"collector/{name}.py",
            default_cron="0 0 * * *",
            tables=(table,),
            run_order=run_order,
            run_last=run_last,
        )

    def test_run_last_is_a_final_phase_regardless_of_numeric_order(self):
        specs = {
            "late_regular": self.spec("late_regular", 9999),
            "final_dataset": self.spec("final_dataset", -1, run_last=True),
            "early_regular": self.spec("early_regular", 1),
        }

        with patch.dict(scheduler.JOB_SPECS, specs, clear=True):
            phases = scheduler._run_all_once_phases()

        self.assertEqual(
            phases,
            (("early_regular", "late_regular"), ("final_dataset",)),
        )

    def test_prior_phase_failure_skips_expensive_final_phase(self):
        specs = {
            "first": self.spec("first", 1),
            "second": self.spec("second", 2),
            "final_dataset": self.spec("final_dataset", 3, run_last=True),
        }
        called = []

        def run_job(job_id, *, skip_db=False):
            called.append((job_id, skip_db))
            if job_id == "first":
                raise RuntimeError("failed")

        with (
            patch.dict(scheduler.JOB_SPECS, specs, clear=True),
            patch.object(scheduler, "configure_logging"),
            patch.object(scheduler, "load_environment"),
            patch.object(scheduler, "run_job", side_effect=run_job),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "선행 실패로 미실행: final_dataset",
            ):
                scheduler.main(["--run-all-once", "--skip-db"])

        self.assertEqual(called, [("first", True), ("second", True)])


if __name__ == "__main__":
    unittest.main()
