import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


PIPELINE_DIR = Path(__file__).resolve().parents[1]
COLLECTOR_DIR = PIPELINE_DIR / "collector"
TEST_DEPENDENCIES = PIPELINE_DIR / ".test_deps"
sys.path.insert(0, str(TEST_DEPENDENCIES))
sys.path.insert(0, str(PIPELINE_DIR))
sys.path.insert(0, str(COLLECTOR_DIR))
os.environ.setdefault("FACILITY_API_KEY", "test-key")
os.environ.setdefault("AED_API_KEY", "test-key")

import AED_api as aed  # noqa: E402
import facility_api_v2 as facility  # noqa: E402


class FacilityCheckpointRecoveryTest(unittest.TestCase):
    def test_failed_page_is_only_page_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            checkpoint = Path(directory) / "facility.jsonl"

            def first_run(page_no):
                if page_no == 2:
                    return None, None
                return {"totalCount": 3}, [{"id": page_no}]

            with (
                patch.object(facility, "CHECKPOINT_PATH", checkpoint),
                patch.object(facility, "NUM_OF_ROWS", 1),
                patch.object(facility, "fetch_page", side_effect=first_run),
                patch.object(facility, "replace_raw_dataset_group") as replace_raw,
                patch.object(facility.time, "sleep"),
            ):
                with self.assertRaisesRegex(RuntimeError, "실패 페이지"):
                    facility.main()
                replace_raw.assert_not_called()

            called = []

            def resumed_run(page_no):
                called.append(page_no)
                return {"totalCount": 3}, [{"id": page_no}]

            with (
                patch.object(facility, "CHECKPOINT_PATH", checkpoint),
                patch.object(facility, "NUM_OF_ROWS", 1),
                patch.object(facility, "fetch_page", side_effect=resumed_run),
                patch.object(facility, "replace_raw_dataset_group") as replace_raw,
                patch.object(facility.time, "sleep"),
            ):
                facility.main()

            self.assertEqual(called, [2])
            frame = replace_raw.call_args.args[0]["facility"]
            self.assertEqual(frame["id"].tolist(), [1, 2, 3])
            self.assertFalse(checkpoint.exists())


class AedCheckpointRecoveryTest(unittest.TestCase):
    def test_failed_page_is_only_page_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            checkpoint = Path(directory) / "aed.jsonl"

            def first_run(page_no):
                if page_no == 2:
                    raise RuntimeError("temporary failure")
                return 3, [{"id": page_no}]

            with (
                patch.object(aed, "CHECKPOINT_PATH", checkpoint),
                patch.object(aed, "NUM_OF_ROWS", 1),
                patch.object(aed, "collect_page", side_effect=first_run),
                patch.object(aed, "replace_raw_dataset_group") as replace_raw,
                patch.object(aed.time, "sleep"),
            ):
                with self.assertRaisesRegex(RuntimeError, "실패 페이지"):
                    aed.main()
                replace_raw.assert_not_called()

            called = []

            def resumed_run(page_no):
                called.append(page_no)
                return 3, [{"id": page_no}]

            with (
                patch.object(aed, "CHECKPOINT_PATH", checkpoint),
                patch.object(aed, "NUM_OF_ROWS", 1),
                patch.object(aed, "collect_page", side_effect=resumed_run),
                patch.object(aed, "replace_raw_dataset_group") as replace_raw,
                patch.object(aed.time, "sleep"),
            ):
                aed.main()

            self.assertEqual(called, [2])
            frame = replace_raw.call_args.args[0]["aed"]
            self.assertEqual(frame["id"].tolist(), [1, 2, 3])
            self.assertFalse(checkpoint.exists())


if __name__ == "__main__":
    unittest.main()
