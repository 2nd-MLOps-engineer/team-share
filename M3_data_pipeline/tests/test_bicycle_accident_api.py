import json
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch

COLLECTOR_DIR = Path(__file__).resolve().parents[1] / "collector"
sys.path.insert(0, str(COLLECTOR_DIR))

import bicycle_accident_api as bicycle  # noqa: E402


class BicycleCollectorTest(unittest.TestCase):
    @staticmethod
    def make_scope():
        return {
            "searchYearCd": "2024",
            "siDo": "11",
            "guGun": "680",
            "province_name": "Seoul",
            "district_name": "Gangnam",
            "expected_afos_id": "2024060",
        }

    @staticmethod
    def make_response(payload):
        response = Mock()
        response.status_code = 200
        response.json.return_value = payload
        response.text = json.dumps(payload)
        return response

    @classmethod
    def make_scopes(cls, count):
        return [
            {
                **cls.make_scope(),
                "guGun": f"{index:03d}",
                "district_name": f"district-{index}",
            }
            for index in range(count)
        ]

    @staticmethod
    def make_record(scope, afos_fid):
        item = {column: f"{column}-{afos_fid}" for column in bicycle.API_COLUMNS}
        item["afos_fid"] = afos_fid
        item["afos_id"] = scope["expected_afos_id"]
        return bicycle.attach_request_metadata(
            item,
            scope,
            1,
            "2026-09-27T00:00:00+09:00",
        )

    def test_code_list_builds_complete_official_scope(self):
        scopes, years = bicycle.load_request_scopes(
            COLLECTOR_DIR / "AccidentHazard_CodeList.xlsx"
        )
        self.assertEqual(len(scopes), 3510)
        self.assertEqual(sorted(years), list(range(2012, 2025)))

    @patch.object(bicycle.time, "sleep", return_value=None)
    @patch.object(bicycle, "request_page_with_retry")
    def test_collect_scope_fetches_every_page(self, request_page, _sleep):
        request_page.side_effect = [
            {
                "total_count": 101,
                "items": [{"afos_fid": str(i)} for i in range(100)],
            },
            {"total_count": 101, "items": [{"afos_fid": "100"}]},
        ]
        scope = {
            "searchYearCd": "2024",
            "siDo": "11",
            "guGun": "680",
            "province_name": "서울특별시",
            "district_name": "강남구",
            "expected_afos_id": "2024060",
        }

        rows = bicycle.collect_scope(object(), scope, "2026-09-22T00:00:00+09:00")

        self.assertEqual([call.args[2] for call in request_page.call_args_list], [1, 2])
        self.assertEqual(len(rows), 101)
        self.assertEqual(rows[0]["request_year"], "2024")
        self.assertEqual(rows[0]["request_district_name"], "강남구")

    @patch.object(bicycle, "request_page_with_retry")
    def test_collect_scope_accepts_successful_zero_result(self, request_page):
        request_page.return_value = {"total_count": 0, "items": []}

        rows = bicycle.collect_scope(
            object(),
            self.make_scope(),
            "2026-09-26T00:00:00+09:00",
        )

        self.assertEqual(rows, [])
        request_page.assert_called_once()

    def test_request_page_preserves_nonzero_result_and_occurrence_count(self):
        item = {
            "afos_fid": "A",
            "occrrnc_cnt": "4",
            "geom_json": (
                '{"type":"Polygon","coordinates":'
                '[[[127,37],[127.1,37],[127,37]]]}'
            ),
        }
        session = Mock()
        session.get.return_value = self.make_response(
            {
                "resultCode": "0000",
                "resultMsg": "Success",
                "totalCount": 1,
                "items": {"item": [item]},
            }
        )

        result = bicycle.request_page_once(session, self.make_scope(), 1)

        self.assertEqual(result["total_count"], 1)
        self.assertEqual(result["items"], [item])
        self.assertEqual(result["items"][0]["occrrnc_cnt"], "4")
        self.assertNotIn("accident_count", result["items"][0])

    def test_request_page_raises_for_error_result_code(self):
        session = Mock()
        session.get.return_value = self.make_response(
            {
                "resultCode": "30",
                "resultMsg": "service error",
                "totalCount": 0,
                "items": [],
            }
        )

        with self.assertRaises(bicycle.GlobalServiceError):
            bicycle.request_page_once(session, self.make_scope(), 1)

    def test_http_429_5xx_and_timeout_remain_transient(self):
        for status_code, error in (
            (429, None),
            (503, None),
            (None, bicycle.requests.Timeout("timeout")),
        ):
            with self.subTest(status_code=status_code, error=type(error).__name__):
                session = Mock()

                if error is not None:
                    session.get.side_effect = error
                else:
                    response = self.make_response({})
                    response.status_code = status_code
                    session.get.return_value = response

                with self.assertRaises(bicycle.TransientApiError):
                    bicycle.request_page_once(session, self.make_scope(), 1)

    @patch.object(bicycle.time, "sleep", return_value=None)
    @patch.object(bicycle, "request_page_once")
    def test_transient_failure_retries_with_existing_backoff(
        self,
        request_once,
        sleep,
    ):
        expected = {"total_count": 0, "items": []}
        request_once.side_effect = [
            bicycle.TransientApiError("temporary"),
            expected,
        ]

        result = bicycle.request_page_with_retry(
            object(),
            self.make_scope(),
            1,
        )

        self.assertEqual(result, expected)
        self.assertEqual(request_once.call_count, 2)
        sleep.assert_called_once_with(3)

    def test_api_result_codes_keep_no_data_unsupported_and_permanent_meaning(self):
        session = Mock()
        session.get.return_value = self.make_response(
            {"resultCode": "03", "resultMsg": "no data"}
        )
        no_data = bicycle.request_page_once(session, self.make_scope(), 1)
        self.assertEqual(no_data["total_count"], 0)
        self.assertEqual(no_data["items"], [])

        session.get.return_value = self.make_response(
            {"resultCode": "10", "resultMsg": "unsupported"}
        )
        with self.assertRaises(bicycle.UnsupportedScopeError):
            bicycle.request_page_once(session, self.make_scope(), 1)

        response = self.make_response({})
        response.status_code = 400
        session.get.return_value = response
        with self.assertRaises(bicycle.PermanentApiError):
            bicycle.request_page_once(session, self.make_scope(), 1)

    @patch.object(bicycle.time, "sleep", return_value=None)
    @patch.object(bicycle, "save_failure_log", return_value=Path("failures.csv"))
    @patch.object(bicycle, "append_checkpoint_entry")
    @patch.object(bicycle, "collect_scope")
    @patch.object(bicycle, "request_page_with_retry")
    @patch.object(bicycle, "load_checkpoint", return_value=({}, {}))
    def test_partial_scope_failure_is_not_accepted_as_complete(
        self,
        _load_checkpoint,
        request_page,
        collect_scope,
        _append_checkpoint,
        save_failure_log,
        _sleep,
    ):
        first_scope = self.make_scope()
        second_scope = {
            **self.make_scope(),
            "guGun": "740",
            "district_name": "Gangdong",
        }
        request_page.return_value = {"total_count": 1}
        collect_scope.side_effect = [
            [{"afos_fid": "A"}],
            bicycle.PermanentApiError("scope failed"),
        ]

        with self.assertRaises(bicycle.PipelineError):
            bicycle.collect_all_scopes(
                [first_scope, second_scope],
                "test-run",
            )

        save_failure_log.assert_called_once()

    @patch.object(bicycle.time, "sleep", return_value=None)
    @patch.object(bicycle, "collect_scope")
    @patch.object(bicycle, "get_thread_session", return_value=object())
    def test_worker_applies_scope_pacing_even_for_one_page_scope(
        self,
        _get_session,
        collect_scope,
        sleep,
    ):
        collect_scope.return_value = []

        result = bicycle.collect_scope_worker(
            self.make_scope(),
            "2026-09-27T00:00:00+09:00",
        )

        self.assertEqual(result, [])
        sleep.assert_called_once_with(bicycle.REQUEST_DELAY_SECONDS)

    def test_thread_local_sessions_are_not_shared_between_workers(self):
        barrier = threading.Barrier(2)

        def get_twice():
            first = bicycle.get_thread_session()
            barrier.wait(timeout=3)
            second = bicycle.get_thread_session()
            return first, second

        with (
            patch.object(bicycle, "_thread_local", threading.local()),
            patch.object(
                bicycle.requests,
                "Session",
                side_effect=lambda: Mock(headers=Mock()),
            ) as session_factory,
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            results = list(executor.map(lambda _: get_twice(), range(2)))

        self.assertIs(results[0][0], results[0][1])
        self.assertIs(results[1][0], results[1][1])
        self.assertIsNot(results[0][0], results[1][0])
        self.assertEqual(session_factory.call_count, 2)

    @patch.object(bicycle.time, "sleep", return_value=None)
    @patch.object(bicycle, "request_page_with_retry")
    def test_total_count_mismatch_is_checked_after_all_pages(
        self,
        request_page,
        _sleep,
    ):
        request_page.side_effect = [
            {
                "total_count": 101,
                "items": [{"afos_fid": str(index)} for index in range(100)],
            },
            {"total_count": 101, "items": []},
        ]

        with self.assertRaisesRegex(
            bicycle.PermanentApiError,
            "수집=100",
        ):
            bicycle.collect_scope(
                object(),
                self.make_scope(),
                "2026-09-27T00:00:00+09:00",
            )

        self.assertEqual(
            [item.args[2] for item in request_page.call_args_list],
            [1, 2],
        )

    def test_bounded_in_flight_refills_one_scope_and_checkpoints_on_main_thread(self):
        scopes = self.make_scopes(4)
        release = {
            bicycle.scope_key(scope): threading.Event()
            for scope in scopes
        }
        condition = threading.Condition()
        started = []
        active = 0
        maximum_active = 0
        checkpoint_threads = []
        result_holder = {}

        def worker(scope, _collected_at):
            nonlocal active, maximum_active
            key = bicycle.scope_key(scope)
            with condition:
                started.append(key)
                active += 1
                maximum_active = max(maximum_active, active)
                condition.notify_all()
            self.assertTrue(release[key].wait(timeout=3))
            with condition:
                active -= 1
            return [self.make_record(scope, key)]

        def append_checkpoint(_entry):
            checkpoint_threads.append(threading.get_ident())

        def run_collection():
            result_holder["thread_id"] = threading.get_ident()
            try:
                result_holder["result"] = bicycle.collect_all_scopes(
                    scopes,
                    "bounded",
                )
            except BaseException as exc:
                result_holder["error"] = exc

        with (
            patch.object(bicycle, "MAX_WORKERS", 2),
            patch.object(bicycle, "MIN_YEAR", 2024),
            patch.object(bicycle, "MAX_YEAR", 2024),
            patch.object(bicycle, "load_checkpoint", return_value=({}, {})),
            patch.object(
                bicycle,
                "request_page_with_retry",
                return_value={"total_count": 1},
            ),
            patch.object(bicycle, "collect_scope_worker", side_effect=worker),
            patch.object(
                bicycle,
                "append_checkpoint_entry",
                side_effect=append_checkpoint,
            ),
        ):
            collection_thread = threading.Thread(target=run_collection)
            collection_thread.start()

            with condition:
                self.assertTrue(
                    condition.wait_for(lambda: len(started) == 2, timeout=3)
                )
            self.assertEqual(len(started), 2)

            release[bicycle.scope_key(scopes[0])].set()
            with condition:
                self.assertTrue(
                    condition.wait_for(lambda: len(started) == 3, timeout=3)
                )
            self.assertEqual(len(started), 3)

            for event in release.values():
                event.set()
            collection_thread.join(timeout=5)

        self.assertFalse(collection_thread.is_alive())
        self.assertNotIn("error", result_holder)
        self.assertLessEqual(maximum_active, 2)
        self.assertEqual(len(started), 4)
        self.assertEqual(
            checkpoint_threads,
            [result_holder["thread_id"]] * 4,
        )

    @patch.object(bicycle, "save_failure_log", return_value=Path("failures.csv"))
    @patch.object(bicycle, "append_checkpoint_entry")
    @patch.object(bicycle, "load_checkpoint", return_value=({}, {}))
    @patch.object(
        bicycle,
        "request_page_with_retry",
        return_value={"total_count": 1},
    )
    def test_global_service_error_stops_new_scope_submission(
        self,
        _preflight,
        _load_checkpoint,
        _append_checkpoint,
        _save_failure_log,
    ):
        scopes = self.make_scopes(5)
        barrier = threading.Barrier(2)
        global_finished = threading.Event()
        called = []
        lock = threading.Lock()

        def worker(scope, _collected_at):
            with lock:
                called.append(bicycle.scope_key(scope))
            barrier.wait(timeout=3)
            if scope is scopes[0]:
                global_finished.set()
                raise bicycle.GlobalServiceError("global")
            self.assertTrue(global_finished.wait(timeout=3))
            return [self.make_record(scope, bicycle.scope_key(scope))]

        with (
            patch.object(bicycle, "MAX_WORKERS", 2),
            patch.object(bicycle, "collect_scope_worker", side_effect=worker),
        ):
            with self.assertRaisesRegex(bicycle.PipelineError, "전역 오류"):
                bicycle.collect_all_scopes(scopes, "global")

        self.assertEqual(set(called), {bicycle.scope_key(item) for item in scopes[:2]})

    def test_resume_skips_completed_scope_and_raw_order_follows_scope_order(self):
        scopes = self.make_scopes(3)
        resumed_entry = bicycle.make_checkpoint_entry(
            scopes[0],
            "success",
            records=[self.make_record(scopes[0], "first")],
        )
        checkpoint = {bicycle.scope_key(scopes[0]): resumed_entry}
        second_finished = threading.Event()
        called = []

        def worker(scope, _collected_at):
            called.append(bicycle.scope_key(scope))
            if scope is scopes[1]:
                self.assertTrue(second_finished.wait(timeout=3))
                marker = "second"
            else:
                second_finished.set()
                marker = "third"
            return [self.make_record(scope, marker)]

        with (
            patch.object(bicycle, "MAX_WORKERS", 2),
            patch.object(bicycle, "MIN_YEAR", 2024),
            patch.object(bicycle, "MAX_YEAR", 2024),
            patch.object(
                bicycle,
                "load_checkpoint",
                return_value=({}, checkpoint.copy()),
            ),
            patch.object(
                bicycle,
                "request_page_with_retry",
                return_value={"total_count": 1},
            ),
            patch.object(bicycle, "collect_scope_worker", side_effect=worker),
            patch.object(bicycle, "append_checkpoint_entry"),
        ):
            raw_df = bicycle.collect_all_scopes(scopes, "resume")

        self.assertNotIn(bicycle.scope_key(scopes[0]), called)
        self.assertEqual(raw_df["afos_fid"].tolist(), ["first", "second", "third"])

    def test_success_no_data_and_unsupported_checkpoint_meaning_is_preserved(self):
        scopes = self.make_scopes(3)
        saved_entries = []

        def worker(scope, _collected_at):
            if scope is scopes[0]:
                return [self.make_record(scope, "success")]
            if scope is scopes[1]:
                return []
            raise bicycle.UnsupportedScopeError("unsupported")

        with (
            patch.object(bicycle, "MIN_YEAR", 2024),
            patch.object(bicycle, "MAX_YEAR", 2024),
            patch.object(bicycle, "load_checkpoint", return_value=({}, {})),
            patch.object(
                bicycle,
                "request_page_with_retry",
                return_value={"total_count": 1},
            ),
            patch.object(bicycle, "collect_scope_worker", side_effect=worker),
            patch.object(
                bicycle,
                "append_checkpoint_entry",
                side_effect=saved_entries.append,
            ),
            patch.object(bicycle, "save_skipped_scope_log"),
        ):
            raw_df = bicycle.collect_all_scopes(scopes, "statuses")

        self.assertEqual(
            {entry["status"] for entry in saved_entries},
            {"success", "no_data", "unsupported"},
        )
        self.assertEqual(raw_df["afos_fid"].tolist(), ["success"])

    @patch.object(bicycle, "save_failure_log", return_value=Path("failures.csv"))
    @patch.object(bicycle, "load_checkpoint", return_value=({}, {}))
    @patch.object(
        bicycle,
        "request_page_with_retry",
        return_value={"total_count": 1},
    )
    def test_keyboard_interrupt_stops_new_scope_submission(
        self,
        _preflight,
        _load_checkpoint,
        _save_failure_log,
    ):
        scopes = self.make_scopes(5)
        barrier = threading.Barrier(2)
        called = []
        lock = threading.Lock()

        def worker(scope, _collected_at):
            with lock:
                called.append(bicycle.scope_key(scope))
            barrier.wait(timeout=3)
            if scope is scopes[0]:
                raise KeyboardInterrupt()
            return [self.make_record(scope, "other")]

        with (
            patch.object(bicycle, "MAX_WORKERS", 2),
            patch.object(bicycle, "load_checkpoint", return_value=({}, {})),
            patch.object(bicycle, "append_checkpoint_entry"),
            patch.object(bicycle, "collect_scope_worker", side_effect=worker),
        ):
            with self.assertRaises(KeyboardInterrupt):
                bicycle.collect_all_scopes(scopes, "interrupt")

        # 다른 worker가 interrupt보다 먼저 끝나면 한 slot이 먼저 refill될 수 있다.
        self.assertTrue(
            set(called).issubset(
                {bicycle.scope_key(item) for item in scopes[:3]}
            )
        )
        self.assertLessEqual(len(called), 3)

    def test_collection_failure_prevents_raw_db_replacement(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            env_path = Path(temp_dir) / ".env"
            env_path.write_text("test", encoding="utf-8")

            with (
                patch.object(bicycle, "ENV_PATH", env_path),
                patch.object(bicycle, "API_KEY", "test-key"),
                patch.object(bicycle, "RAW_DIR", Path(temp_dir)),
                patch.object(
                    bicycle,
                    "load_request_scopes",
                    return_value=(self.make_scopes(1), {2024: "2024060"}),
                ),
                patch.object(
                    bicycle,
                    "collect_all_scopes",
                    side_effect=bicycle.PipelineError("scope failed"),
                ),
                patch.object(bicycle, "replace_raw_snapshot") as replace_raw,
                patch.object(bicycle, "remove_checkpoint_after_success") as cleanup,
            ):
                with self.assertRaisesRegex(bicycle.PipelineError, "scope failed"):
                    bicycle.main()

        replace_raw.assert_not_called()
        cleanup.assert_not_called()


if __name__ == "__main__":
    unittest.main()
