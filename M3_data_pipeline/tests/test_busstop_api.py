import os
import sys
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch


COLLECTOR_DIR = Path(__file__).resolve().parents[1] / "collector"
sys.path.insert(0, str(COLLECTOR_DIR))
os.environ.setdefault("BUSSTOP_API_KEY", "test-key")

import busstop_api as busstop  # noqa: E402


def station(city_code, node_id, node_name=None):
    return {
        "city_code": city_code,
        "city_name": f"city-{city_code}",
        "node_id": node_id,
        "node_name": node_name or f"node-{node_id}",
        "node_no": node_id,
        "latitude": "37.5",
        "longitude": "127.0",
    }


def city(city_code):
    return {
        "citycode": city_code,
        "cityname": f"city-{city_code}",
    }


class BusStopParallelCollectionTest(unittest.TestCase):
    @patch.object(busstop, "save_raw")
    @patch.object(busstop, "get_city_codes")
    def test_multiple_cities_actually_run_concurrently(self, get_cities, save_raw):
        get_cities.return_value = [city(str(number)) for number in range(4)]
        barrier = threading.Barrier(4)
        lock = threading.Lock()
        active = 0
        maximum_active = 0

        def collect(city_code, _city_name):
            nonlocal active, maximum_active
            with lock:
                active += 1
                maximum_active = max(maximum_active, active)
            barrier.wait(timeout=2)
            with lock:
                active -= 1
            return [station(city_code, "N1")]

        with (
            patch.object(busstop, "MAX_WORKERS", 4),
            patch.object(busstop, "get_city_stations", side_effect=collect),
        ):
            busstop.main()

        self.assertGreater(maximum_active, 1)
        self.assertEqual(save_raw.call_count, 1)

    @patch.object(busstop, "save_raw")
    @patch.object(busstop, "get_city_codes")
    def test_merges_every_city_and_sorts_before_save(self, get_cities, save_raw):
        get_cities.return_value = [city("01"), city("02")]
        second_city_finished = threading.Event()

        def collect(city_code, _city_name):
            if city_code == "02":
                second_city_finished.set()
                return [station("02", "B")]
            self.assertTrue(second_city_finished.wait(timeout=2))
            return [station("01", "Z"), station("01", "A")]

        with patch.object(busstop, "get_city_stations", side_effect=collect):
            busstop.main()

        saved = save_raw.call_args.args[0]
        self.assertEqual(
            [(row["city_code"], row["node_id"]) for row in saved],
            [("01", "A"), ("01", "Z"), ("02", "B")],
        )

    @patch.object(busstop, "save_raw")
    @patch.object(busstop, "get_city_codes")
    def test_city_failure_does_not_stop_other_tasks_and_prevents_save(
        self,
        get_cities,
        save_raw,
    ):
        get_cities.return_value = [city("01"), city("02"), city("03")]
        called = []
        lock = threading.Lock()

        def collect(city_code, _city_name):
            with lock:
                called.append(city_code)
            if city_code == "02":
                raise RuntimeError("city failed")
            return [station(city_code, "N1")]

        with patch.object(busstop, "get_city_stations", side_effect=collect):
            with self.assertRaisesRegex(RuntimeError, "기존 RAW DB snapshot을 유지"):
                busstop.main()

        self.assertEqual(set(called), {"01", "02", "03"})
        save_raw.assert_not_called()


class BusStopSessionTest(unittest.TestCase):
    def test_each_worker_thread_uses_its_own_reused_session(self):
        barrier = threading.Barrier(2)
        created_sessions = []

        def make_session():
            session = object()
            created_sessions.append(session)
            return session

        def get_twice():
            first = busstop.get_session()
            barrier.wait(timeout=2)
            second = busstop.get_session()
            return first, second

        with (
            patch.object(busstop, "thread_local", threading.local()),
            patch.object(busstop.requests, "Session", side_effect=make_session),
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            results = list(executor.map(lambda _: get_twice(), range(2)))

        self.assertIs(results[0][0], results[0][1])
        self.assertIs(results[1][0], results[1][1])
        self.assertIsNot(results[0][0], results[1][0])
        self.assertEqual(len(created_sessions), 2)


class BusStopPaginationTest(unittest.TestCase):
    @staticmethod
    def response(total_count, items):
        return {
            "response": {
                "body": {
                    "totalCount": total_count,
                    "items": {"item": items},
                }
            }
        }

    @patch.object(busstop, "request_json")
    def test_total_count_mismatch_still_fails_city(self, request_json):
        request_json.return_value = self.response(
            2,
            [{"nodeid": "N1"}],
        )

        with self.assertRaisesRegex(RuntimeError, "건수 불일치"):
            busstop.get_city_stations("01", "city-01")

    @patch.object(busstop.time, "sleep", return_value=None)
    @patch.object(busstop, "request_json")
    def test_pages_remain_sequential_within_one_city(self, request_json, _sleep):
        request_json.side_effect = [
            self.response(2, [{"nodeid": "N1"}]),
            self.response(2, [{"nodeid": "N2"}]),
        ]

        with patch.object(busstop, "NUM_OF_ROWS", 1):
            result = busstop.get_city_stations("01", "city-01")

        self.assertEqual([row["node_id"] for row in result], ["N1", "N2"])
        self.assertEqual(
            [item.args[1]["pageNo"] for item in request_json.call_args_list],
            ["1", "2"],
        )


if __name__ == "__main__":
    unittest.main()
