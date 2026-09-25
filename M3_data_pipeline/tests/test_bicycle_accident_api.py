import json
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd


COLLECTOR_DIR = Path(__file__).resolve().parents[1] / "collector"
sys.path.insert(0, str(COLLECTOR_DIR))

import bicycle_accident_pipeline as bicycle  # noqa: E402


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

    def test_processed_removes_invalid_coordinates_and_converts_counts(self):
        base = {
            "afos_id": "2024060",
            "bjd_cd": "1168010100",
            "spot_cd": "1",
            "sido_sgg_nm": "서울특별시 강남구",
            "spot_nm": "테스트 지점",
            "occrrnc_cnt": "4",
            "caslt_cnt": "5",
            "dth_dnv_cnt": "0",
            "se_dnv_cnt": "1",
            "sl_dnv_cnt": "4",
            "wnd_dnv_cnt": "0",
            "geom_json": '{"type":"Polygon","coordinates":[[[127,37],[127.1,37],[127,37]]]}',
            "request_year": "2024",
            "request_sido": "11",
            "request_gugun": "680",
            "request_province_name": "서울특별시",
            "request_district_name": "강남구",
            "expected_afos_id": "2024060",
            "request_page_no": "1",
            "collected_at": "2026-09-22T00:00:00+09:00",
        }
        valid = {**base, "afos_fid": "A", "lo_crd": "127.0", "la_crd": "37.5"}
        invalid = {**base, "afos_fid": "B", "lo_crd": "0", "la_crd": "0"}

        result, quality = bicycle.prepare_processed(pd.DataFrame([valid, invalid]))

        self.assertEqual(result["afos_fid"].tolist(), ["A"])
        self.assertEqual(int(result.loc[result.index[0], "occrrnc_cnt"]), 4)
        self.assertEqual(quality["invalid_coordinate_count"], 1)
        self.assertTrue(bool(result.loc[result.index[0], "coordinate_valid"]))
        self.assertEqual(
            json.loads(result.loc[result.index[0], "geom_json"]),
            json.loads(base["geom_json"]),
        )


if __name__ == "__main__":
    unittest.main()
