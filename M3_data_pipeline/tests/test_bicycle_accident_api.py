import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd


COLLECTOR_DIR = Path(__file__).resolve().parents[1] / "collector"
sys.path.insert(0, str(COLLECTOR_DIR))

import bicycle_accident_pipeline as bicycle  # noqa: E402


class BicycleCollectorTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
