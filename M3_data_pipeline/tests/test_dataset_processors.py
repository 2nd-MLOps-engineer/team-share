import sys
import unittest
from pathlib import Path

import pandas as pd


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dataset_processors import (  # noqa: E402
    process_aed,
    process_air_quality,
    process_facility,
    process_public_open_facility,
)


class DatasetProcessorsTest(unittest.TestCase):
    def test_facility_keeps_only_open_unique_valid_korea_coordinates(self):
        raw = pd.DataFrame(
            {
                "faci_cd": ["A", "B", "A"],
                "faci_stat_nm": ["정상운영", "폐업", "정상운영"],
                "faci_lat": ["37.5", "37.4", "999"],
                "faci_lot": ["127.0", "127.1", "127.0"],
                "faci_gfa": ["12.5", "10", "11"],
                "base_ymd": ["00000101", "20210310", "20150400"],
                "reg_dt": ["2021-08-11", "2021-08-12", "2021-08-13"],
                "updt_dt": ["2025-06-21", "2025-06-22", "2025-06-23"],
            }
        )
        raw_before = raw.copy(deep=True)

        result = process_facility(raw)

        self.assertEqual(result["faci_cd"].tolist(), ["A"])
        self.assertEqual(float(result.loc[0, "faci_lat"]), 37.5)
        self.assertEqual(float(result.loc[0, "faci_gfa"]), 12.5)
        self.assertTrue(pd.isna(result.loc[0, "base_ymd"]))
        self.assertEqual(result.loc[0, "reg_dt"], pd.Timestamp("2021-08-11"))
        self.assertEqual(result.loc[0, "updt_dt"], pd.Timestamp("2025-06-21"))
        pd.testing.assert_frame_equal(raw, raw_before)

    def test_facility_invalid_dates_become_null_and_valid_dates_survive(self):
        raw = pd.DataFrame(
            {
                "faci_cd": ["YEAR_ZERO", "VALID", "ZERO_DAY"],
                "faci_stat_nm": ["정상운영", "정상운영", "정상운영"],
                "faci_lat": ["37.5", "37.5", "37.5"],
                "faci_lot": ["127.0", "127.0", "127.0"],
                "base_ymd": ["00000101", "20210310", "20150400"],
                "reg_dt": ["2021-08-11", "2021-08-11", "2021-08-11"],
                "updt_dt": ["2025-06-21", "2025-06-21", "2025-06-21"],
            }
        )

        result = process_facility(raw).set_index("faci_cd")

        self.assertTrue(pd.isna(result.loc["YEAR_ZERO", "base_ymd"]))
        self.assertEqual(result.loc["VALID", "base_ymd"], pd.Timestamp("2021-03-10"))
        self.assertTrue(pd.isna(result.loc["ZERO_DAY", "base_ymd"]))
        self.assertEqual(result.loc["VALID", "reg_dt"], pd.Timestamp("2021-08-11"))

    def test_aed_removes_missing_and_out_of_korea_coordinates(self):
        raw = pd.DataFrame(
            {
                "serialSeq": ["1", "2", "3"],
                "wgs84Lat": ["37.5", "", "51.5"],
                "wgs84Lon": ["127.0", "127.0", "0"],
            }
        )

        result = process_aed(raw)

        self.assertEqual(result["serialSeq"].tolist(), ["1"])

    def test_open_facility_preserves_address_logic_and_filters_coordinates(self):
        raw = pd.DataFrame(
            {
                "rdnmadr": ["강원도 춘천시 중앙로", "서울특별시 중구 세종대로"],
                "lnmadr": ["", ""],
                "latitude": ["37.8", "0"],
                "longitude": ["127.7", "0"],
            }
        )

        result = process_public_open_facility(raw)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.loc[0, "sido_standard"], "강원특별자치도")
        self.assertTrue(bool(result.loc[0, "coordinate_valid"]))

    def test_air_quality_converts_numeric_and_marks_missing_for_review(self):
        raw = pd.DataFrame(
            {
                "sidoName": ["서울"],
                "stationName": ["중구"],
                "dataTime": ["2026-09-22 10:00"],
                "pm10Value": ["20"],
                "pm25Value": ["-"],
                "o3Value": ["0.031"],
                "khaiValue": ["55"],
                "pm10Grade": ["1"],
                "pm25Grade": ["-"],
                "khaiGrade": ["2"],
                "collected_at": ["2026-09-22 10:05:00"],
            }
        )

        result = process_air_quality(raw)

        self.assertEqual(int(result.loc[0, "pm10Value"]), 20)
        self.assertAlmostEqual(float(result.loc[0, "o3Value"]), 0.031)
        self.assertTrue(bool(result.loc[0, "needs_review"]))


if __name__ == "__main__":
    unittest.main()
