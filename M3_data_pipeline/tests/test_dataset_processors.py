import sys
import unittest
from pathlib import Path

import pandas as pd


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dataset_processors import (  # noqa: E402
    process_aed,
    process_air_quality,
    process_dataset,
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

    def test_weather_forecast_preserves_no_rain_text_without_new_nulls(self):
        raw = pd.DataFrame(
            {
                "baseDate": ["20260926", "20260926", "20260926"],
                "baseTime": ["1200", "1200", "1200"],
                "category": ["RN1", "T1H", "POP"],
                "fcstDate": ["20260926", "20260926", "20260926"],
                "fcstTime": ["1300", "1300", "1300"],
                "fcstValue": ["강수없음", "21", "0"],
                "nx": ["60", "60", "60"],
                "ny": ["127", "127", "127"],
                "data_type": ["forecast", "forecast", "forecast"],
                "collected_at": [
                    "2026-09-26 12:05:00",
                    "2026-09-26 12:05:00",
                    "2026-09-26 12:05:00",
                ],
            }
        )

        result = process_dataset("weather_ultra_fcst", raw)

        self.assertEqual(len(result), len(raw))
        self.assertEqual(
            result.loc[result["category"] == "RN1", "fcstValue"].iloc[0],
            "강수없음",
        )
        self.assertEqual(
            int(result["fcstValue"].isna().sum()),
            int(raw["fcstValue"].isna().sum()),
        )
        self.assertEqual(
            float(result.loc[result["category"] == "T1H", "fcstValue"].iloc[0]),
            21.0,
        )
        self.assertEqual(
            float(result.loc[result["category"] == "POP", "fcstValue"].iloc[0]),
            0.0,
        )

    def test_bicycle_processor_renames_raw_coordinates_and_keeps_valid_row(self):
        polygon = (
            '{"type":"Polygon","coordinates":'
            '[[[127,37],[127.1,37],[127,37]]]}'
        )
        raw = pd.DataFrame(
            {
                "afos_fid": ["A"],
                "spot_nm": ["테스트 지점"],
                "lo_crd": ["127.0123"],
                "la_crd": ["37.4567"],
                "occrrnc_cnt": ["4"],
                "geom_json": [polygon],
                "collected_at": ["2026-09-26T12:00:00+09:00"],
            }
        )

        result = process_dataset(
            "koroad_bicycle_accident_hotspots",
            raw,
        )

        self.assertEqual(len(result), 1)
        self.assertNotIn("lo_crd", result.columns)
        self.assertNotIn("la_crd", result.columns)
        self.assertAlmostEqual(float(result.loc[0, "longitude"]), 127.0123)
        self.assertAlmostEqual(float(result.loc[0, "latitude"]), 37.4567)
        self.assertEqual(result.loc[0, "geom_json"], polygon)

    def test_culture_open_school_preserves_nearby_count_source_strings(self):
        source_values = [
            "570㎡ /1개 코트",
            "788.5㎡ /3개 코트",
            "957㎡ /4개 코트",
            "492.7㎡ /3개 코트",
            "852.2㎡ /농구개 코트",
            "200㎡ /10개 코트",
        ]
        raw = pd.DataFrame(
            {
                "BASE_YEAR": ["2025"] * len(source_values),
                "ARBY_COT_CO_VALUE": source_values,
            }
        )

        result = process_dataset(
            "culture_open_school_sports_facilities",
            raw,
        )

        self.assertEqual(
            result["ARBY_COT_CO_VALUE"].tolist(),
            source_values,
        )
        self.assertEqual(int(result["ARBY_COT_CO_VALUE"].isna().sum()), 0)

    def test_culture_fitness_preserves_measure_time_source_strings(self):
        source_values = ["2", "1", "4", "10", "23", "67", "1591", "1599"]
        raw = pd.DataFrame(
            {
                "MESURE_AGE_CO": ["30"] * len(source_values),
                "MESURE_DE": ["20260926"] * len(source_values),
                "MESURE_TME": source_values,
            }
        )

        result = process_dataset(
            "culture_fitness_measurement_prescriptions",
            raw,
        )

        self.assertEqual(result["MESURE_TME"].tolist(), source_values)
        self.assertEqual(int(result["MESURE_TME"].isna().sum()), 0)

    def test_weather_warning_removes_only_duplicate_keys(self):
        raw = pd.DataFrame(
            {
                "stnId": ["108", "108", "109"],
                "title": ["이전", "최신", "고유"],
                "tmFc": ["202609261200", "202609261200", "202609261300"],
                "tmSeq": ["1", "1", "2"],
                "collected_at": [
                    "2026-09-26 12:01:00",
                    "2026-09-26 12:02:00",
                    "2026-09-26 13:01:00",
                ],
            }
        )

        result = process_dataset("weather_warning", raw)
        expected_removed = len(raw) - len(
            raw.drop_duplicates(
                subset=["stnId", "tmFc", "tmSeq"],
                keep="last",
            )
        )
        actual_removed = len(raw) - len(result)

        self.assertEqual(expected_removed, 1)
        self.assertEqual(actual_removed, expected_removed)
        self.assertEqual(actual_removed - expected_removed, 0)
        self.assertEqual(set(result["title"]), {"최신", "고유"})


if __name__ == "__main__":
    unittest.main()
