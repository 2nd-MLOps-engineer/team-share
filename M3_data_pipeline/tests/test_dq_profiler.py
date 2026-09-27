import sys
import unittest
from pathlib import Path

import pandas as pd


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dataset_processors import normalize_nulls, process_dataset  # noqa: E402
from dq_profiler import (  # noqa: E402
    DQAuditStatus,
    DQ_LINEAGE_COLUMN,
    LineageError,
    MetricAvailability,
    add_stable_lineage,
    calculate_conversion_loss,
    calculate_dedup_metrics,
    calculate_needs_review_metrics,
    calculate_null_transitions,
    profile_processor_run,
    reconcile_row_counts,
    render_dq_audit_report,
    remove_stable_lineage,
)
from pipeline_elt import process_with_dq_profile  # noqa: E402


class StableLineageTest(unittest.TestCase):
    def test_assigns_unique_ids_without_using_or_changing_dataframe_index(self):
        raw = pd.DataFrame({"value": ["a", "b"]}, index=[99, 99])
        before = raw.copy(deep=True)

        result = add_stable_lineage(raw, audit_id="audit")

        pd.testing.assert_frame_equal(raw, before)
        self.assertEqual(result[DQ_LINEAGE_COLUMN].tolist(), ["audit:0", "audit:1"])
        self.assertEqual(result.index.tolist(), [99, 99])
        self.assertEqual(
            remove_stable_lineage(result).columns.tolist(),
            ["value"],
        )

    def test_refuses_to_overwrite_reserved_lineage_column(self):
        raw = pd.DataFrame({DQ_LINEAGE_COLUMN: ["business-value"]})

        with self.assertRaises(LineageError):
            add_stable_lineage(raw)


class NullStageMetricsTest(unittest.TestCase):
    def test_separates_source_null_and_nulls_from_normalization(self):
        raw = pd.DataFrame({"value": [None, " - ", "10"]})

        processed, result = profile_processor_run(
            "example",
            raw,
            normalize_nulls,
            normalize_nulls,
        )

        self.assertEqual(result.nulls.source_null, 1)
        self.assertEqual(result.nulls.nulls_from_normalization, 1)
        self.assertEqual(result.nulls.nulls_from_conversion, 0)
        self.assertEqual(result.nulls.final_null, 2)
        self.assertNotIn(DQ_LINEAGE_COLUMN, processed.columns)

    def test_conversion_loss_uses_lineage_after_sort_and_index_reset(self):
        before = add_stable_lineage(
            pd.DataFrame({"value": ["20", "10", "bad"]}),
            audit_id="air",
        )
        after = before.copy()
        after["value"] = pd.to_numeric(after["value"], errors="coerce")
        after = after.sort_values("value").reset_index(drop=True)

        metric = calculate_conversion_loss(before, after, columns=("value",))

        self.assertEqual(metric.total, 1)
        self.assertEqual(metric.by_column, {"value": 1})
        self.assertEqual(metric.affected_rows, 1)

    def test_reordering_alone_does_not_create_false_new_null(self):
        before = add_stable_lineage(
            pd.DataFrame({"value": [None, "20", "10"]}),
            audit_id="air",
        )
        after = before.sort_values("value", na_position="last").reset_index(drop=True)

        metric = calculate_null_transitions(before, after)

        self.assertEqual(metric.total, 0)


class RowReconciliationTest(unittest.TestCase):
    def test_splits_explained_and_unexplained_row_loss(self):
        raw = add_stable_lineage(
            pd.DataFrame({"value": [1, 2, 3, 4]}),
            audit_id="rows",
        )
        processed = raw.loc[raw["value"].isin([2, 4])].copy()

        metric = reconcile_row_counts(
            raw,
            processed,
            removal_reasons={"filtered": ["rows:0"]},
        )

        self.assertEqual(metric.raw_rows, 4)
        self.assertEqual(metric.processed_rows, 2)
        self.assertEqual(metric.removed_rows, 2)
        self.assertEqual(metric.explained_removed_rows, 1)
        self.assertEqual(metric.unexplained_row_loss, 1)
        self.assertEqual(metric.expected_final, metric.actual_final)
        self.assertTrue(metric.row_count_matches)

    def test_missing_lineage_makes_audit_invalid_not_dataset_failure(self):
        raw = add_stable_lineage(pd.DataFrame({"value": [1, 2]}), audit_id="rows")
        processed = raw.drop(columns=[DQ_LINEAGE_COLUMN])

        metric = reconcile_row_counts(raw, processed)

        self.assertFalse(metric.row_count_matches)
        self.assertIsNone(metric.unexplained_row_loss)
        self.assertEqual(
            metric.explanation_availability,
            MetricAvailability.NOT_AVAILABLE,
        )


class ExtensibleMetricStructuresTest(unittest.TestCase):
    def test_dedup_metrics_distinguish_identical_and_conflicting_payloads(self):
        frame = pd.DataFrame(
            {
                "key": ["A", "A", "B", "B", "B", None, None],
                "payload": ["same", "same", "x", "y", "y", "n", "n"],
            }
        )

        metric = calculate_dedup_metrics(frame, ("key",))

        self.assertEqual(metric.key_columns, ("key",))
        self.assertEqual(metric.duplicate_groups, 2)
        self.assertEqual(metric.duplicate_rows, 5)
        self.assertEqual(metric.removed_rows, 3)
        self.assertEqual(metric.identical_payload_groups, 1)
        self.assertEqual(metric.conflicting_payload_groups, 1)

    def test_needs_review_keeps_boolean_count_and_accepts_reason_ids(self):
        frame = pd.DataFrame({"needs_review": [True, False, True]})

        metric = calculate_needs_review_metrics(
            frame,
            review_reason_row_ids={
                "missing_measurement": ["row-1", "row-3"],
            },
        )

        self.assertEqual(metric.rows_needing_review, 2)
        self.assertEqual(metric.review_reasons, {"missing_measurement": 2})


class HumanAuditReportTest(unittest.TestCase):
    def test_column_profile_is_observation_and_does_not_change_status(self):
        raw = pd.DataFrame(
            {
                "number": [1.0, float("inf"), None],
                "text": ["", "abc", None],
            }
        )

        processed, result = profile_processor_run(
            "profile_observation",
            raw,
            lambda frame: frame.copy(),
            normalize_nulls,
        )

        self.assertEqual(result.status, DQAuditStatus.CHECK_PASSED)
        self.assertEqual(
            result.column_profiles["number"].raw.infinite_count,
            1,
        )
        self.assertEqual(result.column_profiles["text"].raw.empty_count, 1)
        self.assertNotIn(DQ_LINEAGE_COLUMN, result.column_profiles)
        self.assertNotIn(DQ_LINEAGE_COLUMN, processed.columns)

    def test_report_has_requested_sections_and_na_for_unconfigured_rules(self):
        _processed, result = profile_processor_run(
            "report",
            pd.DataFrame({"value": ["1", "2"]}),
            lambda frame: frame.copy(),
            normalize_nulls,
        )

        report = render_dq_audit_report(result)
        payload = result.as_dict()

        self.assertIn("1. Validation Summary", report)
        self.assertIn("2. Row Transformation", report)
        self.assertIn("3. Missing Value Profile", report)
        self.assertIn("4. Column Profile RAW <-> PROCESSED", report)
        self.assertIn("5. Duplicate / Needs Review / Issues", report)
        self.assertIn("N/A", report)
        self.assertEqual(payload["status"], "CHECK_PASSED")
        self.assertIn("row_tracking", payload)
        self.assertIn("column_profiles", payload)

    def test_missing_value_profile_includes_removed_row_nulls_by_column(self):
        def remove_null_row(frame):
            return frame.loc[frame["value"].notna()].copy()

        _processed, result = profile_processor_run(
            "removed_nulls",
            pd.DataFrame({"value": [None, "keep"]}),
            remove_null_row,
            normalize_nulls,
        )

        self.assertEqual(result.nulls.removed_row_null, 1)
        self.assertEqual(result.nulls.removed_row_null_by_column, {"value": 1})


class PipelineDQIntegrationTest(unittest.TestCase):
    def test_air_quality_sort_has_zero_false_conversion_loss_and_same_output(self):
        raw = pd.DataFrame(
            {
                "sidoName": ["서울", "서울"],
                "stationName": ["종로", "강남"],
                "dataTime": ["2026-09-22 11:00", "2026-09-22 10:00"],
                "pm10Value": ["20", "10"],
                "pm25Value": ["10", "5"],
                "o3Value": ["0.031", "0.021"],
                "khaiValue": ["55", "35"],
                "pm10Grade": ["1", "1"],
                "pm25Grade": ["1", "1"],
                "khaiGrade": ["2", "1"],
                "collected_at": [
                    "2026-09-22 11:05:00",
                    "2026-09-22 10:05:00",
                ],
            }
        )
        expected = process_dataset("air_quality", raw)

        actual, result = process_with_dq_profile("air_quality", raw)

        pd.testing.assert_frame_equal(actual, expected)
        self.assertEqual(result.status, DQAuditStatus.CHECK_PASSED)
        self.assertEqual(result.nulls.nulls_from_conversion, 0)
        self.assertNotIn(DQ_LINEAGE_COLUMN, actual.columns)

    def test_processor_column_selection_is_reported_as_audit_invalid(self):
        raw = pd.DataFrame({"value": ["keep"]})

        def selecting_processor(frame):
            return frame[["value"]].copy()

        processed, result = profile_processor_run(
            "selecting",
            raw,
            selecting_processor,
            normalize_nulls,
        )

        self.assertEqual(processed["value"].tolist(), ["keep"])
        self.assertEqual(result.status, DQAuditStatus.CHECK_FAILED)
        self.assertIn("processor output did not preserve _dq_row_id", result.issues)
        self.assertIsNone(result.nulls.removed_row_null)

    def test_weather_no_rain_text_is_not_recorded_as_conversion_loss(self):
        raw = pd.DataFrame(
            {
                "baseDate": ["20260926", "20260926"],
                "baseTime": ["1200", "1200"],
                "category": ["RN1", "T1H"],
                "fcstDate": ["20260926", "20260926"],
                "fcstTime": ["1300", "1300"],
                "fcstValue": ["강수없음", "21"],
                "nx": ["60", "60"],
                "ny": ["127", "127"],
                "data_type": ["forecast", "forecast"],
                "collected_at": [
                    "2026-09-26 12:05:00",
                    "2026-09-26 12:05:00",
                ],
            }
        )

        processed, result = process_with_dq_profile("weather_ultra_fcst", raw)

        rain = processed.loc[processed["category"] == "RN1", "fcstValue"].iloc[0]
        self.assertEqual(rain, "강수없음")
        self.assertEqual(result.nulls.conversion_by_column.get("fcstValue", 0), 0)

    def test_sensitive_culture_strings_are_unchanged_by_profile(self):
        cases = (
            (
                "culture_open_school_sports_facilities",
                pd.DataFrame(
                    {
                        "BASE_YEAR": ["2025", "2025"],
                        "ARBY_COT_CO_VALUE": ["570m/1개 코트", "852.2m/축구장 코트"],
                    }
                ),
                "ARBY_COT_CO_VALUE",
            ),
            (
                "culture_fitness_measurement_prescriptions",
                pd.DataFrame(
                    {
                        "MESURE_AGE_CO": ["30", "40"],
                        "MESURE_DE": ["20260926", "20260927"],
                        "MESURE_TME": ["2", "1599"],
                    }
                ),
                "MESURE_TME",
            ),
        )

        for table, raw, sensitive_column in cases:
            with self.subTest(table=table):
                expected = process_dataset(table, raw)
                actual, result = process_with_dq_profile(table, raw)

                pd.testing.assert_frame_equal(actual, expected)
                self.assertEqual(
                    result.nulls.conversion_by_column.get(sensitive_column, 0),
                    0,
                )

    def test_bicycle_profile_uses_real_raw_coordinate_columns(self):
        raw = pd.DataFrame(
            {
                "afos_fid": ["A"],
                "afos_id": ["2024060"],
                "bjd_cd": ["1111010100"],
                "spot_cd": ["1"],
                "sido_sgg_nm": ["서울 종로구"],
                "spot_nm": ["테스트 지점"],
                "request_year": ["2024"],
                "request_sido": ["11"],
                "request_gugun": ["110"],
                "request_province_name": ["서울특별시"],
                "request_district_name": ["종로구"],
                "expected_afos_id": ["2024060"],
                "request_page_no": ["1"],
                "lo_crd": ["127.0123"],
                "la_crd": ["37.4567"],
                "occrrnc_cnt": ["4"],
                "geom_json": [
                    '{"type":"Polygon","coordinates":'
                    '[[[127,37],[127.1,37],[127,37]]]}'
                ],
                "collected_at": ["2026-09-26T12:00:00+09:00"],
            }
        )
        expected = process_dataset("koroad_bicycle_accident_hotspots", raw)

        actual, result = process_with_dq_profile(
            "koroad_bicycle_accident_hotspots",
            raw,
        )

        pd.testing.assert_frame_equal(actual, expected)
        self.assertAlmostEqual(float(actual.loc[0, "longitude"]), 127.0123)
        self.assertAlmostEqual(float(actual.loc[0, "latitude"]), 37.4567)
        self.assertEqual(result.nulls.nulls_from_conversion, 0)


if __name__ == "__main__":
    unittest.main()
