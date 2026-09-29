"""Regression cases based on local audits: 6/672 rows lack dataTime."""
import copy
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dataset_processors import add_air_quality_observation_key
from test_timeseries_load import MemorySQL, SPECS, observation
import pipeline_elt as elt


def undated(collected="2026-09-29 12:00:00"):
    frame = observation("air_quality")
    frame["dataTime"] = None
    frame["collected_at"] = collected
    return frame


@pytest.mark.parametrize("marker", [None, "", "-", " NULL "])
def test_undated_capture_keeps_source_null_and_requires_real_collection_time(marker):
    frame = undated()
    frame["dataTime"] = marker
    before = frame.copy(deep=True)
    result = elt._prepare_timeseries_frame(frame, SPECS["air_quality"])
    assert result.iloc[0]["observation_time_key"] == "collected:2026-09-29T12:00:00"
    # Common key normalization changes station string dtypes; source payload
    # (particularly the unknown measurement time) must remain byte-for-value.
    payload = [c for c in frame.columns if c not in ("sidoName", "stationName")]
    pd.testing.assert_frame_equal(result[payload], before[payload])
    pd.testing.assert_frame_equal(result[frame.columns], before, check_dtype=False)
    pd.testing.assert_frame_equal(frame, before)


@pytest.mark.parametrize("schema", ["raw", "processed"])
def test_capture_replay_new_capture_and_real_measurement_are_distinct(schema):
    db = MemorySQL()
    dated = observation("air_quality", "1200")
    with patch.object(elt, "_write_staging_frame", side_effect=db.stage):
        for source in (undated(), undated(), undated("2026-09-29 13:00:00"), dated):
            frame = source
            if schema == "processed":
                frame, audit = elt.process_with_dq_profile("air_quality", source)
                assert audit.as_dict()["row_tracking"]["passed"]
            elt._upsert_timeseries_group(db, {"air_quality": frame}, {"air_quality": SPECS["air_quality"]}, schema=schema)
    saved = db.tables[f"{schema}.air_quality"]
    assert len(saved) == 3
    assert set(saved["observation_time_key"]) == {
        "collected:2026-09-29T12:00:00", "collected:2026-09-29T13:00:00",
        "measured:2026-09-29T12:00:00",
    }
    assert saved["dataTime"].isna().sum() == 2


def test_measurement_retry_ignores_collection_time_and_normalizes_timestamp_representation():
    first = observation("air_quality")
    second = first.copy()
    second["dataTime"] = pd.Timestamp("2026-09-29 09:00:00")
    second["collected_at"] = "2026-09-29 15:00:00"
    a = add_air_quality_observation_key(first)
    b = add_air_quality_observation_key(second)
    assert a["observation_time_key"].tolist() == b["observation_time_key"].tolist()


@pytest.mark.parametrize("measured,collected", [(None, None), (None, "-"), (None, "bad-time"), ("bad-time", "2026-09-29 12:00:00")])
def test_unidentifiable_or_malformed_time_aborts_before_any_db_access(measured, collected):
    frame = undated(collected)
    frame["dataTime"] = measured
    engine = Mock()
    with pytest.raises(ValueError, match="air_quality:"):
        elt.upsert_raw_dataset_group({"air_quality": frame}, engine)
    engine.begin.assert_not_called()


@pytest.mark.parametrize("schema", ["raw", "processed"])
def test_legacy_snapshot_backfills_identity_without_losing_null_rows(schema):
    db = MemorySQL()
    frame = pd.concat([observation("air_quality"), undated()], ignore_index=True)
    if schema == "processed":
        frame, _ = elt.process_with_dq_profile("air_quality", frame)
        frame = frame.drop(columns="observation_time_key")
    db.tables[f"{schema}.air_quality"] = frame.copy()
    with patch.object(elt, "_write_staging_frame", side_effect=db.stage):
        elt._upsert_timeseries_group(db, {"air_quality": frame}, {"air_quality": SPECS["air_quality"]}, schema=schema)
    saved = db.tables[f"{schema}.air_quality"]
    assert len(saved) == 2
    assert saved["dataTime"].isna().sum() == 1
    assert saved["observation_time_key"].nunique() == 2
    sql = "\n".join(db.statements)
    assert 'uq_air_quality_observation_v2' in sql
    assert 'ALTER COLUMN "dataTime" DROP NOT NULL' in sql
    assert 'ALTER COLUMN "observation_time_key" SET NOT NULL' in sql
    assert not any(x.startswith("DELETE") for x in db.statements)


def test_invalid_legacy_snapshot_rolls_back_schema_and_data():
    db = MemorySQL()
    db.tables["raw.air_quality"] = undated(None)
    before = copy.deepcopy(db.tables)
    with patch.object(elt, "_write_staging_frame", side_effect=db.stage):
        with pytest.raises(ValueError, match="missing dataTime and collected_at"):
            elt.upsert_raw_dataset_group({"air_quality": observation("air_quality")}, db)
    assert set(db.tables) == set(before)
    pd.testing.assert_frame_equal(db.tables["raw.air_quality"], before["raw.air_quality"])


def test_insert_failure_after_backfill_preserves_legacy_snapshot():
    db = MemorySQL()
    original = pd.concat([observation("air_quality"), undated()], ignore_index=True)
    db.tables["raw.air_quality"] = original.copy()
    db.fail_insert = "raw.air_quality"
    with patch.object(elt, "_write_staging_frame", side_effect=db.stage):
        with pytest.raises(RuntimeError, match="injected"):
            elt.upsert_raw_dataset_group({"air_quality": observation("air_quality", "1000")}, db)
    assert list(db.tables) == ["raw.air_quality"]
    pd.testing.assert_frame_equal(db.tables["raw.air_quality"], original)


def test_audit_preserves_undated_rows_and_explains_capture_replay_dedup():
    raw = pd.concat([observation("air_quality"), undated(), undated()], ignore_index=True)
    processed, audit = elt.process_with_dq_profile("air_quality", raw, dedup_keys=SPECS["air_quality"].upsert_key)
    report = audit.as_dict()
    assert len(processed) == 2
    assert processed["dataTime"].isna().sum() == 1
    assert processed.loc[processed["dataTime"].isna(), "needs_review"].all()
    assert report["rows"]["removal_reasons"] == {"air_quality_duplicate_observation": 1}
    assert report["rows"]["unexplained_row_loss"] == 0
    assert report["rows"]["row_count_matches"]
    assert report["row_tracking"]["passed"]
    assert report["nulls"]["nulls_from_conversion"] == 0
