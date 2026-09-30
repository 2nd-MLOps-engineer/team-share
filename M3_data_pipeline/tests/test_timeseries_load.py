"""시계열 데이터의 누적 적재 동작을 검증합니다.

SQL은 테스트 환경에서 모의 실행하며 실제 데이터베이스에는 연결하지 않습니다.
"""

import copy
import re
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pipeline_elt as elt
from pipeline_metadata import LoadPolicy, get_dataset_specs


SPECS = {t.table: t for d in get_dataset_specs().values() for t in d.tables}
TABLES = ("weather_ultra_ncst", "weather_ultra_fcst", "air_quality")


def observation(table, hour="0900", value="10"):
    if table == "air_quality":
        return pd.DataFrame([dict(
            sidoName="서울", stationName="중구", dataTime=f"2026-09-29 {hour[:2]}:00",
            pm10Value=value, pm25Value="5", o3Value="0.01", khaiValue="20",
            collected_at="2026-09-29 12:00:00", data_type="air_quality",
        )])
    row = dict(baseDate="20260929", baseTime=hour, nx=60, ny=127,
               category="T1H", collected_at="2026-09-29 12:00:00")
    if table.endswith("ncst"):
        row.update(obsrValue=value, data_type="ncst")
    else:
        row.update(fcstDate="20260929", fcstTime="1200", fcstValue=value, data_type="fcst")
    return pd.DataFrame([row])


class MemorySQL:
    """Small transactional SQL recorder for the emitted PostgreSQL operations.

    This tests stored observations without SQLite/PostgreSQL writes. SQL syntax
    and dialect-specific clauses are asserted separately below.
    """
    def __init__(self):
        self.tables = {}
        self.statements = []
        self.fail_insert = None
        self.transactions = 0

    @contextmanager
    def begin(self):
        before = copy.deepcopy(self.tables)
        self.transactions += 1
        try:
            yield self
        except Exception:
            self.tables = before
            raise

    @contextmanager
    def connect(self):
        yield self

    def stage(self, connection, frame, schema, name):
        assert connection is self
        self.tables[f"{schema}.{name}"] = frame.copy()

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        names = re.findall(r'"(raw|processed)"\."([a-z0-9_]+)"', sql)
        names = [f"{schema}.{name}" for schema, name in names]
        result = Mock()
        if "FROM pg_attribute" in sql:
            columns = self.tables[params["name"]].columns
            result.all.return_value = [(column, "TEXT") for column in columns]
            result.scalars.return_value.all.return_value = list(columns)
        elif sql.startswith('SELECT ctid::text'):
            frame = self.tables[names[0]]
            rows = []
            for index, row in frame.iterrows():
                if pd.isna(row["observation_time_key"]):
                    rows.append(dict(row, _migration_row_id=str(index)))
            result.mappings.return_value.all.return_value = rows
        elif sql.startswith("UPDATE"):
            for update in params:
                self.tables[names[0]].at[int(update["row_id"]), "observation_time_key"] = update["key"]
        elif "SET NOT NULL" in sql:
            column = re.search(r'ALTER COLUMN "([^"]+)"', sql)[1]
            assert not self.tables[names[0]][column].isna().any()
        elif sql.startswith("CREATE TABLE"):
            if names[0] not in self.tables:
                self.tables[names[0]] = self.tables[names[1]].iloc[:0].copy()
        elif "ADD COLUMN IF NOT EXISTS" in sql:
            column = re.search(r'ADD COLUMN IF NOT EXISTS "([^"]+)"', sql)[1]
            if column not in self.tables[names[0]]:
                self.tables[names[0]][column] = None
        elif sql.startswith("CREATE UNIQUE INDEX"):
            keys = re.findall(r'"([^"]+)"', sql.split("(", 1)[1])
            assert not self.tables[names[0]].duplicated(keys).any(), "legacy duplicate keys"
        elif sql.startswith("INSERT INTO"):
            target, stage = names
            if target == self.fail_insert:
                raise RuntimeError("injected insert failure")
            keys = re.findall(r'"([^"]+)"', sql.split("ON CONFLICT (", 1)[1].split(")", 1)[0])
            old, new = self.tables[target], self.tables[stage].copy()
            for column in re.findall(r'"([^"]+)" = NULL', sql):
                new[column] = None
            self.tables[target] = pd.concat([old, new], ignore_index=True).drop_duplicates(keys, keep="last")
        elif sql.startswith("DROP TABLE"):
            del self.tables[names[0]]
        return result


@pytest.mark.parametrize("table", TABLES)
@pytest.mark.parametrize("schema", ["raw", "processed"])
def test_first_load_new_time_retry_and_correction(table, schema):
    db = MemorySQL()
    key = f"{schema}.{table}"

    def load(frame):
        if schema == "processed":
            frame, audit = elt.process_with_dq_profile(table, frame, dedup_keys=SPECS[table].upsert_key)
            assert audit.row_tracking.passed
        elt._upsert_timeseries_group(db, {table: frame}, {table: SPECS[table]}, schema=schema)

    with patch.object(elt, "_write_staging_frame", side_effect=db.stage):
        load(observation(table))
        assert len(db.tables[key]) == 1
        load(observation(table, "1000"))
        assert len(db.tables[key]) == 2
        retry = observation(table, "1000")
        retry["collected_at"] = "2026-09-29 13:00:00"
        load(pd.concat([retry, retry], ignore_index=True))
        assert len(db.tables[key]) == 2
        load(observation(table, "1000", "99"))
        assert len(db.tables[key]) == 2
        column = "pm10Value" if table == "air_quality" else "obsrValue" if table.endswith("ncst") else "fcstValue"
        assert float(db.tables[key].iloc[-1][column]) == 99
        assert float(db.tables[key].iloc[0][column]) == 10

    sql = "\n".join(db.statements)
    assert "ON CONFLICT" in sql and "DO UPDATE SET" in sql
    assert "CREATE UNIQUE INDEX IF NOT EXISTS" in sql
    assert "pg_advisory_xact_lock" in sql
    assert "SET NOT NULL" in sql
    assert f'DROP TABLE "{schema}"."{table}"' not in sql
    assert "RENAME TO" not in sql
    assert not any("stg_" in name for name in db.tables)


def test_forecasts_keep_distinct_issue_and_valid_times_and_grids():
    table = "weather_ultra_fcst"
    db = MemorySQL()
    frames = [observation(table), observation(table, "1000"), observation(table), observation(table)]
    frames[2]["fcstTime"] = "1300"
    frames[3]["nx"] = 61
    with patch.object(elt, "_write_staging_frame", side_effect=db.stage):
        elt.upsert_raw_dataset_group({table: pd.concat(frames)}, db)
    assert len(db.tables[f"raw.{table}"]) == 4


def test_existing_snapshot_is_extended_and_group_failure_rolls_back():
    db = MemorySQL()
    frames = {table: observation(table) for table in TABLES[:2]}
    db.tables = {f"raw.{t}": f.copy() for t, f in frames.items()}
    with patch.object(elt, "_write_staging_frame", side_effect=db.stage):
        elt.upsert_raw_dataset_group({t: observation(t, "1000") for t in frames}, db)
        assert all(len(db.tables[f"raw.{t}"]) == 2 for t in frames)
        before = copy.deepcopy(db.tables)
        db.fail_insert = "raw.weather_ultra_fcst"
        with pytest.raises(RuntimeError, match="injected"):
            elt.upsert_raw_dataset_group({t: observation(t, "1100") for t in frames}, db)
    assert set(before) == set(db.tables)
    for table in before:
        pd.testing.assert_frame_equal(before[table], db.tables[table])


@pytest.mark.parametrize("bad", [None, "", "-", " NULL "])
def test_null_keys_fail_before_database_access(bad):
    frame = observation("air_quality")
    frame["stationName"] = bad
    engine = Mock()
    with pytest.raises(ValueError, match="NULL observation key"):
        elt.upsert_raw_dataset_group({"air_quality": frame}, engine)
    engine.begin.assert_not_called()


def test_missing_key_and_snapshot_upsert_are_rejected():
    engine = Mock()
    with pytest.raises(ValueError, match="missing observation keys"):
        elt.upsert_raw_dataset_group({"air_quality": observation("air_quality").drop(columns="stationName")}, engine)
    with pytest.raises(ValueError, match="not a timeseries"):
        elt.upsert_raw_dataset_group({"facility": pd.DataFrame({"id": [1]})}, engine)
    engine.begin.assert_not_called()


@pytest.mark.parametrize("dataset", ["weather", "air_quality"])
def test_full_elt_routes_to_upsert_and_preserves_dq(dataset):
    db = MemorySQL()
    spec = get_dataset_specs()[dataset]
    def read_raw(sql, connection):
        table = re.search(r'FROM "raw"\."([^"]+)"', str(sql))[1]
        return db.tables[f"raw.{table}"].copy()

    with patch.object(elt, "_write_staging_frame", side_effect=db.stage), patch.object(elt, "_atomic_replace_processed_group") as snapshot:
        for hour in ("0900", "1000", "1000"):
            elt.upsert_raw_dataset_group({t.table: observation(t.table, hour) for t in spec.tables}, db)
            with patch.object(elt.pd, "read_sql_query", side_effect=read_raw):
                results, audits = elt.load_dataset_from_raw(spec, db)
            for table in spec.tables:
                size = 1 if hour == "0900" else 2
                assert len(db.tables[f"processed.{table.table}"]) == size
                assert results[table.table][0] == size
                report = audits[table.table].as_dict()
                assert report["row_tracking"]["passed"]
                assert report["rows"]["row_count_matches"]
                assert report["rows"]["unexplained_row_loss"] == 0
                assert report["nulls"]["nulls_from_conversion"] == 0
                assert "_dq_row_id" not in db.tables[f"processed.{table.table}"]
        snapshot.assert_not_called()


def test_new_optional_column_and_null_correction_do_not_retain_old_value():
    table = "air_quality"
    db = MemorySQL()
    first = observation(table)
    with patch.object(elt, "_write_staging_frame", side_effect=db.stage):
        for frame in (first, first.assign(optional="present"), first):
            elt._upsert_timeseries_group(db, {table: frame}, {table: SPECS[table]}, schema="processed")
    assert len(db.tables[f"processed.{table}"]) == 1
    assert pd.isna(db.tables[f"processed.{table}"].iloc[0]["optional"])


def test_null_transition_audit_and_dedup_reconciliation_survive_upsert():
    raw = pd.concat([
        observation("air_quality", "0900", "1"),
        observation("air_quality", "0900", "-"),
        observation("air_quality", "1000", "bad-number"),
    ], ignore_index=True)
    processed, audit = elt.process_with_dq_profile(
        "air_quality", raw, dedup_keys=SPECS["air_quality"].upsert_key,
    )
    report = audit.as_dict()
    assert report["rows"]["removed_rows"] == 1
    assert report["rows"]["unexplained_row_loss"] == 0
    assert report["rows"]["row_count_matches"]
    assert report["row_tracking"]["passed"]
    assert report["nulls"]["nulls_from_normalization"] == 1
    assert report["nulls"]["nulls_from_conversion"] == 1
    db = MemorySQL()
    with patch.object(elt, "_write_staging_frame", side_effect=db.stage):
        elt._upsert_timeseries_group(db, {"air_quality": processed}, {"air_quality": SPECS["air_quality"]}, schema="processed")
    assert len(db.tables["processed.air_quality"]) == 2
    assert db.tables["processed.air_quality"]["pm10Value"].isna().all()


def test_weather_dedup_uses_observation_identity_and_explains_removal():
    first = observation("weather_ultra_ncst")
    second = observation("weather_ultra_ncst", value="20")
    second["data_type"] = "different-label"
    processed, audit = elt.process_with_dq_profile(
        "weather_ultra_ncst", pd.concat([first, second], ignore_index=True),
        dedup_keys=SPECS["weather_ultra_ncst"].upsert_key,
    )
    assert len(processed) == 1
    assert processed.iloc[0]["obsrValue"] == 20
    report = audit.as_dict()
    assert report["rows"]["removal_reasons"] == {"weather_duplicate_observation": 1}
    assert report["rows"]["unexplained_row_loss"] == 0


def test_rain_text_survives_numeric_first_batch():
    table = "weather_ultra_fcst"
    db = MemorySQL()
    with patch.object(elt, "_write_staging_frame", side_effect=db.stage):
        for value in ("10", "강수없음"):
            frame = observation(table, value=value)
            frame["category"] = "RN1"
            processed, _ = elt.process_with_dq_profile(table, frame)
            elt._upsert_timeseries_group(db, {table: processed}, {table: SPECS[table]}, schema="processed")
    assert len(db.tables[f"processed.{table}"]) == 1
    assert db.tables[f"processed.{table}"].iloc[0]["fcstValue"] == "강수없음"
    assert any('ALTER COLUMN "fcstValue" TYPE TEXT' in sql for sql in db.statements)


def test_legacy_duplicate_keys_fail_without_deleting_history():
    db = MemorySQL()
    frame = observation("air_quality")
    original = pd.concat([frame, frame], ignore_index=True)
    db.tables["raw.air_quality"] = original.copy()
    with patch.object(elt, "_write_staging_frame", side_effect=db.stage):
        with pytest.raises(AssertionError, match="legacy duplicate keys"):
            elt.upsert_raw_dataset_group({"air_quality": observation("air_quality", "1000")}, db)
    pd.testing.assert_frame_equal(db.tables["raw.air_quality"], original)
    assert list(db.tables) == ["raw.air_quality"]


@pytest.mark.parametrize("dataset", ["facility", "public_open_facility", "aed", "bicycle_accident", "durunubi", "culture_bigdata", "weather_warning"])
def test_snapshot_metadata_and_elt_routing_are_unchanged(dataset):
    spec = get_dataset_specs()[dataset]
    assert all(t.load_policy is LoadPolicy.SNAPSHOT and not t.upsert_key for t in spec.tables)
    frames = {t.table: pd.DataFrame({"id": [1]}) for t in spec.tables}
    with patch.object(elt.pd, "read_sql_query", return_value=pd.DataFrame({"id": [1]})), patch.object(elt, "_process_dataset_frames", return_value=(frames, {}, {})), patch.object(elt, "_atomic_replace_processed_group") as snapshot, patch.object(elt, "_upsert_timeseries_group") as upsert:
        elt.load_dataset_from_raw(spec, MemorySQL())
    snapshot.assert_called_once()
    upsert.assert_not_called()


def test_collectors_use_declared_raw_policy_without_importing_api_scripts():
    root = Path(__file__).resolve().parents[1]
    for name in ("weather", "air_quality"):
        source = (root / "collector" / f"{name}.py").read_text(encoding="utf-8")
        assert "upsert_raw_dataset_group(" in source
        assert "replace_raw_dataset_group" not in source
    warning = (root / "collector" / "weather_warning.py").read_text(encoding="utf-8")
    assert "replace_raw_dataset_group(" in warning
