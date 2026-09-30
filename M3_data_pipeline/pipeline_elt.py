"""
수집 데이터를 PostgreSQL RAW/PROCESSED 계층으로 관리하는 공통 ELT 파이프라인 모듈.

RAW에는 수집 원본을 보존하고,
dataset_processors.py의 데이터셋별 정제 함수를 통해
PROCESSED 데이터를 생성한다.

처리 경로:

1. RAW DB 기반 데이터
   - 이미 PostgreSQL raw 스키마에 적재된 데이터를 읽는다.
   - dataset_processors.py의 정제 함수를 실행하고 비차단 DQ 감사를 수행한다.
   - 선언된 데이터셋 관계 검증을 수행한다.
   - PROCESSED 데이터를 staging 테이블에 먼저 저장한다.
   - staging 테이블의 행 수를 검증한다.
   - 검증에 성공하면 metadata 정책에 따라 PROCESSED를 교체하거나 시계열 upsert한다.
   - 기존 RAW 테이블은 수정하지 않는다.

각 처리 과정에서 RAW/PROCESSED 건수, NULL 수,
데이터 읽기·정제·DB 적재·전체 파이프라인 처리시간을 로그로 남긴다.

중간에 오류가 발생하면 검증되지 않은 staging 데이터를
최종 RAW/PROCESSED 테이블로 교체하지 않는다.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from collections.abc import Iterator, Mapping

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

if __package__:
    from .dataset_processors import add_air_quality_observation_key, normalize_nulls, process_dataset
    from .dq_profiler import (
        DQProfileResult,
        profile_processor_run,
        render_dq_audit_report,
    )
    from .pipeline_metadata import DatasetSpec, LoadPolicy, SourceKind, TableSpec, get_dataset_specs
else:
    from dataset_processors import add_air_quality_observation_key, normalize_nulls, process_dataset
    from dq_profiler import (
        DQProfileResult,
        profile_processor_run,
        render_dq_audit_report,
    )
    from pipeline_metadata import DatasetSpec, LoadPolicy, SourceKind, TableSpec, get_dataset_specs


LOGGER = logging.getLogger(__name__)

PIPELINE_DIR = Path(__file__).resolve().parent
WORKSPACE_ROOT = PIPELINE_DIR.parent
ENV_PATH = PIPELINE_DIR / "collector" / ".env"


def validate_durunubi_relations(
    frames: dict[str, pd.DataFrame],
) -> None:
    """
    Durunubi trails ↔ segments 관계 무결성을 검증한다.
    """

    trails = frames["durunubi_trails"]
    segments = frames["durunubi_segments"]

    trail_keys = set(
        trails["routeIdx"]
        .dropna()
        .astype("string")
    )

    segment_route_keys = (
        segments["routeIdx"]
        .dropna()
        .astype("string")
    )

    orphan_mask = ~segment_route_keys.isin(trail_keys)
    orphan_count = int(orphan_mask.sum())

    if orphan_count:
        orphan_values = (
            segment_route_keys.loc[orphan_mask]
            .drop_duplicates()
            .head(10)
            .tolist()
        )

        raise RuntimeError(
            "Durunubi 관계 무결성 실패: "
            f"segments orphan routeIdx={orphan_count:,}건 / "
            f"예시={orphan_values}"
        )


DATASET_VALIDATORS = {
    "durunubi_relations": validate_durunubi_relations,
}


@contextmanager
def _timed_dataset_stage(
    spec: DatasetSpec,
    source: SourceKind,
    stage: str,
    durations: dict[str, float],
) -> Iterator[None]:
    """단계별 시작·성공·실패를 로그에 남기고 durations에 소요시간을 기록한다."""

    started = time.monotonic()
    LOGGER.info(
        "dataset=%s source=%s stage=%s status=START",
        spec.name,
        source.value,
        stage,
    )
    try:
        yield
    except Exception:
        elapsed = time.monotonic() - started
        durations[stage] = elapsed
        LOGGER.exception(
            "dataset=%s source=%s stage=%s status=FAILED "
            "elapsed_seconds=%.3f",
            spec.name,
            source.value,
            stage,
            elapsed,
        )
        raise
    else:
        elapsed = time.monotonic() - started
        durations[stage] = elapsed
        LOGGER.info(
            "dataset=%s source=%s stage=%s status=OK elapsed_seconds=%.3f",
            spec.name,
            source.value,
            stage,
            elapsed,
        )


def load_environment() -> None:
    """프로젝트의 단일 .env를 현재 환경보다 낮은 우선순위로 읽는다."""
    load_dotenv(ENV_PATH, override=False)


def create_db_engine() -> Engine:
    """환경변수로 PostgreSQL SQLAlchemy 엔진을 만든다."""
    load_environment()

    required = {
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_NAME": os.getenv("DB_NAME"),
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
    }

    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError(f"DB 환경변수 누락: {', '.join(missing)}")

    url = URL.create(
        drivername="postgresql+psycopg",
        username=required["DB_USER"],
        password=required["DB_PASSWORD"],
        host=required["DB_HOST"],
        port=int(os.getenv("DB_PORT", "5432")),
        database=required["DB_NAME"],
    )

    engine = create_engine(url, pool_pre_ping=True)

    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))

    return engine


def process_with_dq_profile(
    processor: str,
    raw_frame: pd.DataFrame,
    *,
    allow_empty: bool = False,
    dedup_keys: tuple[str, ...] = (),
) -> tuple[pd.DataFrame, DQProfileResult]:
    """기존 processor를 lineage 기반 비차단 DQ 감사와 함께 실행한다."""

    processed_frame, dq_result = profile_processor_run(
        processor,
        raw_frame,
        lambda frame: process_dataset(
            processor,
            frame,
            allow_empty=allow_empty,
        ),
        normalize_nulls,
        dedup_keys=dedup_keys,
    )
    serialized = json.dumps(
        dq_result.as_dict(),
        ensure_ascii=False,
        sort_keys=True,
    )
    if dq_result.status.value == "CHECK_FAILED":
        LOGGER.warning("dq_audit=%s", serialized)
    else:
        LOGGER.info("dq_audit=%s", serialized)

    print(render_dq_audit_report(dq_result))

    return processed_frame, dq_result


def _validate_identifier(value: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise ValueError(f"안전하지 않은 DB 식별자: {value!r}")

    return value

def _validate_column_identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(
            f"안전하지 않은 DB 컬럼 식별자: {value!r}"
        )

    return value


STAGING_IDENTIFIER_MAX_LENGTH = 55


def _make_staging_identifier(
    table: str,
    stage: str,
) -> str:
    """
    PostgreSQL identifier 제한 63자 보다 여유 있게
    staging 테이블명을 생성한다(최대 55자).

    형식:
        stg_<table 일부>_<stage>_<uuid8>
    """

    table = _validate_identifier(table)
    stage = _validate_identifier(stage)

    suffix = uuid.uuid4().hex[:8]

    prefix = "stg_"
    tail = f"_{stage}_{suffix}"

    max_table_length = (
        STAGING_IDENTIFIER_MAX_LENGTH
        - len(prefix)
        - len(tail)
    )

    if max_table_length < 1:
        raise ValueError(
            f"staging identifier 생성 불가: "
            f"stage={stage!r}"
        )

    shortened_table = table[:max_table_length]

    staging = (
        f"{prefix}"
        f"{shortened_table}"
        f"{tail}"
    )

    return _validate_identifier(staging)


def _write_staging_frame(
    connection,
    frame: pd.DataFrame,
    schema: str,
    staging: str,
) -> None:
    """staging 적재와 row-count 검증의 단일 구현."""

    schema = _validate_identifier(schema)
    staging = _validate_identifier(staging)
    if frame.empty and not len(frame.columns):
        raise RuntimeError(
            f"{schema}.{staging}: 0건 frame에 컬럼 정의가 없습니다."
        )
    frame.to_sql(
        staging,
        connection,
        schema=schema,
        if_exists="fail",
        index=False,
        chunksize=max(1, min(1000, 30_000 // max(len(frame.columns), 1))),
        method="multi",
    )
    count = connection.execute(
        text(f'SELECT COUNT(*) FROM "{schema}"."{staging}"')
    ).scalar_one()
    if count != len(frame):
        raise RuntimeError(
            f"{schema}.{staging} 건수 불일치: frame={len(frame)}, db={count}"
        )


def _drop_final_tables(connection, schema: str, tables: tuple[str, ...]) -> None:
    schema = _validate_identifier(schema)
    for table in tables:
        table = _validate_identifier(table)
        connection.execute(
            text(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
        )


def _promote_staging_tables(
    connection,
    schema: str,
    staging_tables: Mapping[str, str],
) -> None:
    schema = _validate_identifier(schema)
    for table, staging in staging_tables.items():
        table = _validate_identifier(table)
        staging = _validate_identifier(staging)
        connection.execute(
            text(
                f'ALTER TABLE "{schema}"."{staging}" '
                f'RENAME TO "{table}"'
            )
        )


def replace_raw_dataset_group(
    raw_frames: Mapping[str, pd.DataFrame],
    engine: Engine | None = None,
    *,
    allow_empty_tables: set[str] | None = None,
) -> None:
    """Collector 결과 전체를 하나의 transaction으로 RAW에 교체한다.

    모든 staging table의 적재와 row-count 검증이 성공한 뒤에만 기존
    final table을 교체한다. 따라서 multi-output dataset도 부분 갱신되지
    않는다.
    """

    if not raw_frames:
        raise ValueError("raw_frames가 비어 있습니다.")

    allow_empty_tables = allow_empty_tables or set()
    validated_frames: dict[str, pd.DataFrame] = {}
    staging_tables: dict[str, str] = {}

    for table, frame in raw_frames.items():
        table = _validate_identifier(table)
        if not isinstance(frame, pd.DataFrame):
            raise TypeError(f"raw.{table}: pandas DataFrame이 아닙니다.")
        if frame.empty and table not in allow_empty_tables:
            raise RuntimeError(
                f"0건 RAW 결과는 교체하지 않습니다: raw.{table}"
            )
        if not len(frame.columns):
            raise RuntimeError(f"raw.{table}: 컬럼 정의가 없습니다.")
        validated_frames[table] = frame
        staging_tables[table] = _make_staging_identifier(table, "raw")

    own_engine = engine is None
    db_engine = engine or create_db_engine()
    try:
        with db_engine.begin() as connection:
            connection.execute(text('CREATE SCHEMA IF NOT EXISTS "raw"'))
            for table, frame in validated_frames.items():
                _write_staging_frame(
                    connection,
                    frame,
                    "raw",
                    staging_tables[table],
                )

            table_order = tuple(validated_frames)
            _drop_final_tables(connection, "raw", table_order)
            _promote_staging_tables(connection, "raw", staging_tables)
    finally:
        if own_engine:
            db_engine.dispose()


def _prepare_timeseries_frame(frame: pd.DataFrame, spec: TableSpec) -> pd.DataFrame:
    """Reject ambiguous observation identities; a batch's last occurrence wins."""
    if spec.load_policy is not LoadPolicy.TIMESERIES:
        raise ValueError(f"{spec.table}: not a timeseries table")
    if spec.table == "air_quality":
        frame = add_air_quality_observation_key(frame)
    _validate_identifier(spec.table)
    for column in frame.columns:
        _validate_column_identifier(column)
    for column in spec.upsert_key:
        _validate_column_identifier(column)
    missing = set(spec.upsert_key) - set(frame.columns)
    if missing:
        raise ValueError(f"{spec.table}: missing observation keys: {sorted(missing)}")
    if frame.empty:
        raise ValueError(f"{spec.table}: empty timeseries batch")
    result = frame.copy()
    # Only identity fields are normalized here; payload NULL transitions remain
    # the processor/DQ profiler's responsibility.
    keys = normalize_nulls(result[list(spec.upsert_key)])
    if keys.isna().any().any():
        raise ValueError(f"{spec.table}: NULL observation key")
    result[list(spec.upsert_key)] = keys
    return result.drop_duplicates(subset=list(spec.upsert_key), keep="last")


def _backfill_air_quality_observation_keys(connection, schema: str) -> None:
    """Upgrade a legacy snapshot in the enclosing locked transaction.

    ctid is used only within this transaction to address each existing row;
    it is never an observation key. Invalid legacy rows abort the whole load.
    """
    target = f'"{_validate_identifier(schema)}"."air_quality"'
    rows = connection.execute(text(
        f'SELECT ctid::text AS "_migration_row_id", * FROM {target} '
        'WHERE "observation_time_key" IS NULL'
    )).mappings().all()
    if rows:
        keyed = add_air_quality_observation_key(pd.DataFrame(rows))
        connection.execute(text(
            f'UPDATE {target} SET "observation_time_key" = :key '
            'WHERE ctid = CAST(:row_id AS tid)'
        ), [
            {"row_id": row_id, "key": key}
            for row_id, key in keyed[["_migration_row_id", "observation_time_key"]].itertuples(index=False, name=None)
        ])


def _upsert_timeseries_group(
    engine: Engine,
    frames: Mapping[str, pd.DataFrame],
    specs: Mapping[str, TableSpec],
    *,
    schema: str,
) -> None:
    """Transactional PostgreSQL upsert, without replacing historical tables.

    A unique index enforces identity across retries. Advisory locks serialize
    first creation/schema changes across our writers. Existing duplicate/NULL
    identities fail and roll back instead of silently deleting legacy records.
    DQ counts describe the input/processed frames, not INSERT row counts.
    """
    if schema not in {"raw", "processed"} or not frames or set(frames) != set(specs):
        raise ValueError("Invalid timeseries group")
    if any(table != spec.table for table, spec in specs.items()):
        raise ValueError("Table/spec mismatch")
    prepared = {
        table: _prepare_timeseries_frame(frame, specs[table])
        for table, frame in frames.items()
    }
    with engine.begin() as connection:
        connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        # Stable lock order also handles the weather multi-table group.
        for table in sorted(prepared):
            _validate_identifier(table)
            connection.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:name, 0))"),
                {"name": f"{schema}.{table}"},
            )
        for table, frame in prepared.items():
            stage = _make_staging_identifier(table, schema)
            target = f'"{schema}"."{table}"'
            staging = f'"{schema}"."{stage}"'
            # Forecast rain amounts include text such as '강수없음'. Keep a
            # stable storage type even when the first batch is entirely numeric.
            if table == "weather_ultra_fcst" and "fcstValue" in frame:
                frame = frame.copy()
                frame["fcstValue"] = frame["fcstValue"].astype("string")
            _write_staging_frame(connection, frame, schema, stage)
            connection.execute(text(f'CREATE TABLE IF NOT EXISTS {target} (LIKE {staging})'))
            connection.execute(text(f'LOCK TABLE {target} IN SHARE ROW EXCLUSIVE MODE'))
            # PROCESSED can omit all-NULL columns on its first run. Add columns
            # when later observations supply them, preserving historical rows.
            stage_types = connection.execute(
                text("SELECT attname, format_type(atttypid, atttypmod) "
                     "FROM pg_attribute WHERE attrelid = to_regclass(:name) "
                     "AND attnum > 0 AND NOT attisdropped"),
                {"name": f"{schema}.{stage}"},
            ).all()
            for column, sql_type in stage_types:
                _validate_column_identifier(column)
                connection.execute(text(
                    f'ALTER TABLE {target} ADD COLUMN IF NOT EXISTS "{column}" {sql_type}'
                ))
            if table == "weather_ultra_fcst" and "fcstValue" in frame:
                connection.execute(text(
                    f'ALTER TABLE {target} ALTER COLUMN "fcstValue" TYPE TEXT '
                    'USING "fcstValue"::text'
                ))
            if table == "air_quality":
                _backfill_air_quality_observation_keys(connection, schema)
            keys = specs[table].upsert_key
            for key in keys:
                connection.execute(text(f'ALTER TABLE {target} ALTER COLUMN "{key}" SET NOT NULL'))
            key_sql = ", ".join(f'"{key}"' for key in keys)
            index = _validate_identifier(
                f"uq_{table}_observation_v2" if table == "air_quality"
                else f"uq_{table}_observation"
            )
            connection.execute(text(
                f'CREATE UNIQUE INDEX IF NOT EXISTS "{index}" ON {target} ({key_sql})'
            ))
            if table == "air_quality":
                # Replace the obsolete key constraint only after the stricter
                # measured-or-captured identity has been validated/indexed.
                connection.execute(text(f'DROP INDEX IF EXISTS "{schema}"."uq_air_quality_observation"'))
                target_columns = connection.execute(
                    text("SELECT attname FROM pg_attribute WHERE attrelid = to_regclass(:name) "
                         "AND attnum > 0 AND NOT attisdropped"),
                    {"name": f"{schema}.{table}"},
                ).scalars().all()
                if "dataTime" in target_columns:
                    connection.execute(text(f'ALTER TABLE {target} ALTER COLUMN "dataTime" DROP NOT NULL'))
            columns = ", ".join(f'"{column}"' for column in frame.columns)
            updates = ", ".join(
                f'"{column}" = EXCLUDED."{column}"'
                for column in frame.columns if column not in keys
            )
            if schema == "processed":
                target_columns = connection.execute(
                    text("SELECT attname FROM pg_attribute "
                         "WHERE attrelid = to_regclass(:name) "
                         "AND attnum > 0 AND NOT attisdropped"),
                    {"name": f"{schema}.{table}"},
                ).scalars().all()
                # A processor intentionally drops fully NULL columns. An
                # updated observation must not retain its previous value.
                cleared = [
                    f'"{_validate_column_identifier(column)}" = NULL'
                    for column in target_columns if column not in frame.columns
                ]
                updates = ", ".join(filter(None, [updates, *cleared]))
            action = f"DO UPDATE SET {updates}" if updates else "DO NOTHING"
            connection.execute(text(
                f'INSERT INTO {target} ({columns}) SELECT {columns} FROM {staging} WHERE TRUE '
                f'ON CONFLICT ({key_sql}) {action}'
            ))
            connection.execute(text(f'DROP TABLE {staging}'))


def upsert_raw_dataset_group(
    raw_frames: Mapping[str, pd.DataFrame],
    engine: Engine | None = None,
) -> None:
    """Collector entry point; the metadata owns the observation identity."""
    registered = {
        table.table: table for dataset in get_dataset_specs().values()
        for table in dataset.tables
    }
    specs = {table: registered[table] for table in raw_frames}
    # Validate before opening a connection, including rejection of snapshots.
    for table, frame in raw_frames.items():
        _prepare_timeseries_frame(frame, specs[table])
    if not raw_frames:
        raise ValueError("Empty timeseries group")
    own_engine = engine is None
    db_engine = engine or create_db_engine()
    try:
        _upsert_timeseries_group(db_engine, raw_frames, specs, schema="raw")
    finally:
        if own_engine:
            db_engine.dispose()


def read_raw_table_if_exists(
    table: str,
    engine: Engine | None = None,
) -> pd.DataFrame:
    """기존 RAW snapshot을 읽고, 아직 없는 테이블이면 빈 frame을 반환한다."""

    table = _validate_identifier(table)
    own_engine = engine is None
    db_engine = engine or create_db_engine()
    try:
        with db_engine.connect() as connection:
            exists = connection.execute(
                text("SELECT to_regclass(:qualified_name)"),
                {"qualified_name": f"raw.{table}"},
            ).scalar_one()
            if exists is None:
                return pd.DataFrame()
            return pd.read_sql_query(
                text(f'SELECT * FROM "raw"."{table}"'),
                connection,
            )
    finally:
        if own_engine:
            db_engine.dispose()


def _atomic_replace_processed_group(
    engine: Engine,
    processed_frames: dict[str, pd.DataFrame],
    *,
    primary_keys: dict[str, tuple[str, ...]] | None = None,
    indexes: (
        dict[
            str,
            tuple[
                tuple[str, tuple[str, ...]],
                ...,
            ],
        ]
        | None
    ) = None,
    allow_empty_tables: set[str] | None = None,
) -> None:
    """
    여러 PROCESSED 테이블을 하나의 transaction에서 교체한다.

    순서:
    1. 모든 staging 적재
    2. staging row count 검증
    3. 기존 final 제거
    4. staging → final rename
    5. PRIMARY KEY 생성
    6. INDEX 생성

    PK/INDEX 생성까지 모두 성공해야 commit된다.
    하나라도 실패하면 전체 transaction이 rollback된다.
    """

    if not processed_frames:
        raise ValueError(
            "processed_frames가 비어 있습니다."
        )

    primary_keys = primary_keys or {}
    indexes = indexes or {}
    allow_empty_tables = allow_empty_tables or set()

    validated_frames: dict[str, pd.DataFrame] = {}
    staging_tables: dict[str, str] = {}

    for table, frame in processed_frames.items():
        table = _validate_identifier(table)

        if frame.empty and table not in allow_empty_tables:
            raise RuntimeError(
                f"0건 PROCESSED 결과는 교체하지 않습니다: "
                f"processed.{table}"
            )

        validated_frames[table] = frame
        staging_tables[table] = _make_staging_identifier(
            table,
            "processed",
        )

    _validate_table_constraints(
        validated_frames,
        primary_keys,
        indexes,
    )

    with engine.begin() as connection:
        connection.execute(
            text(
                'CREATE SCHEMA IF NOT EXISTS "processed"'
            )
        )

        # -----------------------------------------------------
        # 1. 모든 staging 적재 + row count 검증
        # -----------------------------------------------------

        for table, frame in validated_frames.items():
            _write_staging_frame(
                connection,
                frame,
                "processed",
                staging_tables[table],
            )

        # -----------------------------------------------------
        # 2. 기존 final 제거
        # -----------------------------------------------------

        table_order = tuple(validated_frames)
        _drop_final_tables(connection, "processed", table_order)

        # -----------------------------------------------------
        # 3. staging → final
        # -----------------------------------------------------

        _promote_staging_tables(connection, "processed", staging_tables)

        _create_declared_constraints(connection, primary_keys, indexes)


def _validate_table_constraints(
    frames: Mapping[str, pd.DataFrame],
    primary_keys: Mapping[str, tuple[str, ...]],
    indexes: Mapping[
        str,
        tuple[tuple[str, tuple[str, ...]], ...],
    ],
) -> None:
    """DDL 실행 전에 선언된 테이블/컬럼 계약을 검증한다."""

    unknown_tables = (set(primary_keys) | set(indexes)) - set(frames)
    if unknown_tables:
        raise ValueError(
            "등록되지 않은 테이블의 constraint 정의: "
            f"{sorted(unknown_tables)}"
        )

    used_index_names: set[str] = set()
    for table, frame in frames.items():
        _validate_identifier(table)
        columns = set(frame.columns)
        primary_key = primary_keys.get(table, ())
        if primary_key:
            if len(primary_key) != len(set(primary_key)):
                raise ValueError(f"{table}: PRIMARY KEY 컬럼이 중복됩니다.")
            for column in primary_key:
                _validate_column_identifier(column)
            missing = set(primary_key) - columns
            if missing:
                raise ValueError(
                    f"{table}: PRIMARY KEY 컬럼 누락: {sorted(missing)}"
                )

        for index_name, index_columns in indexes.get(table, ()):
            _validate_identifier(index_name)
            if index_name in used_index_names:
                raise ValueError(f"중복 INDEX 이름: {index_name}")
            used_index_names.add(index_name)
            if not index_columns:
                raise ValueError(f"{index_name}: INDEX 컬럼이 없습니다.")
            if len(index_columns) != len(set(index_columns)):
                raise ValueError(f"{index_name}: INDEX 컬럼이 중복됩니다.")
            for column in index_columns:
                _validate_column_identifier(column)
            missing = set(index_columns) - columns
            if missing:
                raise ValueError(
                    f"{table}: INDEX {index_name} 컬럼 누락: {sorted(missing)}"
                )


def _constraints_from_specs(
    table_specs: tuple[TableSpec, ...],
) -> tuple[
    dict[str, tuple[str, ...]],
    dict[str, tuple[tuple[str, tuple[str, ...]], ...]],
    set[str],
]:
    primary_keys = {
        table.table: table.primary_key
        for table in table_specs
        if table.primary_key
    }
    indexes = {
        table.table: tuple(
            (index.name, index.columns)
            for index in table.indexes
        )
        for table in table_specs
        if table.indexes
    }
    allow_empty_tables = {
        table.table
        for table in table_specs
        if table.allow_empty
    }
    return primary_keys, indexes, allow_empty_tables


def _create_declared_constraints(
    connection,
    primary_keys: Mapping[str, tuple[str, ...]],
    indexes: Mapping[str, tuple[tuple[str, tuple[str, ...]], ...]],
) -> None:
    for table, columns in primary_keys.items():
        table = _validate_identifier(table)
        column_sql = ", ".join(
            f'"{_validate_column_identifier(column)}"'
            for column in columns
        )
        connection.execute(
            text(
                f'ALTER TABLE "processed"."{table}" '
                f'ADD PRIMARY KEY ({column_sql})'
            )
        )

    for table, table_indexes in indexes.items():
        table = _validate_identifier(table)
        for index_name, columns in table_indexes:
            index_name = _validate_identifier(index_name)
            column_sql = ", ".join(
                f'"{_validate_column_identifier(column)}"'
                for column in columns
            )
            connection.execute(
                text(
                    f'CREATE INDEX "{index_name}" '
                    f'ON "processed"."{table}" ({column_sql})'
                )
            )


def _validate_dataset_frames(
    spec: DatasetSpec,
    processed_frames: Mapping[str, pd.DataFrame],
) -> None:
    if spec.validator is None:
        return
    try:
        validator = DATASET_VALIDATORS[spec.validator]
    except KeyError as exc:
        raise KeyError(
            f"{spec.name}: 등록되지 않은 dataset validator: {spec.validator}"
        ) from exc
    validator(dict(processed_frames))


def _process_dataset_frames(
    spec: DatasetSpec,
    raw_frames: Mapping[str, pd.DataFrame],
) -> tuple[
    dict[str, pd.DataFrame],
    dict[str, tuple[int, int]],
    dict[str, DQProfileResult],
]:
    processed_frames: dict[str, pd.DataFrame] = {}
    results: dict[str, tuple[int, int]] = {}
    dq_results: dict[str, DQProfileResult] = {}

    for table_spec in spec.tables:
        raw_frame = raw_frames[table_spec.table]
        if raw_frame.empty and not table_spec.allow_empty:
            raise RuntimeError(
                f"0건 RAW 결과는 처리할 수 없습니다: raw.{table_spec.table}"
            )

        processed_frame, dq_result = process_with_dq_profile(
            table_spec.processor,
            raw_frame,
            allow_empty=table_spec.allow_empty,
            dedup_keys=table_spec.upsert_key or table_spec.primary_key,
        )

        if processed_frame.empty and not table_spec.allow_empty:
            raise RuntimeError(
                "0건 PROCESSED 결과는 적재할 수 없습니다: "
                f"processed.{table_spec.table}"
            )

        processed_frames[table_spec.table] = processed_frame
        results[table_spec.table] = (
            len(processed_frame),
            int(processed_frame.isna().sum().sum()),
        )
        dq_results[table_spec.table] = dq_result

    _validate_dataset_frames(spec, processed_frames)
    return processed_frames, results, dq_results


def _log_dataset_result(
    spec: DatasetSpec,
    results: Mapping[str, tuple[int, int]],
    source: SourceKind,
    durations: Mapping[str, float],
) -> None:
    print()
    print("=" * 70)
    print(f"[DATASET {source.value} → PROCESSED] {spec.name}")
    for table, (row_count, null_count) in results.items():
        print(f"{table:<45} ROWS={row_count:,} NULL={null_count:,}")
    for stage, elapsed in durations.items():
        print(f"{stage:<45} SECONDS={elapsed:.3f}")
    print("상태                          : SUCCESS")
    print("=" * 70)
    print()
    LOGGER.info(
        "dataset=%s source=%s tables=%s status=SUCCESS "
        "elapsed_seconds=%.3f stage_durations=%s",
        spec.name,
        source.value,
        ",".join(table.table for table in spec.tables),
        sum(durations.values()),
        json.dumps(dict(durations), sort_keys=True),
    )


def load_dataset_from_raw(
    spec: DatasetSpec,
    engine: Engine | None = None,
) -> tuple[
    dict[str, tuple[int, int]],
    dict[str, DQProfileResult],
]:
    """데이터셋의 RAW 테이블 전체를 정제·DQ 감사하고 PROCESSED 적재 정책을 적용한다.

    테이블별 (PROCESSED 행 수, NULL 셀 수) 매핑과 DQ 결과 매핑을 반환한다."""

    if spec.source_kind is not SourceKind.RAW_DATABASE:
        raise ValueError(f"{spec.name}: RAW_DATABASE dataset이 아닙니다.")

    durations: dict[str, float] = {}
    own_engine = engine is None
    db_engine = engine
    try:
        with _timed_dataset_stage(
            spec,
            SourceKind.RAW_DATABASE,
            "source_read",
            durations,
        ):
            db_engine = db_engine or create_db_engine()
            raw_frames: dict[str, pd.DataFrame] = {}
            with db_engine.connect() as connection:
                for table_spec in spec.tables:
                    table = _validate_identifier(table_spec.table)
                    raw_frames[table] = pd.read_sql_query(
                        text(f'SELECT * FROM "raw"."{table}"'),
                        connection,
                    )

        with _timed_dataset_stage(
            spec,
            SourceKind.RAW_DATABASE,
            "process",
            durations,
        ):
            processed_frames, results, dq_results = _process_dataset_frames(
                spec,
                raw_frames
            )
            primary_keys, indexes, allow_empty_tables = _constraints_from_specs(
                spec.tables
            )

        with _timed_dataset_stage(
            spec,
            SourceKind.RAW_DATABASE,
            "db",
            durations,
        ):
            policies = {table.load_policy for table in spec.tables}
            if policies == {LoadPolicy.TIMESERIES}:
                _upsert_timeseries_group(
                    db_engine, processed_frames,
                    {table.table: table for table in spec.tables},
                    schema="processed",
                )
            elif policies == {LoadPolicy.SNAPSHOT}:
                _atomic_replace_processed_group(
                    db_engine,
                    processed_frames,
                    primary_keys=primary_keys,
                    indexes=indexes,
                    allow_empty_tables=allow_empty_tables,
                )
            else:
                raise ValueError("Mixed load policies in one dataset are not supported")
    finally:
        if own_engine and db_engine is not None:
            db_engine.dispose()

    _log_dataset_result(spec, results, SourceKind.RAW_DATABASE, durations)
    return results, dq_results


def load_dataset(
    spec: DatasetSpec,
    engine: Engine | None = None,
) -> tuple[
    dict[str, tuple[int, int]],
    dict[str, DQProfileResult],
]:
    """RAW DB를 읽어 공통 processor/DQ/PROCESSED 적재를 실행한다."""

    if spec.source_kind is not SourceKind.RAW_DATABASE:
        raise ValueError(f"{spec.name}: 지원하지 않는 source kind입니다.")
    return load_dataset_from_raw(spec, engine)
