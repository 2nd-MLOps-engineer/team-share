"""
수집 데이터를 PostgreSQL RAW/PROCESSED 계층으로 관리하는 공통 ELT 파이프라인 모듈.

RAW(Bronze)에는 수집 원본을 보존하고,
dataset_processors.py의 데이터셋별 정제 함수를 통해
PROCESSED(Silver) 데이터를 생성한다.

처리 경로:

1. CSV 기반 데이터
   - 수집된 CSV를 원본 그대로 읽는다.
   - dataset_processors.py의 데이터셋별 정제 함수를 실행한다.
   - RAW와 PROCESSED 데이터를 staging 테이블에 먼저 저장한다.
   - staging 테이블의 행 수를 검증한다.
   - 검증에 성공하면 기존 RAW/PROCESSED 테이블을 새 데이터로 교체한다.

2. RAW DB 기반 데이터
   - 이미 PostgreSQL raw 스키마에 적재된 데이터를 읽는다.
   - dataset_processors.py의 데이터셋별 정제 함수를 실행한다.
   - PROCESSED 데이터를 staging 테이블에 먼저 저장한다.
   - staging 테이블의 행 수를 검증한다.
   - 검증에 성공하면 기존 PROCESSED 테이블만 새 데이터로 교체한다.
   - 기존 RAW 테이블은 수정하지 않는다.

각 처리 과정에서 RAW/PROCESSED 건수, NULL 수,
데이터 읽기·정제·DB 적재·전체 파이프라인 처리시간을 로그로 남긴다.

중간에 오류가 발생하면 검증되지 않은 staging 데이터를
최종 RAW/PROCESSED 테이블로 교체하지 않는다.
"""

from __future__ import annotations

import logging
import os
import re
import time
import uuid
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

if __package__:
    from .dataset_processors import normalize_nulls, process_dataset
else:
    from dataset_processors import normalize_nulls, process_dataset


LOGGER = logging.getLogger(__name__)

PIPELINE_DIR = Path(__file__).resolve().parent
WORKSPACE_ROOT = PIPELINE_DIR.parent
ENV_PATH = PIPELINE_DIR / "collector" / ".env"


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


def read_raw_csv(csv_path: Path) -> pd.DataFrame:
    """빈 문자열을 NaN으로 바꾸지 않고 CSV를 문자열 그대로 읽는다."""
    if not csv_path.is_file() or csv_path.stat().st_size == 0:
        raise RuntimeError(f"CSV가 없거나 비어 있습니다: {csv_path}")

    frame = pd.read_csv(
        csv_path,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        encoding="utf-8-sig",
    )

    if frame.empty:
        raise RuntimeError(f"0건 CSV는 DB에 적재하지 않습니다: {csv_path}")

    if frame.columns.empty:
        raise RuntimeError(f"컬럼이 없는 CSV입니다: {csv_path}")

    return frame


def normalize_missing_values(frame: pd.DataFrame) -> pd.DataFrame:
    """하위 호환용 결측치 정규화 함수."""
    return normalize_nulls(frame)


def _validate_identifier(value: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", value):
        raise ValueError(f"안전하지 않은 DB 식별자: {value!r}")

    return value


def _atomic_replace_pair(
    engine: Engine,
    raw_frame: pd.DataFrame,
    processed_frame: pd.DataFrame,
    table: str,
) -> None:
    """raw/processed 스테이징 검증과 두 최종 테이블 교체를 한 트랜잭션으로 묶는다."""

    table = _validate_identifier(table)

    suffix = uuid.uuid4().hex[:8]

    raw_staging = _validate_identifier(
        f"stg_{table}_raw_{suffix}"
    )
    processed_staging = _validate_identifier(
        f"stg_{table}_processed_{suffix}"
    )

    def write_staging(connection, frame, schema, staging):
        frame.to_sql(
            staging,
            connection,
            schema=schema,
            if_exists="fail",
            index=False,
            chunksize=max(
                1,
                min(
                    1000,
                    30_000 // max(len(frame.columns), 1),
                ),
            ),
            method="multi",
        )

        count = connection.execute(
            text(
                f'SELECT COUNT(*) '
                f'FROM "{schema}"."{staging}"'
            )
        ).scalar_one()

        if count != len(frame):
            raise RuntimeError(
                f"{schema}.{staging} 건수 불일치: "
                f"frame={len(frame)}, db={count}"
            )

    with engine.begin() as connection:
        connection.execute(
            text('CREATE SCHEMA IF NOT EXISTS "raw"')
        )
        connection.execute(
            text('CREATE SCHEMA IF NOT EXISTS "processed"')
        )

        write_staging(
            connection,
            raw_frame,
            "raw",
            raw_staging,
        )

        write_staging(
            connection,
            processed_frame,
            "processed",
            processed_staging,
        )

        connection.execute(
            text(f'DROP TABLE IF EXISTS "raw"."{table}"')
        )
        connection.execute(
            text(
                f'DROP TABLE IF EXISTS '
                f'"processed"."{table}"'
            )
        )

        connection.execute(
            text(
                f'ALTER TABLE "raw"."{raw_staging}" '
                f'RENAME TO "{table}"'
            )
        )

        connection.execute(
            text(
                f'ALTER TABLE '
                f'"processed"."{processed_staging}" '
                f'RENAME TO "{table}"'
            )
        )

def _atomic_replace_processed(
    engine: Engine,
    processed_frame: pd.DataFrame,
    table: str,
) -> None:
    """processed만 staging 검증 후 원자적으로 교체한다."""

    table = _validate_identifier(table)

    suffix = uuid.uuid4().hex[:8]
    staging = _validate_identifier(
        f"stg_{table}_processed_{suffix}"
    )

    with engine.begin() as connection:
        connection.execute(
            text('CREATE SCHEMA IF NOT EXISTS "processed"')
        )

        processed_frame.to_sql(
            staging,
            connection,
            schema="processed",
            if_exists="fail",
            index=False,
            chunksize=max(
                1,
                min(
                    1000,
                    30_000 // max(len(processed_frame.columns), 1),
                ),
            ),
            method="multi",
        )

        count = connection.execute(
            text(
                f'SELECT COUNT(*) '
                f'FROM "processed"."{staging}"'
            )
        ).scalar_one()

        if count != len(processed_frame):
            raise RuntimeError(
                f"processed.{staging} 건수 불일치: "
                f"frame={len(processed_frame)}, db={count}"
            )

        connection.execute(
            text(
                f'DROP TABLE IF EXISTS '
                f'"processed"."{table}"'
            )
        )

        connection.execute(
            text(
                f'ALTER TABLE "processed"."{staging}" '
                f'RENAME TO "{table}"'
            )
        )

def _format_elapsed(seconds: float) -> str:
    """초 단위 실행시간을 읽기 쉬운 문자열로 변환한다."""
    if seconds < 60:
        return f"{seconds:.1f}초"

    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60

    return f"{minutes}분 {remaining_seconds:.1f}초"


def load_csv_to_raw_and_processed(
    csv_path: Path,
    table: str,
    engine: Engine | None = None,
) -> tuple[int, int]:
    """CSV 하나를 raw와 정제된 processed 테이블에 원자적으로 적재한다."""

    pipeline_started = time.perf_counter()

    # ---------------------------------------------------------
    # 1. CSV 읽기
    # ---------------------------------------------------------

    csv_started = time.perf_counter()

    raw_frame = read_raw_csv(csv_path)

    csv_elapsed = time.perf_counter() - csv_started

    # ---------------------------------------------------------
    # 2. 데이터 정제
    # ---------------------------------------------------------

    processing_started = time.perf_counter()

    processed_frame = process_dataset(
        table,
        raw_frame,
    )

    processing_elapsed = (
        time.perf_counter() - processing_started
    )

    # ---------------------------------------------------------
    # 3. DB 적재
    # ---------------------------------------------------------

    database_started = time.perf_counter()

    own_engine = engine is None
    db_engine = engine or create_db_engine()

    try:
        _atomic_replace_pair(
            db_engine,
            raw_frame,
            processed_frame,
            table,
        )
    finally:
        if own_engine:
            db_engine.dispose()

    database_elapsed = (
        time.perf_counter() - database_started
    )

    # ---------------------------------------------------------
    # 4. 결과 집계
    # ---------------------------------------------------------

    null_count = int(
        processed_frame.isna().sum().sum()
    )

    pipeline_elapsed = (
        time.perf_counter() - pipeline_started
    )

    # ---------------------------------------------------------
    # 5. 화면 출력
    # ---------------------------------------------------------

    print()
    print("=" * 70)
    print(f"[PIPELINE] {table}")
    print(
        f"RAW                  : "
        f"{len(raw_frame):,}건"
    )
    print(
        f"PROCESSED            : "
        f"{len(processed_frame):,}건"
    )
    print(
        f"NULL                 : "
        f"{null_count:,}개"
    )
    print(
        f"CSV 읽기시간         : "
        f"{_format_elapsed(csv_elapsed)}"
    )
    print(
        f"정제시간             : "
        f"{_format_elapsed(processing_elapsed)}"
    )
    print(
        f"DB 적재시간          : "
        f"{_format_elapsed(database_elapsed)}"
    )
    print(
        f"파이프라인 처리시간  : "
        f"{_format_elapsed(pipeline_elapsed)}"
    )
    print("=" * 70)
    print()

    # ---------------------------------------------------------
    # 6. 로그
    # ---------------------------------------------------------

    LOGGER.info(
        "table=%s "
        "raw_rows=%s "
        "processed_rows=%s "
        "nulls=%s "
        "csv_elapsed_seconds=%.3f "
        "processing_elapsed_seconds=%.3f "
        "database_elapsed_seconds=%.3f "
        "pipeline_elapsed_seconds=%.3f "
        "source=%s",
        table,
        len(raw_frame),
        len(processed_frame),
        null_count,
        csv_elapsed,
        processing_elapsed,
        database_elapsed,
        pipeline_elapsed,
        csv_path,
    )

    return len(raw_frame), null_count

def load_raw_to_processed(
    table: str,
    engine: Engine | None = None,
) -> tuple[int, int]:
    """
    PostgreSQL raw 테이블을 읽어 dataset_processors.py로 정제한 뒤
    processed 테이블만 원자적으로 교체한다.

    raw 테이블은 수정하지 않는다.
    """

    pipeline_started = time.perf_counter()

    table = _validate_identifier(table)

    own_engine = engine is None
    db_engine = engine or create_db_engine()

    try:
        # ---------------------------------------------------------
        # 1. RAW DB 읽기
        # ---------------------------------------------------------

        raw_read_started = time.perf_counter()

        with db_engine.connect() as connection:
            raw_frame = pd.read_sql_query(
                text(
                    f'SELECT * '
                    f'FROM "raw"."{table}"'
                ),
                connection,
            )

        raw_read_elapsed = (
            time.perf_counter() - raw_read_started
        )

        if raw_frame.empty:
            raise RuntimeError(
                f"0건 RAW 테이블은 처리하지 않습니다: raw.{table}"
            )

        # ---------------------------------------------------------
        # 2. dataset_processors 정제
        # ---------------------------------------------------------

        processing_started = time.perf_counter()

        processed_frame = process_dataset(
            table,
            raw_frame,
        )

        processing_elapsed = (
            time.perf_counter() - processing_started
        )

        # ---------------------------------------------------------
        # 3. PROCESSED DB 적재
        # ---------------------------------------------------------

        database_started = time.perf_counter()

        _atomic_replace_processed(
            db_engine,
            processed_frame,
            table,
        )

        database_elapsed = (
            time.perf_counter() - database_started
        )

        # ---------------------------------------------------------
        # 4. 결과 집계
        # ---------------------------------------------------------

        null_count = int(
            processed_frame.isna().sum().sum()
        )

        pipeline_elapsed = (
            time.perf_counter() - pipeline_started
        )

        # ---------------------------------------------------------
        # 5. 화면 출력
        # ---------------------------------------------------------

        print()
        print("=" * 70)
        print(f"[RAW → PROCESSED] {table}")
        print(
            f"RAW                  : "
            f"{len(raw_frame):,}건"
        )
        print(
            f"PROCESSED            : "
            f"{len(processed_frame):,}건"
        )
        print(
            f"NULL                 : "
            f"{null_count:,}개"
        )
        print(
            f"RAW DB 읽기시간      : "
            f"{_format_elapsed(raw_read_elapsed)}"
        )
        print(
            f"정제시간             : "
            f"{_format_elapsed(processing_elapsed)}"
        )
        print(
            f"PROCESSED 적재시간   : "
            f"{_format_elapsed(database_elapsed)}"
        )
        print(
            f"파이프라인 처리시간  : "
            f"{_format_elapsed(pipeline_elapsed)}"
        )
        print("=" * 70)
        print()

        # ---------------------------------------------------------
        # 6. 로그
        # ---------------------------------------------------------

        LOGGER.info(
            "table=%s "
            "source=raw.%s "
            "target=processed.%s "
            "raw_rows=%s "
            "processed_rows=%s "
            "nulls=%s "
            "raw_read_elapsed_seconds=%.3f "
            "processing_elapsed_seconds=%.3f "
            "database_elapsed_seconds=%.3f "
            "pipeline_elapsed_seconds=%.3f",
            table,
            table,
            table,
            len(raw_frame),
            len(processed_frame),
            null_count,
            raw_read_elapsed,
            processing_elapsed,
            database_elapsed,
            pipeline_elapsed,
        )

        return len(processed_frame), null_count

    finally:
        if own_engine:
            db_engine.dispose()    