"""기존 facility CSV를 processed staging에 적재하고 트랜잭션을 롤백한다."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path


PIPELINE_DIR = Path(__file__).resolve().parents[1]
TEST_DEPENDENCIES = PIPELINE_DIR / ".test_deps"
if TEST_DEPENDENCIES.is_dir():
    sys.path.insert(0, str(TEST_DEPENDENCIES))
sys.path.insert(0, str(PIPELINE_DIR))

from sqlalchemy import text  # noqa: E402

from dataset_processors import process_facility  # noqa: E402
from pipeline_common import create_db_engine, read_raw_csv  # noqa: E402


CSV_PATH = (
    PIPELINE_DIR.parent
    / "data"
    / "raw"
    / "facility"
    / "facility_all_20260922.csv"
)


def main() -> None:
    raw = read_raw_csv(CSV_PATH)
    processed = process_facility(raw)
    staging = f"stg_facility_validation_{uuid.uuid4().hex[:8]}"
    chunksize = max(1, min(1000, 30_000 // max(len(processed.columns), 1)))
    engine = create_db_engine()

    try:
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                connection.execute(text('CREATE SCHEMA IF NOT EXISTS "processed"'))
                processed.to_sql(
                    staging,
                    connection,
                    schema="processed",
                    if_exists="fail",
                    index=False,
                    chunksize=chunksize,
                    method="multi",
                )
                db_count = connection.execute(
                    text(f'SELECT COUNT(*) FROM "processed"."{staging}"')
                ).scalar_one()
                base_ymd_nulls = connection.execute(
                    text(
                        f'SELECT COUNT(*) FROM "processed"."{staging}" '
                        'WHERE "base_ymd" IS NULL'
                    )
                ).scalar_one()
                if db_count != len(processed):
                    raise RuntimeError(
                        f"staging 건수 불일치: frame={len(processed)}, db={db_count}"
                    )
                print(f"raw_rows={len(raw)}")
                print(f"processed_rows={len(processed)}")
                print(f"staging_rows={db_count}")
                print(f"staging_base_ymd_nulls={base_ymd_nulls}")
                print("staging_insert=OK")
            finally:
                # 운영 테이블을 건드리지 않고 테스트 테이블 생성/적재도 되돌린다.
                transaction.rollback()
                print("staging_transaction=ROLLED_BACK")
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
