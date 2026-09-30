"""로컬 파이프라인 데이터를 Supabase의 m3_* 스키마로 복사합니다.

기본 실행은 변경 계획만 확인하며, --execute 옵션 사용 시 실제 반영합니다.
"""


from __future__ import annotations

import argparse
from collections import defaultdict
import os
from pathlib import Path
import time
from urllib.parse import urlsplit

from dotenv import load_dotenv
import psycopg
from psycopg import sql


ENV_PATH = Path(__file__).resolve().parent / "collector" / ".env"
SCHEMAS = {
    "raw": "m3_raw",
    "processed": "m3_processed",
    "derived": "m3_derived",
    "monitoring": "m3_monitoring",
}
RAW_TABLES = {
    "aed", "aed_raw", "air_quality", "culture_fitness_measurement_prescriptions",
    "culture_location_fitness_measurement_prescriptions",
    "culture_national_sports_facility_status",
    "culture_open_school_sports_facilities", "culture_public_sports_facilities",
    "culture_public_sports_facility_programs",
    "culture_sports_facility_nearby_public_transport",
    "culture_sports_facility_safety_inspections", "durunubi_segments",
    "durunubi_trails", "facility", "koroad_bicycle_accident_hotspots",
    "public_open_facility", "weather_ultra_fcst", "weather_ultra_ncst",
    "weather_warning", "weather_warning_status",
}
EXPECTED = {
    "raw": RAW_TABLES,
    "processed": RAW_TABLES - {"aed_raw"},
    "derived": {"facility_recommendation"},
    "monitoring": {"pipeline_run_history"},
}


def _metadata(source):
    with source.cursor() as cursor:
        cursor.execute("""
            SELECT n.nspname, c.relname, a.attname,
                   format_type(a.atttypid, a.atttypmod), a.attnotnull,
                   pg_get_expr(d.adbin, d.adrelid), a.attgenerated, a.attidentity
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid
                 AND a.attnum > 0 AND NOT a.attisdropped
            LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
            WHERE n.nspname IN ('raw','processed','derived','monitoring')
              AND c.relkind IN ('r','p')
            ORDER BY n.nspname, c.relname, a.attnum
        """)
        columns = defaultdict(list)
        for schema, table, name, typ, required, default, generated, identity in cursor:
            if generated or identity:
                raise ValueError(f"Unsupported generated/identity column: {schema}.{table}.{name}")
            columns[(schema, table)].append((name, typ, required, default))

        actual = defaultdict(set)
        for schema, table in columns:
            actual[schema].add(table)
        if dict(actual) != EXPECTED:
            raise ValueError("Source table list differs from the approved 41-table plan")

        cursor.execute("""
            SELECT n.nspname, c.relname, con.conname, pg_get_constraintdef(con.oid)
            FROM pg_constraint con JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname IN ('raw','processed','derived','monitoring')
            ORDER BY n.nspname, c.relname, con.conname
        """)
        constraints = defaultdict(list)
        for schema, table, name, definition in cursor:
            constraints[(schema, table)].append((name, definition))

        cursor.execute("""
            SELECT n.nspname, c.relname, pg_get_indexdef(i.oid),
                   EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = i.oid)
            FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid
            JOIN pg_class c ON c.oid = x.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname IN ('raw','processed','derived','monitoring')
            ORDER BY n.nspname, c.relname, i.relname
        """)
        indexes = defaultdict(list)
        for schema, table, definition, constraint_backed in cursor:
            if not constraint_backed:
                marker = f" ON {schema}.{table} "
                if definition.count(marker) != 1:
                    raise ValueError(f"Cannot safely map index on {schema}.{table}")
                indexes[(schema, table)].append(
                    definition.replace(marker, f" ON {SCHEMAS[schema]}.{table} ", 1)
                )
    return columns, constraints, indexes


def _check_target(target):
    with target.cursor() as cursor:
        cursor.execute("SELECT nspname FROM pg_namespace WHERE nspname = ANY(%s)",
                       (list(SCHEMAS.values()),))
        existing = [row[0] for row in cursor]
        if existing:
            raise ValueError(f"Proposed target schemas already exist: {existing}")
        cursor.execute("SELECT has_database_privilege(current_user,current_database(),'CREATE')")
        if not cursor.fetchone()[0]:
            raise ValueError("Target role cannot create isolated schemas")
    target.rollback()


def _create_table(target, schema, table, columns):
    definitions = []
    for name, typ, required, default in columns:
        pieces = [sql.Identifier(name), sql.SQL(" "), sql.SQL(typ)]
        if required:
            pieces.append(sql.SQL(" NOT NULL"))
        if default is not None:
            pieces.extend((sql.SQL(" DEFAULT "), sql.SQL(default)))
        definitions.append(sql.Composed(pieces))
    target.execute(
        sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(SCHEMAS[schema]), sql.Identifier(table),
            sql.SQL(", ").join(definitions),
        )
    )


def _copy_table(source, target, schema, table):
    source_name = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))
    target_name = sql.SQL("{}.{}").format(sql.Identifier(SCHEMAS[schema]), sql.Identifier(table))
    with source.cursor() as read_cursor, target.cursor() as write_cursor:
        with read_cursor.copy(sql.SQL("COPY {} TO STDOUT (FORMAT BINARY)").format(source_name)) as reader:
            with write_cursor.copy(sql.SQL("COPY {} FROM STDIN (FORMAT BINARY)").format(target_name)) as writer:
                for chunk in reader:
                    writer.write(chunk)


def _verify_count(source, target, schema, table):
    source_name = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))
    target_name = sql.SQL("{}.{}").format(sql.Identifier(SCHEMAS[schema]), sql.Identifier(table))
    with source.cursor() as source_cursor, target.cursor() as target_cursor:
        source_cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(source_name))
        target_cursor.execute(sql.SQL("SELECT count(*) FROM {}").format(target_name))
        source_count, target_count = source_cursor.fetchone()[0], target_cursor.fetchone()[0]
    if source_count != target_count:
        raise ValueError(f"Row count mismatch for {schema}.{table}: {source_count} != {target_count}")
    return source_count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="Write only new m3_* schemas")
    args = parser.parse_args()
    load_dotenv(ENV_PATH, override=False)
    if os.getenv("DB_HOST") not in {"localhost", "127.0.0.1", "::1"}:
        raise ValueError("Source DB_HOST must be local")
    remote_url = os.environ["SUPABASE_DATABASE_URL"]
    if not (urlsplit(remote_url).hostname or "").endswith(".supabase.com"):
        raise ValueError("Target must be the configured Supabase PostgreSQL host")

    source = psycopg.connect(
        host=os.environ["DB_HOST"], port=os.getenv("DB_PORT", "5432"),
        dbname=os.environ["DB_NAME"], user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"], autocommit=True,
    )
    target = psycopg.connect(remote_url, autocommit=False, connect_timeout=15)
    current = "preflight"
    try:
        source.execute("BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        columns, constraints, indexes = _metadata(source)
        _check_target(target)
        print(f"approved_tables={len(columns)} target_schemas={','.join(SCHEMAS.values())}", flush=True)
        if not args.execute:
            print("read_only_plan_ok", flush=True)
            return

        for target_schema in SCHEMAS.values():
            target.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(target_schema)))
        target.commit()
        print("isolated_schemas_created", flush=True)

        completed = 0
        total_rows = 0
        for schema, table in sorted(columns):
            current = f"{schema}.{table}"
            started = time.monotonic()
            try:
                _create_table(target, schema, table, columns[(schema, table)])
                _copy_table(source, target, schema, table)
                name = sql.SQL("{}.{}").format(sql.Identifier(SCHEMAS[schema]), sql.Identifier(table))
                for constraint_name, definition in constraints[(schema, table)]:
                    target.execute(sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} {}").format(
                        name, sql.Identifier(constraint_name), sql.SQL(definition)
                    ))
                for definition in indexes[(schema, table)]:
                    target.execute(definition)
                count = _verify_count(source, target, schema, table)
                target.commit()
            except Exception:
                target.rollback()
                raise
            completed += 1
            total_rows += count
            print(f"copied {current} -> {SCHEMAS[schema]}.{table} rows={count} "
                  f"elapsed_seconds={time.monotonic()-started:.1f} progress={completed}/41", flush=True)
        print(f"migration_complete tables={completed} rows={total_rows}", flush=True)
    except Exception as exc:
        print(f"migration_stopped_at={current} error_type={type(exc).__name__} "
              f"sqlstate={getattr(exc, 'sqlstate', None)}", flush=True)
        raise SystemExit(1) from None
    finally:
        try:
            source.execute("ROLLBACK")
        except Exception:
            pass
        source.close()
        target.close()


if __name__ == "__main__":
    main()
