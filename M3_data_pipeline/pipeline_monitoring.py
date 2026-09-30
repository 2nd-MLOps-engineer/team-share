"""파이프라인 실행 이력과 상태를 monitoring.pipeline_run_history에 기록합니다."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.engine import Engine


MONITORING_SCHEMA = "monitoring"
MONITORING_TABLE = "pipeline_run_history"


def new_run_id() -> str:
    """파이프라인 실행을 식별하는 고유 run_id를 생성한다."""
    return uuid4().hex


def utc_now() -> datetime:
    """UTC 기준 현재 시각을 반환한다."""
    return datetime.now(timezone.utc)


def aggregate_dq_status(
    dq_results: Mapping[str, Any] | None,
) -> str | None:
    """테이블별 DQ 결과를 run-level DQ 상태로 집계한다."""
    if not dq_results:
        return None

    statuses: list[str] = []

    for dq_result in dq_results.values():
        if hasattr(dq_result, "as_dict"):
            dq_result = dq_result.as_dict()

        if isinstance(dq_result, Mapping):
            status = dq_result.get("status")
            if status is not None:
                statuses.append(str(status))

    if not statuses:
        return None
    if "CHECK_FAILED" in statuses:
        return "CHECK_FAILED"
    if all(status == "CHECK_PASSED" for status in statuses):
        return "CHECK_PASSED"
    return "CHECK_FAILED"

def _json_ready_mapping(
    values: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    """Mapping 내부의 DQ 객체 등을 JSON 직렬화 가능한 값으로 변환한다."""
    if values is None:
        return None

    result: dict[str, Any] = {}

    for key, value in values.items():
        if hasattr(value, "as_dict"):
            value = value.as_dict()

        result[str(key)] = value

    return result


def ensure_monitoring_table(engine: Engine) -> None:
    """monitoring.pipeline_run_history 테이블이 없으면 생성한다."""

    ddl = """
    CREATE SCHEMA IF NOT EXISTS monitoring;

    CREATE TABLE IF NOT EXISTS monitoring.pipeline_run_history (
        run_id              TEXT PRIMARY KEY,
        job_id              TEXT NOT NULL,
        dataset             TEXT,
        source_kind         TEXT,

        run_status          TEXT NOT NULL,
        dq_status           TEXT,

        results             JSONB,
        dq_results          JSONB,
        stage_durations     JSONB,

        failure_stage       TEXT,
        error_type          TEXT,
        error_message       TEXT,

        started_at          TIMESTAMPTZ NOT NULL,
        finished_at         TIMESTAMPTZ,
        elapsed_seconds     DOUBLE PRECISION,

        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """

    with engine.begin() as connection:
        connection.execute(text(ddl))


def write_pipeline_run_history(
    engine: Engine,
    *,
    run_id: str,
    job_id: str,
    run_status: str,
    started_at: datetime,
    dataset: str | None = None,
    source_kind: str | None = None,
    results: Mapping[str, Any] | None = None,
    dq_results: Mapping[str, Any] | None = None,
    stage_durations: Mapping[str, float] | None = None,
    failure_stage: str | None = None,
    error: BaseException | None = None,
    finished_at: datetime | None = None,
) -> None:
    """
    파이프라인 실행 결과를 별도 transaction으로 기록한다.

    데이터 적재 transaction과 분리하여 데이터 적재가 실패하더라도
    실행 이력 자체는 보존할 수 있도록 한다.
    """

    ensure_monitoring_table(engine)

    finished_at = finished_at or utc_now()
    elapsed_seconds = (finished_at - started_at).total_seconds()

    json_results = _json_ready_mapping(results)
    json_dq_results = _json_ready_mapping(dq_results)
    dq_status = aggregate_dq_status(dq_results)

    statement = text(
        """
        INSERT INTO monitoring.pipeline_run_history (
            run_id,
            job_id,
            dataset,
            source_kind,
            run_status,
            dq_status,
            results,
            dq_results,
            stage_durations,
            failure_stage,
            error_type,
            error_message,
            started_at,
            finished_at,
            elapsed_seconds
        )
        VALUES (
            :run_id,
            :job_id,
            :dataset,
            :source_kind,
            :run_status,
            :dq_status,
            CAST(:results AS JSONB),
            CAST(:dq_results AS JSONB),
            CAST(:stage_durations AS JSONB),
            :failure_stage,
            :error_type,
            :error_message,
            :started_at,
            :finished_at,
            :elapsed_seconds
        )
        ON CONFLICT (run_id)
        DO UPDATE SET
            dataset = EXCLUDED.dataset,
            source_kind = EXCLUDED.source_kind,
            run_status = EXCLUDED.run_status,
            dq_status = EXCLUDED.dq_status,
            results = EXCLUDED.results,
            dq_results = EXCLUDED.dq_results,
            stage_durations = EXCLUDED.stage_durations,
            failure_stage = EXCLUDED.failure_stage,
            error_type = EXCLUDED.error_type,
            error_message = EXCLUDED.error_message,
            finished_at = EXCLUDED.finished_at,
            elapsed_seconds = EXCLUDED.elapsed_seconds
        """
    )

    parameters = {
        "run_id": run_id,
        "job_id": job_id,
        "dataset": dataset,
        "source_kind": source_kind,
        "run_status": run_status,
        "dq_status": dq_status,
        "results": (
            json.dumps(json_results, ensure_ascii=False)
            if json_results is not None
            else None
        ),
        "dq_results": (
            json.dumps(json_dq_results, ensure_ascii=False)
            if json_dq_results is not None
            else None
        ),
        "stage_durations": (
            json.dumps(stage_durations)
            if stage_durations is not None
            else None
        ),
        "failure_stage": failure_stage,
        "error_type": (
            type(error).__name__
            if error is not None
            else None
        ),
        "error_message": (
            str(error)
            if error is not None
            else None
        ),
        "started_at": started_at,
        "finished_at": finished_at,
        "elapsed_seconds": elapsed_seconds,
    }

    with engine.begin() as connection:
        connection.execute(statement, parameters)


def get_operations_summary(
    engine: Engine,
    *,
    period_start: datetime,
    period_end: datetime,
) -> dict[str, Any]:
    """
    monitoring.pipeline_run_history에서 지정 기간의 운영 현황을 집계한다.

    조회 구간은 [period_start, period_end) 이다.
    즉 시작 시각은 포함하고 종료 시각은 포함하지 않는다.
    """
    statement = text(
        """
        SELECT
            COUNT(*) AS total_runs,

            COUNT(*) FILTER (
                WHERE run_status = 'SUCCESS'
            ) AS success_runs,

            COUNT(*) FILTER (
                WHERE run_status = 'WARNING'
            ) AS warning_runs,

            COUNT(*) FILTER (
                WHERE run_status = 'FAILED'
            ) AS failed_runs,

            COUNT(*) FILTER (
                WHERE dq_status = 'CHECK_PASSED'
            ) AS dq_passed,

            COUNT(*) FILTER (
                WHERE dq_status = 'CHECK_FAILED'
            ) AS dq_failed,
            AVG(elapsed_seconds) AS avg_duration_seconds,
            MAX(elapsed_seconds) AS max_duration_seconds,

                        ARRAY_AGG(DISTINCT job_id) FILTER (
                WHERE run_status = 'WARNING'
            ) AS warning_jobs,

            ARRAY_AGG(DISTINCT job_id) FILTER (
                WHERE run_status = 'FAILED'
            ) AS failed_jobs,

            ARRAY_AGG(DISTINCT dataset) FILTER (
                WHERE dq_status = 'CHECK_FAILED'
                  AND dataset IS NOT NULL
            ) AS dq_failed_datasets,

            (
                ARRAY_AGG(
                    job_id
                    ORDER BY elapsed_seconds DESC NULLS LAST
                )
            )[1] AS max_duration_job
        FROM monitoring.pipeline_run_history
        WHERE started_at >= :period_start
          AND started_at < :period_end
        """
    )

    with engine.connect() as connection:
        row = connection.execute(
            statement,
            {
                "period_start": period_start,
                "period_end": period_end,
            },
        ).mappings().one()

    return {
        "total_runs": int(row["total_runs"] or 0),
        "success_runs": int(row["success_runs"] or 0),
        "warning_runs": int(row["warning_runs"] or 0),
        "failed_runs": int(row["failed_runs"] or 0),
        "dq_passed": int(row["dq_passed"] or 0),
        "dq_failed": int(row["dq_failed"] or 0),
        "avg_duration_seconds": (
            float(row["avg_duration_seconds"])
            if row["avg_duration_seconds"] is not None
            else None
        ),
        "max_duration_seconds": (
            float(row["max_duration_seconds"])
            if row["max_duration_seconds"] is not None
            else None
        ),
        "warning_jobs": list(row["warning_jobs"] or []),
        "failed_jobs": list(row["failed_jobs"] or []),
        "dq_failed_datasets": list(
            row["dq_failed_datasets"] or []
        ),
        "max_duration_job": row["max_duration_job"],
    }