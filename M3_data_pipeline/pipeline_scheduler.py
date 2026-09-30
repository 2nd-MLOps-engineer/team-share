"""
M3 데이터 통합 파이프라인 스케줄러.

데이터 수집부터 RAW 저장, 정제, PROCESSED 적재까지의
데이터 파이프라인 작업을 데이터셋별로 스케줄링하고 실행한다.

주요 역할:
1. 데이터셋별 collector를 지정된 주기에 실행한다.
2. 타임아웃, 연결 오류 등 일시적인 수집 오류에 한해서만 제한적으로 재시도한다.
3. collector가 수집 원본을 RAW DB에 적재한다.
4. 원본 데이터를 dataset_processors.py의 데이터셋별 정제 과정을 거쳐
   PROCESSED 영역에 저장한다.
5. --load-existing 실행 시 기존 RAW DB 데이터를 재사용한다.
6. 예약 작업은 독립 실행하며, --run-all-once는 실패해도 같은 phase를 계속한다.
   선행 phase에 실패가 있으면 run_last phase는 건너뛴다.
7. 실행 시작·성공·실패·재시도 상태를 로그로 남긴다.
8. single/multi-table 데이터셋을 같은 RAW DB 기반 metadata 계약으로 처리한다.
9. 실행 이력 기록과 실패/DQ 알림, 정기 운영 요약 전송을 수행한다.
   DQ CHECK_FAILED만으로 적재를 차단하지 않고 WARNING으로 기록한다.

--run-all-once는 metadata의 실행 phase/run_order 순서대로 직렬 실행된다.
run_last dataset은 앞선 phase가 모두 성공한 뒤 마지막에 실행된다.
"""

from __future__ import annotations

import argparse
import logging
from logging.handlers import RotatingFileHandler
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Sequence

from sqlalchemy.engine import Engine

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

if __package__:
    from .dq_profiler import DQProfileResult
    from .pipeline_elt import (
        WORKSPACE_ROOT,
        create_db_engine,
        load_dataset,
        load_environment,
    )
    from .pipeline_metadata import (
        DatasetSpec,
        get_dataset_specs,
    )
    from .pipeline_monitoring import (
        aggregate_dq_status,
        get_operations_summary,
        new_run_id,
        utc_now,
        write_pipeline_run_history,
    )
    from .webhook_notifier import (
        send_dq_failure_alert,
        send_operations_summary,
        send_pipeline_failure_alert,
    )
else:
    from dq_profiler import DQProfileResult
    from pipeline_elt import (
        WORKSPACE_ROOT,
        create_db_engine,
        load_dataset,
        load_environment,
    )
    from pipeline_metadata import (
        DatasetSpec,
        get_dataset_specs,
    )
    from pipeline_monitoring import (
        aggregate_dq_status,
        get_operations_summary,
        new_run_id,
        utc_now,
        write_pipeline_run_history,
    )
    from webhook_notifier import (
        send_dq_failure_alert,
        send_operations_summary,
        send_pipeline_failure_alert,
    )


TIMEZONE = timezone(timedelta(hours=9), name="Asia/Seoul")
LOGGER = logging.getLogger("pipeline_scheduler")
PIPELINE_DIR = Path(__file__).resolve().parent


class CollectionStageError(RuntimeError):
    """collector 실행 실패의 기본 예외. 일시적 오류는 하위 예외로 구분한다."""


class TransientCollectionError(CollectionStageError):
    """timeout/연결/일시적 서버 오류로 재시도할 수 있는 수집 실패."""


JOB_SPECS: dict[str, DatasetSpec] = dict(get_dataset_specs())


def _run_all_once_phases() -> tuple[tuple[str, ...], ...]:
    """run_order 순서의 일반 작업 phase와 run_last 작업 phase를 반환한다."""

    ordered = sorted(
        JOB_SPECS,
        key=lambda job_id: JOB_SPECS[job_id].run_order,
    )
    regular = tuple(
        job_id for job_id in ordered if not JOB_SPECS[job_id].run_last
    )
    deferred = tuple(
        job_id for job_id in ordered if JOB_SPECS[job_id].run_last
    )
    return tuple(phase for phase in (regular, deferred) if phase)


def _cron_for(job_id: str, spec: DatasetSpec) -> str:
    return os.getenv(f"{job_id.upper()}_CRON", spec.default_cron)


def _is_transient_collection_error(stderr: str) -> bool:
    message = stderr.lower()

    # data.go.kr 일일 호출 한도 초과는 같은 날 재시도해도 복구되지 않는다.
    # 일반 HTTP 429와 구분해서 scheduler 재시도 대상에서 제외한다.
    non_retryable_markers = (
        "limited_number_of_service_requests_exceeds_error",
        "returnreasoncode\": \"22",
        "일일 서비스 요청제한 횟수 초과",
    )
    if any(marker in message for marker in non_retryable_markers):
        return False

    markers = (
        "timeout",
        "timed out",
        "connectionerror",
        "connection error",
        "connection reset",
        "temporarily unavailable",
        "too many requests",
        "http 429",
        "http 502",
        "http 503",
        "http 504",
        "시간 초과",
        "네트워크 오류",
        "요청 최종 실패",
        "재시도 후에도 실패",
    )
    return any(marker in message for marker in markers)


def _run_script(spec: DatasetSpec) -> None:
    script_path = PIPELINE_DIR / spec.collector_script
    if not script_path.is_file():
        raise FileNotFoundError(script_path)

    environment = os.environ.copy()
    environment.setdefault("PYTHONUTF8", "1")
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=WORKSPACE_ROOT,
        env=environment,
        check=False,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if result.stderr:
        sys.stderr.write(result.stderr)
    if result.returncode != 0:
        error_type = (
            TransientCollectionError
            if _is_transient_collection_error(result.stderr or "")
            else CollectionStageError
        )
        raise error_type(
            f"collector exited with code {result.returncode}: {spec.collector_script}"
        )


def _collect_with_transient_retry(job_id: str, spec: DatasetSpec) -> None:
    max_attempts = int(
        os.getenv(
            f"{job_id.upper()}_MAX_ATTEMPTS",
            os.getenv("PIPELINE_JOB_MAX_ATTEMPTS", str(spec.default_attempts)),
        )
    )
    retry_seconds = float(os.getenv("PIPELINE_JOB_RETRY_SECONDS", "30"))
    for attempt in range(1, max_attempts + 1):
        attempt_started = time.monotonic()
        try:
            LOGGER.info(
                "job=%s stage=collect status=START attempt=%s/%s",
                job_id,
                attempt,
                max_attempts,
            )
            _run_script(spec)
            LOGGER.info(
                "job=%s stage=collect status=OK attempt=%s/%s "
                "elapsed_seconds=%.3f",
                job_id,
                attempt,
                max_attempts,
                time.monotonic() - attempt_started,
            )
            return
        except TransientCollectionError:
            LOGGER.exception(
                "job=%s stage=collect status=TRANSIENT_FAILED attempt=%s/%s "
                "elapsed_seconds=%.3f",
                job_id,
                attempt,
                max_attempts,
                time.monotonic() - attempt_started,
            )
            if attempt == max_attempts:
                raise
            time.sleep(retry_seconds * attempt)
        except Exception:
            LOGGER.exception(
                "job=%s stage=collect status=FAILED attempt=%s/%s "
                "elapsed_seconds=%.3f",
                job_id,
                attempt,
                max_attempts,
                time.monotonic() - attempt_started,
            )
            raise


def _process_and_load_dataset(
    spec: DatasetSpec,
    engine: Engine,
) -> tuple[
    dict[str, tuple[int, int]],
    dict[str, DQProfileResult],
]:
    return load_dataset(spec, engine)


def run_job(
    job_id: str,
    *,
    skip_db: bool = False,
    load_existing: bool = False,
) -> None:
    """일시적 수집 오류만 재시도하고 collector가 적재한 RAW를 공통 ELT로 처리한다.

    load_existing은 수집을 생략한다. skip_db는 공통 정제·PROCESSED 적재와
    성공 이력 기록을 생략하며, collector 자체의 RAW DB 적재는 수행된다."""

    if job_id not in JOB_SPECS:
        raise KeyError(f"알 수 없는 job: {job_id}")

    job_started = time.monotonic()
    started_at = utc_now()
    run_id = new_run_id()

    spec = JOB_SPECS[job_id]

    results = None
    dq_results = None
    engine = None

    collect_seconds = 0.0
    load_seconds = 0.0
    failure_stage = None

    LOGGER.info(
        "job=%s run_id=%s status=START",
        job_id,
        run_id,
    )

    try:
        collect_started = time.monotonic()
        failure_stage = "collect"

        if load_existing:
            LOGGER.info(
                "job=%s stage=collect status=SKIPPED reason=load_existing_raw",
                job_id,
            )
        else:
            _collect_with_transient_retry(job_id, spec)

        collect_seconds = time.monotonic() - collect_started

        if skip_db:
            LOGGER.info(
                "job=%s run_id=%s status=OK db=SKIPPED elapsed_seconds=%.3f",
                job_id,
                run_id,
                time.monotonic() - job_started,
            )
            return

        failure_stage = "load"
        load_started = time.monotonic()

        LOGGER.info(
            "job=%s stage=load status=START",
            job_id,
        )

        engine = create_db_engine()

        results, dq_results = _process_and_load_dataset(
            spec,
            engine,
        )

        load_seconds = time.monotonic() - load_started

        LOGGER.info(
            "job=%s stage=load status=OK elapsed_seconds=%.3f",
            job_id,
            load_seconds,
        )

        dq_status = aggregate_dq_status(dq_results)
        run_status = (
            "WARNING"
            if dq_status == "CHECK_FAILED"
            else "SUCCESS"
        )


        failure_stage = None
        finished_at = utc_now()
        total_seconds = time.monotonic() - job_started

        try:
            write_pipeline_run_history(
                engine,
                run_id=run_id,
                job_id=job_id,
                dataset=job_id,
                source_kind=spec.source_kind.value,
                run_status=run_status,
                started_at=started_at,
                results=results,
                dq_results=dq_results,
                stage_durations={
                    "collect_seconds": collect_seconds,
                    "load_seconds": load_seconds,
                    "total_seconds": total_seconds,
                },
                finished_at=finished_at,
            )
        except Exception:
            LOGGER.exception(
                "job=%s run_id=%s stage=monitoring status=FAILED",
                job_id,
                run_id,
            )

        LOGGER.info(
            "job=%s run_id=%s status=%s dq_status=%s "
            "elapsed_seconds=%.3f",
            job_id,
            run_id,
            run_status,
            dq_status,
            total_seconds,
        )

        if dq_status == "CHECK_FAILED":
            send_dq_failure_alert(
                job_id=job_id,
                run_id=run_id,
                dq_status=dq_status,
                dq_results=dq_results,
                elapsed_seconds=total_seconds,
            )

    except Exception as error:
        total_seconds = time.monotonic() - job_started

        LOGGER.exception(
            "job=%s run_id=%s stage=%s status=FAILED "
            "elapsed_seconds=%.3f",
            job_id,
            run_id,
            failure_stage,
            total_seconds,
        )

        try:
            monitoring_engine = engine or create_db_engine()

            write_pipeline_run_history(
                monitoring_engine,
                run_id=run_id,
                job_id=job_id,
                dataset=job_id,
                source_kind=spec.source_kind.value,
                run_status="FAILED",
                started_at=started_at,
                results=results,
                dq_results=dq_results,
                stage_durations={
                    "collect_seconds": collect_seconds,
                    "load_seconds": load_seconds,
                    "total_seconds": total_seconds,
                },
                failure_stage=failure_stage,
                error=error,
                finished_at=utc_now(),
            )

            if engine is None:
                monitoring_engine.dispose()

        except Exception:
            LOGGER.exception(
                "job=%s run_id=%s stage=monitoring status=FAILED "
                "original_failure_stage=%s",
                job_id,
                run_id,
                failure_stage,
            )

        send_pipeline_failure_alert(
            job_id=job_id,
            run_id=run_id,
            failure_stage=failure_stage,
            error=error,
            elapsed_seconds=total_seconds,
        )

        raise

    finally:
        if engine is not None:
            engine.dispose()


def send_scheduled_operations_summary() -> None:
    """09:15 / 17:00 KST 기준 파이프라인 운영 현황을 Discord로 전송한다."""
    load_environment()

    now = datetime.now(TIMEZONE).astimezone(TIMEZONE)

    if now.hour == 9 and now.minute == 15:
        period_end = now.replace(
            hour=9,
            minute=15,
            second=0,
            microsecond=0,
        )
        period_start = (
            period_end - timedelta(days=1)
        ).replace(
            hour=17,
            minute=0,
        )

        period_label = "운영 결과 · 야간"

    elif now.hour == 17:
        period_end = now.replace(
            hour=17,
            minute=0,
            second=0,
            microsecond=0,
        )
        period_start = period_end.replace(
            hour=9,
            minute=15,
        )
        period_label = "운영 결과 · 주간"

    else:
        LOGGER.warning(
            "operations_summary status=SKIPPED unexpected_time=%s",
            now.isoformat(),
        )
        return

    engine = create_db_engine()

    try:
        summary = get_operations_summary(
            engine,
            period_start=period_start,
            period_end=period_end,
        )

        send_operations_summary(
            period_label=period_label,
            period_start=period_start.isoformat(),
            period_end=period_end.isoformat(),
            **summary,
        )

        LOGGER.info(
            "operations_summary status=COMPLETE "
            "period_start=%s period_end=%s total_runs=%s",
            period_start.isoformat(),
            period_end.isoformat(),
            summary["total_runs"],
        )

    except Exception:
        LOGGER.exception(
            "operations_summary status=FAILED "
            "period_start=%s period_end=%s",
            period_start.isoformat(),
            period_end.isoformat(),
        )

    finally:
        engine.dispose()

def create_scheduler(*, skip_db: bool = False) -> BlockingScheduler:
    load_environment()
    scheduler = BlockingScheduler(
        timezone=TIMEZONE,
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 3600,
        },
    )
    for job_id, spec in JOB_SPECS.items():
        scheduler.add_job(
            run_job,
            trigger=CronTrigger.from_crontab(
                _cron_for(job_id, spec),
                timezone=TIMEZONE
            ),
            args=[job_id],
            kwargs={"skip_db": skip_db},
            id=job_id,
            name=f"{job_id}: collect -> process -> database",
            replace_existing=True,
        )

    scheduler.add_job(
        send_scheduled_operations_summary,
        trigger=CronTrigger(
            hour=9,
            minute=15,
            timezone=TIMEZONE,
        ),
        id="operations_summary_morning",
        name="09:15 pipeline operations summary",
        replace_existing=True,
    )

    scheduler.add_job(
        send_scheduled_operations_summary,
        trigger=CronTrigger(
            hour=17,
            minute=0,
            timezone=TIMEZONE,
        ),
        id="operations_summary_evening",
        name="17:00 pipeline operations summary",
        replace_existing=True,
    )

    return scheduler



def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--run-once", choices=sorted(JOB_SPECS))
    action.add_argument("--load-existing", choices=sorted(JOB_SPECS))
    action.add_argument("--run-all-once", action="store_true")
    action.add_argument("--list", action="store_true")
    parser.add_argument(
        "--skip-db",
        action="store_true",
        help="collector만 실행하고 공통 process/database 단계는 생략",
    )
    return parser.parse_args(argv)

class DQAuditFilter(logging.Filter):
    """dq_audit 로그 레코드만 통과시킨다."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.getMessage().startswith("dq_audit=")


class ExcludeDQAuditFilter(logging.Filter):
    """터미널에서는 상세 dq_audit JSON 레코드를 숨긴다."""

    def filter(self, record: logging.LogRecord) -> bool:
        return not record.getMessage().startswith("dq_audit=")


def configure_logging() -> None:
    """메인 데이터 파이프라인의 콘솔 및 영구 파일 로그를 설정한다."""

    log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # 중복 handler 방지
    root_logger.handlers.clear()

    # 1. 콘솔
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(ExcludeDQAuditFilter())
    root_logger.addHandler(console_handler)

    # 2. 전체 파이프라인 실행 로그
    pipeline_handler = RotatingFileHandler(
        log_dir / "pipeline.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    pipeline_handler.setLevel(logging.INFO)
    pipeline_handler.setFormatter(formatter)
    root_logger.addHandler(pipeline_handler)

    # 3. ERROR 이상 전용 로그
    error_handler = RotatingFileHandler(
        log_dir / "error.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    root_logger.addHandler(error_handler)

    # 4. DQ 감사 로그
    dq_handler = RotatingFileHandler(
        log_dir / "dq_audit.log",
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    dq_handler.setLevel(logging.INFO)
    dq_handler.setFormatter(formatter)
    dq_handler.addFilter(DQAuditFilter())
    root_logger.addHandler(dq_handler)


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    load_environment()
    args = parse_args(argv)

    if args.list:
        for job_id, spec in JOB_SPECS.items():
            print(
                f"{job_id:22} {_cron_for(job_id, spec)}  "
                f"{spec.collector_script}"
            )
        return 0
    if args.run_once:
        run_job(args.run_once, skip_db=args.skip_db)
        return 0
    if args.load_existing:
        if args.skip_db:
            raise ValueError("--load-existing과 --skip-db는 함께 사용할 수 없습니다.")
        run_job(args.load_existing, load_existing=True)
        return 0
    if args.run_all_once:
        failures: list[str] = []
        skipped: list[str] = []
        for phase_number, job_ids in enumerate(_run_all_once_phases(), start=1):
            if failures:
                skipped.extend(job_ids)
                LOGGER.warning(
                    "run_all phase=%s status=SKIPPED jobs=%s "
                    "reason=prior_phase_failed",
                    phase_number,
                    ",".join(job_ids),
                )
                break

            for job_id in job_ids:
                try:
                    run_job(job_id, skip_db=args.skip_db)
                except Exception:
                    LOGGER.exception("job=%s 최종 실패", job_id)
                    failures.append(job_id)
        if failures:
            message = "실패 작업: " + ", ".join(failures)
            if skipped:
                message += " / 선행 실패로 미실행: " + ", ".join(skipped)
            raise RuntimeError(message)
        return 0

    scheduler = create_scheduler(skip_db=args.skip_db)
    LOGGER.info("scheduler status=START jobs=%s timezone=%s", len(JOB_SPECS), TIMEZONE)
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        LOGGER.info("scheduler status=STOPPED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
