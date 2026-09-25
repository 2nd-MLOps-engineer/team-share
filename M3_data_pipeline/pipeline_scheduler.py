"""
M3 데이터 수집·정제·DB 적재 작업을 스케줄링하고 실행한다.

주요 역할:
1. 데이터셋별 collector를 지정된 주기에 실행한다.
2. 타임아웃, 연결 오류 등 일시적인 수집 오류에 한해서만 제한적으로 재시도한다.
3. 수집된 원본 데이터를 RAW 영역에 저장한다.
4. 원본 데이터를 dataset_processors.py의 데이터셋별 정제 과정을 거쳐 
   PROCESSED 영역에 저장한다.
5. 수집 완료 데이터를 재사용하여 DB 적재 실패 시 불필요한 재수집을 방지한다
6. 한 작업이 실패해도 다른 데이터셋 작업은 독립적으로 실행한다.
7. 실행 시작·성공·실패·재시도 상태를 로그로 남긴다.

문화빅데이터 수집은 기존 culture_bigdata_scheduler.py가
별도로 담당하므로 이 스케줄러에서 실행하지 않는다.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import timedelta, timezone
from pathlib import Path
from typing import Sequence

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

if __package__:
    from .pipeline_common import (
        WORKSPACE_ROOT,
        create_db_engine,
        load_csv_to_raw_and_processed,
        load_raw_to_processed,
        load_environment,
    )
else:
    from pipeline_common import (
        WORKSPACE_ROOT,
        create_db_engine,
        load_csv_to_raw_and_processed,
        load_raw_to_processed,
        load_environment,
    )


TIMEZONE = timezone(timedelta(hours=9), name="Asia/Seoul")
LOGGER = logging.getLogger("pipeline_scheduler")
PIPELINE_DIR = Path(__file__).resolve().parent
STATE_DIR = WORKSPACE_ROOT / "data" / "pipeline_state"


class CollectionStageError(RuntimeError):
    """재시도해도 해결되지 않는 collector 실행 실패."""


class TransientCollectionError(CollectionStageError):
    """timeout/연결/일시적 서버 오류로 재시도할 수 있는 수집 실패."""


@dataclass(frozen=True)
class OutputSpec:
    pattern: str
    table: str


@dataclass(frozen=True)
class JobSpec:
    script: str
    default_cron: str
    outputs: tuple[OutputSpec, ...] = field(default_factory=tuple)
    manages_database: bool = False
    default_attempts: int = 3


# 분/시/일/월/요일 (APScheduler CronTrigger.from_crontab 형식)
# 문화 빅데이터는 검증 완료된 culture_bigdata_scheduler.py가 독립 운영한다.
# 이 목록에 넣거나 해당 두 문화 빅데이터 파일을 래핑하지 않는다.
JOB_SPECS: dict[str, JobSpec] = {
    "weather": JobSpec(
        "collector/weather.py",
        "10,40 * * * *",
        (
            OutputSpec("data/raw/weather/weather_ultra_ncst.csv", "weather_ultra_ncst"),
            OutputSpec("data/raw/weather/weather_ultra_fcst.csv", "weather_ultra_fcst"),
        ),
    ),
    "air_quality": JobSpec(
        "collector/air_quality.py",
        "15 * * * *",
        (OutputSpec("data/raw/air_quality/air_quality_all.csv", "air_quality"),),
    ),
    "weather_warning": JobSpec(
        "collector/weather_warning.py",
        "*/10 * * * *",
        (OutputSpec("data/raw/weather_warning/weather_warning.csv", "weather_warning"),
            OutputSpec("data/raw/weather_warning/weather_warning_status.csv", "weather_warning_status"),),
    ),
    "bus_stop": JobSpec(
        "collector/busstop_api.py",
        "0 3 * * 0",
        (OutputSpec("data/raw/bus_stop/tago_bus_stops_all.csv", "bus_stop"),),
    ),
    "durunubi": JobSpec(
        "collector/durunubi_api.py",
        "0 4 * * 1",
        (OutputSpec("data/raw/durunubi/durunubi_trails.csv", "durunubi_trails"),
         OutputSpec("data/raw/durunubi/durunubi_segments.csv", "durunubi_segments"),),
    ),
    "facility": JobSpec(
        "collector/facility_api_v2.py",
        "0 4 1 * *",
        (OutputSpec("data/raw/facility/facility_all_*.csv", "facility"),),
    ),
    "public_open_facility": JobSpec(
        "collector/open_facil_api.py",
        "0 5 1 * *",
        (
            OutputSpec(
                "data/raw/public_open_facility/public_open_facility_all.csv",
                "public_open_facility",
            ),
        ),
    ),
    "aed": JobSpec(
        "collector/AED_api.py",
        "0 3 2 * *",
        (OutputSpec("data/raw/aed/aed.csv", "aed"),),
    ),
    "bicycle_accident": JobSpec(
        "collector/bicycle_accident_pipeline.py",
        "0 2 3 * *",
        (
            OutputSpec(
                "data/raw/koroad_bicycle/koroad_bicycle_accident_hotspots.csv",
                "koroad_bicycle_accident_hotspots",
            ),
        ),
        default_attempts=2,
    ),

    "culture_bigdata": JobSpec(
        "culture_bigdata_selenium.py",
        "0 7 1 * *",
        manages_database=True,
        default_attempts=2,
    ),
}

CULTURE_TABLES = (
    "culture_public_sports_facilities",
    "culture_public_sports_facility_programs",
    "culture_sports_facility_nearby_public_transport",
    "culture_national_sports_facility_status",
    "culture_location_fitness_measurement_prescriptions",
    "culture_open_school_sports_facilities",
    "culture_sports_facility_safety_inspections",
    "culture_fitness_measurement_prescriptions",
)



def _cron_for(job_id: str, spec: JobSpec) -> str:
    return os.getenv(f"{job_id.upper()}_CRON", spec.default_cron)


def _resolve_output(output: OutputSpec) -> Path:
    matches = sorted(
        WORKSPACE_ROOT.glob(output.pattern),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not matches:
        raise RuntimeError(f"수집 결과 CSV를 찾을 수 없습니다: {output.pattern}")
    return matches[0]


def _resolve_outputs(spec: JobSpec) -> list[tuple[OutputSpec, Path]]:
    return [(output, _resolve_output(output)) for output in spec.outputs]


def _state_path(job_id: str) -> Path:
    return STATE_DIR / f"{job_id}.json"


def _save_resume_state(
    job_id: str,
    outputs: list[tuple[OutputSpec, Path]],
) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    state_path = _state_path(job_id)
    temporary_path = state_path.with_suffix(".tmp.json")
    payload = {
        "job_id": job_id,
        "stage": "csv_ready",
        "outputs": [
            {
                "table": output.table,
                "path": str(path.resolve()),
                "size": path.stat().st_size,
                "mtime_ns": path.stat().st_mtime_ns,
            }
            for output, path in outputs
        ],
    }
    temporary_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary_path.replace(state_path)


def _load_resume_state(
    job_id: str,
    spec: JobSpec,
) -> list[tuple[OutputSpec, Path]] | None:
    state_path = _state_path(job_id)
    if not state_path.is_file() or spec.manages_database:
        return None
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        records = payload["outputs"]
        by_table = {record["table"]: record for record in records}
        resolved = []
        for output in spec.outputs:
            record = by_table[output.table]
            path = Path(record["path"])
            if (
                not path.is_file()
                or path.stat().st_size != record["size"]
                or path.stat().st_mtime_ns != record["mtime_ns"]
            ):
                return None
            resolved.append((output, path))
        return resolved
    except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
        LOGGER.warning("job=%s invalid resume state ignored: %s", job_id, state_path)
        return None


def _clear_resume_state(job_id: str) -> None:
    _state_path(job_id).unlink(missing_ok=True)


def _is_transient_collection_error(stderr: str) -> bool:
    message = stderr.lower()
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


def _run_script(spec: JobSpec) -> None:
    script_path = PIPELINE_DIR / spec.script
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
            f"collector exited with code {result.returncode}: {spec.script}"
        )


def _collect_with_transient_retry(job_id: str, spec: JobSpec) -> None:
    max_attempts = int(
        os.getenv(
            f"{job_id.upper()}_MAX_ATTEMPTS",
            os.getenv("PIPELINE_JOB_MAX_ATTEMPTS", str(spec.default_attempts)),
        )
    )
    retry_seconds = float(os.getenv("PIPELINE_JOB_RETRY_SECONDS", "30"))
    for attempt in range(1, max_attempts + 1):
        try:
            LOGGER.info(
                "job=%s stage=collect status=START attempt=%s/%s",
                job_id,
                attempt,
                max_attempts,
            )
            _run_script(spec)
            LOGGER.info("job=%s stage=collect status=OK", job_id)
            return
        except TransientCollectionError:
            LOGGER.exception(
                "job=%s stage=collect status=TRANSIENT_FAILED attempt=%s/%s",
                job_id,
                attempt,
                max_attempts,
            )
            if attempt == max_attempts:
                raise
            time.sleep(retry_seconds * attempt)


def _load_outputs_to_database(
    outputs: list[tuple[OutputSpec, Path]],
) -> None:
    engine = create_db_engine()
    try:
        for output, csv_path in outputs:
            load_csv_to_raw_and_processed(csv_path, output.table, engine)
    finally:
        engine.dispose()


def run_job(
    job_id: str,
    *,
    skip_db: bool = False,
    load_existing: bool = False,
) -> None:
    """수집만 일시 오류에 재시도하고, CSV 이후 실패는 기존 CSV에서 재개한다."""

    if job_id not in JOB_SPECS:
        raise KeyError(f"알 수 없는 job: {job_id}")

    spec = JOB_SPECS[job_id]
    LOGGER.info("job=%s status=START", job_id)

    if spec.manages_database:
        if load_existing:
            raise ValueError(
                f"{job_id}는 자체 DB 파이프라인이라 --load-existing 불가"
            )

        _collect_with_transient_retry(job_id, spec)

        if job_id == "culture_bigdata":
            LOGGER.info("job=%s stage=process status=START", job_id)

            for table in CULTURE_TABLES:
                load_raw_to_processed(table)

            LOGGER.info("job=%s stage=process status=OK", job_id)

        LOGGER.info("job=%s status=OK", job_id)
        return

    outputs = None if load_existing else _load_resume_state(job_id, spec)
    if outputs is not None:
        LOGGER.info("job=%s stage=collect status=SKIPPED reason=csv_ready", job_id)
    elif load_existing:
        outputs = _resolve_outputs(spec)
        LOGGER.info("job=%s stage=collect status=SKIPPED reason=load_existing", job_id)
        _save_resume_state(job_id, outputs)
    else:
        _collect_with_transient_retry(job_id, spec)
        outputs = _resolve_outputs(spec)
        _save_resume_state(job_id, outputs)

    if skip_db:
        _clear_resume_state(job_id)
        LOGGER.info("job=%s status=OK db=SKIPPED", job_id)
        return

    try:
        LOGGER.info("job=%s stage=database status=START", job_id)
        _load_outputs_to_database(outputs)
    except Exception:
        LOGGER.exception(
            "job=%s stage=database status=FAILED retry=DISABLED resume=csv_ready",
            job_id,
        )
        raise

    _clear_resume_state(job_id)
    LOGGER.info("job=%s stage=database status=OK", job_id)
    LOGGER.info("job=%s status=OK", job_id)


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
            trigger=CronTrigger.from_crontab(_cron_for(job_id, spec), timezone=TIMEZONE),
            args=[job_id],
            kwargs={"skip_db": skip_db},
            id=job_id,
            name=f"{job_id}: collect -> CSV -> raw/processed",
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
        help="API/CSV 수집만 실행하고 공통 DB 적재는 생략",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    load_environment()
    args = parse_args(argv)

    if args.list:
        for job_id, spec in JOB_SPECS.items():
            print(f"{job_id:22} {_cron_for(job_id, spec)}  {spec.script}")
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
        for job_id in JOB_SPECS:
            try:
                run_job(job_id, skip_db=args.skip_db)
            except Exception:
                LOGGER.exception("job=%s 최종 실패", job_id)
                failures.append(job_id)
        if failures:
            raise RuntimeError("실패 작업: " + ", ".join(failures))
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
