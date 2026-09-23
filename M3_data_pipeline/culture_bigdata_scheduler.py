"""M3 collector jobs를 한 APScheduler 프로세스에서 실행한다."""

from __future__ import annotations

import logging
import os
from zoneinfo import ZoneInfo

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

if __package__:
    from . import culture_bigdata_selenium
else:
    import M3_data_pipeline.culture_bigdata_selenium as culture_bigdata_selenium


TIMEZONE = ZoneInfo("Asia/Seoul")
LOGGER = logging.getLogger("pipeline_scheduler")


def run_bigdata_culture() -> None:
    """수집, raw 적재, processed 생성을 하나의 성공/실패 단위로 실행한다."""

    LOGGER.info("job=bigdata_culture status=START")
    try:
        exit_code = culture_bigdata_selenium.main([])
        if exit_code != 0:
            raise RuntimeError(
                f"bigdata_culture_selenium exited with code {exit_code}"
            )
    except Exception:
        LOGGER.exception("job=bigdata_culture status=FAILED")
        raise
    LOGGER.info("job=bigdata_culture status=OK")


def create_scheduler() -> BlockingScheduler:
    """각 job이 독립적인 trigger를 갖는 단일 scheduler를 구성한다."""

    load_dotenv(culture_bigdata_selenium.ENV_PATH, override=False)
    scheduler = BlockingScheduler(
        timezone=TIMEZONE,
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 3600,
        },
    )
    job_definitions = {
        "bigdata_culture": {
            "func": run_bigdata_culture,
            "name": "Culture Bigdata Selenium → raw → processed",
            "trigger": CronTrigger(
                hour=int(os.getenv("CULTURE_BIGDATA_SCHEDULE_HOUR", "2")),
                minute=int(os.getenv("CULTURE_BIGDATA_SCHEDULE_MINUTE", "0")),
                timezone=TIMEZONE,
            ),
        },
    }
    for job_id, definition in job_definitions.items():
        scheduler.add_job(
            definition["func"],
            trigger=definition["trigger"],
            id=job_id,
            name=definition["name"],
            replace_existing=True,
        )
    return scheduler


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    scheduler = create_scheduler()
    LOGGER.info(
        "scheduler status=START jobs=%s timezone=%s",
        len(scheduler.get_jobs()),
        TIMEZONE,
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        LOGGER.info("scheduler status=STOPPED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
