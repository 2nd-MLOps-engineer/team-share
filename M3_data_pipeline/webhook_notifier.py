from __future__ import annotations

import logging
import os
from typing import Any

import requests


LOGGER = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 10


def _get_webhook_url() -> str | None:
    """Discord Webhook URL을 환경변수에서 읽는다."""
    url = os.getenv("DISCORD_WEBHOOK_URL")

    if not url:
        LOGGER.warning(
            "webhook status=SKIPPED reason=DISCORD_WEBHOOK_URL_NOT_SET"
        )
        return None

    return url


def send_discord_message(
    content: str,
    *,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
) -> bool:
    """
    Discord Webhook 메시지를 전송한다.

    Webhook 전송 실패가 데이터 파이프라인 자체를
    실패시키지 않도록 예외를 외부로 전파하지 않는다.
    """
    webhook_url = _get_webhook_url()

    if webhook_url is None:
        return False

    try:
        response = requests.post(
            webhook_url,
            json={"content": content},
            timeout=timeout,
        )
        response.raise_for_status()

        LOGGER.info(
            "webhook provider=discord status=SENT http_status=%s",
            response.status_code,
        )
        return True

    except requests.RequestException:
        LOGGER.exception(
            "webhook provider=discord status=FAILED"
        )
        return False


def send_pipeline_failure_alert(
    *,
    job_id: str,
    run_id: str,
    failure_stage: str | None,
    error: BaseException,
    elapsed_seconds: float,
) -> bool:
    """파이프라인 FAILED 즉시 알림."""
    message = (
        "🚨 **MOTIVE Pipeline FAILED**\n"
        f"**Job:** `{job_id}`\n"
        f"**Run ID:** `{run_id}`\n"
        f"**Stage:** `{failure_stage or 'unknown'}`\n"
        f"**Error:** `{type(error).__name__}: {error}`\n"
        f"**Elapsed:** `{elapsed_seconds:.3f}s`"
    )

    return send_discord_message(message)


def send_dq_failure_alert(
    *,
    job_id: str,
    run_id: str,
    dq_status: str,
    dq_results: dict[str, Any],
    elapsed_seconds: float,
) -> bool:
    """DQ CHECK_FAILED 즉시 알림."""
    failed_datasets = [
        dataset
        for dataset, result in dq_results.items()
        if result.get("status") == "CHECK_FAILED"
    ]

    dataset_text = (
        ", ".join(f"`{dataset}`" for dataset in failed_datasets)
        if failed_datasets
        else "확인 필요"
    )

    message = (
        "⚠️ **MOTIVE DATA QUALITY CHECK FAILED**\n"
        f"**Job:** `{job_id}`\n"
        f"**Run ID:** `{run_id}`\n"
        f"**DQ Status:** `{dq_status}`\n"
        f"**Dataset:** {dataset_text}\n"
        f"**Elapsed:** `{elapsed_seconds:.3f}s`"
    )

    return send_discord_message(message)


def send_operations_summary(
    *,
    period_label: str,
    period_start: str,
    period_end: str,
    total_runs: int,
    success_runs: int,
    warning_runs: int,
    failed_runs: int,
    dq_passed: int,
    dq_failed: int,
    avg_duration_seconds: float | None,
    max_duration_seconds: float | None,
    failed_jobs: list[str],
    dq_failed_datasets: list[str],
) -> bool:
    """08:00 / 17:00 정기 운영 보고."""
    failed_job_text = (
        ", ".join(f"`{job}`" for job in failed_jobs)
        if failed_jobs
        else "없음"
    )

    dq_failed_text = (
        ", ".join(f"`{dataset}`" for dataset in dq_failed_datasets)
        if dq_failed_datasets
        else "없음"
    )

    avg_text = (
        f"{avg_duration_seconds:.3f}s"
        if avg_duration_seconds is not None
        else "N/A"
    )

    max_text = (
        f"{max_duration_seconds:.3f}s"
        if max_duration_seconds is not None
        else "N/A"
    )

    message = (
        f"📋 **MOTIVE Operations Summary — {period_label}**\n"
        f"**Period:** `{period_start}` → `{period_end}`\n\n"
        f"**Runs:** `{total_runs}`\n"
        f"**Success:** `{success_runs}`\n"
        f"**Warning:** `{warning_runs}`\n"
        f"**Failed:** `{failed_runs}`\n"
        f"**DQ Passed:** `{dq_passed}`\n"
        f"**DQ Failed:** `{dq_failed}`\n"
        f"**Avg Runtime:** `{avg_text}`\n"
        f"**Max Runtime:** `{max_text}`\n\n"
        f"**Failed Jobs:** {failed_job_text}\n"
        f"**DQ Failed Datasets:** {dq_failed_text}"
    )

    return send_discord_message(message)