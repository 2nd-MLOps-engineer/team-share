from __future__ import annotations

from collections.abc import Mapping
import logging
import os
from typing import Any

import requests


LOGGER = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 10


def _get_webhook_url() -> str | None:
    """Discord Webhook URL을 환경변수에서 읽고, 미설정이면 None을 반환한다."""
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
    """Discord Webhook 메시지를 전송하고 성공 여부를 반환한다.

    URL 미설정 또는 requests.RequestException이면 False를 반환한다."""
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


def _format_duration(seconds: float | None) -> str:
    """초 단위 시간을 반올림한 뒤 시·분·초로 표시한다. None은 확인 불가로 표시한다."""
    if seconds is None:
        return "확인 불가"

    total_seconds = max(0, int(round(seconds)))
    minutes, secs = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)

    if hours:
        return f"{hours}시간 {minutes}분 {secs}초"
    if minutes:
        return f"{minutes}분 {secs}초"
    return f"{secs}초"


def _stage_label(stage: str | None) -> str:
    """실행 단계 코드를 알림용 이름으로 변환한다."""
    labels = {
        "collect": "데이터 수집",
        "load": "DB 적재 및 정제",
        "dq": "데이터 품질 검사",
        "monitoring": "운영 기록",
    }
    return labels.get(stage or "", stage or "확인 필요")


def send_pipeline_failure_alert(
    *,
    job_id: str,
    run_id: str,
    failure_stage: str | None,
    error: BaseException,
    elapsed_seconds: float,
) -> bool:
    """작업·실패 단계·실행시간·run_id를 전송한다. error 내용은 메시지에 포함하지 않는다."""

    stage_text = _stage_label(failure_stage)

    message = (
        "🚨 **우심운까 데이터 파이프라인 실패**\n\n"
        f"**작업**  `{job_id}`\n"
        f"**단계**  {stage_text}\n"
        "**상태**  실패\n\n"
        "⚠️ **확인 필요**\n"
        f"`{job_id}` 작업이 정상적으로 완료되지 못했습니다.\n"
        "상세 원인은 pipeline/error 로그에서 확인할 수 있습니다.\n\n"
        f"⏱ **소요시간**  {_format_duration(elapsed_seconds)}\n"
        f"🔎 **Run ID**  `{run_id}`"
    )

    return send_discord_message(message)


def send_dq_failure_alert(
    *,
    job_id: str,
    run_id: str,
    dq_status: str,
    dq_results: Mapping[str, Any],
    elapsed_seconds: float,
) -> bool:
    """CHECK_FAILED인 테이블과 작업의 DQ 상태를 알림으로 전송한다."""

    failed_datasets = [
        dataset
        for dataset, result in dq_results.items()
        if (
            result.as_dict().get("status")
            if hasattr(result, "as_dict")
            else result.get("status")
        )
        == "CHECK_FAILED"
    ]

    dataset_text = (
        ", ".join(f"`{dataset}`" for dataset in failed_datasets)
        if failed_datasets
        else "확인 필요"
    )

    message = (
        "⚠️ **우심운까 데이터 품질 이상 감지**\n\n"
        f"**작업**  `{job_id}`\n"
        f"**상태**  `{dq_status}`\n"
        f"**대상 테이블**  {dataset_text}\n\n"
        "🔎 **확인 필요**\n"
        "데이터 품질 검사에서 기준을 통과하지 못한 항목이 발견되었습니다.\n"
        "상세 DQ 결과는 monitoring 및 DQ 로그에서 확인할 수 있습니다.\n\n"
        f"⏱ **소요시간**  {_format_duration(elapsed_seconds)}\n"
        f"🔎 **Run ID**  `{run_id}`"
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
    warning_jobs: list[str],
    failed_jobs: list[str],
    dq_failed_datasets: list[str],
    max_duration_job: str | None,
) -> bool:
    """실행·DQ 집계와 경고/실패 작업 목록을 운영 요약 메시지로 전송한다."""

    # 상단 상태 요약
    if failed_runs or warning_runs or dq_failed:
        status_text = (
            "⚠️ **확인이 필요한 항목이 있습니다.**\n"
            f"실패 {failed_runs}건 · "
            f"경고 {warning_runs}건 · "
            f"DQ 실패 {dq_failed}건"
        )
    else:
        status_text = "✅ **현재 확인이 필요한 이상 항목이 없습니다.**"

    # 하단 조치 목록
    action_lines: list[str] = []

    if failed_jobs:
        failed_job_text = ", ".join(
            f"`{job}`" for job in failed_jobs
        )
        action_lines.extend([
            "🚨 **실패 작업**",
            failed_job_text,
            "→ 실패 원인 확인 후 재실행 여부 결정",
        ])

    # 전달받은 warning_jobs의 작업 이름과 확인 안내를 표시한다.
    if warning_jobs:
        if action_lines:
            action_lines.append("")

        warning_job_text = ", ".join(
            f"`{job}`" for job in warning_jobs
        )

        action_lines.extend([
            "⚠️ **경고 작업**",
            warning_job_text,
            "→ 경고 내용 확인",
        ])

    if dq_failed_datasets:
        if action_lines:
            action_lines.append("")

        dq_failed_text = ", ".join(
            f"`{dataset}`" for dataset in dq_failed_datasets
        )
        action_lines.extend([
            "🔎 **DQ 실패**",
            dq_failed_text,
            "→ 실패 항목과 영향 범위 확인",
        ])

    if action_lines:
        action_text = "\n".join(action_lines)
    else:
        action_text = "✅ **추가 조치 필요 없음**"

    message = (
        f"📊 **우심운까 운영 현황 · {period_label}**\n"
        f"`{period_start}` → `{period_end}`\n\n"

        f"{status_text}\n\n"

        "📌 **실행 현황**\n"
        f"전체 `{total_runs}`  ·  "
        f"정상 `{success_runs}`  ·  "
        f"경고 `{warning_runs}`  ·  "
        f"실패 `{failed_runs}`\n\n"

        "🔎 **데이터 품질**\n"
        f"검사 통과 `{dq_passed}`  ·  "
        f"실패 `{dq_failed}`\n\n"

        "⏱ **실행시간**\n"
        f"평균 `{_format_duration(avg_duration_seconds)}`\n"
        f"최대 `{_format_duration(max_duration_seconds)}`"
        f"{f' · `{max_duration_job}`' if max_duration_job else ''}\n\n"

        f"{action_text}"
    )

    return send_discord_message(message)
