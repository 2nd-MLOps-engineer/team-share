from __future__ import annotations

from collections.abc import Mapping
import logging
import os
from typing import Any

import requests


LOGGER = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 10


def _get_webhook_url() -> str | None:
    """Discord Webhook URL???˜ê²½ë³€?˜ì—???½ëŠ”??"""
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
    Discord Webhook ë©”ì‹œì§€ë¥??„ì†¡?œë‹¤.

    Webhook ?„ì†¡ ?¤íŒ¨ê°€ ?°ì´???Œì´?„ë¼???ì²´ë¥?
    ?¤íŒ¨?œí‚¤ì§€ ?Šë„ë¡??ˆì™¸ë¥??¸ë?ë¡??„íŒŒ?˜ì? ?ŠëŠ”??
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


def _format_duration(seconds: float | None) -> str:
    """ì´??¨ìœ„ ?¤í–‰ ?œê°„???¬ëŒ???½ê¸° ?¬ìš´ ?•íƒœë¡?ë³€?˜í•œ??"""
    if seconds is None:
        return "?•ì¸ ë¶ˆê?"

    total_seconds = max(0, int(round(seconds)))
    minutes, secs = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)

    if hours:
        return f"{hours}?œê°„ {minutes}ë¶?{secs}ì´?
    if minutes:
        return f"{minutes}ë¶?{secs}ì´?
    return f"{secs}ì´?


def _stage_label(stage: str | None) -> str:
    """?´ë? stage ?´ë¦„???´ì˜?ìš© ?œí˜„?¼ë¡œ ë³€?˜í•œ??"""
    labels = {
        "collect": "?°ì´???˜ì§‘",
        "load": "DB ?ì¬ ë°??•ì œ",
        "dq": "?°ì´???ˆì§ˆ ê²€??,
        "monitoring": "?´ì˜ ê¸°ë¡",
    }
    return labels.get(stage or "", stage or "?•ì¸ ?„ìš”")


def send_pipeline_failure_alert(
    *,
    job_id: str,
    run_id: str,
    failure_stage: str | None,
    error: BaseException,
    elapsed_seconds: float,
) -> bool:
    """?Œì´?„ë¼???¤í–‰ ?¤íŒ¨ë¥??´ì˜?ìš© ë©”ì‹œì§€ë¡??„ì†¡?œë‹¤."""

    stage_text = _stage_label(failure_stage)

    message = (
        "?š¨ **?°ì‹¬?´ê¹Œ ?°ì´???Œì´?„ë¼???¤íŒ¨**\n\n"
        f"**?‘ì—…**  `{job_id}`\n"
        f"**?¨ê³„**  {stage_text}\n"
        "**?íƒœ**  ?¤íŒ¨\n\n"
        "? ï¸ **?•ì¸ ?„ìš”**\n"
        f"`{job_id}` ?‘ì—…???•ìƒ?ìœ¼ë¡??„ë£Œ?˜ì? ëª»í–ˆ?µë‹ˆ??\n"
        "?ì„¸ ?ì¸?€ pipeline/error ë¡œê·¸?ì„œ ?•ì¸?????ˆìŠµ?ˆë‹¤.\n\n"
        f"??**?Œìš”?œê°„**  {_format_duration(elapsed_seconds)}\n"
        f"?” **Run ID**  `{run_id}`"
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
    """?°ì´???ˆì§ˆ ê²€???¤íŒ¨ë¥??´ì˜?ìš© ë©”ì‹œì§€ë¡??„ì†¡?œë‹¤."""

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
        else "?•ì¸ ?„ìš”"
    )

    message = (
        "? ï¸ **?°ì‹¬?´ê¹Œ ?°ì´???ˆì§ˆ ?´ìƒ ê°ì?**\n\n"
        f"**?‘ì—…**  `{job_id}`\n"
        f"**?íƒœ**  `{dq_status}`\n"
        f"**?€???°ì´??*  {dataset_text}\n\n"
        "?” **?•ì¸ ?„ìš”**\n"
        "?°ì´???ˆì§ˆ ê²€?¬ì—??ê¸°ì????µê³¼?˜ì? ëª»í•œ ??ª©??ë°œê²¬?˜ì—ˆ?µë‹ˆ??\n"
        "?ì„¸ DQ ê²°ê³¼??monitoring ë°?DQ ë¡œê·¸?ì„œ ?•ì¸?????ˆìŠµ?ˆë‹¤.\n\n"
        f"??**?Œìš”?œê°„**  {_format_duration(elapsed_seconds)}\n"
        f"?” **Run ID**  `{run_id}`"
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
    """?•ê¸° ?´ì˜ ?„í™©???´ì˜?ìš© ë©”ì‹œì§€ë¡??„ì†¡?œë‹¤."""

    # ?ë‹¨ ?íƒœ ?”ì•½
    if failed_runs or warning_runs or dq_failed:
        status_text = (
            "? ï¸ **?•ì¸???„ìš”????ª©???ˆìŠµ?ˆë‹¤.**\n"
            f"?¤íŒ¨ {failed_runs}ê±?Â· "
            f"ê²½ê³  {warning_runs}ê±?Â· "
            f"DQ ?¤íŒ¨ {dq_failed}ê±?
        )
    else:
        status_text = "??**?„ì¬ ?•ì¸???„ìš”???´ìƒ ??ª©???†ìŠµ?ˆë‹¤.**"

    # ?˜ë‹¨ ì¡°ì¹˜ ?ì—­
    action_lines: list[str] = []

    if failed_jobs:
        failed_job_text = ", ".join(
            f"`{job}`" for job in failed_jobs
        )
        action_lines.extend([
            "?š¨ **?¤íŒ¨ ?‘ì—…**",
            failed_job_text,
            "???¤íŒ¨ ?ì¸ ?•ì¸ ???¬ì‹¤???¬ë? ê²°ì •",
        ])

    # ?„ì¬??warning job ?´ë¦„???„ë‹¬ë°›ì? ?Šìœ¼ë¯€ë¡?
    # ê±´ìˆ˜ë§??œì‹œ?˜ê³  êµ¬ì²´ ?€?ì? ?œì‹œ?˜ì? ?ŠëŠ”??
    if warning_jobs:
        if action_lines:
            action_lines.append("")

        warning_job_text = ", ".join(
            f"`{job}`" for job in warning_jobs
        )

        action_lines.extend([
            "? ï¸ **ê²½ê³  ?‘ì—…**",
            warning_job_text,
            "??ê²½ê³  ?´ìš© ?•ì¸",
        ])

    if dq_failed_datasets:
        if action_lines:
            action_lines.append("")

        dq_failed_text = ", ".join(
            f"`{dataset}`" for dataset in dq_failed_datasets
        )
        action_lines.extend([
            "?” **DQ ?¤íŒ¨**",
            dq_failed_text,
            "???¤íŒ¨ ??ª©ê³??í–¥ ë²”ìœ„ ?•ì¸",
        ])

    if action_lines:
        action_text = "\n".join(action_lines)
    else:
        action_text = "??**ì¶”ê? ì¡°ì¹˜ ?„ìš” ?†ìŒ**"

    message = (
        f"?“Š **?°ì‹¬?´ê¹Œ ?´ì˜ ?„í™© Â· {period_label}**\n"
        f"`{period_start}` ??`{period_end}`\n\n"

        f"{status_text}\n\n"

        "?“Œ **?¤í–‰ ?„í™©**\n"
        f"?„ì²´ `{total_runs}`  Â·  "
        f"?•ìƒ `{success_runs}`  Â·  "
        f"ê²½ê³  `{warning_runs}`  Â·  "
        f"?¤íŒ¨ `{failed_runs}`\n\n"

        "?” **?°ì´???ˆì§ˆ**\n"
        f"ê²€???µê³¼ `{dq_passed}`  Â·  "
        f"?¤íŒ¨ `{dq_failed}`\n\n"

        "??**?¤í–‰?œê°„**\n"
        f"?‰ê·  `{_format_duration(avg_duration_seconds)}`\n"
        f"ìµœë? `{_format_duration(max_duration_seconds)}`"
        f"{f' Â· `{max_duration_job}`' if max_duration_job else ''}\n\n"

        f"{action_text}"
    )

    return send_discord_message(message)
