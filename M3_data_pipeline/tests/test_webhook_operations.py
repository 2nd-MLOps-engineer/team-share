"""KST 기준 주간·야간 운영 집계와 Discord Webhook 메시지 생성을 검증합니다."""

from datetime import datetime
from pathlib import Path
import re
import sys
from unittest.mock import MagicMock, Mock, patch

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pipeline_monitoring as monitoring
import pipeline_scheduler as scheduler
import webhook_notifier as notifier


SUMMARY = dict(
    total_runs=10, success_runs=6, warning_runs=2, failed_runs=2,
    dq_passed=7, dq_failed=2, avg_duration_seconds=61,
    max_duration_seconds=3661, warning_jobs=["air_quality"],
    failed_jobs=["weather"], dq_failed_datasets=["air_quality"],
    max_duration_job="weather",
)


@pytest.mark.parametrize("now,start,end,daypart", [
    ("2026-09-29T09:15:00+09:00", "2026-09-28T17:00:00+09:00", "2026-09-29T09:15:00+09:00", "야간"),
    ("2026-09-29T17:00:00+09:00", "2026-09-29T09:15:00+09:00", "2026-09-29T17:00:00+09:00", "주간"),
    ("2026-09-29T00:15:00+00:00", "2026-09-28T17:00:00+09:00", "2026-09-29T09:15:00+09:00", "야간"),
    ("2026-09-29T08:00:00+00:00", "2026-09-29T09:15:00+09:00", "2026-09-29T17:00:00+09:00", "주간"),
    ("2026-10-01T09:15:00+09:00", "2026-09-30T17:00:00+09:00", "2026-10-01T09:15:00+09:00", "야간"),
])
def test_scheduled_summary_uses_completed_period(now, start, end, daypart):
    engine = Mock()
    with (
        patch.object(scheduler, "datetime") as clock,
        patch.object(scheduler, "load_environment"),
        patch.object(scheduler, "create_db_engine", return_value=engine),
        patch.object(scheduler, "get_operations_summary", return_value=SUMMARY) as query,
        patch.object(notifier, "send_discord_message", return_value=True) as send,
    ):
        clock.now.return_value = datetime.fromisoformat(now)
        scheduler.send_scheduled_operations_summary()
    query.assert_called_once_with(
        engine, period_start=datetime.fromisoformat(start),
        period_end=datetime.fromisoformat(end),
    )
    clock.now.assert_called_once_with(scheduler.TIMEZONE)
    send.assert_called_once()
    message = send.call_args.args[0]
    assert f"우심운까 운영 현황 · 운영 결과 · {daypart}" in message
    assert f"`{start}` → `{end}`" in message
    engine.dispose.assert_called_once()


@pytest.mark.parametrize("clock", ["09:14:59", "09:16:00", "16:59:59", "18:00:00"])
def test_unscheduled_time_does_not_query_or_send(clock):
    with (
        patch.object(scheduler, "datetime") as now,
        patch.object(scheduler, "load_environment"),
        patch.object(scheduler, "create_db_engine") as engine,
        patch.object(scheduler, "send_operations_summary") as send,
    ):
        now.now.return_value = datetime.fromisoformat(f"2026-09-29T{clock}+09:00")
        scheduler.send_scheduled_operations_summary()
    engine.assert_not_called()
    send.assert_not_called()


def test_summary_preserves_independent_run_and_dq_counts():
    with patch.object(notifier, "send_discord_message", return_value=True) as send:
        assert notifier.send_operations_summary(
            period_label="운영 결과 · 주간", period_start="start", period_end="end", **SUMMARY,
        )
    message = send.call_args.args[0]
    assert "전체 `10`  ·  정상 `6`  ·  경고 `2`  ·  실패 `2`" in message
    assert "검사 통과 `7`  ·  실패 `2`" in message
    assert "**실패 작업**\n`weather`" in message
    assert "**경고 작업**\n`air_quality`" in message
    assert "평균 `1분 1초`" in message
    assert "최대 `1시간 1분 1초` · `weather`" in message
    assert "MOTIVE" not in message.upper()


def test_empty_summary_and_missing_duration():
    empty = {**SUMMARY, **dict.fromkeys(
        ["total_runs", "success_runs", "warning_runs", "failed_runs", "dq_passed", "dq_failed"], 0
    ), "failed_jobs": [], "warning_jobs": [], "dq_failed_datasets": [],
        "avg_duration_seconds": None, "max_duration_seconds": None, "max_duration_job": None}
    with patch.object(notifier, "send_discord_message", return_value=True) as send:
        notifier.send_operations_summary(period_label="야간", period_start="start", period_end="end", **empty)
    assert "추가 조치 필요 없음" in send.call_args.args[0]
    assert "확인 불가" in send.call_args.args[0]


def test_failure_and_dq_message_service_names():
    with patch.object(notifier, "send_discord_message", return_value=True) as send:
        notifier.send_pipeline_failure_alert(job_id="weather", run_id="run", failure_stage="collect",
                                            error=RuntimeError("private"), elapsed_seconds=1)
        message = send.call_args.args[0]
        assert "우심운까 데이터 파이프라인 실패" in message
        assert "데이터 수집" in message and "private" not in message
        notifier.send_dq_failure_alert(job_id="weather", run_id="run", dq_status="CHECK_FAILED",
                                      dq_results={"bad": {"status": "CHECK_FAILED"},
                                                  "good": {"status": "CHECK_PASSED"}}, elapsed_seconds=2)
        message = send.call_args.args[0]
        assert "우심운까 데이터 품질 이상 감지" in message
        assert "`bad`" in message and "`good`" not in message


def test_webhook_payload_keeps_configured_sender_and_timeout():
    with patch.dict("os.environ", {"DISCORD_WEBHOOK_URL": "https://example.invalid/webhook"}), \
            patch.object(notifier.requests, "post") as post:
        assert notifier.send_discord_message("우심운까", timeout=3)
    post.assert_called_once_with("https://example.invalid/webhook", json={"content": "우심운까"}, timeout=3)


def test_webhook_failure_and_missing_url_return_false():
    with patch.dict("os.environ", {"DISCORD_WEBHOOK_URL": ""}), patch.object(notifier.requests, "post") as post:
        assert not notifier.send_discord_message("message")
        post.assert_not_called()
    with patch.dict("os.environ", {"DISCORD_WEBHOOK_URL": "https://example.invalid/webhook"}), \
            patch.object(notifier.requests, "post", side_effect=requests.Timeout):
        assert not notifier.send_discord_message("message")


def test_monitoring_query_separates_run_and_dq_filters_and_half_open_window():
    engine = MagicMock()
    db = engine.connect.return_value.__enter__.return_value
    db.execute.return_value.mappings.return_value.one.return_value = SUMMARY
    start = datetime.fromisoformat("2026-09-29T09:15:00+09:00")
    end = datetime.fromisoformat("2026-09-29T17:00:00+09:00")
    assert monitoring.get_operations_summary(engine, period_start=start, period_end=end) == SUMMARY
    statement, params = db.execute.call_args.args
    sql = str(statement)
    for column, status, alias in [
        ("run_status", "SUCCESS", "success_runs"), ("run_status", "WARNING", "warning_runs"),
        ("run_status", "FAILED", "failed_runs"), ("dq_status", "CHECK_PASSED", "dq_passed"),
        ("dq_status", "CHECK_FAILED", "dq_failed"),
    ]:
        assert re.search(rf"COUNT\(\*\) FILTER \(\s*WHERE {column} = '{status}'\s*\) AS {alias}", sql)
    assert "COUNT(*) AS total_runs" in sql
    assert "started_at >= :period_start" in sql and "started_at < :period_end" in sql
    assert params == {"period_start": start, "period_end": end}
    for status, alias in [("WARNING", "warning_jobs"), ("FAILED", "failed_jobs")]:
        assert re.search(rf"ARRAY_AGG\(DISTINCT job_id\) FILTER \(\s*WHERE run_status = '{status}'\s*\) AS {alias}", sql)
