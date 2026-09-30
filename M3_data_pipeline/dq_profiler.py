"""행 추적 기반의 데이터 품질(DQ) 프로파일러.

데이터의 상태와 변환 흔적을 측정해서 프로필을 만든다.
DQ 검사에서 문제가 발견되면 이를 기록하되,
현재는 해당 결과만으로 파이프라인 실행을 중단하지 않는다(비차단,non-blocking).

RAW 데이터가 processor를 거쳐 PROCESSED 데이터로 변환되는 동안
행 추적, 행 수 변화, NULL 변화, 중복, 검토가 필요한 행 등의
품질 지표를 측정하고 그 결과를 DQ 프로파일로 생성한다.

``CHECK_FAILED``는 행 추적 검증 또는 행 수 대사가 통과하지 못했음을 뜻한다.
형변환 근거의 누락, NULL·중복·needs_review 관찰값만으로 이 상태를 결정하지 않는다.
DQ 상태 자체는 파이프라인 실행을 중단하지 않지만 processor 등의 예외는 전파된다.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any
import uuid

import numpy as np
import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
    is_string_dtype,
)


DQ_LINEAGE_COLUMN = "_dq_row_id"


class DQAuditStatus(str, Enum):
    CHECK_PASSED = "CHECK_PASSED"
    CHECK_FAILED = "CHECK_FAILED"


class MetricAvailability(str, Enum):
    AVAILABLE = "available"
    NOT_AVAILABLE = "not_available"


class ValidationStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    NOT_APPLICABLE = "N/A"


class LineageError(ValueError):
    """A row-tracking comparison cannot be performed safely."""


@dataclass(frozen=True)
class NullTransitionMetrics:
    total: int
    by_column: dict[str, int] = field(default_factory=dict)
    affected_rows: int = 0


@dataclass(frozen=True)
class NullMetrics:
    source_null: int
    nulls_from_normalization: int
    nulls_from_conversion: int | None
    removed_row_null: int | None
    final_null: int
    normalization_by_column: dict[str, int] = field(default_factory=dict)
    conversion_by_column: dict[str, int] = field(default_factory=dict)
    removed_row_null_by_column: dict[str, int] = field(default_factory=dict)
    not_available: tuple[str, ...] = ()


@dataclass(frozen=True)
class RowTrackingMetrics:
    column: str
    available: bool
    passed: bool
    raw_rows: int
    processed_rows: int
    missing_ids: int | None
    unknown_ids: int | None
    null_ids: int
    duplicate_ids: int


@dataclass(frozen=True)
class RowCountCheckMetrics:
    raw_rows: int
    processed_rows: int
    removed_rows: int
    explained_removed_rows: int | None
    unexplained_row_loss: int | None
    expected_final: int | None
    actual_final: int
    row_count_matches: bool
    explanation_availability: MetricAvailability
    removal_reasons: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class DedupMetrics:
    key_columns: tuple[str, ...]
    duplicate_groups: int | None
    duplicate_rows: int | None
    removed_rows: int | None
    identical_payload_groups: int | None
    conflicting_payload_groups: int | None
    availability: MetricAvailability = MetricAvailability.AVAILABLE


@dataclass(frozen=True)
class NeedsReviewMetrics:
    rows_needing_review: int
    review_reasons: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class ValidationSummaryItem:
    rule: str
    expected: Any
    observed: Any
    status: ValidationStatus


@dataclass(frozen=True)
class ColumnStatistics:
    dtype: str
    rows: int
    null_count: int
    null_rate: float
    non_null_count: int
    unique_count: int
    unique_rate: float
    numeric_min: int | float | None = None
    numeric_max: int | float | None = None
    numeric_mean: float | None = None
    numeric_median: float | None = None
    infinite_count: int | None = None
    datetime_min: str | None = None
    datetime_max: str | None = None
    string_min_length: int | None = None
    string_max_length: int | None = None
    empty_count: int | None = None


@dataclass(frozen=True)
class ColumnProfile:
    column: str
    raw: ColumnStatistics | None
    processed: ColumnStatistics | None


@dataclass(frozen=True)
class DQProfileResult:
    dataset: str
    status: DQAuditStatus
    row_tracking: RowTrackingMetrics
    nulls: NullMetrics
    rows: RowCountCheckMetrics
    dedup: DedupMetrics
    needs_review: NeedsReviewMetrics
    issues: tuple[str, ...] = ()
    validation_summary: tuple[ValidationSummaryItem, ...] = ()
    column_profiles: dict[str, ColumnProfile] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-friendly representation for audit logs and monitoring storage."""

        def serialize(value: Any) -> Any:
            if isinstance(value, Enum):
                return value.value
            if isinstance(value, dict):
                return {key: serialize(item) for key, item in value.items()}
            if isinstance(value, (list, tuple)):
                return [serialize(item) for item in value]
            return value

        return serialize(asdict(self))


_REMOVAL_REASON_LABELS = {
    "facility_status_not_normal_operation": "비정상 운영 상태",
    "facility_invalid_or_missing_korea_coordinates": "좌표 결측/국내 범위 밖",
    "facility_duplicate_faci_cd": "시설 코드 중복",
    "weather_warning_duplicate_stn_tmfc_tmseq": "기상특보 중복",
    "weather_warning_status_superseded_snapshot": "최신 이전 상태 snapshot",
    "weather_warning_status_no_active_or_preliminary_warning": (
        "현재/예비 특보 없음"
    ),
}
_REMOVAL_REASON_ORDER = {
    reason: position
    for position, reason in enumerate(
        (
            "facility_status_not_normal_operation",
            "facility_invalid_or_missing_korea_coordinates",
            "facility_duplicate_faci_cd",
            "weather_warning_duplicate_stn_tmfc_tmseq",
            "weather_warning_status_superseded_snapshot",
            "weather_warning_status_no_active_or_preliminary_warning",
        )
    )
}


def render_dq_audit_report(result: DQProfileResult) -> str:
    """DQ 프로파일의 핵심 결과만 터미널용 감사 보고서로 변환한다."""

    rows = result.rows
    nulls = result.nulls
    needs_review = result.needs_review
    conversion_nulls = nulls.nulls_from_conversion
    new_nulls = (
        None
        if conversion_nulls is None
        else nulls.nulls_from_normalization + conversion_nulls
    )

    lines = [
        "",
        "=" * 64,
        f"DATA QUALITY AUDIT  |  {result.dataset}",
        "=" * 64,
        "",
        "1. Validation",
        "-" * 64,
        _validation_line(
            "Row lineage",
            "PASS" if result.row_tracking.passed else "FAIL",
        ),
        _validation_line(
            "Row reconciliation",
            "PASS" if rows.row_count_matches else "FAIL",
        ),
        _validation_line(
            "Unexplained loss",
            _human_count_or_unavailable(rows.unexplained_row_loss),
        ),
        _validation_line(
            "New NULLs",
            _human_count_or_unavailable(new_nulls),
        ),
        _validation_line(
            "Review required",
            f"{needs_review.rows_needing_review:,}",
        ),
        "",
        "2. Row Transformation",
        "-" * 64,
        _metric_line("RAW", rows.raw_rows),
        _metric_line("PROCESSED", rows.processed_rows),
        _metric_line("REMOVED", rows.removed_rows),
    ]

    for reason, count in sorted(
        rows.removal_reasons.items(),
        key=lambda item: (_REMOVAL_REASON_ORDER.get(item[0], 999), item[0]),
    ):
        lines.append(
            _metric_line(
                f"  - {_removal_reason_label(reason)}",
                count,
            )
        )

    lines.extend(
        [
            _metric_line("EXPLAINED", rows.explained_removed_rows),
            _metric_line("UNEXPLAINED", rows.unexplained_row_loss),
            "",
            "3. Data Quality Findings",
            "-" * 64,
        ]
    )

    findings = _render_dq_findings(result)
    lines.extend(findings or ["- 중요 발견 없음"])

    lines.extend(
        [
            "",
            "4. Result",
            "-" * 64,
            result.status.value,
            "",
            f"{rows.removed_rows:,}건이 정제 규칙에 따라 제거되었으며",
        ]
    )

    if rows.unexplained_row_loss is None:
        lines.append("설명되지 않은 데이터 손실은 현재 확인할 수 없습니다.")
    else:
        lines.append(
            "설명되지 않은 데이터 손실은 "
            f"{rows.unexplained_row_loss:,}건입니다."
        )

    if result.status == DQAuditStatus.CHECK_FAILED:
        lines.append("행 lineage 또는 reconciliation 결과를 확인해야 합니다.")

    lines.append("=" * 64)
    return "\n".join(lines)


def _display_value(value: object) -> str:
    return "N/A" if value is None else str(value)


def _human_count(value: int | None) -> str:
    return "N/A" if value is None else f"{value:,}"


def _human_count_or_unavailable(value: int | None) -> str:
    return "확인 불가" if value is None else f"{value:,}"


def _validation_line(label: str, value: str) -> str:
    return f"- {label:<24} {value:>12}"


def _metric_line(label: str, value: int | None) -> str:
    return f"{label:<34} {_human_count_or_unavailable(value):>12}"


def _removal_reason_label(reason: str) -> str:
    return _REMOVAL_REASON_LABELS.get(reason, reason.replace("_", " "))


def _render_dq_findings(result: DQProfileResult) -> list[str]:
    nulls = result.nulls
    needs_review = result.needs_review
    conversion_columns = sorted(
        column
        for column, count in nulls.conversion_by_column.items()
        if count
    )
    conversion_label = "Conversion NULL"
    if conversion_columns:
        conversion_label += f" ({', '.join(conversion_columns)})"

    lines = [
        _metric_line("Normalization NULL", nulls.nulls_from_normalization),
        _metric_line(conversion_label, nulls.nulls_from_conversion),
        _metric_line("Review required", needs_review.rows_needing_review),
    ]

    failed_rules = [
        item.rule
        for item in result.validation_summary
        if item.status == ValidationStatus.FAIL
    ]
    audit_findings = [
        *(f"검증 실패: {rule}" for rule in failed_rules),
        *result.issues,
    ]
    if audit_findings:
        lines.append("Issues")
        lines.extend(f"- {finding}" for finding in dict.fromkeys(audit_findings))
    else:
        lines.append(f"{'Issues':<34} {'없음':>12}")

    return lines


def _compact_value(value: object) -> str:
    if value is None:
        return "N/A"
    if isinstance(value, Mapping):
        return ",".join(f"{key}={_display_value(item)}" for key, item in value.items())
    return str(value)


def _format_column_statistics(stats: ColumnStatistics | None) -> str:
    if stats is None:
        return "N/A"
    common = (
        f"dtype={stats.dtype} rows={stats.rows} "
        f"null={stats.null_count}({stats.null_rate:.2%}) "
        f"non-null={stats.non_null_count} "
        f"unique={stats.unique_count}({stats.unique_rate:.2%})"
    )
    if stats.infinite_count is not None:
        return (
            f"{common} numeric[min={stats.numeric_min}, max={stats.numeric_max}, "
            f"mean={stats.numeric_mean}, median={stats.numeric_median}, "
            f"infinite={stats.infinite_count}]"
        )
    if stats.datetime_min is not None or stats.datetime_max is not None:
        return f"{common} datetime[min={stats.datetime_min}, max={stats.datetime_max}]"
    if stats.string_min_length is not None:
        return (
            f"{common} string[min_len={stats.string_min_length}, "
            f"max_len={stats.string_max_length}, empty={stats.empty_count}]"
        )
    return common


def add_stable_lineage(
    frame: pd.DataFrame,
    *,
    audit_id: str | None = None,
    lineage_column: str = DQ_LINEAGE_COLUMN,
) -> pd.DataFrame:
    """Copy a RAW frame and assign one immutable audit identity per input row."""

    if lineage_column in frame.columns:
        raise LineageError(
            f"reserved DQ lineage column already exists: {lineage_column}"
        )

    result = frame.copy()
    prefix = audit_id or uuid.uuid4().hex
    result[lineage_column] = [f"{prefix}:{position}" for position in range(len(result))]
    return result


def remove_stable_lineage(
    frame: pd.DataFrame,
    *,
    lineage_column: str = DQ_LINEAGE_COLUMN,
) -> pd.DataFrame:
    """Remove audit-only lineage before persistence."""

    if lineage_column not in frame.columns:
        return frame.copy()
    return frame.drop(columns=[lineage_column])


def _lineage_values(
    frame: pd.DataFrame,
    lineage_column: str,
) -> tuple[pd.Series, int, int]:
    if lineage_column not in frame.columns:
        raise LineageError(f"lineage column is missing: {lineage_column}")

    values = frame[lineage_column]
    null_ids = int(values.isna().sum())
    non_null = values.dropna()
    duplicate_ids = int(non_null.duplicated(keep="first").sum())
    return values, null_ids, duplicate_ids


def audit_lineage(
    raw: pd.DataFrame,
    processed: pd.DataFrame,
    *,
    lineage_column: str = DQ_LINEAGE_COLUMN,
) -> RowTrackingMetrics:
    """Validate row tracking without treating legitimately removed RAW IDs as errors."""

    try:
        raw_values, raw_null_ids, raw_duplicate_ids = _lineage_values(
            raw, lineage_column
        )
    except LineageError:
        return RowTrackingMetrics(
            column=lineage_column,
            available=False,
            passed=False,
            raw_rows=len(raw),
            processed_rows=len(processed),
            missing_ids=None,
            unknown_ids=None,
            null_ids=0,
            duplicate_ids=0,
        )

    if lineage_column not in processed.columns:
        return RowTrackingMetrics(
            column=lineage_column,
            available=False,
            passed=False,
            raw_rows=len(raw),
            processed_rows=len(processed),
            missing_ids=None,
            unknown_ids=None,
            null_ids=0,
            duplicate_ids=0,
        )

    processed_values, processed_null_ids, processed_duplicate_ids = _lineage_values(
        processed, lineage_column
    )
    raw_ids = set(raw_values.dropna().tolist())
    processed_ids = set(processed_values.dropna().tolist())

    missing_ids = len(raw_ids - processed_ids)
    unknown_ids = len(processed_ids - raw_ids)
    null_ids = raw_null_ids + processed_null_ids
    duplicate_ids = raw_duplicate_ids + processed_duplicate_ids

    return RowTrackingMetrics(
        column=lineage_column,
        available=True,
        passed=(not unknown_ids and null_ids == 0 and duplicate_ids == 0),
        raw_rows=len(raw),
        processed_rows=len(processed),
        missing_ids=missing_ids,
        unknown_ids=unknown_ids,
        null_ids=null_ids,
        duplicate_ids=duplicate_ids,
    )


def _transition_cells(
    before: pd.DataFrame,
    after: pd.DataFrame,
    *,
    columns: Sequence[str] | None = None,
    lineage_column: str = DQ_LINEAGE_COLUMN,
) -> set[tuple[object, str]]:
    _, before_null_ids, before_duplicate_ids = _lineage_values(
        before, lineage_column
    )
    _, after_null_ids, after_duplicate_ids = _lineage_values(after, lineage_column)
    if before_null_ids or after_null_ids or before_duplicate_ids or after_duplicate_ids:
        raise LineageError("lineage IDs must be non-null and unique for comparison")

    before_indexed = before.set_index(lineage_column, drop=False)
    after_indexed = after.set_index(lineage_column, drop=False)
    unknown_ids = set(after_indexed.index) - set(before_indexed.index)
    if unknown_ids:
        raise LineageError("after snapshot contains lineage IDs absent from before")
    common_ids = before_indexed.index[before_indexed.index.isin(after_indexed.index)]

    if columns is None:
        compared_columns = [
            column
            for column in before.columns
            if column != lineage_column and column in after.columns
        ]
    else:
        compared_columns = [
            column
            for column in columns
            if column != lineage_column
            and column in before.columns
            and column in after.columns
        ]

    cells: set[tuple[object, str]] = set()
    for column in compared_columns:
        before_values = before_indexed.loc[common_ids, column]
        after_values = after_indexed.loc[common_ids, column]
        losses = before_values.notna() & after_values.isna()
        cells.update((row_id, column) for row_id in common_ids[losses.to_numpy()])
    return cells


def calculate_null_transitions(
    before: pd.DataFrame,
    after: pd.DataFrame,
    *,
    columns: Sequence[str] | None = None,
    lineage_column: str = DQ_LINEAGE_COLUMN,
) -> NullTransitionMetrics:
    """Count non-null to NULL transitions by stable lineage, never row position."""

    cells = _transition_cells(
        before,
        after,
        columns=columns,
        lineage_column=lineage_column,
    )
    by_column: dict[str, int] = {}
    affected_ids: set[object] = set()
    for row_id, column in cells:
        by_column[column] = by_column.get(column, 0) + 1
        affected_ids.add(row_id)
    return NullTransitionMetrics(
        total=len(cells),
        by_column=dict(sorted(by_column.items())),
        affected_rows=len(affected_ids),
    )


def calculate_conversion_loss(
    before_conversion: pd.DataFrame,
    after_conversion: pd.DataFrame,
    *,
    columns: Sequence[str] | None = None,
    lineage_column: str = DQ_LINEAGE_COLUMN,
) -> NullTransitionMetrics:
    """Conversion-specific alias documenting the required before/after boundary."""

    return calculate_null_transitions(
        before_conversion,
        after_conversion,
        columns=columns,
        lineage_column=lineage_column,
    )


class ConversionLossCollector:
    """Collect unique conversion-loss cells during one processor execution."""

    def __init__(self) -> None:
        self._cells: set[tuple[object, str]] = set()
        self.issues: list[str] = []

    def record(
        self,
        before: pd.DataFrame,
        after: pd.DataFrame,
        *,
        columns: Sequence[str] | None = None,
    ) -> None:
        try:
            self._cells.update(
                _transition_cells(before, after, columns=columns)
            )
        except LineageError as exc:
            self.issues.append(str(exc))

    def metrics(self) -> NullTransitionMetrics:
        by_column: dict[str, int] = {}
        affected_ids: set[object] = set()
        for row_id, column in self._cells:
            by_column[column] = by_column.get(column, 0) + 1
            affected_ids.add(row_id)
        return NullTransitionMetrics(
            total=len(self._cells),
            by_column=dict(sorted(by_column.items())),
            affected_rows=len(affected_ids),
        )


class RemovalReasonCollector:
    """Collect lineage IDs removed by explicit processor rules."""

    def __init__(self) -> None:
        self.reasons: dict[str, set[object]] = {}
        self.issues: list[str] = []

    def record(self, frame: pd.DataFrame, reason: str) -> None:
        if frame.empty:
            return
        if DQ_LINEAGE_COLUMN not in frame.columns:
            self.issues.append(
                f"removal reason '{reason}' was recorded without _dq_row_id"
            )
            return

        row_ids = frame[DQ_LINEAGE_COLUMN].dropna().tolist()
        self.reasons.setdefault(reason, set()).update(row_ids)


_ACTIVE_CONVERSION_COLLECTOR: ContextVar[ConversionLossCollector | None] = (
    ContextVar("dq_conversion_loss_collector", default=None)
)
_ACTIVE_REMOVAL_COLLECTOR: ContextVar[RemovalReasonCollector | None] = (
    ContextVar("dq_removal_reason_collector", default=None)
)


@contextmanager
def collect_conversion_losses(collector: ConversionLossCollector):
    token = _ACTIVE_CONVERSION_COLLECTOR.set(collector)
    try:
        yield collector
    finally:
        _ACTIVE_CONVERSION_COLLECTOR.reset(token)


@contextmanager
def collect_removal_reasons(collector: RemovalReasonCollector):
    token = _ACTIVE_REMOVAL_COLLECTOR.set(collector)
    try:
        yield collector
    finally:
        _ACTIVE_REMOVAL_COLLECTOR.reset(token)


def record_removal_reason(frame: pd.DataFrame, reason: str) -> None:
    """Record removed rows when a profiled processor run is active."""

    collector = _ACTIVE_REMOVAL_COLLECTOR.get()
    if collector is not None:
        collector.record(frame, reason)


def record_conversion_step(
    before: pd.DataFrame,
    after: pd.DataFrame,
    *,
    columns: Sequence[str] | None = None,
) -> None:
    """Record a conversion when a profiled run is active; otherwise do nothing."""

    collector = _ACTIVE_CONVERSION_COLLECTOR.get()
    if collector is not None:
        collector.record(before, after, columns=columns)


def mark_conversion_audit_unavailable(reason: str) -> None:
    """Mark conversion evidence incomplete without failing dataset processing."""

    collector = _ACTIVE_CONVERSION_COLLECTOR.get()
    if collector is not None:
        collector.issues.append(reason)


def conversion_audit_active() -> bool:
    """Return whether the current processor run is collecting conversion loss."""

    return _ACTIVE_CONVERSION_COLLECTOR.get() is not None


def reconcile_row_counts(
    raw: pd.DataFrame,
    processed: pd.DataFrame,
    *,
    removal_reasons: Mapping[str, Iterable[object]] | None = None,
    lineage_column: str = DQ_LINEAGE_COLUMN,
) -> RowCountCheckMetrics:
    """Check physical row counts against row-tracking-backed removal explanations."""

    raw_rows = len(raw)
    processed_rows = len(processed)
    removed_rows = raw_rows - processed_rows
    row_tracking = audit_lineage(
        raw,
        processed,
        lineage_column=lineage_column,
    )

    if not row_tracking.passed or removed_rows < 0:
        return RowCountCheckMetrics(
            raw_rows=raw_rows,
            processed_rows=processed_rows,
            removed_rows=removed_rows,
            explained_removed_rows=None,
            unexplained_row_loss=None,
            expected_final=None,
            actual_final=processed_rows,
            row_count_matches=False,
            explanation_availability=MetricAvailability.NOT_AVAILABLE,
        )

    raw_ids = set(raw[lineage_column].tolist())
    processed_ids = set(processed[lineage_column].tolist())
    removed_ids = raw_ids - processed_ids
    reason_counts: dict[str, int] = {}
    explained_ids: set[object] = set()
    invalid_reason_ids: set[object] = set()

    for reason, row_ids in (removal_reasons or {}).items():
        reason_ids = set(row_ids)
        invalid_reason_ids.update(reason_ids - removed_ids)
        valid_reason_ids = reason_ids & removed_ids
        reason_counts[reason] = len(valid_reason_ids)
        explained_ids.update(valid_reason_ids)

    explained = len(explained_ids)
    unexplained = len(removed_ids - explained_ids)
    expected_final = raw_rows - explained - unexplained
    matches = (
        not invalid_reason_ids
        and removed_rows == explained + unexplained
        and expected_final == processed_rows
    )

    return RowCountCheckMetrics(
        raw_rows=raw_rows,
        processed_rows=processed_rows,
        removed_rows=removed_rows,
        explained_removed_rows=explained,
        unexplained_row_loss=unexplained,
        expected_final=expected_final,
        actual_final=processed_rows,
        row_count_matches=matches,
        explanation_availability=MetricAvailability.AVAILABLE,
        removal_reasons=dict(sorted(reason_counts.items())),
    )


def calculate_dedup_metrics(
    frame: pd.DataFrame,
    key_columns: Sequence[str],
    *,
    payload_columns: Sequence[str] | None = None,
) -> DedupMetrics:
    """Describe duplicate groups without changing or deduplicating the frame."""

    keys = tuple(key_columns)
    if not keys or not all(column in frame.columns for column in keys):
        return DedupMetrics(
            key_columns=keys,
            duplicate_groups=None,
            duplicate_rows=None,
            removed_rows=None,
            identical_payload_groups=None,
            conflicting_payload_groups=None,
            availability=MetricAvailability.NOT_AVAILABLE,
        )

    valid_keys = frame[list(keys)].notna().all(axis=1)
    candidates = frame.loc[valid_keys]
    duplicate_mask = candidates.duplicated(subset=list(keys), keep=False)
    duplicates = candidates.loc[duplicate_mask]
    payload = list(payload_columns) if payload_columns is not None else [
        column
        for column in frame.columns
        if column not in keys and column != DQ_LINEAGE_COLUMN
    ]
    payload = [column for column in payload if column in duplicates.columns]

    duplicate_groups = 0
    duplicate_rows = 0
    identical_groups = 0
    conflicting_groups = 0
    if not duplicates.empty:
        grouper: str | list[str] = keys[0] if len(keys) == 1 else list(keys)
        for _, group in duplicates.groupby(grouper, dropna=False, sort=False):
            duplicate_groups += 1
            duplicate_rows += len(group)
            if not payload or len(group[payload].drop_duplicates()) == 1:
                identical_groups += 1
            else:
                conflicting_groups += 1

    return DedupMetrics(
        key_columns=keys,
        duplicate_groups=duplicate_groups,
        duplicate_rows=duplicate_rows,
        removed_rows=duplicate_rows - duplicate_groups,
        identical_payload_groups=identical_groups,
        conflicting_payload_groups=conflicting_groups,
    )


def calculate_needs_review_metrics(
    processed: pd.DataFrame,
    *,
    review_reason_row_ids: Mapping[str, Iterable[object]] | None = None,
) -> NeedsReviewMetrics:
    """Count needs_review rows and unique row IDs for each supplied review reason."""

    if "needs_review" in processed.columns:
        rows_needing_review = int(processed["needs_review"].fillna(False).sum())
    else:
        rows_needing_review = 0

    reasons = {
        str(reason): len(set(row_ids))
        for reason, row_ids in (review_reason_row_ids or {}).items()
    }
    return NeedsReviewMetrics(
        rows_needing_review=rows_needing_review,
        review_reasons=dict(sorted(reasons.items())),
    )



def _count_null_cells(frame: pd.DataFrame) -> int:
    columns = [column for column in frame.columns if column != DQ_LINEAGE_COLUMN]
    return int(frame[columns].isna().sum().sum()) if columns else 0


def _removed_row_nulls(
    normalized: pd.DataFrame,
    processed: pd.DataFrame,
) -> tuple[int | None, dict[str, int]]:
    row_tracking = audit_lineage(normalized, processed)
    if not row_tracking.passed:
        return None, {}

    processed_ids = set(processed[DQ_LINEAGE_COLUMN].tolist())
    removed = normalized.loc[
        ~normalized[DQ_LINEAGE_COLUMN].isin(processed_ids)
    ]
    columns = [
        column for column in removed.columns if column != DQ_LINEAGE_COLUMN
    ]
    by_column = {
        column: int(removed[column].isna().sum())
        for column in columns
    }
    return _count_null_cells(removed), by_column


def _plain_scalar(value: object) -> int | float | str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, (int, float, str)):
        return value
    return str(value)


def calculate_column_statistics(series: pd.Series) -> ColumnStatistics:
    """한 컬럼의 관찰값을 JSON 직렬화 가능한 값으로 계산한다."""

    rows = len(series)
    null_count = int(series.isna().sum())
    non_null = series.dropna()
    non_null_count = len(non_null)
    try:
        unique_count = int(non_null.nunique(dropna=True))
    except TypeError:
        unique_count = int(non_null.astype("string").nunique(dropna=True))

    values: dict[str, object] = {}
    if is_numeric_dtype(series.dtype) and not is_bool_dtype(series.dtype):
        numeric = pd.to_numeric(non_null, errors="coerce")
        numeric_array = numeric.to_numpy(dtype=float, na_value=np.nan)
        infinite_count = int(np.isinf(numeric_array).sum())
        finite = numeric.loc[np.isfinite(numeric_array)]
        values.update(
            numeric_min=_plain_scalar(finite.min()) if not finite.empty else None,
            numeric_max=_plain_scalar(finite.max()) if not finite.empty else None,
            numeric_mean=(float(finite.mean()) if not finite.empty else None),
            numeric_median=(float(finite.median()) if not finite.empty else None),
            infinite_count=infinite_count,
        )
    elif is_datetime64_any_dtype(series.dtype):
        values.update(
            datetime_min=_plain_scalar(non_null.min()) if not non_null.empty else None,
            datetime_max=_plain_scalar(non_null.max()) if not non_null.empty else None,
        )
    elif is_string_dtype(series.dtype) or is_object_dtype(series.dtype):
        strings = non_null.astype("string")
        lengths = strings.str.len()
        values.update(
            string_min_length=(int(lengths.min()) if not lengths.empty else None),
            string_max_length=(int(lengths.max()) if not lengths.empty else None),
            empty_count=int(strings.str.strip().eq("").sum()),
        )

    return ColumnStatistics(
        dtype=str(series.dtype),
        rows=rows,
        null_count=null_count,
        null_rate=(null_count / rows if rows else 0.0),
        non_null_count=non_null_count,
        unique_count=unique_count,
        unique_rate=(unique_count / non_null_count if non_null_count else 0.0),
        **values,
    )


def build_column_profiles(
    raw: pd.DataFrame,
    processed: pd.DataFrame,
) -> dict[str, ColumnProfile]:
    """RAW/PROCESSED 컬럼별 기술 통계를 만든다(판정에는 사용하지 않음)."""

    columns = [
        column
        for column in dict.fromkeys([*raw.columns, *processed.columns])
        if column != DQ_LINEAGE_COLUMN
    ]
    return {
        column: ColumnProfile(
            column=column,
            raw=(
                calculate_column_statistics(raw[column])
                if column in raw.columns
                else None
            ),
            processed=(
                calculate_column_statistics(processed[column])
                if column in processed.columns
                else None
            ),
        )
        for column in columns
    }


def build_validation_summary(
    row_tracking: RowTrackingMetrics,
    rows: RowCountCheckMetrics,
    nulls: NullMetrics,
    dedup: DedupMetrics,
    needs_review: NeedsReviewMetrics,
) -> tuple[ValidationSummaryItem, ...]:
    """기존 판정 규칙과 관찰 전용 지표를 명시적으로 구분한다."""

    return (
        ValidationSummaryItem(
            rule="row_tracking",
            expected={"unknown": 0, "null": 0, "duplicate": 0},
            observed={
                "missing": row_tracking.missing_ids,
                "unknown": row_tracking.unknown_ids,
                "null": row_tracking.null_ids,
                "duplicate": row_tracking.duplicate_ids,
            },
            status=(
                ValidationStatus.PASS
                if row_tracking.passed
                else ValidationStatus.FAIL
            ),
        ),
        ValidationSummaryItem(
            rule="row_reconciliation",
            expected=rows.expected_final,
            observed=rows.actual_final,
            status=(
                ValidationStatus.PASS
                if rows.row_count_matches
                else ValidationStatus.FAIL
            ),
        ),
        ValidationSummaryItem(
            rule="null_transition",
            expected="no configured threshold",
            observed={
                "normalization": nulls.nulls_from_normalization,
                "conversion": nulls.nulls_from_conversion,
            },
            status=ValidationStatus.NOT_APPLICABLE,
        ),
        ValidationSummaryItem(
            rule="dedup",
            expected="no configured rule",
            observed={
                "keys": list(dedup.key_columns),
                "duplicate_rows": dedup.duplicate_rows,
            },
            status=ValidationStatus.NOT_APPLICABLE,
        ),
        ValidationSummaryItem(
            rule="needs_review",
            expected="no configured threshold",
            observed=needs_review.rows_needing_review,
            status=ValidationStatus.NOT_APPLICABLE,
        ),
        ValidationSummaryItem(
            rule="column_profile",
            expected="observation only",
            observed="see section 4",
            status=ValidationStatus.NOT_APPLICABLE,
        ),
    )


def profile_processor_run(
    dataset: str,
    raw: pd.DataFrame,
    processor: Callable[[pd.DataFrame], pd.DataFrame],
    normalizer: Callable[[pd.DataFrame], pd.DataFrame],
    dedup_keys: Iterable[str] | None = None,
    removal_reasons: Mapping[str, Iterable[object]] | None = None,
) -> tuple[pd.DataFrame, DQProfileResult]:
    """Run a processor with row tracking; return a lineage-free frame and its DQ profile."""

    lineaged_raw = add_stable_lineage(raw)
    normalized = normalizer(lineaged_raw)
    normalization = calculate_null_transitions(lineaged_raw, normalized)
    conversion_collector = ConversionLossCollector()
    removal_collector = RemovalReasonCollector()

    with (
        collect_conversion_losses(conversion_collector),
        collect_removal_reasons(removal_collector),
    ):
        processed_with_lineage = processor(lineaged_raw)

    combined_removal_reasons = {
        reason: set(row_ids)
        for reason, row_ids in (removal_reasons or {}).items()
    }
    for reason, row_ids in removal_collector.reasons.items():
        combined_removal_reasons.setdefault(reason, set()).update(row_ids)

    row_tracking = audit_lineage(lineaged_raw, processed_with_lineage)
    rows = reconcile_row_counts(
        lineaged_raw,
        processed_with_lineage,
        removal_reasons=combined_removal_reasons,
    )
    conversion = conversion_collector.metrics()
    conversion_available = not conversion_collector.issues
    issues = [
        *conversion_collector.issues,
        *removal_collector.issues,
    ]

    if not row_tracking.available:
        issues.append("processor output did not preserve _dq_row_id")
    elif not row_tracking.passed:
        issues.append("processor output contains invalid row-tracking IDs")

    if not rows.row_count_matches:
        issues.append("row count check failed")

    status = (
        DQAuditStatus.CHECK_PASSED
        if row_tracking.passed and rows.row_count_matches
        else DQAuditStatus.CHECK_FAILED
    )

    removed_row_null, removed_row_null_by_column = _removed_row_nulls(
        normalized,
        processed_with_lineage,
    )
    nulls = NullMetrics(
        source_null=_count_null_cells(lineaged_raw),
        nulls_from_normalization=normalization.total,
        nulls_from_conversion=conversion.total if conversion_available else None,
        removed_row_null=removed_row_null,
        final_null=_count_null_cells(processed_with_lineage),
        normalization_by_column=normalization.by_column,
        conversion_by_column=conversion.by_column,
        removed_row_null_by_column=removed_row_null_by_column,
        not_available=tuple(
            metric
            for metric, unavailable in (
                ("nulls_from_conversion", not conversion_available),
                ("removed_row_null", not row_tracking.passed),
            )
            if unavailable
        ),
    )

    dedup = (
        calculate_dedup_metrics(
            processed_with_lineage,
            tuple(dedup_keys),
        )
        if dedup_keys
        else DedupMetrics(
            key_columns=(),
            duplicate_groups=None,
            duplicate_rows=None,
            removed_rows=None,
            identical_payload_groups=None,
            conflicting_payload_groups=None,
            availability=MetricAvailability.NOT_AVAILABLE,
        )
    )
    needs_review = calculate_needs_review_metrics(processed_with_lineage)

    result = DQProfileResult(
        dataset=dataset,
        status=status,
        row_tracking=row_tracking,
        nulls=nulls,
        rows=rows,
        dedup=dedup,
        needs_review=needs_review,
        issues=tuple(dict.fromkeys(issues)),
        validation_summary=build_validation_summary(
            row_tracking,
            rows,
            nulls,
            dedup,
            needs_review,
        ),
        column_profiles=build_column_profiles(
            lineaged_raw,
            processed_with_lineage,
        ),
    )

    return remove_stable_lineage(processed_with_lineage), result
