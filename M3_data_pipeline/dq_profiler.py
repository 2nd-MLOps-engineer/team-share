"""Lineage-based, non-blocking data-quality audit helpers.

The helpers in this module observe a processor run without defining data-quality
gates.  In particular, ``AUDIT_INVALID`` describes an audit whose evidence is
incomplete or internally inconsistent; it does not describe a failed dataset.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any
import uuid

import pandas as pd


DQ_LINEAGE_COLUMN = "_dq_row_id"


class DQAuditStatus(str, Enum):
    VALID = "VALID"
    AUDIT_INVALID = "AUDIT_INVALID"


class MetricAvailability(str, Enum):
    AVAILABLE = "available"
    NOT_AVAILABLE = "not_available"


class LineageError(ValueError):
    """A lineage-based comparison cannot be performed safely."""


@dataclass(frozen=True)
class NullTransitionMetrics:
    total: int
    by_column: dict[str, int] = field(default_factory=dict)
    affected_rows: int = 0


@dataclass(frozen=True)
class NullMetrics:
    source_null: int
    normalization_added_null: int
    conversion_added_null: int | None
    removed_row_null: int | None
    final_null: int
    normalization_by_column: dict[str, int] = field(default_factory=dict)
    conversion_by_column: dict[str, int] = field(default_factory=dict)
    not_available: tuple[str, ...] = ()


@dataclass(frozen=True)
class LineageMetrics:
    column: str
    available: bool
    valid: bool
    raw_rows: int
    processed_rows: int
    missing_ids: int | None
    unknown_ids: int | None
    null_ids: int
    duplicate_ids: int


@dataclass(frozen=True)
class RowReconciliationMetrics:
    raw_rows: int
    processed_rows: int
    removed_rows: int
    explained_removed_rows: int | None
    unexplained_removed_rows: int | None
    expected_final: int | None
    actual_final: int
    reconciliation_matches: bool
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
    needs_review_rows: int
    review_reasons: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class DQProfileResult:
    dataset: str
    status: DQAuditStatus
    lineage: LineageMetrics
    nulls: NullMetrics
    rows: RowReconciliationMetrics
    dedup: DedupMetrics
    needs_review: NeedsReviewMetrics
    issues: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-friendly representation for logs or future storage."""

        def serialize(value: Any) -> Any:
            if isinstance(value, Enum):
                return value.value
            if isinstance(value, dict):
                return {key: serialize(item) for key, item in value.items()}
            if isinstance(value, (list, tuple)):
                return [serialize(item) for item in value]
            return value

        return serialize(asdict(self))


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
) -> LineageMetrics:
    """Validate lineage without treating legitimately removed RAW IDs as errors."""

    try:
        raw_values, raw_null_ids, raw_duplicate_ids = _lineage_values(
            raw, lineage_column
        )
    except LineageError:
        return LineageMetrics(
            column=lineage_column,
            available=False,
            valid=False,
            raw_rows=len(raw),
            processed_rows=len(processed),
            missing_ids=None,
            unknown_ids=None,
            null_ids=0,
            duplicate_ids=0,
        )

    if lineage_column not in processed.columns:
        return LineageMetrics(
            column=lineage_column,
            available=False,
            valid=False,
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
    unknown_ids = processed_ids - raw_ids
    missing_ids = raw_ids - processed_ids
    null_ids = raw_null_ids + processed_null_ids
    duplicate_ids = raw_duplicate_ids + processed_duplicate_ids

    return LineageMetrics(
        column=lineage_column,
        available=True,
        valid=(not unknown_ids and null_ids == 0 and duplicate_ids == 0),
        raw_rows=len(raw),
        processed_rows=len(processed),
        missing_ids=len(missing_ids),
        unknown_ids=len(unknown_ids),
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


_ACTIVE_CONVERSION_COLLECTOR: ContextVar[ConversionLossCollector | None] = (
    ContextVar("dq_conversion_loss_collector", default=None)
)


@contextmanager
def collect_conversion_losses(collector: ConversionLossCollector):
    token = _ACTIVE_CONVERSION_COLLECTOR.set(collector)
    try:
        yield collector
    finally:
        _ACTIVE_CONVERSION_COLLECTOR.reset(token)


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
) -> RowReconciliationMetrics:
    """Reconcile physical row counts with lineage-backed removal explanations."""

    raw_rows = len(raw)
    processed_rows = len(processed)
    removed_rows = raw_rows - processed_rows
    lineage = audit_lineage(raw, processed, lineage_column=lineage_column)

    if not lineage.valid or removed_rows < 0:
        return RowReconciliationMetrics(
            raw_rows=raw_rows,
            processed_rows=processed_rows,
            removed_rows=removed_rows,
            explained_removed_rows=None,
            unexplained_removed_rows=None,
            expected_final=None,
            actual_final=processed_rows,
            reconciliation_matches=False,
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

    return RowReconciliationMetrics(
        raw_rows=raw_rows,
        processed_rows=processed_rows,
        removed_rows=removed_rows,
        explained_removed_rows=explained,
        unexplained_removed_rows=unexplained,
        expected_final=expected_final,
        actual_final=processed_rows,
        reconciliation_matches=matches,
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
    """Preserve the existing boolean while allowing future reason counts."""

    if "needs_review" in processed.columns:
        needs_review_rows = int(processed["needs_review"].fillna(False).sum())
    else:
        needs_review_rows = 0

    reasons = {
        reason: len(set(row_ids))
        for reason, row_ids in (review_reason_row_ids or {}).items()
    }
    return NeedsReviewMetrics(
        needs_review_rows=needs_review_rows,
        review_reasons=dict(sorted(reasons.items())),
    )


def _count_null_cells(frame: pd.DataFrame) -> int:
    columns = [column for column in frame.columns if column != DQ_LINEAGE_COLUMN]
    return int(frame[columns].isna().sum().sum()) if columns else 0


def _removed_row_nulls(
    normalized: pd.DataFrame,
    processed: pd.DataFrame,
) -> int | None:
    lineage = audit_lineage(normalized, processed)
    if not lineage.valid:
        return None
    processed_ids = set(processed[DQ_LINEAGE_COLUMN].tolist())
    removed = normalized.loc[~normalized[DQ_LINEAGE_COLUMN].isin(processed_ids)]
    return _count_null_cells(removed)


def profile_processor_run(
    dataset: str,
    raw: pd.DataFrame,
    processor: Callable[[pd.DataFrame], pd.DataFrame],
    normalizer: Callable[[pd.DataFrame], pd.DataFrame],
    *,
    removal_reasons: Mapping[str, Iterable[object]] | None = None,
) -> tuple[pd.DataFrame, DQProfileResult]:
    """Run a processor with audit lineage and return a persistence-safe frame."""

    lineaged_raw = add_stable_lineage(raw)
    normalized = normalizer(lineaged_raw)
    normalization = calculate_null_transitions(lineaged_raw, normalized)
    conversion_collector = ConversionLossCollector()

    with collect_conversion_losses(conversion_collector):
        processed_with_lineage = processor(lineaged_raw)

    lineage = audit_lineage(lineaged_raw, processed_with_lineage)
    rows = reconcile_row_counts(
        lineaged_raw,
        processed_with_lineage,
        removal_reasons=removal_reasons,
    )
    conversion = conversion_collector.metrics()
    conversion_available = not conversion_collector.issues
    issues = list(conversion_collector.issues)
    if not lineage.available:
        issues.append("processor output did not preserve _dq_row_id")
    elif not lineage.valid:
        issues.append("processor output contains invalid lineage IDs")
    if not rows.reconciliation_matches:
        issues.append("row-count reconciliation is invalid")

    status = (
        DQAuditStatus.VALID
        if lineage.valid and rows.reconciliation_matches
        else DQAuditStatus.AUDIT_INVALID
    )
    nulls = NullMetrics(
        source_null=_count_null_cells(lineaged_raw),
        normalization_added_null=normalization.total,
        conversion_added_null=conversion.total if conversion_available else None,
        removed_row_null=_removed_row_nulls(normalized, processed_with_lineage),
        final_null=_count_null_cells(processed_with_lineage),
        normalization_by_column=normalization.by_column,
        conversion_by_column=conversion.by_column,
        not_available=tuple(
            metric
            for metric, unavailable in (
                ("conversion_added_null", not conversion_available),
                ("removed_row_null", not lineage.valid),
            )
            if unavailable
        ),
    )
    result = DQProfileResult(
        dataset=dataset,
        status=status,
        lineage=lineage,
        nulls=nulls,
        rows=rows,
        dedup=DedupMetrics(
            key_columns=(),
            duplicate_groups=None,
            duplicate_rows=None,
            removed_rows=None,
            identical_payload_groups=None,
            conflicting_payload_groups=None,
            availability=MetricAvailability.NOT_AVAILABLE,
        ),
        needs_review=calculate_needs_review_metrics(processed_with_lineage),
        issues=tuple(dict.fromkeys(issues)),
    )
    return remove_stable_lineage(processed_with_lineage), result
