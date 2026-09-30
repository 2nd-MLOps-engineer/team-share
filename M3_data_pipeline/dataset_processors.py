"""데이터셋별 PROCESSED 정제 규칙.

원칙
----
1. raw DataFrame은 변경하지 않는다.

2. 대표 결측 문자열은 실제 NULL로 정규화한다.

3. 의미가 확인된 숫자 컬럼은 nullable numeric으로 변환한다.

4. 의미가 확인된 날짜/시간 컬럼은 안전하게 datetime으로 변환한다.

5. 위도/경도는 숫자형으로 변환하고 대한민국 유효 범위
   (위도 33.0~39.5, 경도 124.0~132.0)를 검증한다.

6. 서비스의 대표 위치 좌표가 결측/비정상인 행은
   processed에서 제거한다.

7. 주변 교통 등 부가 위도/경도는 행을 유지하고
   잘못된 좌표쌍만 NULL 처리한다.

8. 코드/우편번호/전화번호처럼 숫자로 보여도
   식별 의미인 값은 문자열로 유지한다.

9. 검증된 키가 있는 경우에만 중복 제거한다.
   중복 제거 키가 NULL인 행은 임의로 중복 처리하지 않는다.

10. 핵심 보조정보 결측은 needs_review로 표시한다.

11. 전용 processor가 없는 신규 데이터셋은 임의 정제하지 않는다.
    UnregisteredDatasetError를 발생시켜 해당 데이터셋의
    PROCESSED 적재를 차단한다.

12. 등록된 processor의 정제 과정에서 오류가 발생하면
    DatasetProcessorError로 처리하여 불완전한 processed 적재를 차단한다.

13. allow_empty=False일 때 정제 결과가 0건이면
    EmptyProcessedDatasetError를 발생시켜 기존 processed를 보호한다.
    allow_empty=True라도 유효 컬럼이 없는 결과는 허용하지 않는다.

14. processor에서 발생한 예외는 상위 scheduler/orchestrator로 전달한다.
    예약 작업은 독립 실행하며, --run-all-once는 같은 phase의 작업을 계속한다.
    선행 phase에 실패가 있으면 run_last phase는 실행하지 않는다.

15. 공통 진입점 process_dataset의 정제 성공 시 RAW 건수, PROCESSED 건수,
    제거 건수/비율, NULL 개수, needs_review 건수(해당 시),
    정제시간을 출력하여 정제 결과를 확인할 수 있도록 한다.

16. RAW와 PROCESSED에서 모두 완전히 결측인 컬럼은
    정보량이 없는 컬럼으로 판단하여 PROCESSED에서만 제거한다.
    제거된 컬럼명, 결측 개수, 결측률을 실행 결과에 출력한다.

17. RAW/PROCESSED의 전체 결측 개수와 전체 셀 대비 결측률,
    행 제거율, needs_review 비율을 정량적으로 출력하여
    데이터 품질 변화를 확인할 수 있도록 한다.    
"""

from __future__ import annotations

from collections.abc import Callable
import json
import re
import time

import pandas as pd

if __package__:
    from .dq_profiler import (
        DQ_LINEAGE_COLUMN,
        conversion_audit_active,
        mark_conversion_audit_unavailable,
        record_conversion_step,
        record_removal_reason,
    )
else:
    from dq_profiler import (
        DQ_LINEAGE_COLUMN,
        conversion_audit_active,
        mark_conversion_audit_unavailable,
        record_conversion_step,
        record_removal_reason,
    )


# ============================================================
# 공통 설정
# ============================================================

NULL_STRINGS = {
    "",
    "-",
    "--",
    "null",
    "none",
    "n/a",
    "na",
    "nan",
}


SIDO_REPLACEMENTS = {
    "서울": "서울특별시",
    "부산": "부산광역시",
    "대구": "대구광역시",
    "인천": "인천광역시",
    "광주": "광주광역시",
    "대전": "대전광역시",
    "울산": "울산광역시",
    "세종": "세종특별자치시",
    "경기": "경기도",
    "강원": "강원특별자치도",
    "강원도": "강원특별자치도",
    "충북": "충청북도",
    "충남": "충청남도",
    "전북": "전북특별자치도",
    "전라북도": "전북특별자치도",
    "전남": "전라남도",
    "경북": "경상북도",
    "경남": "경상남도",
    "제주": "제주특별자치도",
    "제주도": "제주특별자치도",
}


# ============================================================
# 예외 클래스
# ============================================================

class DatasetProcessorError(RuntimeError):
    """dataset processor 계층의 기본 예외."""


class UnregisteredDatasetError(DatasetProcessorError):
    """PROCESSORS에 등록되지 않은 데이터셋."""


class EmptyProcessedDatasetError(DatasetProcessorError):
    """정제 후 유효 데이터가 0건이 된 데이터셋."""


# ============================================================
# 공통 함수
# ============================================================

def normalize_nulls(frame: pd.DataFrame) -> pd.DataFrame:
    """
    대표 결측 문자열을 실제 NULL로 정규화한다.

    raw 보호를 위해 최초 1회 copy한다.
    문자열 컬럼은 pandas 벡터 연산으로 처리한다.
    """

    result = frame.copy()

    string_columns = result.select_dtypes(
        include=["object", "string"]
    ).columns

    for column in string_columns:
        values = (
            result[column]
            .astype("string")
            .str.strip()
        )

        null_mask = (
            values.isna()
            | values.str.lower().isin(NULL_STRINGS)
        )

        result[column] = values.mask(
            null_mask,
            pd.NA,
        )

    return result


def convert_numeric(
    frame: pd.DataFrame,
    float_columns: tuple[str, ...] = (),
    integer_columns: tuple[str, ...] = (),
) -> pd.DataFrame:
    """확인된 숫자 컬럼을 nullable numeric으로 변환한다."""

    converted_columns = [
        column
        for column in (*float_columns, *integer_columns)
        if column in frame.columns
    ]
    audit_before = None
    if conversion_audit_active() and DQ_LINEAGE_COLUMN in frame.columns:
        audit_before = frame[[DQ_LINEAGE_COLUMN, *converted_columns]].copy()
    elif conversion_audit_active() and converted_columns:
        mark_conversion_audit_unavailable(
            "numeric conversion ran without _dq_row_id"
        )

    for column in float_columns:
        if column not in frame.columns:
            continue

        values = (
            frame[column]
            .astype("string")
            .str.replace(",", "", regex=False)
            .str.strip()
        )

        frame[column] = pd.to_numeric(
            values,
            errors="coerce",
        ).astype("Float64")

    for column in integer_columns:
        if column not in frame.columns:
            continue

        values = (
            frame[column]
            .astype("string")
            .str.replace(",", "", regex=False)
            .str.strip()
        )

        numeric = pd.to_numeric(
            values,
            errors="coerce",
        )

        # 1.5 같은 값을 정수 1로 잘라버리지 않는다.
        integer_mask = (
            numeric.isna()
            | (numeric % 1 == 0)
        )

        numeric = numeric.where(integer_mask)

        frame[column] = numeric.astype("Int64")

    if audit_before is not None:
        record_conversion_step(
            audit_before,
            frame[[DQ_LINEAGE_COLUMN, *converted_columns]],
            columns=converted_columns,
        )

    return frame


def safe_datetime_series(
    series: pd.Series,
) -> pd.Series:
    """
    확인된 YYYY 계열 날짜/시간을 안전하게 datetime으로 변환한다.

    숫자형 날짜는 길이로 포맷을 구분하고,
    나머지 YYYY-... 계열은 mixed parser를 사용한다.
    """

    values = (
        series
        .astype("string")
        .str.strip()
    )

    result = pd.Series(
        pd.NaT,
        index=series.index,
        dtype="datetime64[ns]",
    )

    not_null = values.notna()

    if not not_null.any():
        return result

    years = pd.to_numeric(
        values.str.extract(
            r"^(\d{4})",
            expand=False,
        ),
        errors="coerce",
    )

    safe_year = years.between(
        1700,
        2200,
        inclusive="both",
    )

    candidates = (
        not_null
        & safe_year
    )

    if not candidates.any():
        return result

    numeric_mask = (
        candidates
        & values.str.fullmatch(
            r"\d+",
            na=False,
        )
    )

    lengths = values.str.len()

    numeric_formats = {
        8: "%Y%m%d",
        12: "%Y%m%d%H%M",
        14: "%Y%m%d%H%M%S",
    }

    for length, date_format in numeric_formats.items():
        mask = (
            numeric_mask
            & lengths.eq(length)
        )

        if not mask.any():
            continue

        result.loc[mask] = pd.to_datetime(
            values.loc[mask],
            format=date_format,
            errors="coerce",
        )

    remaining = (
        candidates
        & result.isna()
        & ~numeric_mask
    )

    if remaining.any():
        result.loc[remaining] = pd.to_datetime(
            values.loc[remaining],
            format="mixed",
            errors="coerce",
        )

    return result


def safe_timestamp_series(
    series: pd.Series,
) -> pd.Series:
    """timezone offset가 포함될 수 있는 timestamp용."""

    values = (
        series
        .astype("string")
        .str.strip()
    )

    return pd.to_datetime(
        values,
        errors="coerce",
        utc=True,
    )


def convert_datetimes(
    frame: pd.DataFrame,
    columns: tuple[str, ...],
) -> pd.DataFrame:
    """확인된 날짜/시간 컬럼을 변환한다."""

    converted_columns = [column for column in columns if column in frame.columns]
    audit_before = None
    if conversion_audit_active() and DQ_LINEAGE_COLUMN in frame.columns:
        audit_before = frame[[DQ_LINEAGE_COLUMN, *converted_columns]].copy()
    elif conversion_audit_active() and converted_columns:
        mark_conversion_audit_unavailable(
            "datetime conversion ran without _dq_row_id"
        )

    for column in columns:
        if column in frame.columns:
            frame[column] = safe_datetime_series(
                frame[column]
            )

    if audit_before is not None:
        record_conversion_step(
            audit_before,
            frame[[DQ_LINEAGE_COLUMN, *converted_columns]],
            columns=converted_columns,
        )

    return frame


def standardize_sido_columns(
    frame: pd.DataFrame,
    columns: tuple[str, ...],
) -> pd.DataFrame:
    """명시된 시도명 컬럼만 표준화한다."""

    for column in columns:
        if column not in frame.columns:
            continue

        frame[column] = (
            frame[column]
            .astype("string")
            .str.strip()
            .replace(SIDO_REPLACEMENTS)
        )

    return frame


def filter_korea_coordinates(
    frame: pd.DataFrame,
    latitude: str,
    longitude: str,
    *,
    removal_reason: str | None = None,
) -> pd.DataFrame:
    """
    대표 위도/경도를 숫자형으로 변환하고 대한민국 범위를 검증한다.

    위도  : 33.0 ~ 39.5
    경도  : 124.0 ~ 132.0

    대표 좌표가 NULL이거나 범위를 벗어나면 processed에서 행을 제거한다.
    """

    missing = [
        name
        for name in (latitude, longitude)
        if name not in frame.columns
    ]

    if missing:
        raise RuntimeError(
            "좌표 컬럼 누락: "
            + ", ".join(missing)
        )

    convert_numeric(
        frame,
        float_columns=(
            latitude,
            longitude,
        ),
    )

    valid = (
        frame[latitude].between(
            33.0,
            39.5,
            inclusive="both",
        )
        & frame[longitude].between(
            124.0,
            132.0,
            inclusive="both",
        )
    ).fillna(False)

    if removal_reason is not None:
        record_removal_reason(frame.loc[~valid], removal_reason)

    return frame.loc[valid].copy()


def clean_optional_coordinates(
    frame: pd.DataFrame,
    latitude: str,
    longitude: str,
) -> pd.DataFrame:
    """
    주변 교통 등 부가 위도/경도를 정제한다.

    좌표가 없거나 대한민국 범위를 벗어나도
    전체 행을 제거하지 않고 좌표쌍만 NULL 처리한다.
    """

    if (
        latitude not in frame.columns
        or longitude not in frame.columns
    ):
        return frame

    convert_numeric(
        frame,
        float_columns=(
            latitude,
            longitude,
        ),
    )

    valid = (
        frame[latitude].between(
            33.0,
            39.5,
            inclusive="both",
        )
        & frame[longitude].between(
            124.0,
            132.0,
            inclusive="both",
        )
    )

    invalid = ~valid

    frame.loc[
        invalid,
        latitude,
    ] = pd.NA

    frame.loc[
        invalid,
        longitude,
    ] = pd.NA

    return frame


def add_needs_review(
    frame: pd.DataFrame,
    columns: tuple[str, ...],
) -> pd.DataFrame:
    """핵심 보조정보 결측 여부를 표시한다."""

    existing = [
        column
        for column in columns
        if column in frame.columns
    ]

    if existing:
        frame["needs_review"] = (
            frame[existing]
            .isna()
            .any(axis=1)
        )
    else:
        frame["needs_review"] = False

    return frame


def format_elapsed(
    seconds: float,
) -> str:
    """정제시간을 초 또는 분+초 형식으로 반환한다."""

    if seconds < 60:
        return f"{seconds:.1f}초"

    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60

    return (
        f"{minutes}분 "
        f"{remaining_seconds:.1f}초"
    )


def deduplicate_valid_keys(
    frame: pd.DataFrame,
    keys: tuple[str, ...],
    *,
    removal_reason: str | None = None,
) -> pd.DataFrame:
    """
    모든 key 값이 존재하는 행만 중복 제거한다.

    key가 NULL인 행끼리 동일 key로 취급되어
    의도치 않게 제거되는 것을 방지한다.
    """

    if not all(
        column in frame.columns
        for column in keys
    ):
        return frame

    valid_key = (
        frame[list(keys)]
        .notna()
        .all(axis=1)
    )

    valid_rows = (
        frame.loc[valid_key]
        .drop_duplicates(
            subset=list(keys),
            keep="last",
        )
    )

    if removal_reason is not None:
        duplicate = valid_key & frame.duplicated(
            subset=list(keys),
            keep="last",
        )
        record_removal_reason(frame.loc[duplicate], removal_reason)

    invalid_rows = frame.loc[~valid_key]

    return pd.concat(
        [
            valid_rows,
            invalid_rows,
        ],
        ignore_index=True,
    )


# ============================================================
# 1. AirKorea 대기질
# ============================================================

def get_effective_null_mask(
    series: pd.Series,
) -> pd.Series:
    """
    원본 데이터에서 실질적인 결측값을 판별한다.

    실제 NULL뿐 아니라 NULL_STRINGS에 정의된 대표 결측 문자열도
    결측으로 간주한다.

    원본 Series 자체는 변경하지 않는다.
    """

    if (
        pd.api.types.is_object_dtype(series.dtype)
        or isinstance(series.dtype, pd.StringDtype)
    ):
        values = (
            series
            .astype("string")
            .str.strip()
        )

        return (
            values.isna()
            | values.str.lower().isin(NULL_STRINGS)
        )

    return series.isna()


def count_effective_nulls(
    frame: pd.DataFrame,
) -> int:
    """
    DataFrame 전체의 실질적인 결측 셀 개수를 계산한다.

    RAW 통계에서도 '-', '--', 'null', 'n/a' 등
    NULL_STRINGS에 정의된 값을 결측으로 집계한다.
    """

    total = 0

    for column in frame.columns:
        total += int(
            get_effective_null_mask(
                frame[column]
            ).sum()
        )

    return total


def drop_fully_empty_columns(
    raw: pd.DataFrame,
    processed: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, object]]]:
    """
    RAW와 PROCESSED에서 모두 100% 결측인 컬럼을
    PROCESSED에서만 제거한다.

    RAW는 변경하지 않는다.

    정제 과정의 형변환 실패 등으로 PROCESSED에서만
    전부 NULL이 된 컬럼은 자동 삭제하지 않는다.
    따라서 데이터 품질 문제를 빈 컬럼 제거로 숨기지 않는다.

    needs_review는 processor가 생성한 품질관리 컬럼이므로
    자동 제거 대상에서 제외한다.
    """

    empty_columns: list[str] = []
    empty_column_stats: list[dict[str, object]] = []

    raw_rows = len(raw)

    for column in processed.columns:

        if column == "needs_review":
            continue

        if column not in raw.columns:
            continue

        raw_null_mask = get_effective_null_mask(
            raw[column]
        )

        raw_null_count = int(
            raw_null_mask.sum()
        )

        raw_fully_empty = (
            raw_rows > 0
            and raw_null_count == raw_rows
        )

        processed_null_count = int(
            processed[column]
            .isna()
            .sum()
        )

        processed_fully_empty = (
            len(processed) > 0
            and processed_null_count == len(processed)
        )

        if (
            raw_fully_empty
            and processed_fully_empty
        ):
            empty_columns.append(column)

            empty_column_stats.append(
                {
                    "column": column,
                    "null_count": raw_null_count,
                    "row_count": raw_rows,
                    "null_rate": (
                        raw_null_count
                        / raw_rows
                        * 100
                        if raw_rows
                        else 0.0
                    ),
                }
            )

    if empty_columns:
        processed = processed.drop(
            columns=empty_columns,
        )

    return (
        processed,
        empty_column_stats,
    )

def add_air_quality_observation_key(frame: pd.DataFrame) -> pd.DataFrame:
    """Distinguish measured observations from undated station status captures.

    Preserve the source dataTime, including NULL. A collection timestamp is
    only an identity for an undated capture, never a fabricated measurement.
    No current clock, arbitrary sentinel, or payload hash participates in keys.
    """
    result = frame.copy()
    times = normalize_nulls(pd.DataFrame({
        column: frame[column] if column in frame else pd.Series(None, index=frame.index, dtype="object")
        for column in ("dataTime", "collected_at")
    }))
    identities = []
    for measured, collected in times.itertuples(index=False, name=None):
        dated = pd.notna(measured)
        value = measured if dated else collected
        source = "dataTime" if dated else "collected_at"
        if pd.isna(value):
            raise ValueError("air_quality: missing dataTime and collected_at; observation identity unavailable")
        # API/local timestamps are unzoned Korean local time. Reject numeric,
        # malformed or zoned values rather than interpreting them as an epoch.
        if not isinstance(value, (str, pd.Timestamp)) and not hasattr(value, "isoformat"):
            raise ValueError(f"air_quality: invalid {source} observation timestamp")
        try:
            timestamp = pd.Timestamp(value)
        except (ValueError, TypeError, OverflowError) as exc:
            raise ValueError(f"air_quality: invalid {source} observation timestamp") from exc
        if pd.isna(timestamp) or timestamp.tzinfo is not None:
            raise ValueError(f"air_quality: invalid {source} observation timestamp")
        prefix = "measured:" if dated else "collected:"
        identities.append(prefix + timestamp.isoformat())
    result["observation_time_key"] = pd.Series(identities, index=frame.index, dtype="string")
    return result


def process_air_quality(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    frame = normalize_nulls(add_air_quality_observation_key(raw))

    convert_numeric(
        frame,
        float_columns=(
            "o3Value",
        ),
        integer_columns=(
            "pm10Value",
            "pm25Value",
            "khaiValue",
            "pm10Grade",
            "pm25Grade",
            "khaiGrade",
        ),
    )

    convert_datetimes(
        frame,
        (
            "dataTime",
            "collected_at",
        ),
    )

    keys = (
        "sidoName",
        "stationName",
        "observation_time_key",
    )

    if all(
        column in frame.columns
        for column in keys
    ):
        valid_time = frame[list(keys)].notna().all(axis=1)
        duplicate = valid_time & frame.duplicated(subset=list(keys), keep="last")
        record_removal_reason(frame.loc[duplicate], "air_quality_duplicate_observation")

        frame = pd.concat(
            [
                frame.loc[
                    valid_time
                ].drop_duplicates(
                    subset=list(keys),
                    keep="last",
                ),
                frame.loc[~valid_time],
            ],
            ignore_index=True,
        )

    add_needs_review(
        frame,
        (
            "dataTime",
            "pm10Value",
            "pm25Value",
            "o3Value",
            "khaiValue",
        ),
    )

    sort_columns = [
        column
        for column in (
            "sidoName",
            "stationName",
            "dataTime",
        )
        if column in frame.columns
    ]

    if sort_columns:
        frame.sort_values(
            sort_columns,
            na_position="last",
            inplace=True,
        )

    return frame.reset_index(drop=True)


# ============================================================
# 2. 공공체육시설 API
# ============================================================

def process_facility(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    frame = normalize_nulls(raw)

    if "faci_stat_nm" in frame.columns:
        normal_operation = frame["faci_stat_nm"].eq("정상운영").fillna(False)
        record_removal_reason(
            frame.loc[~normal_operation],
            "facility_status_not_normal_operation",
        )
        frame = frame.loc[normal_operation].copy()

    frame = filter_korea_coordinates(
        frame,
        "faci_lat",
        "faci_lot",
        removal_reason="facility_invalid_or_missing_korea_coordinates",
    )

    convert_numeric(
        frame,
        float_columns=(
            "faci_gfa",
        ),
    )

    convert_datetimes(
        frame,
        (
            "base_ymd",
            "reg_dt",
            "updt_dt",
        ),
    )

    if "faci_cd" in frame.columns:
        valid_key = frame["faci_cd"].notna()
        duplicate = valid_key & frame.duplicated(
            subset=["faci_cd"],
            keep="last",
        )
        record_removal_reason(
            frame.loc[duplicate],
            "facility_duplicate_faci_cd",
        )

        frame = pd.concat(
            [
                frame.loc[
                    valid_key
                ].drop_duplicates(
                    subset=["faci_cd"],
                    keep="last",
                ),
                frame.loc[~valid_key],
            ],
            ignore_index=True,
        )

    existing_columns = [
        column
        for column in (
            "faci_cd",
            "faci_nm",
            "faci_stat_nm",
            "ftype_nm",
            "fcob_nm",
            "inout_gbn_nm",
            "cp_nm",
            "cpb_nm",
            "addr_emd_nm",
            "faci_road_addr",
            "faci_addr",
            "faci_lat",
            "faci_lot",
            "faci_gfa",
            "base_ymd",
            "reg_dt",
            "updt_dt",
            DQ_LINEAGE_COLUMN,
        )
        if column in frame.columns
    ]

    return (
        frame.loc[:, existing_columns]
        .reset_index(drop=True)
    )


# ============================================================
# 3. 공공개방시설
# ============================================================

def process_public_open_facility(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    frame = normalize_nulls(raw)

    if "sido" in frame.columns:
        frame.drop(
            columns=["sido"],
            inplace=True,
        )

    address = pd.Series(
        pd.NA,
        index=frame.index,
        dtype="string",
    )

    if "rdnmadr" in frame.columns:
        address = (
            frame["rdnmadr"]
            .astype("string")
        )

    if "lnmadr" in frame.columns:
        address = address.fillna(
            frame["lnmadr"]
            .astype("string")
        )

    frame["sido_raw"] = (
        address
        .str.split()
        .str[0]
    )

    frame["sido_standard"] = (
        frame["sido_raw"]
        .replace(SIDO_REPLACEMENTS)
    )

    frame = filter_korea_coordinates(
        frame,
        "latitude",
        "longitude",
    )

    frame["coordinate_exists"] = True
    frame["coordinate_valid"] = True

    frame["address_valid"] = (
        frame["sido_standard"]
        .notna()
    )

    frame["needs_review"] = (
        ~frame["address_valid"]
    )

    return frame.reset_index(drop=True)


# ============================================================
# 5. AED
# ============================================================

def process_aed(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    frame = normalize_nulls(raw)

    frame = filter_korea_coordinates(
        frame,
        "wgs84Lat",
        "wgs84Lon",
    )

    if "serialSeq" in frame.columns:
        valid_key = frame["serialSeq"].notna()

        frame = pd.concat(
            [
                frame.loc[
                    valid_key
                ].drop_duplicates(
                    subset=["serialSeq"],
                    keep="last",
                ),
                frame.loc[~valid_key],
            ],
            ignore_index=True,
        )

    return frame.reset_index(drop=True)


# ============================================================
# 6~7. 초단기 실황 / 예보
# ============================================================

def process_weather(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    frame = normalize_nulls(raw)

    convert_numeric(
        frame,
        float_columns=(
            "obsrValue",
        ),
        integer_columns=(
            "nx",
            "ny",
        ),
    )

    if "fcstValue" in frame.columns:
        audit_before = None
        if conversion_audit_active() and DQ_LINEAGE_COLUMN in frame.columns:
            audit_before = frame[[DQ_LINEAGE_COLUMN, "fcstValue"]].copy()
        elif conversion_audit_active():
            mark_conversion_audit_unavailable(
                "weather value conversion ran without _dq_row_id"
            )

        values = (
            frame["fcstValue"]
            .astype("string")
            .str.replace(",", "", regex=False)
            .str.strip()
        )

        numeric_values = pd.to_numeric(
            values,
            errors="coerce",
        ).astype("Float64")

        # RN1은 "강수없음", "1.0mm 미만"처럼 숫자가 아닌
        # 강수량 표현을 사용하므로 원문 문자열을 보존한다.
        rain_category = (
            frame["category"].eq("RN1").fillna(False)
            if "category" in frame.columns
            else pd.Series(False, index=frame.index)
        )

        converted_values = numeric_values.astype("object")
        converted_values.loc[rain_category] = values.loc[
            rain_category
        ]
        frame["fcstValue"] = converted_values

        if audit_before is not None:
            record_conversion_step(
                audit_before,
                frame[[DQ_LINEAGE_COLUMN, "fcstValue"]],
                columns=("fcstValue",),
            )

    convert_datetimes(
        frame,
        (
            "baseDate",
            "fcstDate",
            "collected_at",
        ),
    )

    possible_keys = (
        "baseDate",
        "baseTime",
        "fcstDate",
        "fcstTime",
        "category",
        "nx",
        "ny",
    )

    keys = tuple(
        column
        for column in possible_keys
        if column in frame.columns
    )

    if keys:
        frame = deduplicate_valid_keys(
            frame,
            keys,
            removal_reason="weather_duplicate_observation",
        )

    return frame.reset_index(drop=True)


# ============================================================
# 8. 두루누비 trails
# ============================================================

def process_durunubi_trails(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    frame = normalize_nulls(raw)

    convert_datetimes(
        frame,
        (
            "createdtime",
            "modifiedtime",
            "collected_at",
        ),
    )

    # routeIdx는 trail 식별자이므로
    # NULL/중복을 임의 제거하지 않고 수집 이상으로 처리한다.
    if "routeIdx" not in frame.columns:
        raise ValueError(
            "durunubi_trails: 필수 컬럼 routeIdx가 없습니다."
        )

    null_key_count = int(
        frame["routeIdx"].isna().sum()
    )

    if null_key_count:
        raise ValueError(
            "durunubi_trails: "
            f"routeIdx NULL {null_key_count:,}건 발견"
        )

    duplicate_count = int(
        frame.duplicated(
            subset=["routeIdx"],
            keep=False,
        ).sum()
    )

    if duplicate_count:
        raise ValueError(
            "durunubi_trails: "
            f"routeIdx 중복 행 {duplicate_count:,}건 발견"
        )

    add_needs_review(
        frame,
        (
            "routeIdx",
            "themeNm",
        ),
    )

    return frame.reset_index(drop=True)


# ============================================================
# 9. 두루누비 segments
# ============================================================

def process_durunubi_segments(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    frame = normalize_nulls(raw)

    convert_numeric(
        frame,
        float_columns=(
            "crsDstnc",
        ),
        integer_columns=(
            "crsLevel",
        ),
    )

    convert_datetimes(
        frame,
        (
            "createdtime",
            "modifiedtime",
            "collected_at",
        ),
    )

    # 기존 collector의 무결성 계약 유지:
    # crsIdx가 segment 자체의 식별자다.
    required_columns = (
        "routeIdx",
        "crsIdx",
    )

    missing_columns = [
        column
        for column in required_columns
        if column not in frame.columns
    ]

    if missing_columns:
        raise ValueError(
            "durunubi_segments: 필수 컬럼 누락: "
            + ", ".join(missing_columns)
        )

    null_route_count = int(
        frame["routeIdx"].isna().sum()
    )

    if null_route_count:
        raise ValueError(
            "durunubi_segments: "
            f"routeIdx NULL {null_route_count:,}건 발견"
        )

    null_key_count = int(
        frame["crsIdx"].isna().sum()
    )

    if null_key_count:
        raise ValueError(
            "durunubi_segments: "
            f"crsIdx NULL {null_key_count:,}건 발견"
        )

    duplicate_count = int(
        frame.duplicated(
            subset=["crsIdx"],
            keep=False,
        ).sum()
    )

    if duplicate_count:
        raise ValueError(
            "durunubi_segments: "
            f"crsIdx 중복 행 {duplicate_count:,}건 발견"
        )

    add_needs_review(
        frame,
        (
            "routeIdx",
            "crsIdx",
            "crsKorNm",
            "crsDstnc",
        ),
    )

    return frame.reset_index(drop=True)


# ============================================================
# 10. 기상특보
# ============================================================

def process_weather_warning(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    frame = normalize_nulls(raw)

    convert_numeric(
        frame,
        integer_columns=(
            "tmSeq",
        ),
    )

    # stnId는 날짜처럼 생겨도 ID이므로 문자열 유지
    convert_datetimes(
        frame,
        (
            "tmFc",
            "collected_at",
        ),
    )

    frame = deduplicate_valid_keys(
        frame,
        (
            "stnId",
            "tmFc",
            "tmSeq",
        ),
        removal_reason="weather_warning_duplicate_stn_tmfc_tmseq",
    )

    return frame.reset_index(drop=True)


# ============================================================
# 11. 기상특보 상태
# ============================================================

WEATHER_WARNING_STATUS_COLUMNS = [
    "warning_type",
    "warning_level",
    "warning_status",
    "area_name",
    "area_type",
    "effective_time",
    "tmEf",
    "tmFc",
    "tmSeq",
    "collected_at",
]


def _weather_warning_area_type(area_name):
    if area_name is None:
        return None
    return (
        "SEA"
        if any(keyword in str(area_name) for keyword in ("앞바다", "먼바다", "해역"))
        else "LAND"
    )


def _parse_active_weather_warning(latest: pd.Series) -> list[dict]:
    warning_text = latest.get("t6")
    if pd.isna(warning_text):
        return []
    warning_text = str(warning_text).strip()
    if not warning_text or re.fullmatch(r"[oO○]?\s*없음", warning_text):
        return []

    rows = []
    for line in warning_text.splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        left, area_text = line.split(":", 1)
        left = re.sub(r"^[oO○]\s*", "", left).strip()
        area_text = area_text.strip()
        if not area_text:
            continue
        warning_type = left
        warning_level = None
        if left.endswith("주의보"):
            warning_type = left[:-3].strip()
            warning_level = "주의보"
        elif left.endswith("경보"):
            warning_type = left[:-2].strip()
            warning_level = "경보"
        row = {
            "warning_type": warning_type,
            "warning_level": warning_level,
            "warning_status": "ACTIVE",
            "area_name": area_text,
            "area_type": _weather_warning_area_type(area_text),
            "effective_time": None,
            "tmEf": latest.get("tmEf"),
            "tmFc": latest.get("tmFc"),
            "tmSeq": latest.get("tmSeq"),
            "collected_at": latest.get("collected_at"),
        }
        if DQ_LINEAGE_COLUMN in latest.index:
            row[DQ_LINEAGE_COLUMN] = latest[DQ_LINEAGE_COLUMN]
        rows.append(row)
    return rows


def _parse_preliminary_weather_warning(latest: pd.Series) -> list[dict]:
    warning_text = latest.get("t7")
    if pd.isna(warning_text):
        return []
    warning_text = str(warning_text).strip()
    if not warning_text or re.fullmatch(r"[oO○]?\s*없음", warning_text):
        return []

    rows = []
    current_warning_type = None
    for line in warning_text.splitlines():
        line = line.strip()
        if not line:
            continue
        if "예비특보" in line and re.match(r"^\(\d+\)", line):
            current_warning_type = re.sub(r"^\(\d+\)\s*", "", line)
            current_warning_type = current_warning_type.replace("예비특보", "").strip()
            continue
        if not re.match(r"^[oO○]\s*", line) or ":" not in line:
            continue
        left, area_text = line.split(":", 1)
        area_text = area_text.strip()
        if not area_text:
            continue
        row = {
            "warning_type": current_warning_type,
            "warning_level": "예비특보",
            "warning_status": "PRELIMINARY",
            "area_name": area_text,
            "area_type": _weather_warning_area_type(area_text),
            "effective_time": re.sub(r"^[oO○]\s*", "", left).strip(),
            "tmEf": latest.get("tmEf"),
            "tmFc": latest.get("tmFc"),
            "tmSeq": latest.get("tmSeq"),
            "collected_at": latest.get("collected_at"),
        }
        if DQ_LINEAGE_COLUMN in latest.index:
            row[DQ_LINEAGE_COLUMN] = latest[DQ_LINEAGE_COLUMN]
        rows.append(row)
    return rows

def process_weather_warning_status(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    if raw.empty:
        columns = WEATHER_WARNING_STATUS_COLUMNS.copy()
        if DQ_LINEAGE_COLUMN in raw.columns:
            columns.append(DQ_LINEAGE_COLUMN)
        return pd.DataFrame(columns=columns)

    required = {"t6", "t7", "tmFc", "tmSeq"}
    missing = required - set(raw.columns)
    if missing:
        raise ValueError(
            "weather_warning_status RAW 컬럼 누락: "
            + ", ".join(sorted(missing))
        )

    work = normalize_nulls(raw)
    work["_tmFc_sort"] = pd.to_numeric(work["tmFc"], errors="coerce")
    work["_tmSeq_sort"] = pd.to_numeric(work["tmSeq"], errors="coerce")
    latest = work.sort_values(
        ["_tmFc_sort", "_tmSeq_sort"],
        na_position="first",
    ).iloc[-1]
    if DQ_LINEAGE_COLUMN in work.columns:
        record_removal_reason(
            work.loc[
                work[DQ_LINEAGE_COLUMN].ne(latest[DQ_LINEAGE_COLUMN])
            ],
            "weather_warning_status_superseded_snapshot",
        )
    rows = (
        _parse_active_weather_warning(latest)
        + _parse_preliminary_weather_warning(latest)
    )
    if not rows:
        record_removal_reason(
            latest.to_frame().T,
            "weather_warning_status_no_active_or_preliminary_warning",
        )
    columns = WEATHER_WARNING_STATUS_COLUMNS.copy()
    if DQ_LINEAGE_COLUMN in raw.columns:
        columns.append(DQ_LINEAGE_COLUMN)
    frame = pd.DataFrame(rows, columns=columns)

    convert_numeric(
        frame,
        integer_columns=(
            "tmSeq",
        ),
    )

    convert_datetimes(
        frame,
        (
            "tmEf",
            "tmFc",
            "collected_at",
        ),
    )

    frame = deduplicate_valid_keys(
        frame,
        (
            "warning_type",
            "warning_level",
            "warning_status",
            "area_name",
            "area_type",
            "tmEf",
            "tmFc",
            "tmSeq",
        ),
    )

    return frame.reset_index(drop=True)


# ============================================================
# 12. 자전거 사고다발지역
# ============================================================

def process_bicycle_accident(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    frame = normalize_nulls(raw)

    rename_map = {
        "request_year": "search_year",
        "request_sido": "si_do",
        "request_gugun": "gu_gun",
        "request_province_name": "province_name",
        "request_district_name": "district_name",
        "lo_crd": "longitude",
        "la_crd": "latitude",
    }
    frame.rename(
        columns={
            source: target
            for source, target in rename_map.items()
            if source in frame.columns and target not in frame.columns
        },
        inplace=True,
    )

    required_columns = {
        "afos_fid",
        "afos_id",
        "bjd_cd",
        "spot_cd",
        "sido_sgg_nm",
        "spot_nm",
        "search_year",
        "si_do",
        "gu_gun",
        "expected_afos_id",
        "request_page_no",
        "longitude",
        "latitude",
        "geom_json",
        "collected_at",
    }
    missing_columns = required_columns - set(frame.columns)
    if missing_columns:
        raise ValueError(
            "koroad bicycle 필수 컬럼 누락: "
            + ", ".join(sorted(missing_columns))
        )

    for column in (
        "afos_fid",
        "afos_id",
        "bjd_cd",
        "spot_cd",
        "si_do",
        "gu_gun",
        "expected_afos_id",
    ):
        frame[column] = frame[column].astype("string")

    numeric_columns = (
        "search_year",
        "occrrnc_cnt",
        "caslt_cnt",
        "dth_dnv_cnt",
        "se_dnv_cnt",
        "sl_dnv_cnt",
        "wnd_dnv_cnt",
        "request_page_no",
        "longitude",
        "latitude",
    )
    numeric_before = {
        column: frame[column].copy()
        for column in numeric_columns
        if column in frame.columns
    }

    convert_numeric(
        frame,
        float_columns=(
            "longitude",
            "latitude",
        ),
        integer_columns=(
            "search_year",
            "occrrnc_cnt",
            "caslt_cnt",
            "dth_dnv_cnt",
            "se_dnv_cnt",
            "sl_dnv_cnt",
            "wnd_dnv_cnt",
            "request_page_no",
        ),
    )

    invalid_numeric = {
        column: numeric_before[column][
            numeric_before[column].notna() & frame[column].isna()
        ].astype(str).head(5).tolist()
        for column in numeric_before
        if (numeric_before[column].notna() & frame[column].isna()).any()
    }
    if invalid_numeric:
        raise ValueError(f"koroad bicycle 숫자 변환 실패: {invalid_numeric}")

    for column in (
        "occrrnc_cnt",
        "caslt_cnt",
        "dth_dnv_cnt",
        "se_dnv_cnt",
        "sl_dnv_cnt",
        "wnd_dnv_cnt",
    ):
        if column in frame.columns and (frame[column].dropna() < 0).any():
            raise ValueError(f"{column}에 음수가 있습니다.")

    coordinate_valid = (
        frame["longitude"].between(124.0, 132.0, inclusive="both")
        & frame["latitude"].between(32.0, 39.5, inclusive="both")
    ).fillna(False)
    frame = frame.loc[coordinate_valid].copy()
    frame["coordinate_valid"] = True

    def normalize_geojson(value):
        if pd.isna(value):
            return None, False
        try:
            parsed = json.loads(value) if isinstance(value, str) else value
        except (TypeError, ValueError, json.JSONDecodeError):
            return None, False
        if not isinstance(parsed, dict):
            return None, False
        if parsed.get("type") not in {"Polygon", "MultiPolygon"}:
            return None, False
        coordinates = parsed.get("coordinates")
        if not isinstance(coordinates, list) or not coordinates:
            return None, False
        return json.dumps(
            parsed,
            ensure_ascii=False,
            separators=(",", ":"),
        ), True

    normalized_geometries = frame["geom_json"].map(normalize_geojson)
    frame["geom_json"] = normalized_geometries.map(lambda item: item[0])
    frame["polygon_valid"] = normalized_geometries.map(
        lambda item: item[1]
    ).astype(bool)

    audit_before = None
    if conversion_audit_active() and DQ_LINEAGE_COLUMN in frame.columns:
        audit_before = frame[[DQ_LINEAGE_COLUMN, "collected_at"]].copy()
    frame["collected_at"] = safe_timestamp_series(frame["collected_at"])
    if audit_before is not None:
        record_conversion_step(
            audit_before,
            frame[[DQ_LINEAGE_COLUMN, "collected_at"]],
            columns=("collected_at",),
        )
    if frame["collected_at"].isna().any():
        raise ValueError("collected_at을 timestamp로 변환할 수 없는 행이 있습니다.")

    critical_columns = (
        "afos_fid",
        "afos_id",
        "bjd_cd",
        "spot_cd",
        "sido_sgg_nm",
        "spot_nm",
        "search_year",
        "si_do",
        "gu_gun",
    )
    critical_nulls = {
        column: int(frame[column].isna().sum())
        for column in critical_columns
        if frame[column].isna().any()
    }
    if critical_nulls:
        raise ValueError(f"필수 컬럼에 NULL이 있습니다: {critical_nulls}")

    mismatch = (frame["afos_id"] != frame["expected_afos_id"]).fillna(True)
    if mismatch.any():
        raise ValueError("search_year와 응답 afos_id 관계가 코드표와 다릅니다.")

    duplicate_mask = frame.duplicated("afos_fid", keep=False)
    if duplicate_mask.any():
        compare_columns = [
            "afos_fid", "afos_id", "bjd_cd", "spot_cd", "sido_sgg_nm",
            "spot_nm", "occrrnc_cnt", "caslt_cnt", "dth_dnv_cnt",
            "se_dnv_cnt", "sl_dnv_cnt", "wnd_dnv_cnt", "longitude",
            "latitude", "geom_json",
        ]
        conflicting_ids = []
        for afos_fid, group in frame.loc[duplicate_mask].groupby(
            "afos_fid",
            dropna=False,
        ):
            if len(
                group[compare_columns]
                .astype("string")
                .fillna("<NULL>")
                .drop_duplicates()
            ) > 1:
                conflicting_ids.append(str(afos_fid))
        if conflicting_ids:
            raise ValueError(
                "같은 afos_fid에 서로 다른 값이 있습니다: "
                + ", ".join(conflicting_ids[:10])
            )
        frame = frame.drop_duplicates("afos_fid", keep="first").copy()

    add_needs_review(
        frame,
        (
            "afos_fid",
            "spot_nm",
            "latitude",
            "longitude",
            "occrrnc_cnt",
        ),
    )

    preferred_columns = [
        "afos_fid", "afos_id", "search_year", "si_do", "gu_gun",
        "province_name", "district_name", "bjd_cd", "spot_cd",
        "sido_sgg_nm", "spot_nm", "occrrnc_cnt", "caslt_cnt",
        "dth_dnv_cnt", "se_dnv_cnt", "sl_dnv_cnt", "wnd_dnv_cnt",
        "longitude", "latitude", "coordinate_valid", "geom_json",
        "polygon_valid", "collected_at", "needs_review",
    ]
    existing_preferred = [
        column for column in preferred_columns if column in frame.columns
    ]
    remaining = [
        column for column in frame.columns if column not in existing_preferred
    ]
    return frame.reindex(columns=existing_preferred + remaining).reset_index(drop=True)


# ============================================================
# CULTURE 공통
# ============================================================

def process_culture_base(
    raw: pd.DataFrame,
) -> pd.DataFrame:
    """문화빅데이터 공통 1차 정제."""

    return normalize_nulls(raw)


# ============================================================
# 13. 문화 - 공공체육시설
# ============================================================

def process_culture_public_sports_facilities(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    frame = process_culture_base(raw)

    frame = filter_korea_coordinates(
        frame,
        "FCLTY_LA",
        "FCLTY_LO",
    )

    convert_numeric(
        frame,
        float_columns=(
            "FCLTY_AR_CO",
        ),
        integer_columns=(
            "ACMD_NMPR_CO",
            "ADTM_CO",
        ),
    )

    standardize_sido_columns(
        frame,
        (
            "POSESN_MBY_CTPRVN_NM",
            "ROAD_NM_CTPRVN_NM",
        ),
    )

    add_needs_review(
        frame,
        (
            "FCLTY_NM",
            "FCLTY_TY_NM",
            "RDNMADR_NM",
        ),
    )

    return frame.reset_index(drop=True)


# ============================================================
# 14. 문화 - 공공체육시설 프로그램
# ============================================================

def process_culture_public_sports_facility_programs(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    frame = process_culture_base(raw)

    # 시설 자체 위치는 서비스 핵심 좌표
    frame = filter_korea_coordinates(
        frame,
        "FCLTY_LA",
        "FCLTY_LO",
    )

    # 주변 대중교통 좌표는 부가정보
    for number in range(1, 6):
        clean_optional_coordinates(
            frame,
            f"PBTRNSP_FCLTY_{number}R_LA",
            f"PBTRNSP_FCLTY_{number}R_LO",
        )

    convert_numeric(
        frame,
        float_columns=(
            "STRT_DSTNC_1R_VALUE",
            "WLKG_DSTNC_1R_VALUE",
            "STRT_DSTNC_2R_VALUE",
            "WLKG_DSTNC_2R_VALUE",
            "STRT_DSTNC_3R_VALUE",
            "WLKG_DSTNC_3R_VALUE",
            "STRT_DSTNC_4R_VALUE",
            "WLKG_DSTNC_4R_VALUE",
            "STRT_DSTNC_5R_VALUE",
            "WLKG_DSTNC_5R_VALUE",
        ),
        integer_columns=(
            "WLKG_MVMN_1R_TIME",
            "WLKG_MVMN_2R_TIME",
            "WLKG_MVMN_3R_TIME",
            "WLKG_MVMN_4R_TIME",
            "WLKG_MVMN_5R_TIME",
            "PROGRM_RCRIT_NMPR_CO",
        ),
    )

    convert_datetimes(
        frame,
        (
            "PROGRM_BEGIN_DE",
            "PROGRM_END_DE",
        ),
    )

    standardize_sido_columns(
        frame,
        (
            "CTPRVN_NM",
        ),
    )

    add_needs_review(
        frame,
        (
            "FCLTY_NM",
            "PROGRM_NM",
            "PROGRM_TY_NM",
            "FCLTY_LA",
            "FCLTY_LO",
        ),
    )

    return frame.reset_index(drop=True)


# ============================================================
# 15. 문화 - 체육시설 주변 대중교통
# ============================================================

def process_culture_nearby_transport(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    frame = process_culture_base(raw)

    # 체육시설 좌표는 핵심
    frame = filter_korea_coordinates(
        frame,
        "ALSFC_LA",
        "ALSFC_LO",
    )

    # 주변 교통 좌표는 부가정보
    clean_optional_coordinates(
        frame,
        "PBTRNSP_FCLTY_LA",
        "PBTRNSP_FCLTY_LO",
    )

    convert_numeric(
        frame,
        float_columns=(
            "STRT_DSTNC_VALUE",
            "WLKG_DSTNC_VALUE",
        ),
        integer_columns=(
            "WLKG_MVMN_TIME",
        ),
    )

    standardize_sido_columns(
        frame,
        (
            "ALSFC_CTPRVN_NM",
        ),
    )

    add_needs_review(
        frame,
        (
            "ALSFC_NM",
            "ALSFC_ADDR",
            "PBTRNSP_FCLTY_SDIV_NM",
        ),
    )

    return frame.reset_index(drop=True)


# ============================================================
# 16. 문화 - 전국 체육시설 현황
# ============================================================

def process_culture_national_sports_facility_status(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    frame = process_culture_base(raw)

    frame = filter_korea_coordinates(
        frame,
        "FCLTY_LA",
        "FCLTY_LO",
    )

    convert_numeric(
        frame,
        float_columns=(
            "FCLTY_AR_CO",
        ),
        integer_columns=(
            "ADTM_CO",
            "ACMD_NMPR_CO",
        ),
    )

    convert_datetimes(
        frame,
        (
            "FCLTY_CRTN_STDR_DE",
            "ALSFC_REGIST_DE",
            "COMPET_DE",
            "SSS_DE",
            "OPER_CLSBIZ_DE",
            "REGIST_DT",
            "UPDT_DT",
        ),
    )

    standardize_sido_columns(
        frame,
        (
            "CTPRVN_NM",
            "FCLTY_MANAGE_CTPRVN_NM",
            "POSESN_MBY_CTPRVN_NM",
        ),
    )

    add_needs_review(
        frame,
        (
            "FCLTY_NM",
            "FCLTY_TY_NM",
            "CTPRVN_NM",
            "SIGNGU_NM",
        ),
    )

    return frame.reset_index(drop=True)


# ============================================================
# 17. 문화 - 위치기반 체력측정/운동처방
# ============================================================

def process_culture_location_fitness(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    frame = process_culture_base(raw)

    frame = filter_korea_coordinates(
        frame,
        "CNTER_LA",
        "CNTER_LO",
    )

    convert_numeric(
        frame,
        integer_columns=(
            "MESURE_AGE_CO",
        ),
    )

    # MESURE_IEM_xxx_VALUE는 컬럼명만으로
    # 단위와 값의 의미를 확정할 수 없으므로
    # 일괄 numeric coercion하지 않는다.

    convert_datetimes(
        frame,
        (
            "MESURE_DE",
        ),
    )

    standardize_sido_columns(
        frame,
        (
            "CTPRVN_NM",
        ),
    )

    add_needs_review(
        frame,
        (
            "CNTER_NM",
            "MESURE_DE",
            "MESURE_AGE_CO",
        ),
    )

    return frame.reset_index(drop=True)


# ============================================================
# 18. 문화 - 개방 학교체육시설
# ============================================================

def process_culture_open_school_facilities(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    frame = process_culture_base(raw)

    convert_numeric(
        frame,
        integer_columns=(
            "BASE_YEAR",
            "SHWERRM_CO",
            "TOILET_CO",
            "LOCKERRM_CO",
        ),
    )

    standardize_sido_columns(
        frame,
        (
            "ALSFC_CTPRVN_NM",
        ),
    )

    add_needs_review(
        frame,
        (
            "BASE_YEAR",
            "SCHUL_NM",
            "ALSFC_ADDR",
            "OPER_ITEM_CN",
        ),
    )

    return frame.reset_index(drop=True)


# ============================================================
# 19. 문화 - 체육시설 안전점검
# ============================================================

def process_culture_safety_inspections(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    frame = process_culture_base(raw)

    frame = filter_korea_coordinates(
        frame,
        "FCLTY_CRDNT_LA",
        "FCLTY_CRDNT_LO",
    )

    convert_numeric(
        frame,
        float_columns=(
            "FCLTY_TOTAR_CO",
        ),
    )

    convert_datetimes(
        frame,
        (
            "OPER_CLSBIZ_DE",
            "FCLTY_INFO_UPDT_DE",
            "SAFECHK_DE",
            "SAFECHK_OTHBC_DE",
            "FCLTY_INFO_REGIST_DE",
        ),
    )

    standardize_sido_columns(
        frame,
        (
            "CMPTNC_CTPRVN_NM",
        ),
    )

    add_needs_review(
        frame,
        (
            "FCLTY_NM",
            "FCLTY_CL_NM",
            "OPER_STATE_NM",
            "SAFECHK_GNRLZ_GRAD_NM",
        ),
    )

    return frame.reset_index(drop=True)


# ============================================================
# 20. 문화 - 체력측정 운동처방
# ============================================================

def process_culture_fitness_prescriptions(
    raw: pd.DataFrame,
) -> pd.DataFrame:

    frame = process_culture_base(raw)

    convert_numeric(
        frame,
        integer_columns=(
            "MESURE_AGE_CO",
        ),
    )

    convert_datetimes(
        frame,
        (
            "MESURE_DE",
        ),
    )

    add_needs_review(
        frame,
        (
            "CNTER_NM",
            "MESURE_DE",
            "MESURE_AGE_CO",
            "MVM_PRSCRPTN_CN",
        ),
    )

    return frame.reset_index(drop=True)


# ============================================================
# Processor Registry
# ============================================================

PROCESSORS: dict[
    str,
    Callable[[pd.DataFrame], pd.DataFrame],
] = {
    # 일반 API 수집 데이터
    "air_quality":
        process_air_quality,

    "facility":
        process_facility,

    "public_open_facility":
        process_public_open_facility,

    "aed":
        process_aed,

    # 날씨
    "weather_ultra_ncst":
        process_weather,

    "weather_ultra_fcst":
        process_weather,

    "weather_warning":
        process_weather_warning,

    "weather_warning_status":
        process_weather_warning_status,

    # 두루누비
    "durunubi_trails":
        process_durunubi_trails,

    "durunubi_segments":
        process_durunubi_segments,

    # 자전거 사고다발지역
    "koroad_bicycle_accident_hotspots":
        process_bicycle_accident,

    # 문화빅데이터
    "culture_public_sports_facilities":
        process_culture_public_sports_facilities,

    "culture_public_sports_facility_programs":
        process_culture_public_sports_facility_programs,

    "culture_sports_facility_nearby_public_transport":
        process_culture_nearby_transport,

    "culture_national_sports_facility_status":
        process_culture_national_sports_facility_status,

    "culture_location_fitness_measurement_prescriptions":
        process_culture_location_fitness,

    "culture_open_school_sports_facilities":
        process_culture_open_school_facilities,

    "culture_sports_facility_safety_inspections":
        process_culture_safety_inspections,

    "culture_fitness_measurement_prescriptions":
        process_culture_fitness_prescriptions,
}


# ============================================================
# 공통 진입점
# ============================================================

def process_dataset(
    table: str,
    raw: pd.DataFrame,
    *,
    allow_empty: bool = False,
) -> pd.DataFrame:
    """
    등록된 테이블의 PROCESSED 정제 함수를 실행한다.

    미등록 데이터셋:
        해당 데이터셋의 processed 적재를 차단한다.

    등록 데이터셋:
        전용 processor를 끝까지 실행한다.

    processor 내부 오류:
        DatasetProcessorError는 그대로 전달하고, 그 외 예외는
        DatasetProcessorError로 감싸서 상위 orchestration 계층에 전달한다.

    정제 결과 0건:
        allow_empty=False이면 기존 processed 보호를 위해 실패 처리한다.
        allow_empty=True이면 컬럼 정의가 있는 빈 DataFrame을 허용한다.

    정제 완료 후:
        결과가 비어 있지 않을 때 RAW와 PROCESSED에서 모두 100% 결측인 컬럼은
        PROCESSED에서만 제거한다.

        RAW/PROCESSED 행 수, 결측률, 행 제거율,
        빈 컬럼 제거 내역, needs_review 비율,
        정제시간을 정량적으로 출력한다.
    """

    start_time = time.perf_counter()

    raw_rows = len(raw)

    processor = PROCESSORS.get(table)

    # --------------------------------------------------------
    # 미등록 데이터셋
    # --------------------------------------------------------

    if processor is None:

        elapsed = (
            time.perf_counter()
            - start_time
        )

        print()
        print("=" * 70)
        print(f"[PROCESSOR ERROR] {table}")
        print(f"RAW             : {raw_rows:,}건")
        print("상태            : 미등록 데이터셋")
        print("PROCESSED       : 적재 안 함")
        print(
            f"정제시간        : "
            f"{format_elapsed(elapsed)}"
        )
        print("=" * 70)

        raise UnregisteredDatasetError(
            f"{table}: 등록된 dataset processor가 없습니다. "
            "검증되지 않은 데이터를 Silver로 적재하지 않습니다. "
            "정제 규칙을 정의한 뒤 PROCESSORS에 등록하세요."
        )


    # --------------------------------------------------------
    # 등록 데이터셋 정제
    # --------------------------------------------------------

    try:

        processed = processor(raw)

    except DatasetProcessorError:
        raise

    except Exception as exc:

        elapsed = (
            time.perf_counter()
            - start_time
        )

        print()
        print("=" * 70)
        print(f"[PROCESSOR ERROR] {table}")
        print(f"RAW             : {raw_rows:,}건")
        print("상태            : 정제 실패")
        print(f"오류            : {exc}")
        print("PROCESSED       : 적재 안 함")
        print(
            f"정제시간        : "
            f"{format_elapsed(elapsed)}"
        )
        print("=" * 70)

        raise DatasetProcessorError(
            f"{table}: Silver 정제 중 오류가 발생했습니다."
        ) from exc

    # --------------------------------------------------------
    # 정제 결과 검증
    # --------------------------------------------------------

    if processed.empty and not allow_empty:

        elapsed = (
            time.perf_counter()
            - start_time
        )

        print()
        print("=" * 70)
        print(f"[PROCESSOR ERROR] {table}")
        print(f"RAW             : {raw_rows:,}건")
        print("PROCESSED       : 0건")
        print("상태            : 정제 결과 없음")
        print("기존 processed : 유지")
        print(
            f"정제시간        : "
            f"{format_elapsed(elapsed)}"
        )
        print("=" * 70)

        raise EmptyProcessedDatasetError(
            f"{table}: 정제 후 유효 데이터가 0건입니다. "
            "기존 processed를 유지합니다."
        )

    # --------------------------------------------------------
    # 완전 결측 컬럼 제거
    # --------------------------------------------------------

    if processed.empty:
        empty_column_stats = []
    else:
        (
            processed,
            empty_column_stats,
        ) = drop_fully_empty_columns(
            raw,
            processed,
        )

    # 컬럼 제거 후에도 유효한 데이터 구조인지 확인
    if len(processed.columns) == 0:

        elapsed = (
            time.perf_counter()
            - start_time
        )

        print()
        print("=" * 70)
        print(f"[PROCESSOR ERROR] {table}")
        print(f"RAW             : {raw_rows:,}건")
        print("상태            : 유효 컬럼 없음")
        print("PROCESSED       : 적재 안 함")
        print("기존 processed : 유지")
        print(
            f"정제시간        : "
            f"{format_elapsed(elapsed)}"
        )
        print("=" * 70)

        raise EmptyProcessedDatasetError(
            f"{table}: 완전 결측 컬럼 제거 후 "
            "유효한 컬럼이 없습니다. "
            "기존 processed를 유지합니다."
        )


    return processed.reset_index(
        drop=True
    )
