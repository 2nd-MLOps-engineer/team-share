from pathlib import Path

import pandas as pd


# ============================================================
# 1. 경로 설정
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[2]

INPUT_FILE = (
    BASE_DIR
    / "data"
    / "raw"
    / "air_quality"
    / "air_quality_all.csv"
)

OUTPUT_DIR = (
    BASE_DIR
    / "data"
    / "processed"
)

OUTPUT_FILE = (
    OUTPUT_DIR
    / "air_quality_processed.csv"
)


# ============================================================
# 2. 시작
# ============================================================

print("=" * 60)
print("대기질 데이터 전처리 시작")
print("=" * 60)

print()
print("입력 파일:")
print(INPUT_FILE)

if not INPUT_FILE.exists():
    raise FileNotFoundError(
        f"RAW CSV 파일을 찾을 수 없습니다: {INPUT_FILE}"
    )


# ============================================================
# 3. RAW CSV 읽기
# ============================================================

df = pd.read_csv(
    INPUT_FILE,
    encoding="utf-8-sig",
    dtype=str,
    keep_default_na=False
)

print()
print("원본 건수:", len(df))
print("원본 컬럼 수:", len(df.columns))

print()
print("원본 컬럼:")
print(df.columns.tolist())


# ============================================================
# 4. 문자열 앞뒤 공백 제거
# ============================================================

for col in df.columns:
    df[col] = df[col].str.strip()


# ============================================================
# 5. 일반 결측값 처리
# ============================================================

GENERAL_MISSING_VALUES = [
    "",
    "null",
    "NULL",
    "None",
    "nan",
]

df = df.replace(
    GENERAL_MISSING_VALUES,
    pd.NA
)


# ============================================================
# 6. 대기질 숫자 컬럼의 "-" 처리
#
# 중요:
# 날짜 2026-09-15의 "-"를 건드리는 것이 아님.
#
# 아래 숫자 컬럼에서
# 셀 전체 값이 정확히 "-"인 경우만
# "측정값 없음"으로 보고 NULL 처리.
# ============================================================

NUMERIC_COLUMNS = [
    "pm10Value",
    "pm25Value",
    "o3Value",
    "khaiValue",
    "pm10Grade",
    "pm25Grade",
    "khaiGrade",
]

for col in NUMERIC_COLUMNS:
    df[col] = df[col].replace("-", pd.NA)


# ============================================================
# 7. 숫자 자료형 변환
# ============================================================

# PM10 : 정수
df["pm10Value"] = pd.to_numeric(
    df["pm10Value"],
    errors="coerce"
).astype("Int64")


# PM2.5 : 정수
df["pm25Value"] = pd.to_numeric(
    df["pm25Value"],
    errors="coerce"
).astype("Int64")


# O3 : 소수값
df["o3Value"] = pd.to_numeric(
    df["o3Value"],
    errors="coerce"
).astype("Float64")


# 통합대기환경지수(KHAI) : 정수
df["khaiValue"] = pd.to_numeric(
    df["khaiValue"],
    errors="coerce"
).astype("Int64")


# PM10 등급 : 정수
df["pm10Grade"] = pd.to_numeric(
    df["pm10Grade"],
    errors="coerce"
).astype("Int64")


# PM2.5 등급 : 정수
df["pm25Grade"] = pd.to_numeric(
    df["pm25Grade"],
    errors="coerce"
).astype("Int64")


# KHAI 등급 : 정수
df["khaiGrade"] = pd.to_numeric(
    df["khaiGrade"],
    errors="coerce"
).astype("Int64")


# ============================================================
# 8. 날짜/시간 자료형 변환
# ============================================================

df["dataTime"] = pd.to_datetime(
    df["dataTime"],
    errors="coerce"
)

df["collected_at"] = pd.to_datetime(
    df["collected_at"],
    errors="coerce"
)


# ============================================================
# 9. 중복 제거
#
# 같은 시도 + 측정소 + 측정시각이면
# 동일 관측 데이터로 판단
#
# 단, dataTime이 없는 행은 여기서 강제로 삭제하지 않음.
# needs_review 대상으로 남겨둔다.
# ============================================================

before_duplicate = len(df)

valid_time = df["dataTime"].notna()

df_valid = df[valid_time].drop_duplicates(
    subset=[
        "sidoName",
        "stationName",
        "dataTime",
    ],
    keep="last"
)

df_invalid = df[~valid_time]

df = pd.concat(
    [
        df_valid,
        df_invalid,
    ],
    ignore_index=True
)

duplicate_removed = (
    before_duplicate - len(df)
)

print()
print("중복 제거:", duplicate_removed)


# ============================================================
# 10. 완전히 빈 행 제거
# ============================================================

before_empty = len(df)

df = df.dropna(
    how="all"
)

empty_removed = (
    before_empty - len(df)
)

print("전체 결측 행 제거:", empty_removed)


# ============================================================
# 11. needs_review 생성
#
# 서비스에서 사용하는 핵심 측정 데이터 중
# 하나라도 없으면 확인 필요.
# ============================================================

REVIEW_COLUMNS = [
    "dataTime",
    "pm10Value",
    "pm25Value",
    "o3Value",
    "khaiValue",
    "pm10Grade",
    "pm25Grade",
    "khaiGrade",
]

df["needs_review"] = (
    df[REVIEW_COLUMNS]
    .isna()
    .any(axis=1)
)


# ============================================================
# 12. 정렬
# ============================================================

df = df.sort_values(
    by=[
        "sidoName",
        "stationName",
        "dataTime",
    ],
    na_position="last"
).reset_index(
    drop=True
)


# ============================================================
# 13. 최종 컬럼 순서
# ============================================================

FINAL_COLUMNS = [
    "sidoName",
    "stationName",
    "dataTime",

    "pm10Value",
    "pm25Value",
    "o3Value",
    "khaiValue",

    "pm10Grade",
    "pm25Grade",
    "khaiGrade",

    "data_type",
    "collected_at",

    "needs_review",
]

df = df[
    FINAL_COLUMNS
]


# ============================================================
# 14. 결과 확인
# ============================================================

print()
print("=" * 60)
print("전처리 결과")
print("=" * 60)

print()
print("최종 건수:", len(df))
print("최종 컬럼 수:", len(df.columns))

print(
    "needs_review:",
    int(df["needs_review"].sum())
)


print()
print("컬럼별 결측값:")

missing = df.isna().sum()
missing = missing[missing > 0]

if len(missing) == 0:
    print("결측값 없음")
else:
    print(missing)


print()
print("최종 자료형:")
print(df.dtypes)


# ============================================================
# 15. Processed CSV 저장
# ============================================================

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)

df.to_csv(
    OUTPUT_FILE,
    index=False,
    encoding="utf-8-sig",
    na_rep=""
)


# ============================================================
# 16. 완료
# ============================================================

print()
print("=" * 60)
print("대기질 데이터 전처리 완료")
print("=" * 60)

print()
print("저장 파일:")
print(OUTPUT_FILE)