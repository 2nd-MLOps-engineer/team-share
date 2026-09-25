from pathlib import Path
import pandas as pd


# ==================================================
# 1. 경로 설정
# ==================================================

# team-share 폴더
PROJECT_ROOT = Path(__file__).resolve().parents[3]

INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "public_open_facility"
    / "public_open_facility_all.csv"
)


# ==================================================
# 2. RAW CSV 읽기
# ==================================================

df = pd.read_csv(INPUT_PATH)

print("=" * 60)
print("RAW 데이터 검사")
print("=" * 60)

print("파일:", INPUT_PATH)
print("전체 건수:", f"{len(df):,}")
print("컬럼 수:", len(df.columns))

print()


# ==================================================
# 3. 컬럼 확인
# ==================================================

print("=== 컬럼 목록 ===")
print(df.columns.tolist())

print()


# ==================================================
# 4. 도로명주소 기준 시도 추출
# ==================================================

df["sido"] = (
    df["rdnmadr"]
    .astype("string")
    .str.strip()
    .str.split()
    .str[0]
)

print("=== 도로명주소 기준 시도 분포 ===")

print(
    df["sido"]
    .value_counts(dropna=False)
)

print()


# ==================================================
# 5. 주요 결측치 확인
# ==================================================

print("=== 결측치 확인 ===")

print(
    "도로명주소 없음:",
    f"{df['rdnmadr'].isna().sum():,}"
)

print(
    "지번주소 없음:",
    f"{df['lnmadr'].isna().sum():,}"
)

print(
    "위도 없음:",
    f"{df['latitude'].isna().sum():,}"
)

print(
    "경도 없음:",
    f"{df['longitude'].isna().sum():,}"
)

print()


# ==================================================
# 6. 도로명주소 없는 데이터 중
#    지번주소가 있는지 확인
# ==================================================

missing_road = df[df["rdnmadr"].isna()]

print("=== 도로명주소 없는 데이터 ===")
print("건수:", f"{len(missing_road):,}")

print(
    "그중 지번주소 있음:",
    f"{missing_road['lnmadr'].notna().sum():,}"
)

print(
    "도로명주소 + 지번주소 둘 다 없음:",
    f"{(
        missing_road['lnmadr'].isna()
    ).sum():,}"
)

print()


# ==================================================
# 7. 주소 없는 데이터 샘플
# ==================================================

columns_to_show = [
    "openFcltyNm",
    "rdnmadr",
    "lnmadr",
    "latitude",
    "longitude",
    "institutionNm",
]

print("=== 주소 확인 필요 데이터 샘플 ===")

print(
    missing_road[
        columns_to_show
    ]
    .head(30)
    .to_string(index=False)
)


print()
print("=" * 60)
print("검사 완료")
print("=" * 60)
