from pathlib import Path
import pandas as pd


# ==================================================
# 1. 경로 설정
# ==================================================

# 현재 파일:
# team-share/M3_data_pipeline/collector/process_open_facility.py
#
# parents[2] = team-share
PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "public_open_facility"
    / "public_open_facility_all.csv"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "processed"
)

OUTPUT_PATH = (
    OUTPUT_DIR
    / "public_open_facility_processed.csv"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ==================================================
# 2. RAW CSV 읽기
# ==================================================

df = pd.read_csv(INPUT_PATH)


# ==================================================
# 3. RAW에 이전 테스트용 sido 컬럼이 있으면 제거
# ==================================================

if "sido" in df.columns:
    df = df.drop(columns=["sido"])


print("=" * 60)
print("공공시설 데이터 정제 시작")
print("=" * 60)

print("RAW 파일:", INPUT_PATH)
print("RAW 건수:", f"{len(df):,}")
print("RAW 컬럼 수:", len(df.columns))

print()


# ==================================================
# 4. 주소 문자열 정리
# ==================================================

df["rdnmadr"] = (
    df["rdnmadr"]
    .astype("string")
    .str.strip()
)

df["lnmadr"] = (
    df["lnmadr"]
    .astype("string")
    .str.strip()
)


# ==================================================
# 5. 도로명주소에서 시도 추출
# ==================================================

df["sido_raw"] = (
    df["rdnmadr"]
    .str.split()
    .str[0]
)


# ==================================================
# 6. 도로명주소가 없으면 지번주소 사용
# ==================================================

missing_sido = df["sido_raw"].isna()

df.loc[
    missing_sido,
    "sido_raw"
] = (
    df.loc[
        missing_sido,
        "lnmadr"
    ]
    .str.split()
    .str[0]
)


# ==================================================
# 7. 시도명 표준화
# ==================================================

sido_map = {
    "강원도": "강원특별자치도",
    "전라북도": "전북특별자치도",
    "울산": "울산광역시",
}

df["sido_standard"] = (
    df["sido_raw"]
    .replace(sido_map)
)


# ==================================================
# 8. 좌표 숫자형 변환
# ==================================================

df["latitude"] = pd.to_numeric(
    df["latitude"],
    errors="coerce"
)

df["longitude"] = pd.to_numeric(
    df["longitude"],
    errors="coerce"
)


# ==================================================
# 9. 좌표 존재 여부
# ==================================================

df["coordinate_exists"] = (
    df["latitude"].notna()
    &
    df["longitude"].notna()
)


# ==================================================
# 10. 한국 근처 좌표 범위 검사
#
# 현재는 삭제용이 아니라
# 이상값 확인용 플래그
# ==================================================

df["coordinate_valid"] = (
    df["latitude"].between(
        33.0,
        39.5
    )
    &
    df["longitude"].between(
        124.0,
        132.0
    )
)


# ==================================================
# 11. 주소 판별 가능 여부
# ==================================================

df["address_valid"] = (
    df["sido_standard"].notna()
)


# ==================================================
# 12. 확인 필요 여부
# ==================================================

df["needs_review"] = (
    (~df["address_valid"])
    |
    (~df["coordinate_valid"])
)


# ==================================================
# 13. 결과 확인
# ==================================================

print("=== 표준화 시도 분포 ===")

print(
    df["sido_standard"]
    .value_counts(dropna=False)
)

print()


print("=== 정제 결과 ===")

print(
    "전체:",
    f"{len(df):,}"
)

print(
    "시도 판별 가능:",
    f"{df['address_valid'].sum():,}"
)

print(
    "시도 판별 불가:",
    f"{(~df['address_valid']).sum():,}"
)

print(
    "좌표 존재:",
    f"{df['coordinate_exists'].sum():,}"
)

print(
    "좌표 정상 범위:",
    f"{df['coordinate_valid'].sum():,}"
)

print(
    "좌표 이상/결측:",
    f"{(~df['coordinate_valid']).sum():,}"
)

print(
    "확인 필요:",
    f"{df['needs_review'].sum():,}"
)

print()


# ==================================================
# 14. PROCESSED CSV 저장
# ==================================================

df.to_csv(
    OUTPUT_PATH,
    index=False,
    encoding="utf-8-sig"
)


print("=" * 60)
print("정제 완료")
print("저장 위치:", OUTPUT_PATH)
print("저장 건수:", f"{len(df):,}")
print("저장 컬럼 수:", len(df.columns))
print("=" * 60)