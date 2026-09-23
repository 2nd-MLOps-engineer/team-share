import pandas as pd

# -----------------------------
# 1. RAW 데이터 불러오기
# -----------------------------
input_path = "data/raw/facility_all.csv"
output_path = "data/processed/facility_clean.csv"

df = pd.read_csv(input_path)

print("=== 정제 전 ===") 
print("전체 행:", len(df))
print("전체 컬럼:", len(df.columns))
print()


# -----------------------------
# 2. 문자열 공백 제거
# -----------------------------
text_columns = df.select_dtypes(include="object").columns

for col in text_columns:
    df[col] = df[col].apply(
        lambda x: x.strip() if isinstance(x, str) else x
    )


# -----------------------------
# 3. 정상운영 시설만 유지
# -----------------------------
df = df[df["faci_stat_nm"] == "정상운영"].copy()

print("정상운영 필터 후:", len(df))


# -----------------------------
# 4. 위도 / 경도 숫자 변환
# -----------------------------
df["faci_lat"] = pd.to_numeric(
    df["faci_lat"],
    errors="coerce"
)

df["faci_lot"] = pd.to_numeric(
    df["faci_lot"],
    errors="coerce"
)


# -----------------------------
# 5. 좌표 결측치 / 0 제거
# -----------------------------
df = df.dropna(
    subset=["faci_lat", "faci_lot"]
)

df = df[
    (df["faci_lat"] != 0) &
    (df["faci_lot"] != 0)
].copy()

print("유효 좌표 필터 후:", len(df))


# -----------------------------
# 6. 대한민국 좌표 범위 간단 검증
# -----------------------------
df = df[
    df["faci_lat"].between(33, 39) &
    df["faci_lot"].between(124, 132)
].copy()

print("한국 좌표 범위 필터 후:", len(df))


# -----------------------------
# 7. 시설코드 중복 제거
# -----------------------------
before = len(df)

df = df.drop_duplicates(
    subset=["faci_cd"],
    keep="last"
)

print(
    "시설코드 중복 제거:",
    before - len(df),
    "건"
)


# -----------------------------
# 8. 핵심 컬럼 선택
# -----------------------------
columns = [
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
    "updt_dt"
]

# 실제 존재하는 컬럼만 선택
columns = [
    col for col in columns
    if col in df.columns
]

df_clean = df[columns].copy()


# -----------------------------
# 9. 인덱스 재설정
# -----------------------------
df_clean = df_clean.reset_index(drop=True)


# -----------------------------
# 10. 결과 확인
# -----------------------------
print()
print("=== 정제 후 ===")
print("행:", len(df_clean))
print("컬럼:", len(df_clean.columns))

print()
print("결측치 TOP 10")
print(
    df_clean.isnull()
    .sum()
    .sort_values(ascending=False)
    .head(10)
)


# -----------------------------
# 11. CSV 저장
# -----------------------------
df_clean.to_csv(
    output_path,
    index=False,
    encoding="utf-8-sig"
)

print()
print("저장 완료:", output_path)