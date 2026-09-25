from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import unquote, quote_plus
import os
import time
import math
import re

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# ============================================================
# 0. 기본 설정
# ============================================================

PIPELINE_START = time.time()

# 코드 폴더와 CSV를 모으는 workspace/data 경로를 분리
COLLECTOR_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = COLLECTOR_DIR.parent
PROJECT_ROOT = PIPELINE_DIR.parent

# 모든 수집기가 공유하는 실제 env 파일
ENV_PATH = COLLECTOR_DIR / ".env"

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "weather_warning"
RAW_DIR.mkdir(parents=True, exist_ok=True)

WARNING_RAW_FILE = RAW_DIR / "weather_warning.csv"
STATUS_RAW_FILE = RAW_DIR / "weather_warning_status.csv"


print()
print("=" * 72)
print("기상특보 데이터 파이프라인 시작")
print("=" * 72)
print(f"실행시각       : {datetime.now():%Y-%m-%d %H:%M:%S}")
print(f"실행 파일      : {Path(__file__).resolve()}")
print(f"PROJECT_ROOT   : {PROJECT_ROOT}")
print(f"ENV_PATH       : {ENV_PATH}")
print(f"ENV EXISTS     : {ENV_PATH.exists()}")
print(f"RAW_DIR        : {RAW_DIR}")
print("=" * 72)


# ============================================================
# 1. ENV
# ============================================================

if not ENV_PATH.is_file():
    raise FileNotFoundError(
        "\ncollector/.env 파일을 찾을 수 없습니다."
        f"\n실행 파일 : {Path(__file__).resolve()}"
        f"\n확인 경로 : {ENV_PATH}"
    )

loaded = load_dotenv(
    dotenv_path=ENV_PATH,
    override=True
)

if not loaded:
    raise RuntimeError(
        f"collector/.env 로드 실패: {ENV_PATH}"
    )

print("[OK] collector/.env 로드 완료")


# ============================================================
# 2. API KEY
# ============================================================

RAW_API_KEY = os.getenv("WEATHER_WARNING_API_KEY")

if not RAW_API_KEY:
    raise ValueError(
        "collector/.env에 WEATHER_WARNING_API_KEY가 없습니다."
    )

API_KEY = unquote(RAW_API_KEY)

print("[OK] WEATHER_WARNING_API_KEY 확인")


# ============================================================
# 3. DB 환경변수
# ============================================================

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "woosimwoonkka")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

db_env = {
    "DB_HOST": DB_HOST,
    "DB_NAME": DB_NAME,
    "DB_USER": DB_USER,
    "DB_PASSWORD": DB_PASSWORD,
}

missing = [
    key
    for key, value in db_env.items()
    if not value
]

if missing:
    raise ValueError(
        "DB 환경변수 누락: " + ", ".join(missing)
    )

print("[OK] DB 환경변수 확인")


# ============================================================
# 4. API 설정
# ============================================================

BASE_URL = (
    "https://apis.data.go.kr/"
    "1360000/WthrWrnInfoService"
)

WARNING_LIST_URL = f"{BASE_URL}/getWthrWrnList"
WARNING_STATUS_URL = f"{BASE_URL}/getPwnStatus"

NUM_OF_ROWS = 1000

MAX_RETRIES = 3
TIMEOUT = 30
RETRY_WAIT = 3

RAW_SCHEMA = "raw"
PROCESSED_SCHEMA = "processed"

WARNING_TABLE = "weather_warning"
STATUS_TABLE = "weather_warning_status"


# 기상특보 목록 API 최대 조회 범위 = 오늘 기준 6일 전
NOW = datetime.now()
TO_DATE = NOW.strftime("%Y%m%d")
FROM_DATE = (NOW - timedelta(days=6)).strftime("%Y%m%d")


# ============================================================
# 5. NULL 정규화
# ============================================================

NULL_VALUES = {
    "",
    "-",
    "--",
    "null",
    "NULL",
    "None",
    "NONE",
    "N/A",
    "NA",
    "nan",
    "NaN",
}


def normalize_null(value):

    if pd.isna(value):
        return None

    if isinstance(value, str):
        value = value.strip()

        if value in NULL_VALUES:
            return None

    return value


# ============================================================
# 6. PostgreSQL 연결
# ============================================================

def create_db_engine():

    print()
    print("=" * 72)
    print("[1/10] PostgreSQL 연결")
    print("=" * 72)

    password = quote_plus(DB_PASSWORD)

    db_url = (
        f"postgresql+psycopg://"
        f"{DB_USER}:{password}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    try:
        engine = create_engine(
            db_url,
            pool_pre_ping=True
        )

        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1")
            ).scalar()

        if result != 1:
            raise RuntimeError("SELECT 1 검증 실패")

        print(f"[OK] DB 연결 성공 : {DB_NAME}")

        return engine

    except Exception as e:
        print(f"[ERROR] DB 연결 실패 : {e}")
        raise


# ============================================================
# 7. 스키마 생성/확인
# ============================================================

def ensure_schemas(engine):

    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    'CREATE SCHEMA IF NOT EXISTS "raw"'
                )
            )

            conn.execute(
                text(
                    'CREATE SCHEMA IF NOT EXISTS "processed"'
                )
            )

        print("[OK] raw / processed 스키마 확인")

    except Exception as e:
        print(f"[ERROR] 스키마 확인 실패 : {e}")
        raise


# ============================================================
# 8. API 공통 요청
# ============================================================

def request_api(url, params, api_name):

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            print(
                f"[{api_name}] 요청 "
                f"{attempt}/{MAX_RETRIES}"
            )

            response = requests.get(
                url,
                params=params,
                timeout=TIMEOUT
            )

            print(
                f"[{api_name}] HTTP STATUS : "
                f"{response.status_code}"
            )

            # 인증/권한 문제는 재시도하지 않음
            if response.status_code in (401, 403):
                raise PermissionError(
                    f"{api_name} 인증/권한 오류 "
                    f"HTTP {response.status_code}"
                )

            # 서버 오류 / 호출 제한은 재시도
            if (
                response.status_code == 429
                or response.status_code >= 500
            ):
                raise requests.exceptions.HTTPError(
                    f"HTTP {response.status_code}"
                )

            response.raise_for_status()

            try:
                data = response.json()

            except ValueError as e:
                print("[ERROR] JSON 변환 실패")
                print(response.text[:500])

                raise RuntimeError(
                    "API 응답 JSON 변환 실패"
                ) from e

            header = (
                data
                .get("response", {})
                .get("header", {})
            )

            result_code = str(
                header.get("resultCode", "")
            ).strip()

            result_msg = str(
                header.get("resultMsg", "")
            ).strip()

            print(
                f"[{api_name}] API RESULT : "
                f"{result_code} {result_msg}"
            )

            if result_code == "00":
                return data

            # NO_DATA
            if result_code == "03":
                print(f"[{api_name}] 데이터 없음")
                return data

            # 파라미터/API 오류는 즉시 실패
            raise ValueError(
                f"{api_name} API 오류 "
                f"[{result_code}] {result_msg}"
            )

        except (PermissionError, ValueError):
            raise

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError,
            RuntimeError,
        ) as e:

            last_error = e

            print(
                f"[WARN] {api_name} 요청 실패 : {e}"
            )

            if attempt < MAX_RETRIES:
                wait = RETRY_WAIT * attempt

                print(
                    f"[RETRY] {wait}초 후 재시도"
                )

                time.sleep(wait)

    raise RuntimeError(
        f"{api_name} 최종 실패 "
        f"({MAX_RETRIES}회 시도): {last_error}"
    )


# ============================================================
# 9. API ITEM 추출
# ============================================================

def extract_items(data):

    body = (
        data
        .get("response", {})
        .get("body", {})
    )

    items = body.get("items")

    if not items:
        return []

    if isinstance(items, dict):
        items = items.get("item", [])

    if isinstance(items, dict):
        return [items]

    if isinstance(items, list):
        return items

    return []


# ============================================================
# 10. getWthrWrnList
# ============================================================

def collect_warning_list():

    print()
    print("=" * 72)
    print("[2/10] 기상특보 목록 수집")
    print("=" * 72)
    print(f"조회기간       : {FROM_DATE} ~ {TO_DATE}")

    params = {
        "serviceKey": API_KEY,
        "pageNo": 1,
        "numOfRows": NUM_OF_ROWS,
        "dataType": "JSON",
        "fromTmFc": FROM_DATE,
        "toTmFc": TO_DATE,
    }

    first_data = request_api(
        WARNING_LIST_URL,
        params,
        "WARNING LIST"
    )

    body = (
        first_data
        .get("response", {})
        .get("body", {})
    )

    total_count = int(
        body.get("totalCount", 0) or 0
    )

    print(f"전체 데이터    : {total_count:,}건")

    if total_count == 0:
        print("수집 결과      : 0건")
        return pd.DataFrame()

    total_pages = math.ceil(
        total_count / NUM_OF_ROWS
    )

    print(f"페이지 크기    : {NUM_OF_ROWS:,}건")
    print(f"전체 페이지    : {total_pages:,}")

    all_items = extract_items(first_data)

    print(
        f"[1/{total_pages}] "
        f"{len(all_items):,}건 수집 "
        f"(누적 {len(all_items):,}건)"
    )

    for page_no in range(2, total_pages + 1):

        params["pageNo"] = page_no

        data = request_api(
            WARNING_LIST_URL,
            params,
            "WARNING LIST"
        )

        page_items = extract_items(data)

        all_items.extend(page_items)

        print(
            f"[{page_no}/{total_pages}] "
            f"{len(page_items):,}건 수집 "
            f"(누적 {len(all_items):,}건)"
        )

    df = pd.DataFrame(all_items)

    df["collected_at"] = (
        datetime.now()
        .strftime("%Y-%m-%d %H:%M:%S")
    )

    print(f"수집 완료      : {len(df):,}건")
    print(f"컬럼           : {df.columns.tolist()}")

    return df


# ============================================================
# 11. LIST LOCAL RAW 누적
# ============================================================

def save_warning_local_raw(new_df):

    print()
    print("=" * 72)
    print("[3/10] 기상특보 LOCAL RAW 누적")
    print("=" * 72)

    if WARNING_RAW_FILE.exists():
        old_df = pd.read_csv(
            WARNING_RAW_FILE,
            dtype=str,
            encoding="utf-8-sig"
        )
    else:
        old_df = pd.DataFrame()

    old_count = len(old_df)
    new_count = len(new_df)

    print(f"기존 RAW       : {old_count:,}건")
    print(f"신규 수집      : {new_count:,}건")

    combined_df = pd.concat(
        [old_df, new_df],
        ignore_index=True
    )

    merged_count = len(combined_df)

    print(f"병합 후        : {merged_count:,}건")

    dedup_keys = [
        "stnId",
        "tmFc",
        "tmSeq",
    ]

    missing = [
        c
        for c in dedup_keys
        if c not in combined_df.columns
    ]

    if missing:
        raise RuntimeError(
            "목록 RAW 중복제거 컬럼 누락: "
            + ", ".join(missing)
        )

    combined_df = (
        combined_df
        .drop_duplicates(
            subset=dedup_keys,
            keep="last"
        )
        .reset_index(drop=True)
    )

    final_count = len(combined_df)
    removed = merged_count - final_count
    actual_added = max(final_count - old_count, 0)

    print(f"중복 제거      : {removed:,}건")
    print(f"실제 신규      : {actual_added:,}건")
    print(f"최종 RAW       : {final_count:,}건")

    combined_df.to_csv(
        WARNING_RAW_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    if not WARNING_RAW_FILE.is_file():
        raise RuntimeError(
            "weather_warning.csv 저장 실패"
        )

    print(f"저장 완료      : {WARNING_RAW_FILE}")

    return combined_df


# ============================================================
# 12. LIST 정제
# ============================================================

def make_warning_processed(raw_df):

    print()
    print("=" * 72)
    print("[4/10] 기상특보 목록 정제")
    print("=" * 72)

    df = raw_df.copy()

    print(f"정제 전        : {len(df):,}건")

    for column in df.columns:
        df[column] = df[column].map(normalize_null)

    before_dedup = len(df)

    df = (
        df
        .drop_duplicates(
            subset=[
                "stnId",
                "tmFc",
                "tmSeq",
            ],
            keep="last"
        )
        .reset_index(drop=True)
    )

    removed = before_dedup - len(df)

    null_count = int(
        df.isna().sum().sum()
    )

    print(f"중복 제거      : {removed:,}건")
    print(f"정제 후        : {len(df):,}건")
    print(f"NULL           : {null_count:,}개")

    return df


# ============================================================
# 13. DB REPLACE + COUNT 검증
# ============================================================

def replace_db_table(
    engine,
    df,
    schema,
    table_name
):

    print(f"테이블         : {schema}.{table_name}")
    print(f"저장 대상      : {len(df):,}건")
    print("저장 방식      : REPLACE")

    db_df = df.copy()

    db_df = (
        db_df
        .astype(object)
        .where(
            pd.notna(db_df),
            None
        )
    )

    try:
        db_df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists="replace",
            index=False,
            chunksize=1000,
            method="multi"
        )

        with engine.connect() as conn:
            db_count = conn.execute(
                text(
                    f'SELECT COUNT(*) '
                    f'FROM "{schema}"."{table_name}"'
                )
            ).scalar()

        print(f"DB 저장 결과   : {db_count:,}건")

        if db_count != len(db_df):
            raise RuntimeError(
                f"행수 불일치: "
                f"DataFrame={len(db_df):,}, "
                f"DB={db_count:,}"
            )

        print("행수 검증      : OK")

        return db_count

    except Exception as e:
        print(
            f"[ERROR] {schema}.{table_name} "
            f"저장 실패 : {e}"
        )
        raise


# ============================================================
# 14. getPwnStatus
# ============================================================

def collect_warning_status():

    print()
    print("=" * 72)
    print("[6/10] 현재/예비 기상특보 현황 수집")
    print("=" * 72)

    params = {
        "serviceKey": API_KEY,
        "pageNo": 1,
        "numOfRows": 1000,
        "dataType": "JSON",
    }

    data = request_api(
        WARNING_STATUS_URL,
        params,
        "WARNING STATUS"
    )

    items = extract_items(data)

    if not items:
        print("현황 수집      : 0건")
        return pd.DataFrame()

    df = pd.DataFrame(items)

    df["collected_at"] = (
        datetime.now()
        .strftime("%Y-%m-%d %H:%M:%S")
    )

    print(f"현황 수집      : {len(df):,}건")
    print(f"컬럼           : {df.columns.tolist()}")

    if "t6" in df.columns:
        print(
            "현재특보(t6)   : "
            + (
                "있음"
                if df["t6"].fillna("").str.strip().ne("").any()
                else "없음"
            )
        )

    if "t7" in df.columns:
        print(
            "예비특보(t7)   : "
            + (
                "있음"
                if df["t7"].fillna("").str.strip().ne("").any()
                else "없음"
            )
        )

    return df


# ============================================================
# 15. STATUS LOCAL RAW 누적
# ============================================================

def save_status_local_raw(new_df):

    print()
    print("=" * 72)
    print("[7/10] 특보현황 LOCAL RAW 누적")
    print("=" * 72)

    if STATUS_RAW_FILE.exists():
        old_df = pd.read_csv(
            STATUS_RAW_FILE,
            dtype=str,
            encoding="utf-8-sig"
        )
    else:
        old_df = pd.DataFrame()

    old_count = len(old_df)
    new_count = len(new_df)

    print(f"기존 RAW       : {old_count:,}건")
    print(f"신규 수집      : {new_count:,}건")

    combined_df = pd.concat(
        [old_df, new_df],
        ignore_index=True
    )

    merged_count = len(combined_df)

    print(f"병합 후        : {merged_count:,}건")

    dedup_keys = [
        "tmFc",
        "tmSeq",
    ]

    missing = [
        c
        for c in dedup_keys
        if c not in combined_df.columns
    ]

    if missing:
        raise RuntimeError(
            "STATUS RAW 중복제거 컬럼 누락: "
            + ", ".join(missing)
        )

    combined_df = (
        combined_df
        .drop_duplicates(
            subset=dedup_keys,
            keep="last"
        )
        .reset_index(drop=True)
    )

    final_count = len(combined_df)

    print(
        f"중복 제거      : "
        f"{merged_count - final_count:,}건"
    )

    print(
        f"실제 신규      : "
        f"{max(final_count - old_count, 0):,}건"
    )

    print(f"최종 RAW       : {final_count:,}건")

    combined_df.to_csv(
        STATUS_RAW_FILE,
        index=False,
        encoding="utf-8-sig"
    )

    if not STATUS_RAW_FILE.is_file():
        raise RuntimeError(
            "weather_warning_status.csv 저장 실패"
        )

    print(f"저장 완료      : {STATUS_RAW_FILE}")

    return combined_df


# ============================================================
# 16. LAND / SEA
# ============================================================

def classify_area_type(area_name):

    if area_name is None:
        return None

    area_name = str(area_name)

    sea_keywords = [
        "앞바다",
        "먼바다",
        "해역",
    ]

    if any(
        keyword in area_name
        for keyword in sea_keywords
    ):
        return "SEA"

    return "LAND"


# ============================================================
# 17. 현재특보 t6 파싱
# ============================================================

def parse_active_warning(
    warning_text,
    tm_fc,
    tm_seq,
    tm_ef,
    collected_at
):

    rows = []

    if pd.isna(warning_text):
        return rows

    warning_text = str(warning_text).strip()

    if not warning_text:
        return rows

    if re.fullmatch(
        r"[oO○]?\s*없음",
        warning_text
    ):
        return rows

    for line in warning_text.splitlines():

        line = line.strip()

        if not line or ":" not in line:
            continue

        left, area_text = line.split(":", 1)

        left = re.sub(
            r"^[oO○]\s*",
            "",
            left
        ).strip()

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

        rows.append(
            {
                "warning_type": warning_type,
                "warning_level": warning_level,
                "warning_status": "ACTIVE",
                "area_name": area_text,
                "area_type": classify_area_type(area_text),
                "effective_time": None,
                "tmEf": tm_ef,
                "tmFc": tm_fc,
                "tmSeq": tm_seq,
                "collected_at": collected_at,
            }
        )

    return rows


# ============================================================
# 18. 예비특보 t7 파싱
# ============================================================

def parse_preliminary_warning(
    warning_text,
    tm_fc,
    tm_seq,
    tm_ef,
    collected_at
):

    rows = []

    if pd.isna(warning_text):
        return rows

    warning_text = str(warning_text).strip()

    if not warning_text:
        return rows

    if re.fullmatch(
        r"[oO○]?\s*없음",
        warning_text
    ):
        return rows

    current_warning_type = None

    for line in warning_text.splitlines():

        line = line.strip()

        if not line:
            continue

        # (1) 강풍 예비특보
        if (
            "예비특보" in line
            and re.match(r"^\(\d+\)", line)
        ):
            header = re.sub(
                r"^\(\d+\)\s*",
                "",
                line
            )

            current_warning_type = (
                header
                .replace("예비특보", "")
                .strip()
            )

            continue

        # o 09월 22일 오후(...) : 지역
        if (
            re.match(r"^[oO○]\s*", line)
            and ":" in line
        ):
            left, area_text = line.split(":", 1)

            effective_time = re.sub(
                r"^[oO○]\s*",
                "",
                left
            ).strip()

            area_text = area_text.strip()

            if not area_text:
                continue

            rows.append(
                {
                    "warning_type": current_warning_type,
                    "warning_level": "예비특보",
                    "warning_status": "PRELIMINARY",
                    "area_name": area_text,
                    "area_type": classify_area_type(area_text),
                    "effective_time": effective_time,
                    "tmEf": tm_ef,
                    "tmFc": tm_fc,
                    "tmSeq": tm_seq,
                    "collected_at": collected_at,
                }
            )

    return rows


# ============================================================
# 19. STATUS 구조화
# ============================================================

STATUS_COLUMNS = [
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


def make_status_processed(raw_df):

    print()
    print("=" * 72)
    print("[9/10] 현재/예비 특보 구조화")
    print("=" * 72)

    if raw_df.empty:
        print("RAW 현황       : 0건")
        return pd.DataFrame(columns=STATUS_COLUMNS)

    print(f"누적 RAW 현황 : {len(raw_df):,}건")

    work_df = raw_df.copy()

    for required in ["tmFc", "tmSeq"]:
        if required not in work_df.columns:
            raise RuntimeError(
                f"STATUS RAW에 {required} 컬럼이 없습니다."
            )

    work_df["_tmFc_sort"] = pd.to_numeric(
        work_df["tmFc"],
        errors="coerce"
    )

    work_df["_tmSeq_sort"] = pd.to_numeric(
        work_df["tmSeq"],
        errors="coerce"
    )

    work_df = (
        work_df
        .sort_values(
            ["_tmFc_sort", "_tmSeq_sort"],
            na_position="first"
        )
        .reset_index(drop=True)
    )

    latest = work_df.iloc[-1]

    print(f"최신 tmFc      : {latest.get('tmFc')}")
    print(f"최신 tmSeq     : {latest.get('tmSeq')}")

    active_rows = parse_active_warning(
        latest.get("t6"),
        latest.get("tmFc"),
        latest.get("tmSeq"),
        latest.get("tmEf"),
        latest.get("collected_at"),
    )

    preliminary_rows = parse_preliminary_warning(
        latest.get("t7"),
        latest.get("tmFc"),
        latest.get("tmSeq"),
        latest.get("tmEf"),
        latest.get("collected_at"),
    )

    print(
        f"ACTIVE         : {len(active_rows):,}건"
    )

    print(
        f"PRELIMINARY    : {len(preliminary_rows):,}건"
    )

    df = pd.DataFrame(
        active_rows + preliminary_rows,
        columns=STATUS_COLUMNS
    )

    if df.empty:
        print("구조화 결과    : 0건")
        return df

    for column in df.columns:
        df[column] = df[column].map(normalize_null)

    land_count = int(
        (df["area_type"] == "LAND").sum()
    )

    sea_count = int(
        (df["area_type"] == "SEA").sum()
    )

    null_count = int(
        df.isna().sum().sum()
    )

    print(f"LAND           : {land_count:,}건")
    print(f"SEA            : {sea_count:,}건")
    print(f"NULL           : {null_count:,}개")
    print(f"구조화 결과    : {len(df):,}건")

    print()
    print("[구조화 결과]")

    print(
        df[
            [
                "warning_type",
                "warning_level",
                "warning_status",
                "area_name",
                "area_type",
            ]
        ].to_string(index=False)
    )

    return df


# ============================================================
# 20. 빈 CURRENT STATUS 테이블
# ============================================================

def replace_empty_status_table(engine):

    empty_df = pd.DataFrame(
        columns=STATUS_COLUMNS
    )

    print("현재 유효 특보 : 0건")
    print(
        "과거 특보가 ACTIVE로 남지 않도록 "
        "processed.weather_warning_status를 0건으로 갱신"
    )

    replace_db_table(
        engine,
        empty_df,
        PROCESSED_SCHEMA,
        STATUS_TABLE
    )


# ============================================================
# 21. MAIN
# ============================================================

def main():

    engine = None

    summary = {
        "warning_api": 0,
        "warning_local": 0,
        "warning_processed": 0,
        "status_api": 0,
        "status_local": 0,
        "status_processed": 0,
    }

    try:
        # ----------------------------------------------------
        # 1. DB
        # ----------------------------------------------------

        engine = create_db_engine()
        ensure_schemas(engine)

        # ----------------------------------------------------
        # 2. LIST API
        # ----------------------------------------------------

        warning_new_df = collect_warning_list()

        summary["warning_api"] = len(warning_new_df)

        if not warning_new_df.empty:

            # ------------------------------------------------
            # 3. LOCAL RAW
            # ------------------------------------------------

            warning_raw_df = save_warning_local_raw(
                warning_new_df
            )

            summary["warning_local"] = len(
                warning_raw_df
            )

            # ------------------------------------------------
            # 4. PROCESSED
            # ------------------------------------------------

            warning_processed_df = (
                make_warning_processed(
                    warning_raw_df
                )
            )

            summary["warning_processed"] = len(
                warning_processed_df
            )

            # ------------------------------------------------
            # 5. DB
            # ------------------------------------------------

            print()
            print("=" * 72)
            print("[5/10] 기상특보 목록 DB 저장")
            print("=" * 72)

            print()
            print("[RAW]")

            replace_db_table(
                engine,
                warning_raw_df,
                RAW_SCHEMA,
                WARNING_TABLE
            )

            print()
            print("[PROCESSED]")

            replace_db_table(
                engine,
                warning_processed_df,
                PROCESSED_SCHEMA,
                WARNING_TABLE
            )

        else:
            print()
            print(
                "[INFO] 목록 API 신규 데이터 0건"
            )
            print(
                "[INFO] 기존 LOCAL RAW / DB 목록 유지"
            )

        # ----------------------------------------------------
        # 6. CURRENT STATUS
        # ----------------------------------------------------

        status_new_df = collect_warning_status()

        summary["status_api"] = len(status_new_df)

        if not status_new_df.empty:

            # ------------------------------------------------
            # 7. LOCAL STATUS RAW
            # ------------------------------------------------

            status_raw_df = save_status_local_raw(
                status_new_df
            )

            summary["status_local"] = len(
                status_raw_df
            )

            # ------------------------------------------------
            # 8. RAW DB
            # ------------------------------------------------

            print()
            print("=" * 72)
            print("[8/10] 특보현황 RAW DB 저장")
            print("=" * 72)

            replace_db_table(
                engine,
                status_raw_df,
                RAW_SCHEMA,
                STATUS_TABLE
            )

            # ------------------------------------------------
            # 9. STATUS PROCESSED
            # ------------------------------------------------

            status_processed_df = (
                make_status_processed(
                    status_raw_df
                )
            )

            summary["status_processed"] = len(
                status_processed_df
            )

            # ------------------------------------------------
            # 10. PROCESSED DB
            # ------------------------------------------------

            print()
            print("=" * 72)
            print("[10/10] 특보현황 PROCESSED DB 저장")
            print("=" * 72)

            if status_processed_df.empty:
                replace_empty_status_table(engine)

            else:
                replace_db_table(
                    engine,
                    status_processed_df,
                    PROCESSED_SCHEMA,
                    STATUS_TABLE
                )

        else:
            print()
            print("=" * 72)
            print("[7~10/10] 현재 특보현황 없음")
            print("=" * 72)

            print("LOCAL STATUS RAW : 기존 이력 유지")
            print("DB RAW STATUS    : 기존 이력 유지")

            # 현재 상태 테이블만 비움
            replace_empty_status_table(engine)

        # ----------------------------------------------------
        # 완료
        # ----------------------------------------------------

        elapsed = time.time() - PIPELINE_START

        print()
        print("=" * 72)
        print("기상특보 파이프라인 완료")
        print("=" * 72)

        print(
            f"목록 API 신규      : "
            f"{summary['warning_api']:,}건"
        )

        print(
            f"목록 LOCAL RAW     : "
            f"{summary['warning_local']:,}건"
        )

        print(
            f"목록 PROCESSED     : "
            f"{summary['warning_processed']:,}건"
        )

        print(
            f"현황 API 신규      : "
            f"{summary['status_api']:,}건"
        )

        print(
            f"현황 LOCAL RAW     : "
            f"{summary['status_local']:,}건"
        )

        print(
            f"현황 PROCESSED     : "
            f"{summary['status_processed']:,}건"
        )

        print()
        print(f"목록 RAW 파일      : {WARNING_RAW_FILE}")
        print(f"현황 RAW 파일      : {STATUS_RAW_FILE}")

        print()
        print("DB RAW 목록        : raw.weather_warning")
        print(
            "DB PROCESSED 목록  : "
            "processed.weather_warning"
        )
        print(
            "DB RAW 현황        : "
            "raw.weather_warning_status"
        )
        print(
            "DB PROCESSED 현황  : "
            "processed.weather_warning_status"
        )

        print()
        print(f"총 실행시간        : {elapsed:.2f}초")
        print("=" * 72)

    except KeyboardInterrupt:
        print()
        print("=" * 72)
        print("사용자가 파이프라인 실행을 중단했습니다.")
        print("=" * 72)
        raise

    except Exception as e:
        elapsed = time.time() - PIPELINE_START

        print()
        print("=" * 72)
        print("기상특보 파이프라인 실패")
        print("=" * 72)
        print(f"오류 종류 : {type(e).__name__}")
        print(f"오류 내용 : {e}")
        print(f"실행시간  : {elapsed:.2f}초")
        print("=" * 72)

        raise

    finally:
        if engine is not None:
            engine.dispose()
            print("[OK] PostgreSQL 연결 종료")


# ============================================================
# 22. 실행
# ============================================================

if __name__ == "__main__":
    main()
