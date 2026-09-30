from pathlib import Path
from datetime import datetime
from urllib.parse import unquote, quote_plus
import math
import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# ============================================================
# 1. PATH
# ============================================================

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parents[2]

ENV_PATH = CURRENT_DIR / ".env"


# ============================================================
# 2. ENV
# ============================================================

if not ENV_PATH.exists():
    raise FileNotFoundError(
        f".env 파일을 찾을 수 없습니다.\n"
        f"확인 경로: {ENV_PATH}"
    )

load_dotenv(ENV_PATH, override=True)


# ------------------------------------------------------------
# API KEY
# ------------------------------------------------------------

RAW_API_KEY = os.getenv("DURUNUBI_API_KEY")

if not RAW_API_KEY:
    raise ValueError(
        "DURUNUBI_API_KEY가 .env에 없습니다."
    )

SERVICE_KEY = unquote(RAW_API_KEY)


# ------------------------------------------------------------
# DB
# ------------------------------------------------------------

DB_HOST = (
    os.getenv("DB_HOST")
    or os.getenv("POSTGRES_HOST")
    or "localhost"
)

DB_PORT = (
    os.getenv("DB_PORT")
    or os.getenv("POSTGRES_PORT")
    or "5432"
)

DB_NAME = (
    os.getenv("DB_NAME")
    or os.getenv("POSTGRES_DB")
    or "woosimwoonkka"
)

DB_USER = (
    os.getenv("DB_USER")
    or os.getenv("POSTGRES_USER")
)

DB_PASSWORD = (
    os.getenv("DB_PASSWORD")
    or os.getenv("POSTGRES_PASSWORD")
)

if not DB_USER:
    raise ValueError(
        "DB_USER 또는 POSTGRES_USER가 .env에 없습니다."
    )

if not DB_PASSWORD:
    raise ValueError(
        "DB_PASSWORD 또는 POSTGRES_PASSWORD가 .env에 없습니다."
    )


# 비밀번호 특수문자 대응
DATABASE_URL = (
    f"postgresql+psycopg://"
    f"{quote_plus(DB_USER)}:"
    f"{quote_plus(DB_PASSWORD)}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)


# ============================================================
# 3. API
# ============================================================

BASE_URL = "https://apis.data.go.kr/B551011/Durunubi"

# API 이름은 공공데이터 원본 그대로
ROUTE_URL = f"{BASE_URL}/routeList"
COURSE_URL = f"{BASE_URL}/courseList"

NUM_OF_ROWS = 100

MAX_RETRIES = 3
RETRY_WAIT_SECONDS = [3, 5]


# ============================================================
# 4. API 1회 요청
# ============================================================

def request_page(url, page_no):

    params = {
        "serviceKey": SERVICE_KEY,
        "numOfRows": NUM_OF_ROWS,
        "pageNo": page_no,
        "MobileOS": "ETC",
        "MobileApp": "WooSimWoonKka",
        "brdDiv": "DNWW",
        "_type": "json",
    }

    response = requests.get(
        url,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    try:
        data = response.json()

    except ValueError as e:
        raise RuntimeError(
            "두루누비 API 응답이 JSON이 아닙니다."
        ) from e

    response_data = data.get("response")

    if not isinstance(response_data, dict):
        raise RuntimeError(
            "두루누비 API response 구조가 없습니다."
        )

    header = response_data.get("header", {})

    result_code = str(
        header.get("resultCode", "")
    ).strip()

    result_msg = str(
        header.get("resultMsg", "")
    ).strip()

    if result_code not in {"0000", "00", "0"}:
        raise RuntimeError(
            f"두루누비 API 오류: "
            f"{result_code} / {result_msg}"
        )

    body = response_data.get("body")

    if not isinstance(body, dict):
        raise RuntimeError(
            "두루누비 API body 구조가 없습니다."
        )

    return body


# ============================================================
# 5. RETRY
# ============================================================

def request_page_with_retry(url, page_no):

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            return request_page(
                url=url,
                page_no=page_no
            )

        except Exception as e:

            last_error = e

            if attempt < MAX_RETRIES:

                wait_seconds = RETRY_WAIT_SECONDS[
                    attempt - 1
                ]

                print(
                    f"  페이지 {page_no} 요청 실패 "
                    f"({attempt}/{MAX_RETRIES}) "
                    f"→ {wait_seconds}초 후 재시도"
                )

                time.sleep(wait_seconds)

    raise RuntimeError(
        f"페이지 {page_no} 요청이 "
        f"{MAX_RETRIES}회 모두 실패했습니다."
    ) from last_error


# ============================================================
# 6. ITEMS 추출
# ============================================================

def extract_items(body):

    items_container = body.get("items")

    if not items_container:
        return []

    if isinstance(items_container, dict):
        items = items_container.get("item", [])
    else:
        items = items_container

    if not items:
        return []

    if isinstance(items, dict):
        return [items]

    if isinstance(items, list):
        return items

    raise RuntimeError(
        "두루누비 items.item 구조가 예상과 다릅니다."
    )


# ============================================================
# 7. 전체 페이지 수집
# ============================================================

def collect_all(name, url):

    print()
    print(f"[{name}]")

    first_body = request_page_with_retry(
        url=url,
        page_no=1
    )

    try:
        total_count = int(
            first_body.get("totalCount", 0)
        )

    except (TypeError, ValueError) as e:
        raise RuntimeError(
            f"{name}: totalCount를 숫자로 변환할 수 없습니다."
        ) from e

    print(
        f"전체 데이터 : {total_count:,}건"
    )

    if total_count <= 0:
        raise RuntimeError(
            f"{name}: API 데이터가 0건입니다. "
            f"기존 데이터를 0건으로 교체하지 않습니다."
        )

    total_pages = math.ceil(
        total_count / NUM_OF_ROWS
    )

    all_items = extract_items(first_body)

    for page_no in range(
        2,
        total_pages + 1
    ):

        body = request_page_with_retry(
            url=url,
            page_no=page_no
        )

        page_items = extract_items(body)

        all_items.extend(page_items)

        time.sleep(0.1)

    actual_count = len(all_items)

    if actual_count != total_count:
        raise RuntimeError(
            f"{name} 수집 건수 불일치: "
            f"API totalCount={total_count:,} / "
            f"실제 수집={actual_count:,}"
        )

    df = pd.DataFrame(all_items)

    if df.empty:
        raise RuntimeError(
            f"{name}: DataFrame이 비어 있습니다."
        )

    # 원본 API 필드는 변경하지 않고
    # 파이프라인 메타데이터만 추가
    df["collected_at"] = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    print(
        f"수집 완료   : {len(df):,}건"
    )

    return df


# ============================================================
# 8. DB 연결 확인
# ============================================================

def test_db_connection():

    with engine.connect() as conn:

        db_name = conn.execute(
            text("SELECT current_database()")
        ).scalar_one()

    if db_name != DB_NAME:
        raise RuntimeError(
            f"DB 연결 대상 불일치: "
            f"예상={DB_NAME} / 실제={db_name}"
        )

    print(
        f"DB 연결     : {db_name}"
    )


# ============================================================
# 9. SCHEMA
# ============================================================

def create_raw_schema():

    with engine.begin() as conn:

        conn.execute(
            text(
                "CREATE SCHEMA IF NOT EXISTS raw"
            )
        )

# ============================================================
# 10. RAW 적재
#
# trails / segments를 각각 staging에 먼저 적재한 뒤
# 두 테이블의 건수를 모두 검증하고
# 하나의 transaction에서 함께 교체한다.
#
# 둘 중 하나라도 실패하면 기존 RAW 두 테이블은 유지된다.
# ============================================================

def load_raw_pair_atomic(
    trails_df,
    segments_df,
):

    datasets = {
        "durunubi_trails": trails_df,
        "durunubi_segments": segments_df,
    }

    # --------------------------------------------------------
    # 빈 데이터 차단
    # --------------------------------------------------------

    for final_table, df in datasets.items():

        if df.empty:
            raise RuntimeError(
                f"raw.{final_table}: "
                f"빈 데이터 적재를 차단했습니다."
            )

    # --------------------------------------------------------
    # 1. 두 staging 테이블 적재
    #
    # 아직 기존 final 테이블은 건드리지 않는다.
    # --------------------------------------------------------

    for final_table, df in datasets.items():

        staging_table = f"{final_table}_staging"

        df.to_sql(
            name=staging_table,
            con=engine,
            schema="raw",
            if_exists="replace",
            index=False,
            chunksize=1000,
            method="multi",
        )

    # --------------------------------------------------------
    # 2. staging 검증 + 두 final 테이블 동시 교체
    #
    # 아래 작업 전체가 하나의 transaction이다.
    # --------------------------------------------------------

    with engine.begin() as conn:

        # staging 건수 검증
        for final_table, df in datasets.items():

            staging_table = f"{final_table}_staging"

            staging_count = conn.execute(
                text(
                    f'''
                    SELECT COUNT(*)
                    FROM raw."{staging_table}"
                    '''
                )
            ).scalar_one()

            if staging_count != len(df):
                raise RuntimeError(
                    f"raw.{staging_table} 건수 불일치: "
                    f"수집={len(df):,} / "
                    f"DB={staging_count:,}"
                )

        # ----------------------------------------------------
        # 두 staging이 모두 정상일 때만 기존 final 제거
        # ----------------------------------------------------

        for final_table in datasets:

            conn.execute(
                text(
                    f'''
                    DROP TABLE IF EXISTS
                    raw."{final_table}"
                    '''
                )
            )

        # ----------------------------------------------------
        # 두 staging을 final로 승격
        # ----------------------------------------------------

        for final_table in datasets:

            staging_table = f"{final_table}_staging"

            conn.execute(
                text(
                    f'''
                    ALTER TABLE
                    raw."{staging_table}"
                    RENAME TO "{final_table}"
                    '''
                )
            )

    print(
        "RAW DB 저장 : "
        f"raw.durunubi_trails "
        f"({len(trails_df):,}건)"
    )

    print(
        "RAW DB 저장 : "
        f"raw.durunubi_segments "
        f"({len(segments_df):,}건)"
    )


# ============================================================
# 11. MAIN
# ============================================================

def main():

    start_time = time.time()

    print("=" * 60)
    print("두루누비 데이터 수집 시작")
    print("=" * 60)

    print("ENV          : 확인")
    print("API KEY      : 확인")

    # --------------------------------------------------------
    # DB 연결 / 스키마 확인
    # --------------------------------------------------------

    test_db_connection()
    create_raw_schema()

    # ========================================================
    # 1. API 수집
    #
    # routeList  -> trails
    # courseList -> segments
    # ========================================================

    trails_df = collect_all(
        name="routeList -> trails",
        url=ROUTE_URL,
    )

    segments_df = collect_all(
        name="courseList -> segments",
        url=COURSE_URL,
    )

    # ========================================================
    # 2. RAW DB 적재
    # ========================================================

    print()
    print("[RAW DB]")

    load_raw_pair_atomic(
        trails_df=trails_df,
        segments_df=segments_df,
    )

    # ========================================================
    # 3. RAW DB 건수 검증
    # ========================================================

    with engine.connect() as conn:

        raw_trails_count = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM raw.durunubi_trails
                """
            )
        ).scalar_one()

        raw_segments_count = conn.execute(
            text(
                """
                SELECT COUNT(*)
                FROM raw.durunubi_segments
                """
            )
        ).scalar_one()

    expected_trails = len(trails_df)
    expected_segments = len(segments_df)

    if raw_trails_count != expected_trails:
        raise RuntimeError(
            "raw.durunubi_trails 건수 검증 실패: "
            f"API={expected_trails:,}, "
            f"DB={raw_trails_count:,}"
        )

    if raw_segments_count != expected_segments:
        raise RuntimeError(
            "raw.durunubi_segments 건수 검증 실패: "
            f"API={expected_segments:,}, "
            f"DB={raw_segments_count:,}"
        )

    # ========================================================
    # 완료
    # ========================================================

    elapsed = time.time() - start_time

    print()
    print("=" * 60)
    print("두루누비 RAW 수집 완료")
    print("=" * 60)
    print(f"trails   : {raw_trails_count:,}건")
    print(f"segments : {raw_segments_count:,}건")
    print(f"소요시간 : {elapsed:.1f}초")
    print("=" * 60)


# ============================================================
# 12. 실행
# ============================================================

if __name__ == "__main__":
    main()
