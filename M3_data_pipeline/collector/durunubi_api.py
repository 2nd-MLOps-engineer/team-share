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

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "durunubi"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# 파일명도 의미가 바로 보이게 통일
TRAILS_CSV = RAW_DIR / "durunubi_trails.csv"
SEGMENTS_CSV = RAW_DIR / "durunubi_segments.csv"


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
# 8. CSV 저장
# ============================================================

def save_csv(df, output_path):

    if df.empty:
        raise RuntimeError(
            f"빈 데이터는 CSV로 저장하지 않습니다: "
            f"{output_path}"
        )

    # 임시 파일에 먼저 저장
    temp_path = output_path.with_suffix(".tmp.csv")

    df.to_csv(
        temp_path,
        index=False,
        encoding="utf-8-sig"
    )

    # 정상 저장 확인 후 교체
    if not temp_path.exists():
        raise RuntimeError(
            f"CSV 임시파일 생성 실패: {temp_path}"
        )

    if temp_path.stat().st_size == 0:
        raise RuntimeError(
            f"CSV 임시파일이 비어 있습니다: {temp_path}"
        )

    temp_path.replace(output_path)

    print(
        f"CSV 저장    : {output_path}"
    )


# ============================================================
# 9. DB 연결 확인
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
# 10. SCHEMA
# ============================================================

def create_schemas():

    with engine.begin() as conn:

        conn.execute(
            text(
                "CREATE SCHEMA IF NOT EXISTS raw"
            )
        )

        conn.execute(
            text(
                "CREATE SCHEMA IF NOT EXISTS processed"
            )
        )


# ============================================================
# 11. RAW 적재
#
# 중요:
# 바로 본 테이블을 replace하지 않는다.
# staging에 먼저 적재 → 건수 확인 → 성공하면 교체
# ============================================================

def load_raw_atomic(
    df,
    final_table
):

    staging_table = (
        f"{final_table}_staging"
    )

    if df.empty:
        raise RuntimeError(
            f"raw.{final_table}: "
            f"빈 데이터 적재를 차단했습니다."
        )

    # --------------------------------------------------------
    # staging 적재
    # --------------------------------------------------------

    df.to_sql(
        name=staging_table,
        con=engine,
        schema="raw",
        if_exists="replace",
        index=False,
        chunksize=1000,
        method="multi"
    )

    with engine.begin() as conn:

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

        # 검증 완료 후 기존 테이블 교체
        conn.execute(
            text(
                f'''
                DROP TABLE IF EXISTS
                raw."{final_table}"
                '''
            )
        )

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
        f"RAW DB 저장 : "
        f"raw.{final_table} "
        f"({len(df):,}건)"
    )


# ============================================================
# 12. PROCESSED 공통 결측치 SQL
# ============================================================

MISSING_VALUES_SQL = """
'',
'-',
'--',
'null',
'NULL',
'None',
'N/A',
'정보없음'
"""


# ============================================================
# 13. processed.durunubi_trails
#
# routeList → trails
#
# 컬럼명은 API 원본 그대로 유지한다.
# 값 정제만 한다.
# ============================================================

def build_processed_trails():

    staging = "durunubi_trails_staging"
    final = "durunubi_trails"

    with engine.begin() as conn:

        conn.execute(
            text(
                f'''
                DROP TABLE IF EXISTS
                processed."{staging}"
                '''
            )
        )

        conn.execute(
            text(
                f'''
                CREATE TABLE
                processed."{staging}"
                AS
                SELECT

                    CASE
                        WHEN BTRIM(
                            COALESCE("routeIdx", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("routeIdx")
                    END AS "routeIdx",

                    CASE
                        WHEN BTRIM(
                            COALESCE("themeNm", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("themeNm")
                    END AS "themeNm",

                    CASE
                        WHEN BTRIM(
                            COALESCE("linemsg", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("linemsg")
                    END AS "linemsg",

                    CASE
                        WHEN BTRIM(
                            COALESCE("themedescs", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("themedescs")
                    END AS "themedescs",

                    CASE
                        WHEN BTRIM(
                            COALESCE("brdDiv", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("brdDiv")
                    END AS "brdDiv",

                    CASE
                        WHEN BTRIM(
                            COALESCE("createdtime", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("createdtime")
                    END AS "createdtime",

                    CASE
                        WHEN BTRIM(
                            COALESCE("modifiedtime", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("modifiedtime")
                    END AS "modifiedtime",

                    "collected_at"

                FROM raw.durunubi_trails
                '''
            )
        )

        # --------------------------------------------
        # 필수 식별자 검증
        # --------------------------------------------

        null_key_count = conn.execute(
            text(
                f'''
                SELECT COUNT(*)
                FROM processed."{staging}"
                WHERE "routeIdx" IS NULL
                '''
            )
        ).scalar_one()

        if null_key_count > 0:
            raise RuntimeError(
                f"processed trails: "
                f"routeIdx NULL "
                f"{null_key_count:,}건 발견"
            )

        duplicate_count = conn.execute(
            text(
                f'''
                SELECT COUNT(*)
                FROM (
                    SELECT "routeIdx"
                    FROM processed."{staging}"
                    GROUP BY "routeIdx"
                    HAVING COUNT(*) > 1
                ) x
                '''
            )
        ).scalar_one()

        if duplicate_count > 0:
            raise RuntimeError(
                f"processed trails: "
                f"routeIdx 중복 "
                f"{duplicate_count:,}개 발견"
            )

        staging_count = conn.execute(
            text(
                f'''
                SELECT COUNT(*)
                FROM processed."{staging}"
                '''
            )
        ).scalar_one()

        raw_count = conn.execute(
            text(
                '''
                SELECT COUNT(*)
                FROM raw.durunubi_trails
                '''
            )
        ).scalar_one()

        if staging_count != raw_count:
            raise RuntimeError(
                f"trails processed 건수 불일치: "
                f"raw={raw_count:,} / "
                f"processed={staging_count:,}"
            )

        # --------------------------------------------
        # 검증 성공 후 교체
        # --------------------------------------------

        conn.execute(
            text(
                f'''
                DROP TABLE IF EXISTS
                processed."{final}"
                '''
            )
        )

        conn.execute(
            text(
                f'''
                ALTER TABLE
                processed."{staging}"
                RENAME TO "{final}"
                '''
            )
        )

        conn.execute(
            text(
                '''
                ALTER TABLE
                processed.durunubi_trails
                ADD PRIMARY KEY ("routeIdx")
                '''
            )
        )

    print(
        f"PROCESSED   : "
        f"processed.durunubi_trails "
        f"({staging_count:,}건)"
    )


# ============================================================
# 14. processed.durunubi_segments
#
# courseList → segments
#
# API 컬럼명 그대로 유지.
# 숫자로 명확한 필드만 안전하게 숫자 변환.
# ============================================================

def build_processed_segments():

    staging = "durunubi_segments_staging"
    final = "durunubi_segments"

    with engine.begin() as conn:

        conn.execute(
            text(
                f'''
                DROP TABLE IF EXISTS
                processed."{staging}"
                '''
            )
        )

        conn.execute(
            text(
                f'''
                CREATE TABLE
                processed."{staging}"
                AS
                SELECT

                    CASE
                        WHEN BTRIM(
                            COALESCE("routeIdx", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("routeIdx")
                    END AS "routeIdx",

                    CASE
                        WHEN BTRIM(
                            COALESCE("crsIdx", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("crsIdx")
                    END AS "crsIdx",

                    CASE
                        WHEN BTRIM(
                            COALESCE("crsKorNm", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("crsKorNm")
                    END AS "crsKorNm",

                    CASE
                        WHEN BTRIM(
                            COALESCE("crsDstnc", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL

                        WHEN BTRIM("crsDstnc")
                             ~ '^[0-9]+([.][0-9]+)?$'
                        THEN BTRIM(
                            "crsDstnc"
                        )::numeric

                        ELSE NULL
                    END AS "crsDstnc",

                    /*
                    crsTotlRqrmHour는 API 원본 필드 의미를
                    임의로 분/시간으로 재정의하지 않는다.
                    원본 표현 그대로 정제만 수행.
                    */
                    CASE
                        WHEN BTRIM(
                            COALESCE(
                                "crsTotlRqrmHour",
                                ''
                            )
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM(
                            "crsTotlRqrmHour"
                        )
                    END AS "crsTotlRqrmHour",

                    CASE
                        WHEN BTRIM(
                            COALESCE("crsLevel", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL

                        WHEN BTRIM("crsLevel")
                             ~ '^[0-9]+$'
                        THEN BTRIM(
                            "crsLevel"
                        )::integer

                        ELSE NULL
                    END AS "crsLevel",

                    CASE
                        WHEN BTRIM(
                            COALESCE("crsCycle", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("crsCycle")
                    END AS "crsCycle",

                    CASE
                        WHEN BTRIM(
                            COALESCE("crsContents", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("crsContents")
                    END AS "crsContents",

                    CASE
                        WHEN BTRIM(
                            COALESCE("crsSummary", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("crsSummary")
                    END AS "crsSummary",

                    CASE
                        WHEN BTRIM(
                            COALESCE("crsTourInfo", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("crsTourInfo")
                    END AS "crsTourInfo",

                    CASE
                        WHEN BTRIM(
                            COALESCE("travelerinfo", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("travelerinfo")
                    END AS "travelerinfo",

                    CASE
                        WHEN BTRIM(
                            COALESCE("sigun", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("sigun")
                    END AS "sigun",

                    CASE
                        WHEN BTRIM(
                            COALESCE("brdDiv", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("brdDiv")
                    END AS "brdDiv",

                    CASE
                        WHEN BTRIM(
                            COALESCE("gpxpath", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("gpxpath")
                    END AS "gpxpath",

                    CASE
                        WHEN BTRIM(
                            COALESCE("createdtime", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("createdtime")
                    END AS "createdtime",

                    CASE
                        WHEN BTRIM(
                            COALESCE("modifiedtime", '')
                        ) IN ({MISSING_VALUES_SQL})
                        THEN NULL
                        ELSE BTRIM("modifiedtime")
                    END AS "modifiedtime",

                    "collected_at"

                FROM raw.durunubi_segments
                '''
            )
        )

        # --------------------------------------------
        # course 식별자 NULL 검증
        # --------------------------------------------

        null_key_count = conn.execute(
            text(
                f'''
                SELECT COUNT(*)
                FROM processed."{staging}"
                WHERE "crsIdx" IS NULL
                '''
            )
        ).scalar_one()

        if null_key_count > 0:
            raise RuntimeError(
                f"processed segments: "
                f"crsIdx NULL "
                f"{null_key_count:,}건 발견"
            )

        # --------------------------------------------
        # 중복 검증
        # --------------------------------------------

        duplicate_count = conn.execute(
            text(
                f'''
                SELECT COUNT(*)
                FROM (
                    SELECT "crsIdx"
                    FROM processed."{staging}"
                    GROUP BY "crsIdx"
                    HAVING COUNT(*) > 1
                ) x
                '''
            )
        ).scalar_one()

        if duplicate_count > 0:
            raise RuntimeError(
                f"processed segments: "
                f"crsIdx 중복 "
                f"{duplicate_count:,}개 발견"
            )

        # --------------------------------------------
        # routeIdx 관계 검증
        #
        # course가 존재하지만 상위 route가 없으면
        # 데이터 구조 이상으로 판단하고 교체 중단.
        # --------------------------------------------

        orphan_count = conn.execute(
            text(
                f'''
                SELECT COUNT(*)
                FROM processed."{staging}" s
                LEFT JOIN processed.durunubi_trails t
                    ON s."routeIdx" = t."routeIdx"
                WHERE
                    s."routeIdx" IS NOT NULL
                    AND t."routeIdx" IS NULL
                '''
            )
        ).scalar_one()

        if orphan_count > 0:
            raise RuntimeError(
                f"processed segments: "
                f"상위 trail이 없는 segment "
                f"{orphan_count:,}건 발견"
            )

        staging_count = conn.execute(
            text(
                f'''
                SELECT COUNT(*)
                FROM processed."{staging}"
                '''
            )
        ).scalar_one()

        raw_count = conn.execute(
            text(
                '''
                SELECT COUNT(*)
                FROM raw.durunubi_segments
                '''
            )
        ).scalar_one()

        if staging_count != raw_count:
            raise RuntimeError(
                f"segments processed 건수 불일치: "
                f"raw={raw_count:,} / "
                f"processed={staging_count:,}"
            )

        # --------------------------------------------
        # 성공 후 기존 테이블 교체
        # --------------------------------------------

        conn.execute(
            text(
                f'''
                DROP TABLE IF EXISTS
                processed."{final}"
                '''
            )
        )

        conn.execute(
            text(
                f'''
                ALTER TABLE
                processed."{staging}"
                RENAME TO "{final}"
                '''
            )
        )

        conn.execute(
            text(
                '''
                ALTER TABLE
                processed.durunubi_segments
                ADD PRIMARY KEY ("crsIdx")
                '''
            )
        )

        # routeIdx 검색용 index
        conn.execute(
            text(
                '''
                CREATE INDEX
                idx_durunubi_segments_routeidx
                ON processed.durunubi_segments
                ("routeIdx")
                '''
            )
        )

        # 지역 검색용 index
        conn.execute(
            text(
                '''
                CREATE INDEX
                idx_durunubi_segments_sigun
                ON processed.durunubi_segments
                ("sigun")
                '''
            )
        )

        # 난이도 검색용 index
        conn.execute(
            text(
                '''
                CREATE INDEX
                idx_durunubi_segments_level
                ON processed.durunubi_segments
                ("crsLevel")
                '''
            )
        )

    print(
        f"PROCESSED   : "
        f"processed.durunubi_segments "
        f"({staging_count:,}건)"
    )


# ============================================================
# 15. 최종 검증
# ============================================================

def verify_database():

    queries = {
        "raw trails":
            "SELECT COUNT(*) FROM raw.durunubi_trails",

        "raw segments":
            "SELECT COUNT(*) FROM raw.durunubi_segments",

        "processed trails":
            "SELECT COUNT(*) FROM processed.durunubi_trails",

        "processed segments":
            "SELECT COUNT(*) FROM processed.durunubi_segments",
    }

    result = {}

    with engine.connect() as conn:

        for name, sql in queries.items():

            result[name] = conn.execute(
                text(sql)
            ).scalar_one()

    print()
    print("[DB 최종 검증]")

    for name, count in result.items():

        print(
            f"{name:<20} : "
            f"{count:,}건"
        )

    return result


# ============================================================
# 16. MAIN
# ============================================================

def main():

    start_time = time.time()

    print("=" * 60)
    print("두루누비 파이프라인 시작")
    print("=" * 60)

    print("ENV          : 확인")
    print("API KEY      : 확인")

    # --------------------------------------------------------
    # DB 연결
    # --------------------------------------------------------

    test_db_connection()

    create_schemas()


    # ========================================================
    # 1. API 수집
    #
    # routeList  → trails
    # courseList → segments
    # ========================================================

    trails_df = collect_all(
        name="routeList → trails",
        url=ROUTE_URL
    )

    segments_df = collect_all(
        name="courseList → segments",
        url=COURSE_URL
    )


    # ========================================================
    # 2. CSV
    # ========================================================

    print()
    print("[RAW CSV]")

    save_csv(
        trails_df,
        TRAILS_CSV
    )

    save_csv(
        segments_df,
        SEGMENTS_CSV
    )


    # ========================================================
    # 3. RAW DB
    # ========================================================

    print()
    print("[RAW DB]")

    load_raw_atomic(
        df=trails_df,
        final_table="durunubi_trails"
    )

    load_raw_atomic(
        df=segments_df,
        final_table="durunubi_segments"
    )


    # ========================================================
    # 4. PROCESSED
    #
    # trail 먼저 만들어야
    # segment의 routeIdx 관계를 검증할 수 있음.
    # ========================================================

    print()
    print("[PROCESSED DB]")

    build_processed_trails()
    build_processed_segments()


    # ========================================================
    # 5. 최종 검증
    # ========================================================

    result = verify_database()

    expected_trails = len(trails_df)
    expected_segments = len(segments_df)

    if result["raw trails"] != expected_trails:
        raise RuntimeError(
            "raw.durunubi_trails 최종 건수 검증 실패"
        )

    if result["processed trails"] != expected_trails:
        raise RuntimeError(
            "processed.durunubi_trails 최종 건수 검증 실패"
        )

    if result["raw segments"] != expected_segments:
        raise RuntimeError(
            "raw.durunubi_segments 최종 건수 검증 실패"
        )

    if result["processed segments"] != expected_segments:
        raise RuntimeError(
            "processed.durunubi_segments 최종 건수 검증 실패"
        )


    # ========================================================
    # 완료
    # ========================================================

    elapsed = time.time() - start_time

    print()
    print("=" * 60)
    print("두루누비 파이프라인 완료")
    print("=" * 60)

    print(
        f"trails   : "
        f"{expected_trails:,}건"
    )

    print(
        f"segments : "
        f"{expected_segments:,}건"
    )

    print(
        f"소요시간 : "
        f"{elapsed:.1f}초"
    )

    print("=" * 60)


# ============================================================
# 17. 실행
# ============================================================

if __name__ == "__main__":
    main()
