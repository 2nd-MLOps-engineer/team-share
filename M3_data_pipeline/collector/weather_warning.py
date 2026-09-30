from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import unquote
import os
import sys
import time
import math

import pandas as pd
import requests
from dotenv import load_dotenv

PIPELINE_DIR = Path(__file__).resolve().parents[1]
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from pipeline_elt import read_raw_table_if_exists, replace_raw_dataset_group


# ============================================================
# 0. 기본 설정
# ============================================================

PIPELINE_START = time.time()

COLLECTOR_DIR = Path(__file__).resolve().parent
PIPELINE_DIR = COLLECTOR_DIR.parent
PROJECT_ROOT = PIPELINE_DIR.parent

# 모든 수집기가 공유하는 실제 env 파일
ENV_PATH = COLLECTOR_DIR / ".env"

print()
print("=" * 72)
print("기상특보 데이터 파이프라인 시작")
print("=" * 72)
print(f"실행시각       : {datetime.now():%Y-%m-%d %H:%M:%S}")
print(f"실행 파일      : {Path(__file__).resolve()}")
print(f"PROJECT_ROOT   : {PROJECT_ROOT}")
print(f"ENV_PATH       : {ENV_PATH}")
print(f"ENV EXISTS     : {ENV_PATH.exists()}")
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
    override=False
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
# 3. API 설정
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

# 기상특보 목록 API 최대 조회 범위 = 오늘 기준 6일 전
NOW = datetime.now()
TO_DATE = NOW.strftime("%Y%m%d")
FROM_DATE = (NOW - timedelta(days=6)).strftime("%Y%m%d")


# ============================================================
# 4. API 공통 요청
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
# 11. 기존 RAW DB 목록과 신규 수집분 병합
# ============================================================

def merge_warning_raw(old_df, new_df):

    print()
    print("=" * 72)
    print("[3/10] 기상특보 LOCAL RAW 누적")
    print("=" * 72)

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

    return combined_df


# ============================================================
# 12. getPwnStatus
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
# 13. 전달받은 현재 특보현황의 중복 제거(과거 상태와 병합하지 않음)
# ============================================================

def deduplicate_status(new_df):

    print()
    print("=" * 72)
    print("[7/10] 특보현황 LOCAL RAW 누적")
    print("=" * 72)

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

    return combined_df


STATUS_RAW_COLUMNS = [
    "t6",
    "t7",
    "tmEf",
    "tmFc",
    "tmSeq",
    "collected_at",
]


def build_status_current_raw(status_df):
    """공통 processor용 현재 상태 RAW DataFrame을 반환한다(필수 컬럼 보완)."""

    if status_df.empty:
        snapshot = pd.DataFrame(columns=STATUS_RAW_COLUMNS)
    else:
        snapshot = status_df.copy()
        for column in STATUS_RAW_COLUMNS:
            if column not in snapshot.columns:
                snapshot[column] = ""

    return snapshot


def build_warning_raw(warning_new_df):
    """기존 RAW DB 목록과 설정된 조회기간의 신규 수집분을 병합한다."""

    old_df = read_raw_table_if_exists("weather_warning")
    if warning_new_df.empty:
        return old_df
    return merge_warning_raw(old_df, warning_new_df)


def replace_warning_raw_group(warning_df, status_df):
    """목록/현재상태 RAW를 동일 transaction에서 교체한다."""

    replace_raw_dataset_group(
        {
            "weather_warning": warning_df,
            "weather_warning_status": status_df,
        },
        allow_empty_tables={"weather_warning_status"},
    )


# ============================================================
# 14. MAIN
# ============================================================

def main():
    summary = {
        "warning_api": 0,
        "warning_raw": 0,
        "status_api": 0,
        "status_current": 0,
    }

    try:
        warning_new_df = collect_warning_list()
        summary["warning_api"] = len(warning_new_df)

        warning_raw_df = build_warning_raw(warning_new_df)
        summary["warning_raw"] = len(warning_raw_df)
        if warning_new_df.empty:
            print()
            print("[INFO] 목록 API 신규 데이터 0건")
            print("[INFO] 기존 RAW DB 목록 snapshot 유지")

        status_new_df = collect_warning_status()
        summary["status_api"] = len(status_new_df)
        current_df = build_status_current_raw(status_new_df)
        summary["status_current"] = len(current_df)

        replace_warning_raw_group(warning_raw_df, current_df)

        elapsed = time.time() - PIPELINE_START
        print()
        print("=" * 72)
        print("기상특보 RAW 수집 완료")
        print("=" * 72)
        print(f"목록 API 신규      : {summary['warning_api']:,}건")
        print(f"목록 RAW DB        : {summary['warning_raw']:,}건")
        print(f"현황 API 현재      : {summary['status_api']:,}건")
        print(f"현황 RAW DB        : {summary['status_current']:,}건")
        print("원자 교체 테이블   : raw.weather_warning, "
              "raw.weather_warning_status")
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

# ============================================================
# 15. 실행
# ============================================================

if __name__ == "__main__":
    main()
