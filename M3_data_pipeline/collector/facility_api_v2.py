import os
import math
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote

import requests
import pandas as pd
from dotenv import load_dotenv


# --------------------------------------------------
# 1. 환경변수 / 기본 설정
# --------------------------------------------------

# 현재 파일:
# team-share/M3_data_pipeline/collector/facility_api_v2.py
#
# ROOT_DIR:
# team-share/
ROOT_DIR = Path(__file__).resolve().parents[2]

ENV_PATH = Path(__file__).resolve().parent / ".env"

load_dotenv(ENV_PATH)

RAW_KEY = os.getenv("FACILITY_API_KEY")

if not RAW_KEY:
    raise ValueError("FACILITY_API_KEY가 .env에 없습니다.")

SERVICE_KEY = unquote(RAW_KEY)

URL = (
    "https://apis.data.go.kr/"
    "B551014/SRVC_API_SFMS_FACI/"
    "TODZ_API_SFMS_FACI"
)

NUM_OF_ROWS = 1000
DAILY_LIMIT = 10000

# 페이지별 최대 재시도 횟수
MAX_RETRIES = 3

# 재시도 간격(초)
RETRY_DELAY = 2

# 정상 요청 사이 간격(초)
REQUEST_DELAY = 0.1


# --------------------------------------------------
# 2. 저장 경로
# --------------------------------------------------

OUTPUT_DIR = ROOT_DIR / "data" / "raw" / "facility"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

snapshot_date = datetime.now().strftime("%Y%m%d")

OUTPUT_PATH = OUTPUT_DIR / f"facility_all_{snapshot_date}.csv"


# --------------------------------------------------
# 3. 시간 표시 함수
# --------------------------------------------------

def format_time(seconds):
    seconds = int(seconds)

    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60

    if hours > 0:
        return f"{hours}시간 {minutes}분 {secs}초"
    elif minutes > 0:
        return f"{minutes}분 {secs}초"
    else:
        return f"{secs}초"


# --------------------------------------------------
# 4. 페이지 요청 함수
# --------------------------------------------------

def fetch_page(page_no):
    """
    API 한 페이지를 요청한다.
    실패하면 최대 MAX_RETRIES회까지 재시도한다.
    """

    params = {
        "serviceKey": SERVICE_KEY,
        "pageNo": page_no,
        "numOfRows": NUM_OF_ROWS,
        "resultType": "json",
    }

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            response = requests.get(
                URL,
                params=params,
                timeout=30,
            )

            response.raise_for_status()

            data = response.json()

            header = data["response"]["header"]

            if header["resultCode"] != "00":
                raise RuntimeError(
                    f"API 오류: "
                    f"{header['resultCode']} / "
                    f"{header['resultMsg']}"
                )

            body = data["response"]["body"]

            items = body.get("items", {}).get("item", [])

            if isinstance(items, dict):
                items = [items]

            return body, items

        except Exception as e:

            print(
                f"[페이지 {page_no}] "
                f"요청 실패 "
                f"({attempt}/{MAX_RETRIES}): {e}"
            )

            if attempt < MAX_RETRIES:
                print(
                    f"→ {RETRY_DELAY}초 후 재시도"
                )
                time.sleep(RETRY_DELAY)

    # 모든 재시도 실패
    return None, None


# --------------------------------------------------
# 5. 전체 수집 시작
# --------------------------------------------------

def main():

    start_time = time.perf_counter()

    all_items = []
    failed_pages = []

    print("=" * 60)
    print("시설 데이터 수집 시작")
    print("=" * 60)

    # --------------------------------------------------
    # 첫 페이지 호출
    # --------------------------------------------------

    page_start = time.perf_counter()

    body, items = fetch_page(1)

    if body is None:
        raise RuntimeError(
            "첫 페이지 수집에 실패했습니다. "
            "전체 수집을 중단합니다."
        )

    page_elapsed = time.perf_counter() - page_start

    total_count = int(body["totalCount"])

    if total_count <= 0:
        raise RuntimeError("시설 API totalCount가 0입니다. 기존 파일을 유지합니다.")

    total_pages = math.ceil(
        total_count / NUM_OF_ROWS
    )

    print()
    print("=" * 60)
    print("전체 건수:", f"{total_count:,}")
    print("페이지당 요청 건수:", NUM_OF_ROWS)
    print("필요 호출 횟수:", total_pages)
    print("=" * 60)

    # --------------------------------------------------
    # 일일 트래픽 확인
    # --------------------------------------------------

    if total_pages > DAILY_LIMIT:
        raise RuntimeError(
            f"필요 호출 횟수 {total_pages}회가 "
            f"일일 트래픽 제한 "
            f"{DAILY_LIMIT}회를 초과합니다."
        )

    # --------------------------------------------------
    # 첫 페이지 데이터 저장
    # --------------------------------------------------

    all_items.extend(items)

    elapsed = time.perf_counter() - start_time

    print()
    print("첫 페이지 실제 건수:", len(items))

    print(
        f"[1/{total_pages}] "
        f"누적 {len(all_items):,}건 | "
        f"페이지 {page_elapsed:.2f}초 | "
        f"경과 {format_time(elapsed)}"
    )

    # --------------------------------------------------
    # 2페이지 ~ 마지막 페이지
    # --------------------------------------------------

    for page_no in range(2, total_pages + 1):

        page_start = time.perf_counter()

        body, items = fetch_page(page_no)

        # 재시도까지 모두 실패한 페이지
        if body is None:
            failed_pages.append(page_no)

            print(
                f"[{page_no}/{total_pages}] "
                f"최종 수집 실패"
            )

            continue

        all_items.extend(items)

        page_elapsed = (
            time.perf_counter() - page_start
        )

        elapsed = (
            time.perf_counter() - start_time
        )

        avg_time = elapsed / page_no

        remaining_pages = (
            total_pages - page_no
        )

        eta = avg_time * remaining_pages

        print(
            f"[{page_no}/{total_pages}] "
            f"누적 {len(all_items):,}건 | "
            f"페이지 {page_elapsed:.2f}초 | "
            f"경과 {format_time(elapsed)} | "
            f"예상 남은시간 {format_time(eta)}"
        )

        time.sleep(REQUEST_DELAY)

    # --------------------------------------------------
    # 6. 수집 결과 검증
    # --------------------------------------------------

    print()
    print("=" * 60)
    print("수집 결과 검증")
    print("=" * 60)

    print(
        "API 전체 건수:",
        f"{total_count:,}"
    )

    print(
        "실제 수집 건수:",
        f"{len(all_items):,}"
    )

    # 실패 페이지가 존재하면 저장 금지
    if failed_pages:

        print(
            "실패 페이지:",
            failed_pages
        )

        raise RuntimeError(
            "수집 실패 페이지가 존재합니다. "
            "불완전한 RAW CSV는 저장하지 않습니다."
        )

    # totalCount와 실제 건수 비교
    if len(all_items) != total_count:

        raise RuntimeError(
            f"건수 불일치: "
            f"API={total_count:,} / "
            f"수집={len(all_items):,}. "
            f"RAW CSV는 저장하지 않습니다."
        )

    print("건수 검증: 정상")


    # --------------------------------------------------
    # 7. DataFrame 생성
    # --------------------------------------------------

    print()
    print("DataFrame 생성 중...")

    df = pd.DataFrame(all_items)

    print(
        "최종 수집 건수:",
        f"{len(df):,}"
    )

    print(
        "컬럼 수:",
        len(df.columns)
    )


    # --------------------------------------------------
    # 8. RAW CSV 저장
    # --------------------------------------------------

    print()
    print("CSV 저장 중...")

    temporary_path = OUTPUT_PATH.with_suffix(".tmp.csv")
    df.to_csv(
        temporary_path,
        index=False,
        encoding="utf-8-sig",
    )
    temporary_path.replace(OUTPUT_PATH)


    # --------------------------------------------------
    # 9. 최종 결과
    # --------------------------------------------------

    total_elapsed = (
        time.perf_counter() - start_time
    )

    print()
    print("=" * 60)
    print("수집 완료")
    print(
        "최종 수집 건수:",
        f"{len(df):,}"
    )
    print(
        "CSV:",
        OUTPUT_PATH
    )
    print(
        "전체 소요시간:",
        format_time(total_elapsed)
    )
    print("=" * 60)


# --------------------------------------------------
# 실행
# --------------------------------------------------

if __name__ == "__main__":
    main()
