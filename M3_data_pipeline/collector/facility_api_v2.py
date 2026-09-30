import os
import math
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote

import requests
import pandas as pd
from dotenv import load_dotenv

PIPELINE_DIR = Path(__file__).resolve().parents[1]
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from pipeline_elt import replace_raw_dataset_group
from jsonl_checkpoint import (
    append_checkpoint_entry,
    load_checkpoint,
    remove_checkpoint_after_success,
    rewrite_checkpoint,
    unit_fingerprint,
    validate_entries,
    validate_meta,
)


# --------------------------------------------------
# 1. 환경변수 / 기본 설정
# --------------------------------------------------

# 현재 파일:
# team-share/M3_data_pipeline/collector/facility_api_v2.py
#
# ROOT_DIR:
# team-share/
ROOT_DIR = Path(__file__).resolve().parents[2]
CHECKPOINT_PATH = (
    ROOT_DIR / "data" / "raw" / "facility" / "facility_checkpoint.jsonl"
)

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

# 페이지별 최대 요청 시도 횟수(최초 요청 포함)
MAX_RETRIES = 3

# 재시도 간격(초)
RETRY_DELAY = 2

# 정상 요청 사이 간격(초)
REQUEST_DELAY = 0.1


# --------------------------------------------------
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
    최초 요청을 포함해 최대 MAX_RETRIES회 시도한다.
    성공하면 (body, items), 모든 시도가 실패하면 (None, None)을 반환한다.
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

    # 모든 요청 시도 실패
    return None, None


def page_key(page_no):
    return str(page_no)


def make_checkpoint_meta(total_count, total_pages):
    keys = [page_key(page_no) for page_no in range(1, total_pages + 1)]
    return {
        "type": "meta",
        "version": 1,
        "dataset": "facility",
        "source_url": URL,
        "num_of_rows": NUM_OF_ROWS,
        "total_count": total_count,
        "total_pages": total_pages,
        "unit_count": total_pages,
        "unit_fingerprint": unit_fingerprint(keys),
    }


def make_page_entry(page_no, items):
    return {
        "type": "unit",
        "key": page_key(page_no),
        "status": "success",
        "unit": {"page_no": page_no},
        "records": items,
        "saved_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }


def records_in_page_order(total_pages, entries):
    records = []
    for page_no in range(1, total_pages + 1):
        entry = entries.get(page_key(page_no))
        if entry is not None:
            records.extend(entry.get("records") or [])
    return records


# --------------------------------------------------
# 5. 전체 수집 시작
# --------------------------------------------------

def main():
    start_time = time.perf_counter()
    failed_pages = []
    print("=" * 60)
    print("시설 데이터 수집 시작")
    print("=" * 60)
    meta, checkpoint_entries = load_checkpoint(CHECKPOINT_PATH)
    if meta is None:
        page_start = time.perf_counter()
        body, items = fetch_page(1)
        if body is None:
            raise RuntimeError(
                "첫 페이지 수집에 실패했습니다. 전체 수집을 중단합니다."
            )
        total_count = int(body["totalCount"])
        if total_count <= 0:
            raise RuntimeError("시설 API totalCount가 0입니다. 기존 파일을 유지합니다.")
        total_pages = math.ceil(total_count / NUM_OF_ROWS)
        meta = make_checkpoint_meta(total_count, total_pages)
        first_entry = make_page_entry(1, items)
        checkpoint_entries = {page_key(1): first_entry}
        rewrite_checkpoint(CHECKPOINT_PATH, meta, checkpoint_entries)
        print(
            f"[1/{total_pages}] 누적 {len(items):,}건 | "
            f"페이지 {time.perf_counter() - page_start:.2f}초"
        )
    else:
        validate_meta(
            meta,
            {
                "version": 1,
                "dataset": "facility",
                "source_url": URL,
                "num_of_rows": NUM_OF_ROWS,
            },
            ("version", "dataset", "source_url", "num_of_rows"),
            CHECKPOINT_PATH,
        )
        total_count = int(meta["total_count"])
        total_pages = int(meta["total_pages"])
        expected_meta = make_checkpoint_meta(total_count, total_pages)
        validate_meta(
            meta,
            expected_meta,
            ("unit_count", "unit_fingerprint"),
            CHECKPOINT_PATH,
        )
        validate_entries(
            checkpoint_entries,
            (page_key(page_no) for page_no in range(1, total_pages + 1)),
            CHECKPOINT_PATH,
        )
        print(f"체크포인트 재개: 완료 페이지 {len(checkpoint_entries):,}개")

    if total_pages > DAILY_LIMIT:
        raise RuntimeError(
            f"필요 호출 횟수 {total_pages}회가 일일 트래픽 제한 "
            f"{DAILY_LIMIT}회를 초과합니다."
        )

    print("전체 건수:", f"{total_count:,}")
    print("페이지당 요청 건수:", NUM_OF_ROWS)
    print("필요 호출 횟수:", total_pages)

    for page_no in range(1, total_pages + 1):
        if page_key(page_no) in checkpoint_entries:
            continue
        page_start = time.perf_counter()
        body, items = fetch_page(page_no)
        if body is None:
            failed_pages.append(page_no)
            print(f"[{page_no}/{total_pages}] 최종 수집 실패")
            continue
        entry = make_page_entry(page_no, items)
        append_checkpoint_entry(CHECKPOINT_PATH, entry)
        checkpoint_entries[page_key(page_no)] = entry
        all_items = records_in_page_order(total_pages, checkpoint_entries)
        elapsed = time.perf_counter() - start_time
        avg_time = elapsed / max(len(checkpoint_entries), 1)
        remaining_pages = total_pages - len(checkpoint_entries)
        print(
            f"[{page_no}/{total_pages}] "
            f"누적 {len(all_items):,}건 | "
            f"페이지 {time.perf_counter() - page_start:.2f}초 | "
            f"경과 {format_time(elapsed)} | "
            f"예상 남은시간 {format_time(avg_time * remaining_pages)}"
        )
        time.sleep(REQUEST_DELAY)

    all_items = records_in_page_order(total_pages, checkpoint_entries)
    if failed_pages:
        raise RuntimeError(
            f"수집 실패 페이지가 존재합니다: {failed_pages}. "
            "불완전한 RAW DB snapshot은 저장하지 않습니다."
        )
    if len(checkpoint_entries) != total_pages:
        raise RuntimeError(
            "완료 페이지 수가 전체 페이지 수와 다릅니다. "
            "불완전한 RAW DB snapshot은 저장하지 않습니다."
        )
    if len(all_items) != total_count:
        raise RuntimeError(
            f"건수 불일치: API={total_count:,} / 수집={len(all_items):,}. "
            f"RAW DB snapshot은 저장하지 않습니다."
        )
    df = pd.DataFrame(all_items)
    replace_raw_dataset_group(
        {"facility": df}
    )
    remove_checkpoint_after_success(CHECKPOINT_PATH)
    total_elapsed = time.perf_counter() - start_time
    print("수집 완료")
    print("최종 수집 건수:", f"{len(df):,}")
    print("RAW DB: raw.facility")
    print("전체 소요시간:", format_time(total_elapsed))


# --------------------------------------------------
# 실행
# --------------------------------------------------

if __name__ == "__main__":
    main()
