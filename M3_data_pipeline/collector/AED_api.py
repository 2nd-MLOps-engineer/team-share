from __future__ import annotations

import math
import os
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote

import pandas as pd
import requests
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

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = CURRENT_DIR / ".env"
CHECKPOINT_PATH = PROJECT_ROOT / "data" / "raw" / "aed" / "aed_checkpoint.jsonl"

load_dotenv(ENV_PATH, override=False)
RAW_KEY = os.getenv("AED_API_KEY")
if not RAW_KEY:
    raise ValueError("AED_API_KEY가 .env에 없습니다.")
SERVICE_KEY = unquote(RAW_KEY)

URL = (
    "https://apis.data.go.kr/"
    "B552657/AEDInfoInqireService/getAedFullDown"
)
NUM_OF_ROWS = 1000
MAX_RETRIES = 3
RETRY_DELAY = 3


def request_page(page_no):
    params = {
        "serviceKey": SERVICE_KEY,
        "pageNo": page_no,
        "numOfRows": NUM_OF_ROWS,
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(URL, params=params, timeout=30)
            response.raise_for_status()
            return response
        except requests.RequestException as exc:
            print(f"[재시도 {attempt}/{MAX_RETRIES}] page={page_no} / {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
    raise RuntimeError(f"page={page_no} API 요청 최종 실패")


def parse_items(root):
    rows = []
    for item in root.findall(".//item"):
        rows.append({child.tag: child.text for child in item})
    return rows


def collect_page(page_no):
    response = request_page(page_no)
    try:
        root = ET.fromstring(response.content)
    except ET.ParseError as exc:
        raise RuntimeError(f"page={page_no} XML 파싱 실패: {exc}") from exc

    result_code = root.findtext(".//resultCode")
    result_msg = root.findtext(".//resultMsg")
    if result_code not in (None, "00", "0"):
        raise RuntimeError(f"page={page_no} API 오류: {result_code} / {result_msg}")

    total_count_text = root.findtext(".//totalCount")
    total_count = int(total_count_text) if total_count_text is not None else None
    return total_count, parse_items(root)


def page_key(page_no):
    return str(page_no)


def make_checkpoint_meta(total_count, total_pages):
    keys = [page_key(page_no) for page_no in range(1, total_pages + 1)]
    return {
        "type": "meta",
        "version": 1,
        "dataset": "aed",
        "source_url": URL,
        "num_of_rows": NUM_OF_ROWS,
        "total_count": total_count,
        "total_pages": total_pages,
        "unit_count": total_pages,
        "unit_fingerprint": unit_fingerprint(keys),
    }


def make_page_entry(page_no, records):
    return {
        "type": "unit",
        "key": page_key(page_no),
        "status": "success",
        "unit": {"page_no": page_no},
        "records": records,
        "saved_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }


def records_in_page_order(total_pages, entries):
    records = []
    for page_no in range(1, total_pages + 1):
        entry = entries.get(page_key(page_no))
        if entry is not None:
            records.extend(entry.get("records") or [])
    return records


def _load_or_start_checkpoint():
    meta, entries = load_checkpoint(CHECKPOINT_PATH)
    if meta is None:
        total_count, first_records = collect_page(1)
        if total_count is None:
            raise RuntimeError("API 응답에서 totalCount를 찾을 수 없습니다.")
        if total_count <= 0:
            raise RuntimeError("AED API totalCount가 0입니다. 기존 파일을 유지합니다.")
        total_pages = math.ceil(total_count / NUM_OF_ROWS)
        meta = make_checkpoint_meta(total_count, total_pages)
        first_entry = make_page_entry(1, first_records)
        entries = {page_key(1): first_entry}
        rewrite_checkpoint(CHECKPOINT_PATH, meta, entries)
        return meta, entries

    validate_meta(
        meta,
        {
            "version": 1,
            "dataset": "aed",
            "source_url": URL,
            "num_of_rows": NUM_OF_ROWS,
        },
        ("version", "dataset", "source_url", "num_of_rows"),
        CHECKPOINT_PATH,
    )
    expected = make_checkpoint_meta(int(meta["total_count"]), int(meta["total_pages"]))
    validate_meta(
        meta,
        expected,
        ("unit_count", "unit_fingerprint"),
        CHECKPOINT_PATH,
    )
    validate_entries(
        entries,
        (page_key(page_no) for page_no in range(1, int(meta["total_pages"]) + 1)),
        CHECKPOINT_PATH,
    )
    return meta, entries


def main():
    start_time = time.time()
    failed_pages = []
    print("=" * 60)
    print("AED FullData 수집 시작")
    print("=" * 60)

    meta, checkpoint_entries = _load_or_start_checkpoint()
    total_count = int(meta["total_count"])
    total_pages = int(meta["total_pages"])
    print(f"체크포인트 완료 페이지: {len(checkpoint_entries):,}개")

    for page_no in range(1, total_pages + 1):
        if page_key(page_no) in checkpoint_entries:
            continue
        try:
            _page_total_count, page_records = collect_page(page_no)
        except Exception as exc:
            print(f"[ERROR] page={page_no} {exc}")
            failed_pages.append(page_no)
            continue
        entry = make_page_entry(page_no, page_records)
        append_checkpoint_entry(CHECKPOINT_PATH, entry)
        checkpoint_entries[page_key(page_no)] = entry
        print(
            f"[{page_no}/{total_pages}] {len(page_records):,}건 수집 "
            f"(완료 페이지 {len(checkpoint_entries):,}개)"
        )
        time.sleep(0.2)

    if failed_pages:
        raise RuntimeError(f"최종 실패 페이지가 있습니다: {failed_pages}")
    if len(checkpoint_entries) != total_pages:
        raise RuntimeError("완료 페이지 수가 전체 페이지 수와 다릅니다.")

    all_items = records_in_page_order(total_pages, checkpoint_entries)
    df = pd.DataFrame(all_items)
    if len(df) != total_count:
        raise RuntimeError(
            f"수집 건수 불일치: API={total_count:,}건 / 수집={len(df):,}건"
        )

    replace_raw_dataset_group({"aed": df})
    remove_checkpoint_after_success(CHECKPOINT_PATH)
    print("AED 수집 완료")
    print("RAW DB : raw.aed")
    print(f"저장 건수 : {len(df):,}건")
    print(f"소요 시간 : {time.time() - start_time:.1f}초")


if __name__ == "__main__":
    main()
