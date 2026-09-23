from pathlib import Path
import os
import math
import time
import requests
import pandas as pd

from urllib.parse import unquote
from dotenv import load_dotenv


# ==================================================
# 1. 환경변수
# ==================================================
CURRENT_DIR = Path(__file__).resolve().parent
ENV_PATH = CURRENT_DIR / ".env"
load_dotenv(ENV_PATH, override=False)

RAW_KEY = os.getenv("OPEN_FACILITY_API_KEY")

if not RAW_KEY:
    raise ValueError("OPEN_FACILITY_API_KEY가 .env에 없습니다.")

SERVICE_KEY = unquote(RAW_KEY)


# ==================================================
# 2. API 설정
# ==================================================
URL = "https://api.data.go.kr/openapi/tn_pubr_public_pblfclt_opn_info_api"

NUM_OF_ROWS = 1000

PROJECT_ROOT = Path(__file__).resolve().parents[2]

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "public_open_facility"
)

OUTPUT_PATH = (
    OUTPUT_DIR
    / "public_open_facility_all.csv"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)

all_items = []
start_time = time.perf_counter()


def format_time(seconds):
    seconds = int(seconds)
    minutes = seconds // 60
    secs = seconds % 60

    if minutes:
        return f"{minutes}분 {secs}초"

    return f"{secs}초"


# ==================================================
# 3. 첫 페이지 요청
# ==================================================
params = {
    "serviceKey": SERVICE_KEY,
    "pageNo": 1,
    "numOfRows": NUM_OF_ROWS,
    "type": "json"
}

response = requests.get(URL, params=params, timeout=30)
response.raise_for_status()

data = response.json()

header = data["header"]

if str(header["resultCode"]) not in ["00", "0"]:
    raise Exception(
        f"API 오류: {header['resultCode']} / {header['resultMsg']}"
    )

body = data["body"]

total_count = int(body["totalCount"])
if total_count <= 0:
    raise RuntimeError("공공개방시설 API totalCount가 0입니다. 기존 파일을 유지합니다.")
total_pages = math.ceil(total_count / NUM_OF_ROWS)

print("전체 건수:", f"{total_count:,}")
print("필요 호출 횟수:", total_pages)


# ==================================================
# 4. 첫 페이지 데이터
# ==================================================
items = body.get("items", [])

if isinstance(items, dict):
    items = items.get("item", items)

if isinstance(items, dict):
    items = [items]

all_items.extend(items)

print(f"[1/{total_pages}] 누적 {len(all_items):,}건")


# ==================================================
# 5. 나머지 페이지 수집
# ==================================================
for page_no in range(2, total_pages + 1):

    params["pageNo"] = page_no

    response = requests.get(
        URL,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    data = response.json()

    header = data["header"]

    if str(header["resultCode"]) not in ["00", "0"]:
        raise Exception(
            f"API 오류: {header['resultCode']} / {header['resultMsg']}"
        )

    body = data["body"]

    items = body.get("items", [])

    if isinstance(items, dict):
        items = items.get("item", items)

    if isinstance(items, dict):
        items = [items]

    all_items.extend(items)

    print(
        f"[{page_no}/{total_pages}] "
        f"누적 {len(all_items):,}건"
    )

    time.sleep(0.1)


# ==================================================
# 6. DataFrame
# ==================================================
df = pd.DataFrame(all_items)

if len(df) != total_count:
    raise RuntimeError(
        f"수집 건수 불일치: API={total_count:,} / 실제={len(df):,}. "
        "기존 파일을 유지합니다."
    )

print()
print("최종 수집 건수:", f"{len(df):,}")
print("컬럼 수:", len(df.columns))


# ==================================================
# 7. RAW CSV 저장
# ==================================================
temporary_path = OUTPUT_PATH.with_suffix(".tmp.csv")
df.to_csv(
    temporary_path,
    index=False,
    encoding="utf-8-sig"
)
temporary_path.replace(OUTPUT_PATH)

elapsed = time.perf_counter() - start_time

print()
print("수집 완료")
print("CSV:", OUTPUT_PATH)
print("건수:", f"{len(df):,}")
print("소요시간:", format_time(elapsed))
