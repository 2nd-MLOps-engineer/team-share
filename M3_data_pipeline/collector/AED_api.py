import os
import math
import time
import requests
import pandas as pd
import xml.etree.ElementTree as ET
from pathlib import Path

from dotenv import load_dotenv
from urllib.parse import unquote


# ============================================================
# 1. 환경변수 / 기본 설정
# ============================================================

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = Path(__file__).resolve().parents[2]
ENV_PATH = CURRENT_DIR / ".env"
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "aed"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

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

OUTPUT_PATH = OUTPUT_DIR / "aed.csv"


# ============================================================
# 2. API 요청 함수
# ============================================================

def request_page(page_no):

    params = {
        "serviceKey": SERVICE_KEY,
        "pageNo": page_no,
        "numOfRows": NUM_OF_ROWS,
    }

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            response = requests.get(
                URL,
                params=params,
                timeout=30
            )

            response.raise_for_status()

            return response

        except requests.RequestException as e:

            print(
                f"[재시도 {attempt}/{MAX_RETRIES}] "
                f"page={page_no} / {e}"
            )

            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    raise RuntimeError(
        f"page={page_no} API 요청 최종 실패"
    )


# ============================================================
# 3. XML item → dict
# ============================================================

def parse_items(root):

    rows = []

    items = root.findall(".//item")

    for item in items:

        row = {}

        for child in item:
            row[child.tag] = child.text

        rows.append(row)

    return rows


# ============================================================
# 4. 첫 페이지 호출
# ============================================================

print("=" * 60)
print("AED FullData 수집 시작")
print("=" * 60)

start_time = time.time()

response = request_page(1)


# ============================================================
# 5. XML 파싱
# ============================================================

try:
    root = ET.fromstring(response.content)

except ET.ParseError as e:
    raise RuntimeError(
        f"XML 파싱 실패: {e}\n"
        f"응답 앞부분: {response.text[:500]}"
    )


# ============================================================
# 6. API 응답 상태 확인
# ============================================================

result_code = root.findtext(".//resultCode")
result_msg = root.findtext(".//resultMsg")

print("HTTP STATUS:", response.status_code)
print("API RESULT:", result_code, result_msg)

if result_code not in (None, "00", "0"):
    raise RuntimeError(
        f"API 오류: {result_code} / {result_msg}"
    )


# ============================================================
# 7. 전체 데이터 수 확인
# ============================================================

total_count_text = root.findtext(".//totalCount")

if total_count_text is None:
    raise RuntimeError(
        "API 응답에서 totalCount를 찾을 수 없습니다.\n"
        f"응답 앞부분:\n{response.text[:1000]}"
    )

total_count = int(total_count_text)

if total_count <= 0:
    raise RuntimeError("AED API totalCount가 0입니다. 기존 파일을 유지합니다.")

total_pages = math.ceil(
    total_count / NUM_OF_ROWS
)

print()
print(f"전체 데이터 : {total_count:,}건")
print(f"페이지 크기 : {NUM_OF_ROWS:,}건")
print(f"전체 페이지 : {total_pages:,}페이지")
print("=" * 60)


# ============================================================
# 8. 첫 페이지 데이터 저장
# ============================================================

all_items = parse_items(root)
failed_pages = []

print(
    f"[1/{total_pages}] "
    f"{len(all_items):,}건 수집 "
    f"(누적 {len(all_items):,}건)"
)


# ============================================================
# 9. 나머지 페이지 수집
# ============================================================

for page_no in range(2, total_pages + 1):

    response = request_page(page_no)

    try:
        root = ET.fromstring(response.content)

    except ET.ParseError as e:
        print(
            f"[ERROR] page={page_no} "
            f"XML 파싱 실패: {e}"
        )
        failed_pages.append(page_no)
        continue


    result_code = root.findtext(".//resultCode")
    result_msg = root.findtext(".//resultMsg")

    if result_code not in (None, "00", "0"):

        print(
            f"[ERROR] page={page_no} "
            f"{result_code} / {result_msg}"
        )

        failed_pages.append(page_no)
        continue


    page_items = parse_items(root)

    all_items.extend(page_items)


    print(
        f"[{page_no}/{total_pages}] "
        f"{len(page_items):,}건 수집 "
        f"(누적 {len(all_items):,}건)"
    )


    # 공공 API 과도한 연속 요청 방지
    time.sleep(0.2)


# ============================================================
# 10. DataFrame 생성
# ============================================================

df = pd.DataFrame(all_items)


# ============================================================
# 11. 기본 확인
# ============================================================

print()
print("=" * 60)
print("수집 결과")
print("=" * 60)

print(f"API totalCount : {total_count:,}")
print(f"실제 수집 건수 : {len(df):,}")
print(f"컬럼 수        : {len(df.columns):,}")

print()
print("컬럼 목록:")
print(df.columns.tolist())

print()
print("샘플 데이터:")
print(df.head())


# ============================================================
# 12. 수집 건수 검증
# ============================================================

if failed_pages:
    raise RuntimeError(f"최종 실패 페이지가 있습니다: {failed_pages}")

if len(df) == total_count:

    print()
    print("수집 건수 검증: 정상")

else:
    raise RuntimeError(
        f"수집 건수 불일치: API={total_count:,}건 / "
        f"수집={len(df):,}건"
    )


# ============================================================
# 13. RAW CSV 저장
# ============================================================

temporary_path = OUTPUT_PATH.with_suffix(".tmp.csv")
df.to_csv(
    temporary_path,
    index=False,
    encoding="utf-8-sig"
)
temporary_path.replace(OUTPUT_PATH)


# ============================================================
# 14. 실행시간
# ============================================================

elapsed = time.time() - start_time

print()
print("=" * 60)
print("AED 수집 완료")
print("=" * 60)

print(f"저장 파일 : {OUTPUT_PATH}")
print(f"저장 건수 : {len(df):,}건")
print(f"소요 시간 : {elapsed:.1f}초")
