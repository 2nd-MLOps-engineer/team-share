import os
import math
import time
import requests
import pandas as pd

from urllib.parse import unquote
from dotenv import load_dotenv


# -----------------------------
# 1. 환경변수 / 기본 설정
# -----------------------------
load_dotenv()

RAW_KEY = os.getenv("FACILITY_API_KEY")

if not RAW_KEY:
    raise ValueError("FACILITY_API_KEY가 .env에 없습니다.")

SERVICE_KEY = unquote(RAW_KEY)

URL = "https://apis.data.go.kr/B551014/SRVC_API_SFMS_FACI/TODZ_API_SFMS_FACI"

NUM_OF_ROWS = 1000
DAILY_LIMIT = 10000

all_items = []

# 전체 작업 시작시간
start_time = time.perf_counter()


# -----------------------------
# 시간 표시 함수
# -----------------------------
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


# -----------------------------
# 2. 첫 페이지 호출
# -----------------------------
params = {
    "serviceKey": SERVICE_KEY,
    "pageNo": 1,
    "numOfRows": NUM_OF_ROWS,
    "resultType": "json"
}

page_start = time.perf_counter()

response = requests.get(
    URL,
    params=params,
    timeout=30
)

response.raise_for_status()

data = response.json()

page_elapsed = time.perf_counter() - page_start

header = data["response"]["header"]

if header["resultCode"] != "00":
    raise Exception(
        f"API 오류: {header['resultCode']} / {header['resultMsg']}"
    )

body = data["response"]["body"]

total_count = int(body["totalCount"])
total_pages = math.ceil(total_count / NUM_OF_ROWS)

print("=" * 60)
print("전체 건수:", total_count)
print("페이지당 요청 건수:", NUM_OF_ROWS)
print("필요 호출 횟수:", total_pages)
print("=" * 60)


# -----------------------------
# 3. 일일 트래픽 확인
# -----------------------------
if total_pages > DAILY_LIMIT:
    raise Exception(
        f"필요 호출 횟수 {total_pages}회가 "
        f"일일 트래픽 제한 {DAILY_LIMIT}회를 초과합니다."
    )


# -----------------------------
# 4. 첫 페이지 데이터
# -----------------------------
items = body.get("items", {}).get("item", [])

if isinstance(items, dict):
    items = [items]

print("첫 페이지 실제 건수:", len(items))

all_items.extend(items)

elapsed = time.perf_counter() - start_time

print(
    f"[1/{total_pages}] "
    f"누적 {len(all_items):,}건 | "
    f"페이지 {page_elapsed:.2f}초 | "
    f"누적 {format_time(elapsed)}"
)


# -----------------------------
# 5. 2페이지 ~ 마지막 페이지 수집
# -----------------------------
for page_no in range(2, total_pages + 1):

    params["pageNo"] = page_no

    page_start = time.perf_counter()

    try:
        response = requests.get(
            URL,
            params=params,
            timeout=30
        )

        response.raise_for_status()

        data = response.json()

        header = data["response"]["header"]

        if header["resultCode"] != "00":
            print(
                f"[{page_no}/{total_pages}] API 오류:",
                header["resultCode"],
                header["resultMsg"]
            )
            continue

        body = data["response"]["body"]

        items = body.get("items", {}).get("item", [])

        if isinstance(items, dict):
            items = [items]

        all_items.extend(items)

        # 이번 페이지 처리시간
        page_elapsed = time.perf_counter() - page_start

        # 전체 누적시간
        elapsed = time.perf_counter() - start_time

        # 페이지당 평균 소요시간
        avg_time = elapsed / page_no

        # 남은 페이지
        remaining_pages = total_pages - page_no

        # 예상 남은시간
        eta = avg_time * remaining_pages

        print(
            f"[{page_no}/{total_pages}] "
            f"누적 {len(all_items):,}건 | "
            f"페이지 {page_elapsed:.2f}초 | "
            f"경과 {format_time(elapsed)} | "
            f"예상 남은시간 {format_time(eta)}"
        )

        # 서버 과부하 방지
        time.sleep(0.1)

    except Exception as e:
        print(
            f"[{page_no}/{total_pages}] "
            f"수집 실패: {e}"
        )


# -----------------------------
# 6. DataFrame 생성
# -----------------------------
print()
print("DataFrame 생성 중...")

df = pd.DataFrame(all_items)

print("최종 수집 건수:", f"{len(df):,}")
print("컬럼 수:", len(df.columns))


# -----------------------------
# 7. RAW CSV 저장
# -----------------------------
output_path = "data/raw/facility_all_20260920.csv"

print("CSV 저장 중...")

df.to_csv(
    output_path,
    index=False,
    encoding="utf-8-sig"
)


# -----------------------------
# 8. 최종 결과
# -----------------------------
total_elapsed = time.perf_counter() - start_time

print()
print("=" * 60)
print("수집 완료")
print("최종 수집 건수:", f"{len(df):,}")
print("CSV:", output_path)
print("전체 소요시간:", format_time(total_elapsed))
print("=" * 60)