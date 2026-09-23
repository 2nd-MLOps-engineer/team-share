from pathlib import Path
from datetime import datetime
from urllib.parse import unquote
import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv


# =========================================================
# 1. 경로 설정
# =========================================================

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "air_quality"
RAW_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = RAW_DIR / "air_quality_all.csv"


# =========================================================
# 2. ENV 파일 찾기
# =========================================================

ENV_CANDIDATES = [
    CURRENT_DIR / ".env",
    PROJECT_ROOT / "M3_data_pipeline" / "collector.env",
    CURRENT_DIR / "collector.env",
    PROJECT_ROOT / ".env",
]

ENV_FILE = None

for candidate in ENV_CANDIDATES:
    if candidate.exists():
        ENV_FILE = candidate
        break

if ENV_FILE is None:
    raise FileNotFoundError(
        ".env 또는 collector.env 파일을 찾을 수 없습니다."
    )

load_dotenv(ENV_FILE, override=True)

print("ENV 파일:", ENV_FILE)


# =========================================================
# 3. API KEY
# =========================================================

API_KEY_RAW = os.getenv("AIRKOREA_API_KEY")

if not API_KEY_RAW:
    raise ValueError(
        "AIRKOREA_API_KEY가 ENV 파일에 없습니다."
    )

# 공공데이터포털 Encoding 인증키 사용
API_KEY = unquote(API_KEY_RAW)

print("AIRKOREA_API_KEY: 확인됨")


# =========================================================
# 4. API 설정
# =========================================================

BASE_URL = (
    "https://apis.data.go.kr/B552584/"
    "ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty"
)

SIDO_LIST = [
    "서울",
    "부산",
    "대구",
    "인천",
    "광주",
    "대전",
    "울산",
    "경기",
    "강원",
    "충북",
    "충남",
    "전북",
    "전남",
    "경북",
    "경남",
    "제주",
    "세종",
]

# 지역별 최대 호출 횟수
MAX_RETRIES = 3

# 재시도 대기 시간
RETRY_WAIT_SECONDS = [3, 5]


# =========================================================
# 5. 저장할 컬럼
# =========================================================

WANTED_COLUMNS = [
    "sidoName",
    "stationName",
    "dataTime",
    "pm10Value",
    "pm25Value",
    "o3Value",
    "khaiValue",
    "pm10Grade",
    "pm25Grade",
    "khaiGrade",
    "data_type",
    "collected_at",
]


# =========================================================
# 6. API 1회 호출 함수
# =========================================================

def request_sido(sido_name):

    params = {
        "serviceKey": API_KEY,
        "returnType": "json",
        "numOfRows": 1000,
        "pageNo": 1,
        "sidoName": sido_name,
        "ver": "1.0",
    }

    response = requests.get(
        BASE_URL,
        params=params,
        timeout=30,
    )

    print(
        f"[{sido_name}] HTTP STATUS:",
        response.status_code,
    )

    # -----------------------------------------------------
    # HTTP 오류
    # -----------------------------------------------------

    if response.status_code != 200:

        print(
            f"[{sido_name}] HTTP 오류:",
            response.text[:300],
        )

        return None


    # -----------------------------------------------------
    # JSON 변환
    # -----------------------------------------------------

    try:
        data = response.json()

    except Exception:

        print(
            f"[{sido_name}] JSON 변환 실패"
        )

        print(
            response.text[:300]
        )

        return None


    # -----------------------------------------------------
    # API 결과 코드 확인
    # -----------------------------------------------------

    header = (
        data
        .get("response", {})
        .get("header", {})
    )

    result_code = header.get("resultCode")
    result_msg = header.get("resultMsg")

    print(
        f"[{sido_name}] API RESULT:",
        result_code,
        result_msg,
    )

    if str(result_code) != "00":

        print(
            f"[{sido_name}] API 오류:",
            result_code,
            result_msg,
        )

        return None


    # -----------------------------------------------------
    # 데이터 추출
    # -----------------------------------------------------

    items = (
        data
        .get("response", {})
        .get("body", {})
        .get("items", [])
    )

    if not items:

        print(
            f"[{sido_name}] 데이터 0건"
        )

        return None


    # -----------------------------------------------------
    # DataFrame 생성
    # -----------------------------------------------------

    df = pd.DataFrame(items)

    df["sidoName"] = sido_name

    df["data_type"] = "air_quality"

    df["collected_at"] = (
        datetime.now()
        .strftime("%Y-%m-%d %H:%M:%S")
    )


    # -----------------------------------------------------
    # 없는 컬럼이 있어도 오류 방지
    # -----------------------------------------------------

    for column in WANTED_COLUMNS:

        if column not in df.columns:
            df[column] = None


    df = df[WANTED_COLUMNS]

    return df


# =========================================================
# 7. 지역별 재시도 포함 수집 함수
# =========================================================

def collect_sido_with_retry(sido_name):

    print()
    print("=" * 50)
    print(f"[{sido_name}] 수집 시작")
    print("=" * 50)

    for attempt in range(1, MAX_RETRIES + 1):

        print(
            f"[{sido_name}] 수집 시도 {attempt}/{MAX_RETRIES}"
        )

        try:

            df = request_sido(sido_name)

            if df is not None and not df.empty:

                print(
                    f"[{sido_name}] 수집 성공:",
                    len(df),
                    "건"
                )

                return df


        except requests.exceptions.Timeout:

            print(
                f"[{sido_name}] 요청 시간 초과"
            )


        except requests.exceptions.RequestException as e:

            print(
                f"[{sido_name}] 네트워크 오류:",
                e,
            )


        except Exception as e:

            print(
                f"[{sido_name}] 예상하지 못한 오류:",
                e,
            )


        # -------------------------------------------------
        # 마지막 시도가 아니면 대기 후 재시도
        # -------------------------------------------------

        if attempt < MAX_RETRIES:

            wait_seconds = RETRY_WAIT_SECONDS[
                attempt - 1
            ]

            print(
                f"[{sido_name}] "
                f"{wait_seconds}초 후 재시도..."
            )

            time.sleep(wait_seconds)


    print(
        f"[{sido_name}] "
        f"{MAX_RETRIES}회 모두 실패"
    )

    return None


# =========================================================
# 8. 전국 수집 시작
# =========================================================

print()
print("=" * 60)
print("에어코리아 전국 실시간 대기질 수집 시작")
print("=" * 60)

all_data = []

success_sido = []
failed_sido = []


for sido in SIDO_LIST:

    sido_df = collect_sido_with_retry(sido)

    if sido_df is not None and not sido_df.empty:

        all_data.append(sido_df)

        success_sido.append(sido)

    else:

        failed_sido.append(sido)


    # 지역과 지역 사이에도 잠깐 대기
    time.sleep(0.5)

if failed_sido:
    raise RuntimeError(
        "재시도 후에도 수집하지 못한 시도가 있습니다: "
        + ", ".join(failed_sido)
    )


# =========================================================
# 9. 데이터 존재 여부 확인
# =========================================================

if not all_data:

    raise RuntimeError(
        "전국 대기질 데이터를 한 건도 수집하지 못했습니다."
    )


# =========================================================
# 10. 전국 데이터 합치기
# =========================================================

df_all = pd.concat(
    all_data,
    ignore_index=True,
)


# =========================================================
# 11. CSV 저장
# =========================================================

temporary_file = OUTPUT_FILE.with_suffix(".tmp.csv")
df_all.to_csv(
    temporary_file,
    index=False,
    encoding="utf-8-sig",
)
temporary_file.replace(OUTPUT_FILE)


# =========================================================
# 12. 결과 출력
# =========================================================

print()
print("=" * 60)
print("전국 대기질 수집 완료")
print("=" * 60)

print()
print("저장 파일:")
print(OUTPUT_FILE)

print()
print("전체 건수:")
print(len(df_all))

print()
print("성공 시도:")
print(success_sido)

print()
print("실패 시도:")
print(failed_sido)

print()
print("시도별 건수:")

print(
    df_all
    .groupby("sidoName")
    .size()
    .sort_index()
)

print()
print("컬럼:")

print(
    df_all.columns.tolist()
)


# =========================================================
# 13. 최종 상태
# =========================================================

print()
print("=" * 60)

if not failed_sido:

    print("최종 상태: 전국 17개 시도 수집 성공")

else:

    print("최종 상태: 일부 지역 수집 실패")

    print(
        "최종 실패 지역:",
        ", ".join(failed_sido)
    )

print("=" * 60)
