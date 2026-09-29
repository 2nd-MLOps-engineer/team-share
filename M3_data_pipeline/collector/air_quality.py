from pathlib import Path
from datetime import datetime
from urllib.parse import unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys
import threading
import time

import pandas as pd
import requests
from dotenv import load_dotenv

PIPELINE_DIR = Path(__file__).resolve().parents[1]
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from pipeline_elt import upsert_raw_dataset_group


# =========================================================
# 1. 경로 설정
# =========================================================

CURRENT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = CURRENT_DIR.parent.parent

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

load_dotenv(ENV_FILE, override=False)

print("ENV 파일:", ENV_FILE)


# =========================================================
# 3. API KEY
# =========================================================

API_KEY_RAW = os.getenv("AIRKOREA_API_KEY")

if not API_KEY_RAW:
    raise ValueError(
        "AIRKOREA_API_KEY가 ENV 파일에 없습니다."
    )

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

MAX_RETRIES = 3
RETRY_WAIT_SECONDS = [3, 5]

# 시도 단위 동시 수집 수
# 첫 benchmark는 6으로 시작
MAX_WORKERS = 6

# 각 thread가 자기 Session을 재사용
_thread_local = threading.local()


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
# 6. Thread별 HTTP Session
# =========================================================

def get_session():

    if not hasattr(_thread_local, "session"):

        _thread_local.session = requests.Session()

    return _thread_local.session


# =========================================================
# 7. API 1회 호출 함수
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

    session = get_session()

    response = session.get(
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

    body = (
        data
        .get("response", {})
        .get("body", {})
    )

    items = body.get("items", [])


    # -----------------------------------------------------
    # 데이터 0건 확인
    # -----------------------------------------------------

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
# 8. 지역별 재시도 포함 수집 함수
# =========================================================

def collect_sido_with_retry(sido_name):

    print()
    print("=" * 50)
    print(f"[{sido_name}] 수집 시작")
    print("=" * 50)

    for attempt in range(1, MAX_RETRIES + 1):

        print(
            f"[{sido_name}] 수집 시도 "
            f"{attempt}/{MAX_RETRIES}"
        )

        try:

            df = request_sido(sido_name)

            if df is not None and not df.empty:

                print(
                    f"[{sido_name}] 수집 성공:",
                    len(df),
                    "건",
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
# 9. 전국 병렬 수집
# =========================================================

def collect_all_sidos():

    print()
    print("=" * 60)
    print("에어코리아 전국 실시간 대기질 수집 시작")
    print("=" * 60)

    started = time.perf_counter()

    results_by_sido = {}
    success_sido = []
    failed_sido = []

    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        future_to_sido = {
            executor.submit(
                collect_sido_with_retry,
                sido,
            ): sido
            for sido in SIDO_LIST
        }

        for future in as_completed(
            future_to_sido
        ):

            sido = future_to_sido[future]

            try:

                sido_df = future.result()

                if (
                    sido_df is not None
                    and not sido_df.empty
                ):

                    results_by_sido[sido] = sido_df
                    success_sido.append(sido)

                else:

                    failed_sido.append(sido)

            except Exception as e:

                print(
                    f"[{sido}] Future 처리 실패:",
                    e,
                )

                failed_sido.append(sido)

    elapsed = time.perf_counter() - started

    # -----------------------------------------------------
    # 출력 순서를 SIDO_LIST 기준으로 고정
    # -----------------------------------------------------

    success_sido = [
        sido
        for sido in SIDO_LIST
        if sido in success_sido
    ]

    failed_sido = [
        sido
        for sido in SIDO_LIST
        if sido in failed_sido
    ]

    # -----------------------------------------------------
    # 하나라도 실패하면 기존 RAW DB snapshot 보호
    # -----------------------------------------------------

    if failed_sido:

        print()
        print("=" * 60)
        print("전국 대기질 수집 실패")
        print("=" * 60)

        print(
            "MAX_WORKERS:",
            MAX_WORKERS,
        )

        print(
            "전체 시도 수:",
            len(SIDO_LIST),
        )

        print(
            "성공 시도 수:",
            len(success_sido),
        )

        print(
            "실패 시도 수:",
            len(failed_sido),
        )

        print(
            "실패 시도:",
            failed_sido,
        )

        print(
            f"wall-clock: "
            f"{elapsed:.2f}초 / "
            f"{elapsed / 60:.2f}분"
        )

        print(
            "실패 시도의 기존 RAW 데이터는 유지하고 "
            "성공한 시도 데이터만 누적합니다."
        )

        print(
            "[WARNING] 일부 시도 수집 실패: "
            + ", ".join(failed_sido)
            + " / 성공한 시도 데이터만 누적 저장합니다."
        )

    # -----------------------------------------------------
    # 데이터 존재 여부 확인
    # -----------------------------------------------------

    if not results_by_sido:

        raise RuntimeError(
            "전국 대기질 데이터를 "
            "한 건도 수집하지 못했습니다."
        )

    # -----------------------------------------------------
    # SIDO_LIST 순서로 합치기
    # -----------------------------------------------------

    ordered_frames = [
        results_by_sido[sido]
        for sido in SIDO_LIST
    ]

    df_all = pd.concat(
        ordered_frames,
        ignore_index=True,
    )

    # -----------------------------------------------------
    # 병렬 completion 순서와 무관한 deterministic output
    # -----------------------------------------------------

    sort_columns = [
        column
        for column in [
            "sidoName",
            "stationName",
            "dataTime",
        ]
        if column in df_all.columns
    ]

    if sort_columns:

        df_all = (
            df_all
            .sort_values(
                sort_columns,
                kind="stable",
                na_position="last",
            )
            .reset_index(drop=True)
        )

    return (
        df_all,
        success_sido,
        failed_sido,
        elapsed,
    )


# =========================================================
# 10. 실행
# =========================================================

(
    df_all,
    success_sido,
    failed_sido,
    elapsed,
) = collect_all_sidos()


# =========================================================
# 11. RAW DB 저장
# =========================================================

upsert_raw_dataset_group(
    {"air_quality": df_all}
)


# =========================================================
# 12. 결과 출력
# =========================================================

print()
print("=" * 60)
print("전국 대기질 수집 완료")
print("=" * 60)

print(
    "MAX_WORKERS:",
    MAX_WORKERS,
)

print(
    "전체 시도 수:",
    len(SIDO_LIST),
)

print(
    "성공 시도 수:",
    len(success_sido),
)

print(
    "실패 시도 수:",
    len(failed_sido),
)

print(
    "실패 시도:",
    failed_sido,
)

print(
    "전체 건수:",
    len(df_all),
)

print(
    f"전체 wall-clock: "
    f"{elapsed / 60:.2f}분"
)

print()
print("RAW DB 저장: raw.air_quality")

print()
print("성공 시도:")
print(success_sido)

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

print()
print("=" * 60)
print("최종 상태: 전국 17개 시도 수집 성공")
print("=" * 60)
