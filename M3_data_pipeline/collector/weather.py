from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import unquote
import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv


# ==================================================
# 1. 프로젝트 경로 설정
# ==================================================

# 현재 파일:
# team-share/M3_data_pipeline/collector/collect_weather.py
#
# parents[2] = team-share

PROJECT_ROOT = Path(__file__).resolve().parents[2]

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "weather"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# ==================================================
# 2. .env 파일 자동 탐색
# ==================================================

ENV_CANDIDATES = [
    PROJECT_ROOT / ".env",
    PROJECT_ROOT / "M3_data_pipeline" / ".env",
    Path(__file__).resolve().parent / ".env",
]

ENV_PATH = None

for path in ENV_CANDIDATES:
    if path.exists():
        ENV_PATH = path
        break

if ENV_PATH is None:
    raise FileNotFoundError(
        ".env 파일을 찾을 수 없습니다."
    )


# ==================================================
# 3. 환경변수 로드
# ==================================================

load_dotenv(
    dotenv_path=ENV_PATH,
    override=True
)

API_KEY_RAW = os.getenv("WEATHER_API_KEY")

print("=" * 60)
print("환경변수 확인")
print("=" * 60)
print("ENV 파일:", ENV_PATH)
print(
    "WEATHER_API_KEY:",
    "확인됨" if API_KEY_RAW else "없음"
)
print()

if not API_KEY_RAW:
    raise ValueError(
        f"WEATHER_API_KEY를 읽지 못했습니다. "
        f"ENV 파일: {ENV_PATH}"
    )


# ==================================================
# 4. 공공데이터포털 Encoding Key 디코딩
# ==================================================

API_KEY = unquote(API_KEY_RAW)

MAX_RETRIES = 3
RETRY_SECONDS = (2, 5)


# ==================================================
# 5. 기상청 API 주소
# ==================================================

BASE_URL = (
    "https://apis.data.go.kr/"
    "1360000/"
    "VilageFcstInfoService_2.0"
)

# 초단기실황
NCST_URL = (
    f"{BASE_URL}/getUltraSrtNcst"
)

# 초단기예보
FCST_URL = (
    f"{BASE_URL}/getUltraSrtFcst"
)


# ==================================================
# 6. 테스트 지역
#
# 우선 서울 격자 한 곳으로 테스트
# nx = 60
# ny = 127
# ==================================================

NX = 60
NY = 127


# ==================================================
# 7. 초단기실황 기준시간 계산
#
# API 제공 지연을 고려해서
# 현재보다 1시간 전 실황 요청
# ==================================================

def get_ncst_base_datetime():

    now = datetime.now()

    base = now - timedelta(hours=1)

    base_date = base.strftime("%Y%m%d")
    base_time = base.strftime("%H00")

    return base_date, base_time


# ==================================================
# 8. 초단기예보 기준시간 계산
#
# 초단기예보:
# 매시간 30분 기준
#
# 안전하게 45분 이후 → 현재 시간 30분
# 45분 이전 → 이전 시간 30분
# ==================================================

def get_fcst_base_datetime():

    now = datetime.now()

    if now.minute >= 45:
        base = now
    else:
        base = now - timedelta(hours=1)

    base_date = base.strftime("%Y%m%d")
    base_time = base.strftime("%H30")

    return base_date, base_time


# ==================================================
# 9. API 요청 공통 함수
# ==================================================

def request_weather(
    url,
    base_date,
    base_time
):

    params = {
        "serviceKey": API_KEY,
        "pageNo": 1,
        "numOfRows": 1000,
        "dataType": "JSON",
        "base_date": base_date,
        "base_time": base_time,
        "nx": NX,
        "ny": NY,
    }

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=30)
            print()
            print("HTTP STATUS:", response.status_code)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as error:
            last_error = error
            print(f"날씨 API 요청 실패 ({attempt}/{MAX_RETRIES}): {error}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SECONDS[attempt - 1])
    raise RuntimeError("날씨 API 요청이 모두 실패했습니다.") from last_error


# ==================================================
# 10. API 응답에서 item 추출
# ==================================================

def extract_items(data):

    try:

        header = (
            data["response"]["header"]
        )

        result_code = (
            header["resultCode"]
        )

        result_msg = (
            header["resultMsg"]
        )

        print(
            "API RESULT:",
            result_code,
            result_msg
        )

        if result_code != "00":

            raise RuntimeError(
                f"기상청 API 오류: "
                f"{result_code} "
                f"{result_msg}"
            )

        items = (
            data["response"]
            ["body"]
            ["items"]
            ["item"]
        )

        return items

    except KeyError:

        print()
        print("예상하지 못한 API 응답:")
        print(data)

        raise


# ==================================================
# 11. 초단기실황 수집
# ==================================================

def collect_ncst():

    print()
    print("=" * 60)
    print("초단기실황 수집")
    print("=" * 60)

    base_date, base_time = (
        get_ncst_base_datetime()
    )

    print(
        "base_date:",
        base_date
    )

    print(
        "base_time:",
        base_time
    )

    data = request_weather(
        NCST_URL,
        base_date,
        base_time
    )

    items = extract_items(data)

    if not items:
        raise RuntimeError("초단기실황 API 데이터가 0건입니다.")

    df = pd.DataFrame(items)

    # 데이터 종류
    df["data_type"] = "ncst"

    # 수집 시각
    df["collected_at"] = (
        datetime.now()
        .strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    return df


# ==================================================
# 12. 초단기예보 수집
# ==================================================

def collect_fcst():

    print()
    print("=" * 60)
    print("초단기예보 수집")
    print("=" * 60)

    base_date, base_time = (
        get_fcst_base_datetime()
    )

    print(
        "base_date:",
        base_date
    )

    print(
        "base_time:",
        base_time
    )

    data = request_weather(
        FCST_URL,
        base_date,
        base_time
    )

    items = extract_items(data)

    if not items:
        raise RuntimeError("초단기예보 API 데이터가 0건입니다.")

    df = pd.DataFrame(items)

    # 데이터 종류
    df["data_type"] = "fcst"

    # 수집 시각
    df["collected_at"] = (
        datetime.now()
        .strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    return df


# ==================================================
# 13. 메인 실행
# ==================================================

def main():

    print("=" * 60)
    print("기상청 초단기 날씨 수집 시작")
    print("=" * 60)

    print(
        "테스트 격자:",
        f"nx={NX}, ny={NY}"
    )

    # ------------------------------------------------
    # 초단기실황
    # ------------------------------------------------

    ncst_df = collect_ncst()

    ncst_path = (
        OUTPUT_DIR
        / "weather_ultra_ncst.csv"
    )

    ncst_temporary_path = ncst_path.with_suffix(".tmp.csv")
    ncst_df.to_csv(
        ncst_temporary_path,
        index=False,
        encoding="utf-8-sig"
    )
    ncst_temporary_path.replace(ncst_path)

    print()
    print(
        "실황 저장:",
        ncst_path
    )

    print(
        "실황 건수:",
        f"{len(ncst_df):,}"
    )

    # ------------------------------------------------
    # 초단기예보
    # ------------------------------------------------

    fcst_df = collect_fcst()

    fcst_path = (
        OUTPUT_DIR
        / "weather_ultra_fcst.csv"
    )

    fcst_temporary_path = fcst_path.with_suffix(".tmp.csv")
    fcst_df.to_csv(
        fcst_temporary_path,
        index=False,
        encoding="utf-8-sig"
    )
    fcst_temporary_path.replace(fcst_path)

    print()
    print(
        "예보 저장:",
        fcst_path
    )

    print(
        "예보 건수:",
        f"{len(fcst_df):,}"
    )

    # ------------------------------------------------
    # 결과 출력
    # ------------------------------------------------

    print()
    print("=" * 60)
    print("날씨 데이터 수집 완료")
    print("=" * 60)

    print()
    print("초단기실황 컬럼:")
    print(
        ncst_df.columns.tolist()
    )

    print()
    print("초단기예보 컬럼:")
    print(
        fcst_df.columns.tolist()
    )

    print()
    print("초단기실황 앞 5건:")
    print(
        ncst_df.head()
    )

    print()
    print("초단기예보 앞 5건:")
    print(
        fcst_df.head()
    )

    print()
    print("저장 파일:")
    print(ncst_path)
    print(fcst_path)


# ==================================================
# 14. 실행
# ==================================================

if __name__ == "__main__":
    main()
