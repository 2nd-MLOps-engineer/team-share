import os
import time
from pathlib import Path
from urllib.parse import unquote

import requests
import pandas as pd
from dotenv import load_dotenv


# ============================================================
# TAGO 버스도착정보 - 전국 제공 도시 수집 테스트
#
# 사용 API:
#   ArvlInfoInqireService
#
# 1. 도시코드 목록 조회
# 2. 각 도시 코드 확인
#
# ※ 정류장 nodeId 없이 도착정보 전수수집은 불가능하므로
#   우선 TAGO가 제공하는 전국 도시코드를 실제 API에서 수집한다.
# ============================================================


# ------------------------------------------------------------
# 경로
# ------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


# ------------------------------------------------------------
# ENV
# ------------------------------------------------------------

load_dotenv(ENV_PATH)

ENCODED_KEY = os.getenv("TAGO_API_KEY")

if not ENCODED_KEY:
    raise RuntimeError(
        f"TAGO_API_KEY 없음\n"
        f"ENV: {ENV_PATH}"
    )


# 공공데이터포털 Encoding Key → Decoding
TAGO_API_KEY = unquote("TAGO_API_KEY")


# ------------------------------------------------------------
# URL
# ------------------------------------------------------------

BASE_URL = (
    "https://apis.data.go.kr/1613000/"
    "ArvlInfoInqireService"
)

CITY_URL = BASE_URL + "/getCtyCodeList"


# ------------------------------------------------------------
# 공통 설정
# ------------------------------------------------------------

TIMEOUT = 30
MAX_RETRY = 5


# ------------------------------------------------------------
# 요청 함수
# ------------------------------------------------------------

def request_tago(url, params):

    for attempt in range(1, MAX_RETRY + 1):

        try:

            response = requests.get(
                url,
                params=params,
                timeout=TIMEOUT
            )

            print(
                f"HTTP STATUS: {response.status_code}"
            )

            if response.status_code == 403:

                print("403 응답:")
                print(response.text)

                time.sleep(2)
                continue

            response.raise_for_status()

            data = response.json()

            api_response = data.get(
                "response",
                {}
            )

            header = api_response.get(
                "header",
                {}
            )

            code = header.get(
                "resultCode"
            )

            msg = header.get(
                "resultMsg"
            )

            print(
                f"API RESULT: {code} {msg}"
            )

            if code != "00":

                print(data)
                return None

            return api_response.get(
                "body",
                {}
            )

        except requests.exceptions.Timeout:

            print(
                f"TIMEOUT "
                f"{attempt}/{MAX_RETRY}"
            )

        except requests.exceptions.RequestException as e:

            print(
                f"REQUEST ERROR "
                f"{attempt}/{MAX_RETRY}"
            )

            print(e)

        except ValueError:

            print("JSON 변환 실패")

            try:
                print(response.text)
            except:
                pass

        time.sleep(2)

    return None


# ------------------------------------------------------------
# items 추출
# ------------------------------------------------------------

def extract_items(body):

    if not body:
        return []

    items = body.get("items")

    if not items:
        return []

    item = items.get("item")

    if not item:
        return []

    if isinstance(item, dict):
        return [item]

    return item


# ============================================================
# 실행
# ============================================================

print()
print("=" * 60)
print("TAGO 전국 제공 도시코드 수집 시작")
print("=" * 60)

print()
print("ENV 파일:", ENV_PATH)
print("TAGO_API_KEY: 확인됨")
print("서비스키 Decoding 처리: 완료")


# ------------------------------------------------------------
# 도시코드 조회
# ------------------------------------------------------------

params = {

    "serviceKey": TAGO_API_KEY,

    "pageNo": "1",

    "numOfRows": "1000",

    "_type": "json",
}


print()
print("[도시코드] TAGO API 호출 시작")


body = request_tago(
    CITY_URL,
    params
)


if body is None:

    print()
    print("=" * 60)
    print("도시코드 수집 실패")
    print("=" * 60)

    raise SystemExit()


cities = extract_items(body)


print()
print("=" * 60)
print("도시코드 수집 성공")
print("=" * 60)

print(
    "수집 도시 수:",
    len(cities)
)


# ------------------------------------------------------------
# 결과 출력
# ------------------------------------------------------------

rows = []


for city in cities:

    city_code = (
        city.get("citycode")
        or city.get("cityCode")
        or ""
    )

    city_name = (
        city.get("cityname")
        or city.get("cityName")
        or ""
    )


    print(
        city_code,
        city_name
    )


    rows.append({

        "city_code":
            city_code,

        "city_name":
            city_name,
    })


# ------------------------------------------------------------
# CSV 저장
# ------------------------------------------------------------

df = pd.DataFrame(
    rows
)


OUTPUT_PATH = (
    OUTPUT_DIR
    / "tago_city_codes.csv"
)


df.to_csv(
    OUTPUT_PATH,
    index=False,
    encoding="utf-8-sig"
)


print()
print("=" * 60)
print("완료")
print("=" * 60)

print(
    "도시코드:",
    len(df)
)

print(
    "저장:",
    OUTPUT_PATH
)