import os
import json
from pathlib import Path
from urllib.parse import unquote

import requests
from dotenv import load_dotenv


# ============================================================
# 1. 경로 / ENV
# ============================================================

# 현재 파일은 M3_data_pipeline/tests/experiments 아래에 있다.
EXPERIMENT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = Path(__file__).resolve().parents[2]

# 실제 ENV:
# D:\BAIK\projects\team-share\M3_data_pipeline\collector\.env

ENV_PATH = PROJECT_DIR / "collector" / ".env"

print("=" * 70)
print("두루누비 API 테스트")
print("=" * 70)
print("실행 파일 :", Path(__file__).resolve())
print("현재 폴더 :", EXPERIMENT_DIR)
print("ENV 경로  :", ENV_PATH)
print("ENV 존재  :", ENV_PATH.exists())

if not ENV_PATH.exists():
    raise FileNotFoundError(
        f".env 파일을 찾을 수 없습니다.\n"
        f"실행 파일: {Path(__file__).resolve()}\n"
        f"확인 경로: {ENV_PATH}"
    )

load_dotenv(
    dotenv_path=ENV_PATH,
    override=True,
)

RAW_KEY = os.getenv("DURUNUBI_API_KEY")

if not RAW_KEY:
    raise ValueError(
        f"DURUNUBI_API_KEY가 .env에 없습니다.\n"
        f"ENV 파일: {ENV_PATH}"
    )

SERVICE_KEY = unquote(RAW_KEY)

print("DURUNUBI_API_KEY : 확인됨")


# ============================================================
# 2. API
# ============================================================

BASE_URL = "https://apis.data.go.kr/B551011/Durunubi"

ENDPOINTS = {
    "courseList": f"{BASE_URL}/courseList",
    "routeList": f"{BASE_URL}/routeList",
}


# ============================================================
# 3. API 테스트
# ============================================================

def test_api(name, url):

    print()
    print("=" * 70)
    print(f"{name} 테스트")
    print("=" * 70)

    params = {
        "serviceKey": SERVICE_KEY,
        "numOfRows": 10,
        "pageNo": 1,
        "MobileOS": "ETC",
        "MobileApp": "WooSimWoonKka",
        "brdDiv": "DNWW",
        "_type": "json",
    }

    response = requests.get(
        url,
        params=params,
        timeout=30,
    )

    print("HTTP STATUS :", response.status_code)
    print("Content-Type:", response.headers.get("Content-Type"))

    if response.status_code != 200:
        print()
        print("HTTP 오류 응답:")
        print(response.text[:5000])
        return

    try:
        data = response.json()

    except ValueError:
        print()
        print("JSON 변환 실패")
        print("원본 응답:")
        print(response.text[:5000])
        return

    print()
    print("===== JSON 응답 =====")

    print(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
        )[:20000]
    )

    response_data = data.get("response", {})

    header = response_data.get("header", {})
    body = response_data.get("body", {})

    print()
    print("===== HEADER =====")
    print(header)

    if isinstance(body, dict):

        print()
        print("===== BODY =====")
        print("KEYS       :", list(body.keys()))
        print("totalCount :", body.get("totalCount"))
        print("pageNo     :", body.get("pageNo"))
        print("numOfRows  :", body.get("numOfRows"))

        items = body.get("items")

        print()
        print("===== ITEMS =====")

        print(
            json.dumps(
                items,
                ensure_ascii=False,
                indent=2,
            )[:15000]
        )


# ============================================================
# 4. MAIN
# ============================================================

def main():

    for name, url in ENDPOINTS.items():

        try:
            test_api(name, url)

        except Exception as e:

            print()
            print("=" * 70)
            print(f"{name} 실행 실패")
            print("=" * 70)
            print(type(e).__name__, ":", e)


if __name__ == "__main__":
    main()
