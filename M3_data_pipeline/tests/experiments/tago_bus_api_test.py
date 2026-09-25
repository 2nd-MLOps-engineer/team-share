import os
from pathlib import Path
from urllib.parse import unquote

import requests
from dotenv import load_dotenv


# ============================================================
# TAGO 버스도착정보 API 테스트
# - .env의 Encoding 서비스키 사용
# - 코드에서 unquote()로 Decoding 후 requests에 전달
# ============================================================


# ------------------------------------------------------------
# 1. .env 위치
# 실제 환경변수 파일은 프로젝트의 collector 폴더에 있음
# ------------------------------------------------------------

PROJECT_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = PROJECT_DIR / "collector" / ".env"

print("=" * 60)
print("TAGO 버스도착정보 API 테스트")
print("=" * 60)
print("ENV 파일:", ENV_PATH)


if not ENV_PATH.exists():
    raise RuntimeError(
        f".env 파일을 찾을 수 없습니다.\n"
        f"확인 경로: {ENV_PATH}"
    )


# ------------------------------------------------------------
# 2. .env 로드
# ------------------------------------------------------------

load_dotenv(ENV_PATH)

TAGO_API_KEY_ENCODED = os.getenv("TAGO_API_KEY")


if not TAGO_API_KEY_ENCODED:
    raise RuntimeError(
        "TAGO_API_KEY를 찾을 수 없습니다.\n"
        f"ENV 파일 확인: {ENV_PATH}"
    )


print("TAGO_API_KEY: 확인됨")


# ------------------------------------------------------------
# 3. Encoding 서비스키 → Decoding
#
# .env
# xxxxx%2Bxxxxx%3D%3D
#
# ↓ unquote()
#
# Python
# xxxxx+xxxxx==
#
# requests가 요청 URL을 만들면서 정상적으로 1회 인코딩
# ------------------------------------------------------------

TAGO_API_KEY = unquote(TAGO_API_KEY_ENCODED)

print("서비스키 Decoding 처리: 완료")


# ------------------------------------------------------------
# 4. TAGO 버스도착정보 API
# ------------------------------------------------------------

URL = (
    "https://apis.data.go.kr/1613000/"
    "ArvlInfoInqireService/"
    "getSttnAcctoArvlPrearngeInfoList"
)


# ------------------------------------------------------------
# 5. 테스트 요청값
#
# 현재 목적:
# ① API KEY 인증
# ② TAGO 서버 연결
# ③ Endpoint 정상 여부 확인
#
# nodeId는 아직 실제 정류장 ID가 아님
# ------------------------------------------------------------

params = {
    "serviceKey": TAGO_API_KEY,
    "pageNo": "1",
    "numOfRows": "10",
    "_type": "json",

    # 서울 도시코드
    "cityCode": "11",

    # 연결 테스트용 임시값
    "nodeId": "SEOUL123456",
}


# ------------------------------------------------------------
# 6. API 호출
# ------------------------------------------------------------

print()
print("[서울] TAGO API 호출 시작")


try:

    response = requests.get(
        URL,
        params=params,
        timeout=30
    )

    print("[서울] HTTP STATUS:", response.status_code)


    # --------------------------------------------------------
    # 7. HTTP 오류 확인
    # --------------------------------------------------------

    if response.status_code != 200:

        print()
        print("=" * 60)
        print("HTTP 오류 발생")
        print("=" * 60)

        print("STATUS:", response.status_code)

        print()
        print("서버 응답:")
        print(response.text)

        raise SystemExit()


    # --------------------------------------------------------
    # 8. JSON 변환
    # --------------------------------------------------------

    try:

        data = response.json()

    except ValueError:

        print()
        print("=" * 60)
        print("JSON 변환 실패")
        print("=" * 60)

        print("서버 원본 응답:")
        print(response.text)

        raise SystemExit()


    # --------------------------------------------------------
    # 9. API 응답 확인
    # --------------------------------------------------------

    response_data = data.get("response", {})

    header = response_data.get("header", {})

    result_code = header.get("resultCode")
    result_msg = header.get("resultMsg")


    print()
    print("API RESULT:", result_code, result_msg)


    # --------------------------------------------------------
    # 10. API 정상 응답
    # --------------------------------------------------------

    if result_code == "00":

        print()
        print("=" * 60)
        print("TAGO API 인증 및 연결 성공")
        print("=" * 60)

        body = response_data.get("body", {})

        total_count = body.get("totalCount", 0)

        print("조회 데이터 건수:", total_count)


        # ----------------------------------------------------
        # 실제 데이터가 있는 경우
        # ----------------------------------------------------

        if total_count:

            items = (
                body
                .get("items", {})
                .get("item", [])
            )

            # item 하나만 올 경우 dict일 수도 있으므로 list로 변환
            if isinstance(items, dict):
                items = [items]

            print()
            print("도착정보:")
            print("-" * 60)

            for item in items:

                print(
                    "정류장:",
                    item.get("nodenm", "-")
                )

                print(
                    "노선:",
                    item.get("routeno", "-")
                )

                print(
                    "도착예정:",
                    item.get("arrtime", "-"),
                    "초"
                )

                print(
                    "남은 정류장:",
                    item.get("arrprevstationcnt", "-")
                )

                print("-" * 60)


        # ----------------------------------------------------
        # 현재는 가짜 nodeId이므로 데이터 0건 가능
        # ----------------------------------------------------

        else:

            print()
            print("API 인증은 정상입니다.")
            print("현재 nodeId가 테스트용 임시값이라 조회 데이터가 없습니다.")
            print("다음 단계에서 실제 정류장 ID로 테스트하면 됩니다.")


    # --------------------------------------------------------
    # 11. API 자체 오류
    # --------------------------------------------------------

    else:

        print()
        print("=" * 60)
        print("TAGO API 응답 오류")
        print("=" * 60)

        print("RESULT CODE:", result_code)
        print("RESULT MESSAGE:", result_msg)

        print()
        print("서버 전체 응답:")
        print(data)


# ------------------------------------------------------------
# 12. 네트워크 예외 처리
# ------------------------------------------------------------

except requests.exceptions.Timeout:

    print()
    print("=" * 60)
    print("ERROR: API 요청 시간 초과")
    print("=" * 60)


except requests.exceptions.ConnectionError as e:

    print()
    print("=" * 60)
    print("ERROR: TAGO 서버 연결 실패")
    print("=" * 60)
    print(e)


except requests.exceptions.RequestException as e:

    print()
    print("=" * 60)
    print("ERROR: API 요청 실패")
    print("=" * 60)
    print(e)


print()
print("=" * 60)
print("테스트 종료")
print("=" * 60)
