import os
import csv
import time
from pathlib import Path
from urllib.parse import unquote

import requests
from dotenv import load_dotenv


# ============================================================
# TAGO 전국 버스정류장 수집기
#
# BUSSTOP_API_KEY 사용
#
# 1. 도시코드 전체 조회
# 2. 도시코드별 정류장 전체 조회
# 3. 페이지네이션
# 4. 중복 제거
# 5. 전국 버스정류장 CSV 저장
#
# DB / PostGIS / APScheduler 사용 안 함
# ============================================================


# ============================================================
# 경로
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data" / "raw" / "bus_stop"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = DATA_DIR / "tago_bus_stops_all.csv"


# ============================================================
# ENV
# ============================================================

load_dotenv(ENV_PATH)

BUSSTOP_API_KEY = os.getenv("BUSSTOP_API_KEY")

if not BUSSTOP_API_KEY:
    raise RuntimeError(
        f"BUSSTOP_API_KEY를 찾을 수 없습니다.\n"
        f"ENV 파일: {ENV_PATH}"
    )

BUSSTOP_API_KEY = unquote(BUSSTOP_API_KEY)


# ============================================================
# API URL
# ============================================================

BASE_URL = (
    "http://apis.data.go.kr/1613000/"
    "BusSttnInfoInqireService"
)

CITY_URL = (
    f"{BASE_URL}/getCtyCodeList"
)

STATION_URL = (
    f"{BASE_URL}/getSttnNoList"
)


# ============================================================
# 설정
# ============================================================

NUM_OF_ROWS = 100

REQUEST_DELAY = 0.2

RETRY_COUNT = 3


# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()


# ============================================================
# API 요청
# ============================================================

def request_json(url, params):

    for attempt in range(
        1,
        RETRY_COUNT + 1
    ):

        try:

            response = session.get(
                url,
                params=params,
                timeout=30
            )

            if response.status_code != 200:

                print(
                    f"HTTP STATUS: "
                    f"{response.status_code} "
                    f"({attempt}/{RETRY_COUNT})"
                )

                print(
                    response.text[:500]
                )

            else:

                try:

                    data = response.json()

                except ValueError:

                    print(
                        "JSON 변환 실패"
                    )

                    print(
                        response.text[:500]
                    )

                    data = None


                if data:

                    api_response = data.get(
                        "response",
                        {}
                    )

                    header = api_response.get(
                        "header",
                        {}
                    )

                    result_code = header.get(
                        "resultCode"
                    )

                    result_msg = header.get(
                        "resultMsg"
                    )

                    if result_code == "00":

                        return data

                    print(
                        "API ERROR:",
                        result_code,
                        result_msg
                    )


        except requests.exceptions.Timeout:

            print(
                f"TIMEOUT "
                f"({attempt}/{RETRY_COUNT})"
            )


        except requests.exceptions.RequestException as e:

            print(
                f"REQUEST ERROR "
                f"({attempt}/{RETRY_COUNT}):",
                e
            )


        if attempt < RETRY_COUNT:

            time.sleep(2)


    return None


# ============================================================
# 도시코드 전체 조회
# ============================================================

def get_city_codes():

    print()
    print("=" * 60)
    print("TAGO 도시코드 전체 조회")
    print("=" * 60)


    params = {

        "serviceKey":
            BUSSTOP_API_KEY,

        "_type":
            "json"
    }


    data = request_json(
        CITY_URL,
        params
    )


    if not data:

        raise RuntimeError(
            "도시코드 조회 실패"
        )


    body = (
        data
        .get("response", {})
        .get("body", {})
    )


    items = body.get(
        "items"
    )


    if not items:

        raise RuntimeError(
            "도시코드 데이터 없음"
        )


    cities = items.get(
        "item",
        []
    )


    if isinstance(
        cities,
        dict
    ):

        cities = [cities]


    print(
        "도시코드 수:",
        len(cities)
    )


    for city in cities:

        print(
            city.get(
                "citycode"
            ),
            city.get(
                "cityname"
            )
        )


    return cities


# ============================================================
# 도시 하나의 버스정류장 전체 조회
# ============================================================

def get_city_stations(
    city_code,
    city_name
):

    stations = []

    page_no = 1


    while True:

        params = {

            "serviceKey":
                BUSSTOP_API_KEY,

            "numOfRows":
                str(NUM_OF_ROWS),

            "pageNo":
                str(page_no),

            "_type":
                "json",

            "cityCode":
                str(city_code)
        }


        data = request_json(
            STATION_URL,
            params
        )


        if not data:
            raise RuntimeError(
                f"[{city_name}] 페이지 {page_no} 조회가 재시도 후에도 실패했습니다."
            )


        body = (
            data
            .get("response", {})
            .get("body", {})
        )


        try:

            total_count = int(
                body.get(
                    "totalCount",
                    0
                )
            )

        except (
            TypeError,
            ValueError
        ):

            total_count = 0


        items = body.get(
            "items"
        )


        # ----------------------------------------------------
        # 데이터 없음
        # ----------------------------------------------------

        if not items:

            if page_no == 1:

                print(
                    f"[{city_name}] "
                    f"정류장 0건"
                )

            break


        page_items = items.get(
            "item",
            []
        )


        if isinstance(
            page_items,
            dict
        ):

            page_items = [
                page_items
            ]


        if not page_items:

            break


        # ----------------------------------------------------
        # 데이터 추가
        # ----------------------------------------------------

        for item in page_items:

            stations.append({

                "city_code":
                    str(city_code),

                "city_name":
                    city_name,

                "node_id":
                    item.get(
                        "nodeid"
                    ),

                "node_name":
                    item.get(
                        "nodenm"
                    ),

                "node_no":
                    item.get(
                        "nodeno"
                    ),

                "latitude":
                    item.get(
                        "gpslati"
                    ),

                "longitude":
                    item.get(
                        "gpslong"
                    )
            })


        print(
            f"[{city_name}] "
            f"page={page_no} | "
            f"{len(stations)}/{total_count}"
        )


        # ----------------------------------------------------
        # 전체 수집 완료
        # ----------------------------------------------------

        if (
            total_count > 0
            and
            len(stations) >= total_count
        ):

            break


        # ----------------------------------------------------
        # 마지막 페이지
        # ----------------------------------------------------

        if len(page_items) < NUM_OF_ROWS:

            break


        page_no += 1


        time.sleep(
            REQUEST_DELAY
        )


    if total_count > 0 and len(stations) != total_count:
        raise RuntimeError(
            f"[{city_name}] 건수 불일치: API={total_count}, 실제={len(stations)}"
        )

    return stations


# ============================================================
# 중복 제거
#
# city_code + node_id
# ============================================================

def remove_duplicates(
    stations
):

    unique = {}


    for station in stations:

        city_code = station.get(
            "city_code"
        )

        node_id = station.get(
            "node_id"
        )


        if not node_id:

            continue


        key = (
            city_code,
            node_id
        )


        unique[key] = station


    return list(
        unique.values()
    )


# ============================================================
# CSV 저장
# ============================================================

def save_csv(
    stations
):

    fieldnames = [

        "city_code",

        "city_name",

        "node_id",

        "node_name",

        "node_no",

        "latitude",

        "longitude"
    ]


    temporary_file = OUTPUT_FILE.with_suffix(".tmp.csv")

    with open(
        temporary_file,
        "w",
        newline="",
        encoding="utf-8-sig"
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames
        )

        writer.writeheader()

        writer.writerows(
            stations
        )

    temporary_file.replace(OUTPUT_FILE)


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 60)

    print(
        "TAGO 전국 버스정류장 수집 시작"
    )

    print("=" * 60)


    print(
        "ENV 파일:",
        ENV_PATH
    )

    print(
        "BUSSTOP_API_KEY: 확인됨"
    )

    print(
        "서비스키 Decoding 처리: 완료"
    )


    # ========================================================
    # 1. 전국 도시코드
    # ========================================================

    cities = get_city_codes()


    print()

    print("=" * 60)

    print(
        "전국 버스정류장 수집"
    )

    print("=" * 60)


    all_stations = []


    # ========================================================
    # 2. 도시별 정류장 수집
    # ========================================================

    for index, city in enumerate(
        cities,
        start=1
    ):

        city_code = city.get(
            "citycode"
        )

        city_name = city.get(
            "cityname"
        )


        if not city_code:

            continue


        print()

        print(
            f"[{index}/{len(cities)}] "
            f"{city_name} "
            f"(cityCode={city_code})"
        )


        city_stations = (
            get_city_stations(
                city_code,
                city_name
            )
        )


        all_stations.extend(
            city_stations
        )


        print(
            f"→ {city_name}: "
            f"{len(city_stations)}개"
        )


        print(
            f"→ 전국 누적: "
            f"{len(all_stations)}개"
        )


        time.sleep(
            REQUEST_DELAY
        )


    # ========================================================
    # 3. 중복 제거
    # ========================================================

    if not all_stations:
        raise RuntimeError("전국 버스정류장 데이터가 0건입니다. 기존 파일을 유지합니다.")

    before_count = len(
        all_stations
    )


    all_stations = (
        remove_duplicates(
            all_stations
        )
    )


    after_count = len(
        all_stations
    )


    # ========================================================
    # 4. CSV 저장
    # ========================================================

    save_csv(
        all_stations
    )


    # ========================================================
    # 완료
    # ========================================================

    print()

    print("=" * 60)

    print(
        "TAGO 전국 버스정류장 수집 완료"
    )

    print("=" * 60)


    print(
        "수집 건수:",
        before_count
    )

    print(
        "중복 제거:",
        before_count - after_count
    )

    print(
        "최종 정류장:",
        after_count
    )

    print(
        "CSV:",
        OUTPUT_FILE
    )


# ============================================================
# 실행
# ============================================================

if __name__ == "__main__":

    main()
