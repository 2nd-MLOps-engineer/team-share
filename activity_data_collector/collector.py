"""기상청 + 에어코리아 + 전국체육시설 API 확인용 수집기.

DB를 만들지 않고 API 원문과 정규화 결과를 JSON/CSV로 저장한다.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
import urllib.parse
import urllib.robotparser
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    # python-dotenv가 설치되지 않은 환경에서도 수집기를 실행할 수 있도록
    # 간단한 .env 로더를 제공한다. 따옴표와 주석 정도만 처리한다.
    def load_dotenv(path: Path | str) -> None:
        env_path = Path(path)
        if not env_path.exists():
            return
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, value = line.split("=", 1)
            os.environ.setdefault(name.strip(), value.strip().strip('"').strip("'"))

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "output"
RAW = OUT / "raw"
TIMEOUT = 20
HEADERS = {"User-Agent": "ActivityDataCollector/0.1 (+public-data-test)"}


def key(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f".env에 {name}을(를) 입력하세요.")
    # 공공데이터포털에서 복사한 Encoding 키가 들어온 경우에도
    # requests params에서 이중 인코딩(%252B)이 발생하지 않도록 한 번만 복원한다.
    # 권장 값은 공공데이터포털의 일반 인증키(Decoding)이다.
    if "%" in value:
        value = urllib.parse.unquote(value)
    return value


def get_json(url: str, params: dict) -> dict:
    last_error = None
    for attempt in range(3):
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
            if response.status_code in (502, 503, 504) and attempt < 2:
                time.sleep(1.5 * (attempt + 1))
                continue
            response.raise_for_status()
            try:
                return response.json()
            except ValueError as exc:
                raise RuntimeError(f"JSON 응답이 아닙니다: {response.url}\n{response.text[:500]}") from exc
        except requests.RequestException as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(1.5 * (attempt + 1))
                continue
            raise
    raise last_error or RuntimeError("API 요청에 실패했습니다.")


def items_from_response(data: dict) -> list[dict]:
    node = data
    for name in ("response", "body", "items"):
        if isinstance(node, dict) and name in node:
            node = node[name]
    if isinstance(node, dict) and "item" in node:
        node = node["item"]
    if isinstance(node, dict):
        return [node]
    return node if isinstance(node, list) else []


def fetch_weather(nx: int, ny: int) -> dict:
    service_key = key("KMA_SERVICE_KEY")
    base = "https://apis.data.go.kr/1360000/VilageFcstInfoService_2.0"
    now = datetime.now().astimezone()
    base_date = now.strftime("%Y%m%d")
    # 단기예보 발표 시각은 정시 기준이다. 최근 발표 시각을 계산한다.
    hour = (now.hour // 3) * 3
    if now.hour < 2:
        base_date = (now.date()).strftime("%Y%m%d")
        hour = 23
    base_time = f"{hour:02d}00"
    common = {"serviceKey": service_key, "pageNo": 1, "numOfRows": 1000, "dataType": "JSON", "base_date": base_date, "base_time": base_time, "nx": nx, "ny": ny}
    ultra = get_json(f"{base}/getUltraSrtNcst", {**common, "base_time": f"{now.hour:02d}00"})
    forecast = get_json(f"{base}/getVilageFcst", common)
    return {"ultra_srt_ncst": ultra, "vilage_fcst": forecast, "request": {"nx": nx, "ny": ny, "base_date": base_date, "base_time": base_time}}


def fetch_air(station: str) -> dict:
    service_key = key("AIRKOREA_SERVICE_KEY")
    base = "https://apis.data.go.kr/B552584/ArpltnInforInqireSvc"
    # 최신 측정값만 필요하므로 큰 페이지를 요청하지 않는다. 100건 요청은
    # 게이트웨이에서 504가 날 수 있다.
    common = {"serviceKey": service_key, "returnType": "json", "numOfRows": 10, "pageNo": 1}
    # 측정소 목록 API는 별도 서비스 권한이 필요하다. 현재 기능은 PM10/PM2.5
    # 실측값만 필요하므로 목록 API를 호출하지 않고 실시간 측정 API만 사용한다.
    # 사용자가 '독산역'처럼 입력한 경우 '독산'과 행정구역 후보도 순서대로 시도한다.
    candidates = [station]
    simplified = station.replace("역", "").strip()
    if simplified and simplified not in candidates:
        candidates.append(simplified)
    for token in reversed(station.split()):
        if token.endswith(("구", "군", "시")) and token not in candidates:
            candidates.append(token)
    selected_station = station
    measurements = None
    last_error = None
    for candidate in candidates:
        try:
            response = get_json(
                f"{base}/getMsrstnAcctoRltmMesureDnsty",
                {**common, "stationName": candidate, "dataTerm": "DAILY", "ver": "1.3"},
            )
        except requests.RequestException as exc:
            last_error = exc
            continue
        measurements = response
        if items_from_response(response):
            selected_station = candidate
            break
    if measurements is None and last_error:
        raise last_error
    return {"station_list": None, "measurements": measurements, "request_station": selected_station}


def fetch_air_nearby(sido: str, district: str) -> dict:
    """시도별 실시간 목록에서 현재 위치의 행정구역에 가까운 측정소를 자동 선택한다."""
    # 에어코리아는 구 단위 측정소명이 대부분 행정구역명과 같으므로
    # 먼저 직접 조회한다. 이 요청이 가장 빠르고 안정적이다.
    direct_error = None
    if district:
        try:
            direct = fetch_air(district)
            direct_items = items_from_response(direct.get("measurements", {}))
            if direct_items:
                direct["source"] = "행정구역 대표 측정소 자동 선택"
                return direct
        except requests.RequestException as exc:
            direct_error = exc
    service_key = key("AIRKOREA_SERVICE_KEY")
    sido = sido.replace("특별자치도", "").replace("특별시", "").replace("광역시", "").replace("자치시", "").strip()
    common = {"serviceKey": service_key, "returnType": "json", "numOfRows": 10, "pageNo": 1}
    try:
        data = get_json(
            "https://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty",
            {**common, "sidoName": sido, "ver": "1.3"},
        )
    except requests.RequestException:
        if direct_error:
            raise direct_error
        raise
    rows = items_from_response(data)
    selected = None
    for row in rows:
        if row.get("stationName") == district or district in str(row.get("stationName", "")):
            selected = row
            break
    if selected is None and rows:
        selected = rows[0]
    return {
        "station_list": None,
        "measurements": {"response": {"body": {"items": [selected] if selected else []}}},
        "request_station": selected.get("stationName") if selected else None,
        "source": "시도별 실시간 측정정보",
    }


def fetch_air_nearest(lat: float, lng: float, sido: str, district: str) -> dict:
    """좌표 기준 근접 측정소를 우선 사용하고, 권한/좌표 변환 실패 시 지역 방식으로 대체한다."""
    try:
        from pyproj import Transformer
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:2097", always_xy=True)
        tm_x, tm_y = transformer.transform(lng, lat)
        service_key = key("AIRKOREA_SERVICE_KEY")
        nearby = get_json(
            "https://apis.data.go.kr/B552584/MsrstnInfoInqireSvc/getNearbyMsrstnList",
            {"serviceKey": service_key, "returnType": "json", "numOfRows": 10, "pageNo": 1, "tmX": tm_x, "tmY": tm_y},
        )
        rows = items_from_response(nearby)
        if rows:
            station = first(rows[0], "stationName")
            if station:
                result = fetch_air(station)
                result["source"] = "좌표 기준 근접 측정소 자동 선택"
                result["distance_km"] = rows[0].get("tm")
                return result
    except (ImportError, requests.RequestException, RuntimeError, ValueError, TypeError):
        pass
    return fetch_air_nearby(sido, district)


def fetch_facilities(region: str, limit: int) -> dict:
    url = os.getenv("FACILITY_API_URL", "").strip()
    if not url:
        raise RuntimeError(".env에 FACILITY_API_URL을 입력하세요.")
    parts = [part for part in region.split() if part]
    sido = parts[0] if parts else region
    sigungu = parts[-1] if len(parts) > 1 else ""
    # 이 API의 응답 형식 파라미터명은 type이 아니라 resultType이다.
    params = {
        "serviceKey": key("FACILITY_SERVICE_KEY"),
        "pageNo": 1,
        "numOfRows": limit,
        "resultType": "json",
        "sidoNm": sido,
    }
    if sigungu:
        params["sigunguNm"] = sigungu
    data = get_json(url, params)
    # API가 지역 파라미터를 무시하는 경우가 있어 응답 원문 기준으로 한 번 더 필터링한다.
    rows = items_from_response(data)
    if sigungu:
        rows = [
            row for row in rows
            if row.get("addr_ctpv_nm") == sido
            and (row.get("addr_cpb_nm") == sigungu or sigungu in str(row.get("addr_cpb_nm", "")))
        ]
    if isinstance(data.get("response"), dict) and isinstance(data["response"].get("body"), dict):
        data["response"]["body"]["items"] = {"item": rows, "totalCount": len(rows)}
    return {"data": data, "request_url": url, "request_region": region}


def first(row: dict, *names: str) -> str:
    for name in names:
        if row.get(name) not in (None, ""):
            return str(row[name])
    return ""


def normalize_facilities(data: dict) -> list[dict]:
    result = []
    for row in items_from_response(data["data"]):
        result.append({
            "facility_name": first(row, "faciNm", "시설명", "체육시설명", "facilityName"),
            "facility_type": first(row, "ftype_nm", "faci_gb_nm", "ftypeNm", "시설유형명", "시설구분명", "업종명"),
            "address": first(row, "faciRoadAddr", "faciRoadAddr1", "도로명주소", "소재지도로명주소", "faciAddr"),
            "city": first(row, "cpNm", "시도명", "sidoNm"),
            "district": first(row, "cpbNm", "시군구명", "sigunguNm"),
            "phone": first(row, "faciTel", "전화번호", "연락처"),
            "homepage": first(row, "faciHomePage", "홈페이지", "homepage", "url"),
            "latitude": first(row, "faci_lat", "faciPointY", "위도", "lat"),
            "longitude": first(row, "faci_lot", "faciPointX", "경도", "lon", "lng"),
            "indoor_outdoor": first(row, "inout_gbn_nm", "실내외구분", "indoorOutdoor"),
            "source_row": row,
        })
    return result


def web_evidence(url: str) -> dict:
    if not url:
        return {"status": "no_homepage"}
    if not urllib.parse.urlparse(url).scheme:
        url = "https://" + url
    parsed = urllib.parse.urlparse(url)
    robots = urllib.robotparser.RobotFileParser(f"{parsed.scheme}://{parsed.netloc}/robots.txt")
    try:
        robots.read()
        if not robots.can_fetch(HEADERS["User-Agent"], url):
            return {"status": "blocked_by_robots", "url": url}
    except Exception:
        return {"status": "robots_unavailable", "url": url}
    try:
        response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        response.raise_for_status()
        text = BeautifulSoup(response.text, "html.parser").get_text(" ", strip=True)
        patterns = r"휴관|휴무|운영시간|운영 시간|공사|점검|임시|이용제한|이용 제한|폐관|예약"
        hits = re.findall(r"[^.!?\n]{0,80}(?:" + patterns + r")[^.!?\n]{0,120}", text, flags=re.I)
        return {"status": "ok", "url": response.url, "http_status": response.status_code, "evidence": hits[:20]}
    except requests.RequestException as exc:
        return {"status": "request_failed", "url": url, "error": str(exc)}


def save_results(payload: dict, facilities: list[dict]) -> None:
    OUT.mkdir(exist_ok=True)
    RAW.mkdir(exist_ok=True)
    for name, value in payload["raw"].items():
        (RAW / f"{name}.json").write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    (OUT / "latest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    normalized = [{k: row.get(k, "") for k in [
        "facility_name", "facility_type", "address", "city", "district",
        "phone", "homepage", "latitude", "longitude", "web_status", "web_evidence",
    ]} for row in facilities]
    (OUT / "facilities_normalized.json").write_text(
        json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    fields = ["facility_name", "facility_type", "address", "city", "district", "phone", "homepage", "latitude", "longitude", "web_status", "web_evidence"]
    with (OUT / "facilities.csv").open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in facilities:
            writer.writerow({field: row.get(field, "") for field in fields})


def main() -> None:
    load_dotenv(ROOT / ".env")
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", required=True, help="검색 지역명")
    parser.add_argument("--station", default=os.getenv("AIRKOREA_STATION", ""), help="에어코리아 측정소명")
    parser.add_argument("--nx", type=int, default=int(os.getenv("KMA_NX", "60")))
    parser.add_argument("--ny", type=int, default=int(os.getenv("KMA_NY", "127")))
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--enrich-web", action="store_true")
    args = parser.parse_args()
    started = datetime.now(timezone.utc).isoformat()
    raw = {}
    errors = {}
    for name, fn in [("facilities", lambda: fetch_facilities(args.region, args.limit)), ("weather", lambda: fetch_weather(args.nx, args.ny)), ("air", lambda: fetch_air(args.station or args.region))]:
        try:
            raw[name] = fn()
        except Exception as exc:
            errors[name] = str(exc)
            raw[name] = {"error": str(exc)}
    facilities = normalize_facilities(raw["facilities"]) if "error" not in raw["facilities"] else []
    for row in facilities:
        row["web_status"] = "not_requested"
        row["web_evidence"] = ""
        if args.enrich_web and row["homepage"]:
            evidence = web_evidence(row["homepage"])
            row["web_status"] = evidence.get("status", "")
            row["web_evidence"] = " | ".join(evidence.get("evidence", []))
            time.sleep(0.4)
    payload = {"collected_at": started, "region": args.region, "raw": raw, "facilities": facilities, "errors": errors}
    save_results(payload, facilities)
    print(json.dumps({"facility_count": len(facilities), "errors": errors, "json": str(OUT / "latest.json"), "csv": str(OUT / "facilities.csv")}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
