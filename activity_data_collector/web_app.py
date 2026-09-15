"""로그인 지역 기반 운동 추천 데모 서버.

첫 페이지에서 로그인 프로필의 지역을 받아 추천 데이터를 미리 계산하고,
두 번째 페이지는 지역별 JSON 캐시를 즉시 표시한다. DB와 현재 위치는 사용하지 않는다.
"""
from __future__ import annotations

import csv
import json
import math
import os
import re
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from flask import Flask, jsonify, redirect, request, send_file

import collector

ROOT = Path(__file__).resolve().parent
CACHE_DIR = ROOT / "output" / "recommend_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
app = Flask(__name__)


def load_env() -> None:
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            name, value = line.split("=", 1)
            os.environ.setdefault(name.strip(), value.strip().strip('"').strip("'"))


load_env()


def normalize_region(region: str) -> str:
    """로그인 입력값을 API/백업 데이터의 시도·시군구 형식으로 맞춘다."""
    value = " ".join(region.split())
    aliases = {
        "수원": "경기도 수원시", "수원시": "경기도 수원시",
        "용인": "경기도 용인시", "용인시": "경기도 용인시",
        "성남": "경기도 성남시", "성남시": "경기도 성남시",
        "고양": "경기도 고양시", "고양시": "경기도 고양시",
    }
    if value in aliases:
        return aliases[value]
    if value.startswith(("경기 ", "경기도 ")) and not value.endswith("시"):
        return value + "시"
    return value


def cache_path(region: str) -> Path:
    key = re.sub(r"[^0-9A-Za-z가-힣_-]+", "_", region).strip("_") or "region"
    return CACHE_DIR / f"{key}.json"


def coord_for_region(region: str) -> tuple[float, float] | None:
    key = os.getenv("KAKAO_REST_API_KEY", "").strip()
    if not key:
        return None
    response = requests.get(
        "https://dapi.kakao.com/v2/local/search/address.json",
        headers={"Authorization": f"KakaoAK {key}"},
        params={"query": region, "size": 1}, timeout=8,
    )
    response.raise_for_status()
    row = (response.json().get("documents") or [None])[0]
    if not row:
        return None
    return float(row["y"]), float(row["x"])


def distance_km(a_lat: float, a_lng: float, b_lat: float, b_lng: float) -> float:
    p1, p2 = math.radians(a_lat), math.radians(b_lat)
    dp, dl = math.radians(b_lat - a_lat), math.radians(b_lng - a_lng)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 6371 * 2 * math.atan2(math.sqrt(h), math.sqrt(1 - h))


def environment(row: dict) -> str:
    text = f"{row.get('facility_name', '')} {row.get('facility_type', '')}".lower()
    if any(x in text for x in ("운동장", "공원", "야구장", "축구장", "풋살장", "테니스장", "골프장", "야외")):
        return "outdoor"
    if any(x in text for x in ("체육관", "체력단련장", "헬스", "수영", "볼링", "탁구", "당구", "요가", "필라테스", "태권도", "댄스", "실내")):
        return "indoor"
    return "unknown"


def facilities_from_backup(region: str, origin: tuple[float, float] | None) -> list[dict]:
    parts = region.split()
    province, district = parts[0], parts[-1]
    path = ROOT / "db_backup" / "facility_processed.csv"
    if not path.exists():
        return []
    rows = []
    with path.open(encoding="utf-8-sig", newline="") as file:
        for raw in csv.DictReader(file):
            if raw.get("cp_nm") != province or district not in (raw.get("cpb_nm") or ""):
                continue
            try:
                lat, lng = float(raw.get("faci_lat", "")), float(raw.get("faci_lot", ""))
                if not (-90 < lat < 90 and -180 < lng < 180):
                    continue
            except (TypeError, ValueError):
                continue
            row = {
                "facility_name": raw.get("faci_nm") or "시설명 미등록",
                "facility_type": raw.get("ftype_nm") or raw.get("fcob_nm") or "",
                "address": raw.get("faci_road_addr") or raw.get("faci_addr") or f"{province} {district}",
                "latitude": lat, "longitude": lng, "source": "DB 백업 데이터",
            }
            row["environment"] = environment(row)
            if origin:
                row["distance_km"] = round(distance_km(*origin, lat, lng), 3)
            rows.append(row)
    return rows


def build_recommendation(region: str) -> dict:
    region = normalize_region(region)
    parts = region.split()
    sido, district = (parts[0], parts[-1]) if parts else ("", "")
    origin = coord_for_region(region)
    # 서로 독립적인 API를 병렬 호출해 로그인 대기시간을 줄인다.
    with ThreadPoolExecutor(max_workers=2) as pool:
        weather_job = pool.submit(collector.fetch_weather, int(os.getenv("KMA_NX", "60")), int(os.getenv("KMA_NY", "121")))
        air_job = pool.submit(collector.fetch_air_nearby, sido, district)
        try: weather = weather_job.result()
        except Exception as exc: weather = {"error": str(exc)}
        try: air = air_job.result()
        except Exception as exc: air = {"error": str(exc)}
    facilities = facilities_from_backup(region, origin)
    if not facilities:
        try:
            facilities = collector.normalize_facilities(collector.fetch_facilities(region, 50))[0:50]
        except Exception:
            facilities = []
    measurements = collector.items_from_response(air.get("measurements", {})) if isinstance(air, dict) else []
    pm10 = float(measurements[0].get("pm10Value") or 0) if measurements else 0
    pm25 = float(measurements[0].get("pm25Value") or 0) if measurements else 0
    bad_air = pm10 >= 81 or pm25 >= 36
    preferred = "indoor" if bad_air else "outdoor"
    preferred_rows = [x for x in facilities if x.get("environment") == preferred]
    ordered = preferred_rows or facilities
    ordered.sort(key=lambda x: x.get("distance_km", 999999))
    return {"region": region, "weather": weather, "air": air, "facilities": ordered[:30],
            "recommendation": "실내 시설 우선" if bad_air else "실외 시설 우선",
            "recommendation_reason": "PM10/PM2.5가 높아 실내를 우선 추천합니다." if bad_air else "대기질이 양호해 실외를 우선 추천합니다."}


@app.get("/")
def home(): return send_file(ROOT / "index (2).html")


@app.get("/login")
def login(): return send_file(ROOT / "login-독립실행.html")


@app.get("/recommend")
def recommend(): return send_file(ROOT / "recommend.html")


@app.get("/api/precompute")
def precompute():
    region = normalize_region((request.args.get("region") or "").strip())
    if not region: return jsonify({"error": "지역이 필요합니다."}), 400
    data = build_recommendation(region)
    cache_path(region).write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return jsonify({"ok": True, "region": region})


@app.get("/api/recommendation")
def recommendation():
    region = normalize_region((request.args.get("region") or "").strip())
    path = cache_path(region)
    if not path.exists():
        return jsonify({"error": "사전 계산 결과가 없습니다."}), 404
    return jsonify(json.loads(path.read_text(encoding="utf-8")))


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5050, debug=False)
