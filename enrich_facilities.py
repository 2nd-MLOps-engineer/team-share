"""빈 시설 데이터를 확인하고 외부 공개 정보로 보완하는 별도 명령.

기본값은 리포트만 생성한다. 실제 DB 반영은 명시적으로 --apply를 붙였을 때만 수행한다.
"""
from __future__ import annotations

import csv
import json
import os
import urllib.parse
import urllib.robotparser
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from django.core.management.base import BaseCommand
from django.db import connection, transaction

HEADERS = {"User-Agent": "ActivityDataCollector/0.2 (+facility-enrichment-report)"}
TARGET_FIELDS = {
    "ftype_nm": "시설 유형",
    "fcob_nm": "시설 종목",
    "inout_gbn_nm": "실내외 구분",
    "faci_road_addr": "도로명 주소",
    "faci_addr": "지번 주소",
    "faci_gfa": "시설 면적",
}


def _blank(value):
    return value is None or not str(value).strip()


def _load_env():
    env_path = Path(".env")
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            name, value = line.split("=", 1)
            os.environ.setdefault(name.strip(), value.strip().strip('"').strip("'"))


def _query_kakao(name: str, address: str, key: str) -> dict:
    query = " ".join(part for part in (name, address) if part).strip()
    if not query or not key:
        return {}
    try:
        response = requests.get(
            "https://dapi.kakao.com/v2/local/search/keyword.json",
            headers={"Authorization": f"KakaoAK {key}"},
            params={"query": query, "size": 5, "sort": "accuracy"},
            timeout=5,
        )
        response.raise_for_status()
        documents = response.json().get("documents") or []
        if not documents:
            return {}
        item = documents[0]
        return {
            "place_name": item.get("place_name", ""),
            "category_name": item.get("category_name", ""),
            "road_address_name": item.get("road_address_name", ""),
            "address_name": item.get("address_name", ""),
            "latitude": item.get("y", ""),
            "longitude": item.get("x", ""),
            "place_url": item.get("place_url", ""),
        }
    except (requests.RequestException, ValueError, TypeError):
        return {}


def _crawl_evidence(url: str) -> dict:
    """robots.txt를 확인한 뒤 공개 페이지에서 운영 관련 문구만 추출한다."""
    if not url:
        return {"status": "no_url", "evidence": []}
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return {"status": "invalid_url", "evidence": []}
    try:
        robots = urllib.robotparser.RobotFileParser(f"{parsed.scheme}://{parsed.netloc}/robots.txt")
        robots.read()
        if not robots.can_fetch(HEADERS["User-Agent"], url):
            return {"status": "blocked_by_robots", "url": url, "evidence": []}
        response = requests.get(url, headers=HEADERS, timeout=8)
        response.raise_for_status()
        text = BeautifulSoup(response.text, "html.parser").get_text(" ", strip=True)
        pattern = r"[^.!?\n]{0,80}(?:휴관|휴무|운영시간|운영 시간|공사|점검|임시|이용제한|이용 제한|예약)[^.!?\n]{0,120}"
        evidence = __import__("re").findall(pattern, text, flags=__import__("re").I)
        return {"status": "ok", "url": response.url, "http_status": response.status_code, "evidence": evidence[:20]}
    except requests.RequestException as exc:
        return {"status": "request_failed", "url": url, "error": str(exc), "evidence": []}


class Command(BaseCommand):
    help = "facility_processed의 빈 필드를 외부 공개 정보로 확인하고 리포트를 생성합니다."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=100, help="확인할 빈 데이터 행 수")
        parser.add_argument("--apply", action="store_true", help="확인된 값 중 안전한 항목만 DB에 반영")
        parser.add_argument("--no-web", action="store_true", help="웹 페이지 확인을 생략")

    def handle(self, *args, **options):
        _load_env()
        key = os.getenv("KAKAO_REST_API_KEY", "").strip()
        limit = max(1, min(options["limit"], 5000))
        sql = """
            SELECT faci_cd, faci_nm, ftype_nm, fcob_nm, inout_gbn_nm,
                   faci_road_addr, faci_addr, faci_lat, faci_lot, faci_gfa
            FROM facility_processed
            WHERE COALESCE(TRIM(ftype_nm), '') = ''
               OR COALESCE(TRIM(fcob_nm), '') = ''
               OR COALESCE(TRIM(inout_gbn_nm), '') = ''
               OR COALESCE(TRIM(faci_road_addr), '') = ''
               OR COALESCE(TRIM(faci_addr), '') = ''
               OR COALESCE(TRIM(faci_gfa), '') = ''
            ORDER BY faci_cd
            LIMIT %s
        """
        with connection.cursor() as cursor:
            cursor.execute(sql, [limit])
            columns = [item[0] for item in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        report = []
        for row in rows:
            address = row.get("faci_road_addr") or row.get("faci_addr") or ""
            kakao = _query_kakao(row.get("faci_nm", ""), address, key)
            evidence = {} if options["no_web"] else _crawl_evidence(kakao.get("place_url", ""))
            suggested = {}
            if _blank(row.get("faci_road_addr")) and kakao.get("road_address_name"):
                suggested["faci_road_addr"] = kakao["road_address_name"]
            if _blank(row.get("faci_addr")) and kakao.get("address_name"):
                suggested["faci_addr"] = kakao["address_name"]
            if _blank(row.get("ftype_nm")) and kakao.get("category_name"):
                suggested["ftype_nm"] = kakao["category_name"].split(" > ")[-1]
            report.append({
                "faci_cd": row.get("faci_cd"),
                "facility_name": row.get("faci_nm"),
                "blank_fields": [field for field in TARGET_FIELDS if _blank(row.get(field))],
                "suggested_fields": suggested,
                "kakao_result": kakao,
                "web_evidence": evidence,
            })

        output_dir = Path("output") / "facility_enrichment"
        output_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        json_path = output_dir / f"report_{stamp}.json"
        csv_path = output_dir / f"report_{stamp}.csv"
        json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        with csv_path.open("w", encoding="utf-8-sig", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=["faci_cd", "facility_name", "blank_fields", "suggested_fields", "web_status"])
            writer.writeheader()
            for item in report:
                writer.writerow({
                    "faci_cd": item["faci_cd"],
                    "facility_name": item["facility_name"],
                    "blank_fields": ", ".join(item["blank_fields"]),
                    "suggested_fields": json.dumps(item["suggested_fields"], ensure_ascii=False),
                    "web_status": item["web_evidence"].get("status", "not_requested"),
                })

        applied = 0
        if options["apply"]:
            with transaction.atomic(), connection.cursor() as cursor:
                for item in report:
                    updates = item["suggested_fields"]
                    evidence = item["web_evidence"]
                    if not updates and not evidence.get("evidence"):
                        continue
                    assignments = []
                    values = []
                    for field, value in updates.items():
                        assignments.append(f"{field} = %s")
                        values.append(value)
                    enrichment_payload = {
                        "fields": list(updates),
                        "web_evidence": evidence,
                    }
                    assignments.extend(["enriched_fields = %s", "enriched_at = NOW()", "enrichment_status = %s"])
                    values.extend([json.dumps(enrichment_payload, ensure_ascii=False), "applied"])
                    values.append(item["faci_cd"])
                    cursor.execute(
                        f"UPDATE facility_processed SET {', '.join(assignments)} WHERE faci_cd = %s",
                        values,
                    )
                    applied += cursor.rowcount

        self.stdout.write(self.style.SUCCESS(json.dumps({
            "checked": len(report),
            "applied": applied,
            "json": str(json_path),
            "csv": str(csv_path),
            "mode": "apply" if options["apply"] else "report-only",
        }, ensure_ascii=False, indent=2)))
