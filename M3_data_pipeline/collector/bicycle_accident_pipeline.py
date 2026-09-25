from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus, unquote
import hashlib
import json
import math
import os
import re
import shutil
import time
import xml.etree.ElementTree as ET

import pandas as pd
import requests
from dotenv import load_dotenv
from openpyxl import load_workbook
from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    Integer,
    Text,
    create_engine,
    text,
)


# ============================================================
# 1. PATH / ENV
# ============================================================

CURRENT_DIR = Path(__file__).resolve().parent

# 이 파일을 M3_data_pipeline/collector 안에 두는 현재 프로젝트 구조 기준
PROJECT_ROOT = (
    Path(__file__).resolve().parents[2]
    if len(Path(__file__).resolve().parents) >= 3
    else CURRENT_DIR
)

ENV_PATH = CURRENT_DIR / ".env"

if ENV_PATH.exists():
    load_dotenv(ENV_PATH, override=True)


def env_path(name: str, default: Path) -> Path:
    value = os.getenv(name)
    return Path(value).expanduser().resolve() if value else default.resolve()


CODELIST_PATH = env_path(
    "BICYCLE_CODELIST_PATH",
    CURRENT_DIR / "AccidentHazard_CodeList.xlsx",
)

RAW_DIR = env_path(
    "BICYCLE_RAW_DIR",
    PROJECT_ROOT / "data" / "raw" / "koroad_bicycle",
)

CANONICAL_RAW_CSV = RAW_DIR / "koroad_bicycle_accident_hotspots.csv"
CHECKPOINT_PATH = RAW_DIR / "koroad_bicycle_checkpoint.jsonl"


# ============================================================
# 2. API / COLLECTION SETTINGS
# ============================================================

API_URL = (
    "https://apis.data.go.kr/B552061/"
    "frequentzoneBicycle/getRestFrequentzoneBicycle"
)

RAW_API_KEY = os.getenv("BICYCLE_ACCIDENT_API_KEY", "").strip()

# 포털에서 URL Encode 상태로 준 키를 한 번 복원하고,
# requests가 query string을 만들 때 한 번만 인코딩하게 한다.
API_KEY = unquote(RAW_API_KEY)

MIN_YEAR = 2012
MAX_YEAR = 2024
NUM_OF_ROWS = 100
MAX_RETRIES = 3
RETRY_WAIT_SECONDS = (3, 5)
REQUEST_TIMEOUT = (10, 45)
REQUEST_DELAY_SECONDS = 0.05

RAW_SCHEMA = "raw"
PROCESSED_SCHEMA = "processed"
TABLE_NAME = "koroad_bicycle_accident_hotspots"

# EPSG:4326 기준 대한민국 주변의 보수적인 경계
KOREA_MIN_LONGITUDE = 124.0
KOREA_MAX_LONGITUDE = 132.0
KOREA_MIN_LATITUDE = 32.0
KOREA_MAX_LATITUDE = 39.5

API_COLUMNS = [
    "afos_fid",
    "afos_id",
    "bjd_cd",
    "spot_cd",
    "sido_sgg_nm",
    "spot_nm",
    "occrrnc_cnt",
    "caslt_cnt",
    "dth_dnv_cnt",
    "se_dnv_cnt",
    "sl_dnv_cnt",
    "wnd_dnv_cnt",
    "lo_crd",
    "la_crd",
    "geom_json",
]

REQUEST_METADATA_COLUMNS = [
    "request_year",
    "request_sido",
    "request_gugun",
    "request_province_name",
    "request_district_name",
    "expected_afos_id",
    "request_page_no",
    "collected_at",
]

NULL_TEXT_VALUES = {
    "",
    "null",
    "none",
    "nan",
    "n/a",
    "na",
    "-",
}


# ============================================================
# 3. EXCEPTIONS
# ============================================================


class PipelineError(RuntimeError):
    pass


class TransientApiError(PipelineError):
    """재시도할 수 있는 일시적 API 오류."""


class PermanentApiError(PipelineError):
    """재시도해도 해결되지 않는 요청/응답 오류."""


class UnsupportedScopeError(PermanentApiError):
    """해당 연도에 존재하지 않거나 지원되지 않는 행정구역 조합."""


class GlobalServiceError(PermanentApiError):
    """인증키, 사용승인 또는 호출 한도와 관련된 전역 서비스 오류."""


# ============================================================
# 4. CODE LIST
# ============================================================


def normalize_name(value) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def name_key(value) -> str:
    return re.sub(r"\s+", "", normalize_name(value))


def load_request_scopes(code_list_path: Path):
    if not code_list_path.exists():
        raise FileNotFoundError(
            "요청변수 코드표를 찾을 수 없습니다.\n"
            f"확인 경로: {code_list_path}"
        )

    workbook = load_workbook(
        code_list_path,
        read_only=False,
        data_only=True,
    )

    required_sheets = {
        "serachYearCd 요청값",
        "Sido 요청값",
        "Gugun 요청값",
    }
    missing_sheets = required_sheets.difference(workbook.sheetnames)

    if missing_sheets:
        raise PipelineError(
            "코드표 필수 시트가 없습니다: "
            + ", ".join(sorted(missing_sheets))
        )

    year_sheet = workbook["serachYearCd 요청값"]
    sido_sheet = workbook["Sido 요청값"]
    gugun_sheet = workbook["Gugun 요청값"]

    sido_by_name: dict[str, str] = {}

    for row in range(2, sido_sheet.max_row + 1):
        sido_name = sido_sheet.cell(row, 1).value
        sido_code = sido_sheet.cell(row, 2).value

        if sido_name is None or sido_code is None:
            continue

        sido_by_name[name_key(sido_name)] = f"{int(sido_code):02d}"

    afos_id_by_year: dict[int, str] = {}
    current_category = ""

    for row in range(2, year_sheet.max_row + 1):
        category = year_sheet.cell(row, 1).value
        label = year_sheet.cell(row, 2).value
        dataset_id = year_sheet.cell(row, 3).value

        if category is not None:
            current_category = normalize_name(category)

        if current_category != "자전거 교통사고 다발지역":
            continue

        match = re.match(r"(\d{2})년", normalize_name(label))

        if not match or dataset_id is None:
            continue

        year = 2000 + int(match.group(1))

        if MIN_YEAR <= year <= MAX_YEAR:
            afos_id_by_year[year] = str(int(dataset_id))

    expected_years = list(range(MIN_YEAR, MAX_YEAR + 1))

    if sorted(afos_id_by_year) != expected_years:
        raise PipelineError(
            "자전거 제공연도 코드가 2012~2024와 일치하지 않습니다."
        )

    gugun_by_province: dict[str, list[dict[str, str]]] = defaultdict(list)
    current_province = None

    for row in range(2, gugun_sheet.max_row + 1):
        province = gugun_sheet.cell(row, 1).value
        district = gugun_sheet.cell(row, 2).value
        gugun_code = gugun_sheet.cell(row, 3).value

        if province is not None:
            current_province = normalize_name(province)

        if district is None or gugun_code is None:
            continue

        if current_province is None:
            raise PipelineError(
                f"Gugun 요청값 {row}행에 시도 구분이 없습니다."
            )

        gugun_by_province[current_province].append(
            {
                "district_name": normalize_name(district),
                "guGun": f"{int(gugun_code):03d}",
            }
        )

    def resolve_sido(year: int, province: str) -> str:
        province_key = name_key(province)

        if province_key == name_key("강원특별자치도"):
            source_name = (
                "강원도(구)" if year <= 2022 else "강원특별자치도"
            )
            return sido_by_name[name_key(source_name)]

        if province_key == name_key("전북특별자치도"):
            source_name = (
                "전라북도(구)" if year <= 2022 else "전북특별자치도"
            )
            return sido_by_name[name_key(source_name)]

        if province_key not in sido_by_name:
            raise PipelineError(
                f"시도 코드 매핑을 찾을 수 없습니다: {province}"
            )

        return sido_by_name[province_key]

    scopes = []

    for year in expected_years:
        for province, districts in gugun_by_province.items():
            sido_code = resolve_sido(year, province)

            # 공식 제공기간 2012~2024에서는 구 광주/전남 코드(29/46)를 사용한다.
            if sido_code == "12":
                continue

            for district in districts:
                scopes.append(
                    {
                        "searchYearCd": str(year),
                        "siDo": sido_code,
                        "guGun": district["guGun"],
                        "province_name": province,
                        "district_name": district["district_name"],
                        "expected_afos_id": afos_id_by_year[year],
                    }
                )

    if len(scopes) != 3510:
        raise PipelineError(
            "예상 요청범위 3,510개와 다릅니다: "
            f"{len(scopes):,}개"
        )

    return scopes, afos_id_by_year


# ============================================================
# 5. API RESPONSE PARSING
# ============================================================


def safe_response_preview(response: requests.Response, limit: int = 500) -> str:
    preview = response.text[:limit]

    for secret in (RAW_API_KEY, API_KEY):
        if secret:
            preview = preview.replace(secret, "[API_KEY]")

    return repr(preview)


def normalize_items(items_container) -> list[dict]:
    if not items_container:
        return []

    if isinstance(items_container, dict):
        items = items_container.get("item", [])
    else:
        items = items_container

    if not items:
        return []

    if isinstance(items, dict):
        items = [items]

    if not isinstance(items, list):
        raise PermanentApiError(
            "API items.item 구조가 예상과 다릅니다."
        )

    if not all(isinstance(item, dict) for item in items):
        raise PermanentApiError(
            "API item 중 객체가 아닌 값이 있습니다."
        )

    return items


def parse_json_payload(data: dict) -> dict:
    # KoROAD 현재 형식은 최상위에 resultCode/items/totalCount가 있다.
    # response/header/body 형식도 방어적으로 지원한다.
    if isinstance(data.get("response"), dict):
        response_data = data["response"]
        header = response_data.get("header", {})
        body = response_data.get("body", {})

        result_code = header.get("resultCode")
        result_msg = header.get("resultMsg")
        total_count = body.get("totalCount", 0)
        num_of_rows = body.get("numOfRows")
        page_no = body.get("pageNo")
        items = normalize_items(body.get("items"))
    else:
        result_code = data.get("resultCode")
        result_msg = data.get("resultMsg")
        total_count = data.get("totalCount", 0)
        num_of_rows = data.get("numOfRows")
        page_no = data.get("pageNo")
        items = normalize_items(data.get("items"))

    return {
        "result_code": (
            "" if result_code is None else str(result_code).strip()
        ),
        "result_msg": (
            "" if result_msg is None else str(result_msg).strip()
        ),
        "total_count": total_count,
        "num_of_rows": num_of_rows,
        "page_no": page_no,
        "items": items,
    }


def parse_xml_payload(xml_text: str) -> dict:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise TransientApiError(
            "API 응답을 JSON 또는 XML로 해석할 수 없습니다."
        ) from exc

    items = []

    for item_node in root.findall(".//items/item"):
        item = {}

        for child in list(item_node):
            item[child.tag] = child.text

        items.append(item)

    return {
        "result_code": str(root.findtext(".//resultCode") or "").strip(),
        "result_msg": str(root.findtext(".//resultMsg") or "").strip(),
        "total_count": root.findtext(".//totalCount") or 0,
        "num_of_rows": root.findtext(".//numOfRows"),
        "page_no": root.findtext(".//pageNo"),
        "items": items,
    }


def parse_api_response(response: requests.Response) -> dict:
    try:
        data = response.json()
    except ValueError:
        return parse_xml_payload(response.text)

    if not isinstance(data, dict):
        raise TransientApiError(
            "API JSON 최상위 구조가 객체가 아닙니다."
        )

    return parse_json_payload(data)


def as_nonnegative_int(value, field_name: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise PermanentApiError(
            f"{field_name}를 정수로 변환할 수 없습니다: {value!r}"
        ) from exc

    if result < 0:
        raise PermanentApiError(
            f"{field_name}가 음수입니다: {result}"
        )

    return result


# ============================================================
# 6. API REQUEST / RETRY / PAGINATION
# ============================================================


def request_page_once(
    session: requests.Session,
    scope: dict,
    page_no: int,
) -> dict:
    params = {
        "serviceKey": API_KEY,
        "searchYearCd": scope["searchYearCd"],
        "siDo": scope["siDo"],
        "guGun": scope["guGun"],
        "type": "json",
        "numOfRows": str(NUM_OF_ROWS),
        "pageNo": str(page_no),
    }

    try:
        response = session.get(
            API_URL,
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
    except (requests.Timeout, requests.ConnectionError) as exc:
        raise TransientApiError(str(exc)) from exc
    except requests.RequestException as exc:
        raise TransientApiError(str(exc)) from exc

    if response.status_code == 429 or response.status_code >= 500:
        raise TransientApiError(
            f"HTTP {response.status_code}: "
            f"{safe_response_preview(response)}"
        )

    if response.status_code != 200:
        raise PermanentApiError(
            f"HTTP {response.status_code}: "
            f"{safe_response_preview(response)}"
        )

    parsed = parse_api_response(response)
    result_code = parsed["result_code"]
    result_msg = parsed["result_msg"]

    if result_code == "03":
        parsed["total_count"] = 0
        parsed["items"] = []
        return parsed

    if result_code == "99":
        raise TransientApiError(
            f"API 오류 99: {result_msg}"
        )

    # 코드표는 폐지·신설 행정구역을 모두 포함한다. 인증키와 기본 요청이
    # 정상임을 사전 점검한 뒤에는, 개별 연도에 유효하지 않은 조합에서
    # 반환되는 10을 수집 전체의 장애로 보지 않고 별도로 건너뛴다.
    if result_code == "10":
        raise UnsupportedScopeError(
            f"지원되지 않는 연도/행정구역 조합: {result_msg}"
        )

    if result_code in {"30", "31", "32"}:
        raise GlobalServiceError(
            f"API 전역 서비스 오류 {result_code}: {result_msg}"
        )

    if result_code not in {"00", "0", "0000"}:
        raise PermanentApiError(
            f"API 오류 {result_code or '[없음]'}: {result_msg}"
        )

    parsed["total_count"] = as_nonnegative_int(
        parsed["total_count"],
        "totalCount",
    )
    return parsed


def request_page_with_retry(
    session: requests.Session,
    scope: dict,
    page_no: int,
) -> dict:
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return request_page_once(session, scope, page_no)
        except PermanentApiError:
            raise
        except TransientApiError as exc:
            last_error = exc

            if attempt < MAX_RETRIES:
                wait_seconds = RETRY_WAIT_SECONDS[attempt - 1]
                print(
                    f"    페이지 {page_no} 요청 실패 "
                    f"({attempt}/{MAX_RETRIES}) → "
                    f"{wait_seconds}초 후 재시도"
                )
                time.sleep(wait_seconds)

    raise TransientApiError(
        f"페이지 {page_no} 요청이 {MAX_RETRIES}회 모두 실패했습니다: "
        f"{last_error}"
    ) from last_error


def attach_request_metadata(
    item: dict,
    scope: dict,
    page_no: int,
    collected_at: str,
) -> dict:
    record = dict(item)
    record.update(
        {
            "request_year": scope["searchYearCd"],
            "request_sido": scope["siDo"],
            "request_gugun": scope["guGun"],
            "request_province_name": scope["province_name"],
            "request_district_name": scope["district_name"],
            "expected_afos_id": scope["expected_afos_id"],
            "request_page_no": page_no,
            "collected_at": collected_at,
        }
    )
    return record


def scope_key(scope: dict) -> str:
    return "|".join(
        [
            str(scope["searchYearCd"]),
            str(scope["siDo"]),
            str(scope["guGun"]),
        ]
    )


def checkpoint_meta(scopes: list[dict]) -> dict:
    joined_keys = "\n".join(scope_key(scope) for scope in scopes)
    fingerprint = hashlib.sha256(
        joined_keys.encode("utf-8")
    ).hexdigest()

    return {
        "type": "meta",
        "version": 1,
        "min_year": MIN_YEAR,
        "max_year": MAX_YEAR,
        "scope_count": len(scopes),
        "scope_fingerprint": fingerprint,
    }


def rewrite_checkpoint(meta: dict, entries: dict[str, dict]) -> None:
    temp_path = CHECKPOINT_PATH.with_suffix(".tmp.jsonl")

    with temp_path.open("w", encoding="utf-8", newline="\n") as file:
        file.write(json.dumps(meta, ensure_ascii=False) + "\n")

        for entry in entries.values():
            file.write(
                json.dumps(
                    entry,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=str,
                )
                + "\n"
            )

        file.flush()
        os.fsync(file.fileno())

    os.replace(temp_path, CHECKPOINT_PATH)


def load_checkpoint(scopes: list[dict]) -> tuple[dict, dict[str, dict]]:
    expected_meta = checkpoint_meta(scopes)

    if not CHECKPOINT_PATH.exists():
        rewrite_checkpoint(expected_meta, {})
        return expected_meta, {}

    valid_scope_keys = {scope_key(scope) for scope in scopes}
    entries: dict[str, dict] = {}
    actual_meta = None
    invalid_line_found = False

    with CHECKPOINT_PATH.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            if not line.strip():
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                invalid_line_found = True
                print(
                    f"체크포인트 {line_number}행이 불완전하여 "
                    "마지막 정상 범위부터 복구합니다."
                )
                continue

            if record.get("type") == "meta":
                actual_meta = record
                continue

            if record.get("type") != "scope":
                continue

            key = record.get("key")

            if key in valid_scope_keys:
                entries[key] = record

    if actual_meta is None:
        raise PipelineError(
            f"체크포인트 메타데이터가 없습니다: {CHECKPOINT_PATH}"
        )

    fields_to_compare = [
        "version",
        "min_year",
        "max_year",
        "scope_count",
        "scope_fingerprint",
    ]

    if any(
        actual_meta.get(field) != expected_meta.get(field)
        for field in fields_to_compare
    ):
        raise PipelineError(
            "기존 체크포인트의 코드표/연도 범위가 현재 실행과 다릅니다.\n"
            f"확인 파일: {CHECKPOINT_PATH}"
        )

    if invalid_line_found:
        rewrite_checkpoint(expected_meta, entries)

    return expected_meta, entries


def append_checkpoint_entry(entry: dict) -> None:
    with CHECKPOINT_PATH.open("a", encoding="utf-8", newline="\n") as file:
        file.write(
            json.dumps(
                entry,
                ensure_ascii=False,
                separators=(",", ":"),
                default=str,
            )
            + "\n"
        )
        file.flush()
        os.fsync(file.fileno())


def make_checkpoint_entry(
    scope: dict,
    status: str,
    records: list[dict] | None = None,
    reason: str | None = None,
) -> dict:
    return {
        "type": "scope",
        "key": scope_key(scope),
        "status": status,
        "scope": scope,
        "records": records or [],
        "reason": reason,
        "saved_at": datetime.now()
        .astimezone()
        .isoformat(timespec="seconds"),
    }


def remove_checkpoint_after_success() -> None:
    if CHECKPOINT_PATH.exists():
        CHECKPOINT_PATH.unlink()


def collect_scope(
    session: requests.Session,
    scope: dict,
    collected_at: str,
) -> list[dict]:
    first_page = request_page_with_retry(session, scope, 1)
    total_count = first_page["total_count"]

    if total_count == 0:
        return []

    total_pages = math.ceil(total_count / NUM_OF_ROWS)
    records = [
        attach_request_metadata(item, scope, 1, collected_at)
        for item in first_page["items"]
    ]

    for page_no in range(2, total_pages + 1):
        page = request_page_with_retry(session, scope, page_no)

        records.extend(
            attach_request_metadata(item, scope, page_no, collected_at)
            for item in page["items"]
        )
        time.sleep(REQUEST_DELAY_SECONDS)

    if len(records) != total_count:
        raise PermanentApiError(
            "페이지 수집 건수 불일치: "
            f"totalCount={total_count:,} / 수집={len(records):,}"
        )

    return records


def save_failure_log(failures: list[dict], run_id: str) -> Path:
    failure_path = RAW_DIR / f"koroad_bicycle_failures_{run_id}.csv"
    temp_path = failure_path.with_suffix(".tmp.csv")

    pd.DataFrame(failures).to_csv(
        temp_path,
        index=False,
        encoding="utf-8-sig",
    )
    os.replace(temp_path, failure_path)
    return failure_path


def save_skipped_scope_log(skipped_scopes: list[dict], run_id: str) -> Path:
    skipped_path = RAW_DIR / f"koroad_bicycle_skipped_scopes_{run_id}.csv"
    temp_path = skipped_path.with_suffix(".tmp.csv")

    pd.DataFrame(skipped_scopes).to_csv(
        temp_path,
        index=False,
        encoding="utf-8-sig",
    )
    os.replace(temp_path, skipped_path)
    return skipped_path


def collect_all_scopes(scopes: list[dict], run_id: str) -> pd.DataFrame:
    collected_at = datetime.now().astimezone().isoformat(timespec="seconds")
    all_records: list[dict] = []
    failures: list[dict] = []
    skipped_scopes: list[dict] = []
    no_data_count = 0

    _, checkpoint_entries = load_checkpoint(scopes)

    for entry in checkpoint_entries.values():
        status = entry.get("status")

        if status == "success":
            all_records.extend(entry.get("records") or [])
        elif status == "no_data":
            no_data_count += 1
        elif status == "unsupported":
            saved_scope = entry.get("scope") or {}
            skipped_scopes.append(
                {
                    "searchYearCd": saved_scope.get("searchYearCd"),
                    "siDo": saved_scope.get("siDo"),
                    "guGun": saved_scope.get("guGun"),
                    "province_name": saved_scope.get("province_name"),
                    "district_name": saved_scope.get("district_name"),
                    "reason": entry.get("reason"),
                }
            )

    if checkpoint_entries:
        print(
            "체크포인트 재개: "
            f"완료된 요청 {len(checkpoint_entries):,}개를 건너뜁니다."
        )
        print(f"체크포인트     : {CHECKPOINT_PATH}")
        print()

    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "User-Agent": "WooSimWoonKka-KoROAD-Collector/1.0",
        }
    )

    total_scopes = len(scopes)

    try:
        # 대량 수집 전에 이미 성공이 확인된 공식 예제 조합으로 인증키,
        # URL, JSON 형식과 기본 파라미터를 검증한다. 이 단계가 실패하면
        # resultCode=10을 행정구역 예외로 숨기지 않고 즉시 중단한다.
        preflight_scope = {
            "searchYearCd": "2023",
            "siDo": "11",
            "guGun": "680",
            "province_name": "서울특별시",
            "district_name": "강남구",
            "expected_afos_id": "2024046",
        }
        try:
            preflight = request_page_with_retry(
                session,
                preflight_scope,
                1,
            )
        except GlobalServiceError as exc:
            failure_path = save_failure_log(
                [
                    {
                        "searchYearCd": "2023",
                        "siDo": "11",
                        "guGun": "680",
                        "province_name": "서울특별시",
                        "district_name": "강남구",
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                        "failed_at": datetime.now()
                        .astimezone()
                        .isoformat(timespec="seconds"),
                    }
                ],
                run_id,
            )
            raise PipelineError(
                "사전 API 점검에서 인증키 사용승인 또는 호출 한도 "
                "오류가 발생했습니다. 대량 수집을 시작하지 않습니다.\n"
                f"다음 실행은 체크포인트부터 이어집니다: "
                f"{CHECKPOINT_PATH}\n"
                f"오류 기록: {failure_path}"
            ) from exc
        print(
            "사전 API 점검 : 정상 "
            f"(2023 / 서울 / 강남구, "
            f"{preflight['total_count']:,}건)"
        )
        print()

        for index, scope in enumerate(scopes, start=1):
            key = scope_key(scope)

            if key in checkpoint_entries:
                continue

            label = (
                f"{scope['searchYearCd']} / "
                f"{scope['province_name']}({scope['siDo']}) / "
                f"{scope['district_name']}({scope['guGun']})"
            )

            try:
                records = collect_scope(
                    session,
                    scope,
                    collected_at,
                )

                if records:
                    entry = make_checkpoint_entry(
                        scope,
                        "success",
                        records=records,
                    )
                    append_checkpoint_entry(entry)
                    checkpoint_entries[key] = entry
                    all_records.extend(records)
                    result_text = f"{len(records):,}건"
                else:
                    entry = make_checkpoint_entry(
                        scope,
                        "no_data",
                    )
                    append_checkpoint_entry(entry)
                    checkpoint_entries[key] = entry
                    no_data_count += 1
                    result_text = "데이터 없음"

                print(
                    f"[{index:04d}/{total_scopes:04d}] "
                    f"{label} → {result_text}"
                )

            except UnsupportedScopeError as exc:
                entry = make_checkpoint_entry(
                    scope,
                    "unsupported",
                    reason=str(exc),
                )
                append_checkpoint_entry(entry)
                checkpoint_entries[key] = entry
                skipped_scopes.append(
                    {
                        "searchYearCd": scope["searchYearCd"],
                        "siDo": scope["siDo"],
                        "guGun": scope["guGun"],
                        "province_name": scope["province_name"],
                        "district_name": scope["district_name"],
                        "reason": str(exc),
                    }
                )
                print(
                    f"[{index:04d}/{total_scopes:04d}] "
                    f"{label} → 해당 연도 미지원 조합, 건너뜀"
                )

            except GlobalServiceError as exc:
                global_failure = {
                    "searchYearCd": scope["searchYearCd"],
                    "siDo": scope["siDo"],
                    "guGun": scope["guGun"],
                    "province_name": scope["province_name"],
                    "district_name": scope["district_name"],
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "failed_at": datetime.now()
                    .astimezone()
                    .isoformat(timespec="seconds"),
                }
                failure_path = save_failure_log([global_failure], run_id)
                raise PipelineError(
                    "인증키 사용승인 또는 일일 호출 한도와 관련된 "
                    "전역 오류가 발생해 즉시 중단합니다.\n"
                    f"완료 요청: {len(checkpoint_entries):,}/"
                    f"{total_scopes:,}\n"
                    f"다음 실행은 체크포인트부터 이어집니다: "
                    f"{CHECKPOINT_PATH}\n"
                    f"오류 기록: {failure_path}"
                ) from exc

            except Exception as exc:
                failures.append(
                    {
                        "searchYearCd": scope["searchYearCd"],
                        "siDo": scope["siDo"],
                        "guGun": scope["guGun"],
                        "province_name": scope["province_name"],
                        "district_name": scope["district_name"],
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                        "failed_at": datetime.now()
                        .astimezone()
                        .isoformat(timespec="seconds"),
                    }
                )
                print(
                    f"[{index:04d}/{total_scopes:04d}] "
                    f"{label} → 실패: {exc}"
                )

            time.sleep(REQUEST_DELAY_SECONDS)
    finally:
        session.close()

    print()
    print(f"데이터 있는 범위 : {total_scopes - no_data_count - len(failures):,}개")
    print(
        "데이터 있는 범위 : "
        f"{len(checkpoint_entries) - no_data_count - len(skipped_scopes):,}개"
    )
    print(f"데이터 없는 범위 : {no_data_count:,}개")
    print(f"연도 미지원 조합 : {len(skipped_scopes):,}개")
    print(f"실패 범위        : {len(failures):,}개")
    print(f"완료 체크포인트  : {len(checkpoint_entries):,}/{total_scopes:,}개")

    if skipped_scopes:
        skipped_path = save_skipped_scope_log(skipped_scopes, run_id)
        print(f"건너뛴 조합 목록 : {skipped_path}")

    if failures:
        failure_path = save_failure_log(failures, run_id)
        raise PipelineError(
            "일부 요청이 실패하여 CSV와 DB를 교체하지 않습니다.\n"
            f"실패 목록: {failure_path}"
        )

    if len(checkpoint_entries) != total_scopes:
        raise PipelineError(
            "완료된 요청범위가 전체 요청범위와 다르므로 CSV와 DB를 "
            "교체하지 않습니다.\n"
            f"완료={len(checkpoint_entries):,} / 전체={total_scopes:,}\n"
            f"체크포인트: {CHECKPOINT_PATH}"
        )

    if not all_records:
        raise PipelineError(
            "전체 수집 결과가 0건이므로 기존 CSV와 DB를 교체하지 않습니다."
        )

    raw_df = pd.DataFrame(all_records)

    collected_years = sorted(
        pd.to_numeric(raw_df["request_year"], errors="coerce")
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )
    expected_years = list(range(MIN_YEAR, MAX_YEAR + 1))

    if collected_years != expected_years:
        raise PipelineError(
            "수집 데이터의 연도 범위가 불완전하여 CSV와 DB를 "
            "교체하지 않습니다.\n"
            f"예상={expected_years} / 실제={collected_years}"
        )

    missing_columns = set(API_COLUMNS).difference(raw_df.columns)

    if missing_columns:
        raise PipelineError(
            "API 필수 컬럼이 없습니다: "
            + ", ".join(sorted(missing_columns))
        )

    ordered_columns = API_COLUMNS + REQUEST_METADATA_COLUMNS
    extra_columns = [
        column
        for column in raw_df.columns
        if column not in ordered_columns
    ]

    return raw_df.reindex(columns=ordered_columns + extra_columns)


# ============================================================
# 7. PROCESSED CLEANING / VALIDATION
# ============================================================


def normalize_missing_series(series: pd.Series) -> pd.Series:
    def normalize(value):
        if value is None or value is pd.NA:
            return pd.NA

        if isinstance(value, float) and math.isnan(value):
            return pd.NA

        if isinstance(value, str):
            stripped = value.strip()

            if stripped.lower() in NULL_TEXT_VALUES:
                return pd.NA

            return stripped

        return value

    return series.map(normalize)


def convert_numeric_strict(
    series: pd.Series,
    column_name: str,
    integer: bool,
) -> pd.Series:
    normalized = normalize_missing_series(series)
    converted = pd.to_numeric(normalized, errors="coerce")
    invalid_mask = normalized.notna() & converted.isna()

    if invalid_mask.any():
        examples = normalized[invalid_mask].astype(str).head(5).tolist()
        raise PipelineError(
            f"{column_name} 숫자 변환 실패: {examples}"
        )

    if integer:
        non_integer_mask = converted.notna() & (converted % 1 != 0)

        if non_integer_mask.any():
            examples = converted[non_integer_mask].head(5).tolist()
            raise PipelineError(
                f"{column_name}에 정수가 아닌 값이 있습니다: {examples}"
            )

        return converted.astype("Int64")

    return converted.astype("Float64")


def normalize_geojson(value):
    if value is None or value is pd.NA:
        return None, False

    if isinstance(value, float) and math.isnan(value):
        return None, False

    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (TypeError, ValueError, json.JSONDecodeError):
        return None, False

    if not isinstance(parsed, dict):
        return None, False

    if parsed.get("type") not in {"Polygon", "MultiPolygon"}:
        return None, False

    coordinates = parsed.get("coordinates")

    if not isinstance(coordinates, list) or not coordinates:
        return None, False

    return (
        json.dumps(parsed, ensure_ascii=False, separators=(",", ":")),
        True,
    )


def prepare_processed(raw_df: pd.DataFrame):
    processed = raw_df.copy()

    for column in processed.columns:
        processed[column] = normalize_missing_series(processed[column])

    rename_map = {
        "request_year": "search_year",
        "request_sido": "si_do",
        "request_gugun": "gu_gun",
        "request_province_name": "province_name",
        "request_district_name": "district_name",
        "lo_crd": "longitude",
        "la_crd": "latitude",
    }
    processed = processed.rename(columns=rename_map)

    id_columns = [
        "afos_fid",
        "afos_id",
        "bjd_cd",
        "spot_cd",
        "si_do",
        "gu_gun",
        "expected_afos_id",
    ]

    for column in id_columns:
        processed[column] = processed[column].astype("string")

    processed["search_year"] = convert_numeric_strict(
        processed["search_year"],
        "search_year",
        integer=True,
    )

    processed["request_page_no"] = convert_numeric_strict(
        processed["request_page_no"],
        "request_page_no",
        integer=True,
    )

    count_columns = [
        "occrrnc_cnt",
        "caslt_cnt",
        "dth_dnv_cnt",
        "se_dnv_cnt",
        "sl_dnv_cnt",
        "wnd_dnv_cnt",
    ]

    for column in count_columns:
        processed[column] = convert_numeric_strict(
            processed[column],
            column,
            integer=True,
        )

        negative_mask = processed[column].notna() & (processed[column] < 0)

        if negative_mask.any():
            raise PipelineError(
                f"{column}에 음수가 있습니다."
            )

    processed["longitude"] = convert_numeric_strict(
        processed["longitude"],
        "longitude",
        integer=False,
    )
    processed["latitude"] = convert_numeric_strict(
        processed["latitude"],
        "latitude",
        integer=False,
    )

    coordinate_valid = (
        processed["longitude"].between(
            KOREA_MIN_LONGITUDE,
            KOREA_MAX_LONGITUDE,
            inclusive="both",
        )
        & processed["latitude"].between(
            KOREA_MIN_LATITUDE,
            KOREA_MAX_LATITUDE,
            inclusive="both",
        )
    ).fillna(False)

    # 원본 좌표는 raw에 남기고 processed에서는 결측/범위 밖 좌표 행을 제거한다.
    invalid_coordinate_count = int((~coordinate_valid).sum())
    processed = processed.loc[coordinate_valid].copy()
    processed["coordinate_valid"] = True

    normalized_geometries = processed["geom_json"].map(normalize_geojson)
    processed["geom_json"] = normalized_geometries.map(lambda item: item[0])
    processed["polygon_valid"] = normalized_geometries.map(
        lambda item: item[1]
    ).astype(bool)

    processed["collected_at"] = pd.to_datetime(
        processed["collected_at"],
        errors="coerce",
        utc=True,
    )

    if processed["collected_at"].isna().any():
        raise PipelineError(
            "collected_at을 timestamp로 변환할 수 없는 행이 있습니다."
        )

    critical_columns = [
        "afos_fid",
        "afos_id",
        "bjd_cd",
        "spot_cd",
        "sido_sgg_nm",
        "spot_nm",
        "search_year",
        "si_do",
        "gu_gun",
    ]

    critical_nulls = {
        column: int(processed[column].isna().sum())
        for column in critical_columns
        if processed[column].isna().any()
    }

    if critical_nulls:
        raise PipelineError(
            "필수 컬럼에 NULL이 있습니다: "
            + ", ".join(
                f"{column}={count:,}"
                for column, count in critical_nulls.items()
            )
        )

    expected_afos_mismatch = (
        processed["afos_id"] != processed["expected_afos_id"]
    ).fillna(True)

    if expected_afos_mismatch.any():
        sample = processed.loc[
            expected_afos_mismatch,
            ["search_year", "afos_id", "expected_afos_id"],
        ].head(10)
        raise PipelineError(
            "searchYearCd와 응답 afos_id 관계가 코드표와 다릅니다.\n"
            + sample.to_string(index=False)
        )

    duplicate_mask = processed.duplicated("afos_fid", keep=False)
    duplicate_row_count = int(duplicate_mask.sum())
    duplicate_removed_count = 0

    if duplicate_row_count:
        duplicate_rows = processed.loc[duplicate_mask].copy()
        # 같은 지점이 서로 다른 요청범위에서 중복 조회될 수 있으므로
        # 요청 메타데이터가 아니라 실제 API 데이터만 비교한다.
        compare_columns = [
            "afos_fid",
            "afos_id",
            "bjd_cd",
            "spot_cd",
            "sido_sgg_nm",
            "spot_nm",
            "occrrnc_cnt",
            "caslt_cnt",
            "dth_dnv_cnt",
            "se_dnv_cnt",
            "sl_dnv_cnt",
            "wnd_dnv_cnt",
            "longitude",
            "latitude",
            "geom_json",
        ]

        conflicting_ids = []

        for afos_fid, group in duplicate_rows.groupby(
            "afos_fid",
            dropna=False,
        ):
            comparable = (
                group[compare_columns]
                .astype("string")
                .fillna("<NULL>")
                .drop_duplicates()
            )

            if len(comparable) > 1:
                conflicting_ids.append(str(afos_fid))

        if conflicting_ids:
            raise PipelineError(
                "같은 afos_fid에 서로 다른 값이 있습니다: "
                + ", ".join(conflicting_ids[:10])
            )

        before_count = len(processed)
        processed = processed.drop_duplicates(
            subset=["afos_fid"],
            keep="first",
        ).copy()
        duplicate_removed_count = before_count - len(processed)

    if processed.empty:
        raise PipelineError(
            "정제 후 processed 데이터가 0건입니다."
        )

    quality = {
        "raw_count": len(raw_df),
        "processed_count": len(processed),
        "duplicate_removed_count": duplicate_removed_count,
        "invalid_coordinate_count": invalid_coordinate_count,
        "invalid_polygon_count": int(
            (~processed["polygon_valid"]).sum()
        ),
    }

    if (
        quality["processed_count"]
        != (
            quality["raw_count"]
            - quality["invalid_coordinate_count"]
            - quality["duplicate_removed_count"]
        )
    ):
        raise PipelineError(
            "raw/processed 건수 관계가 맞지 않습니다."
        )

    preferred_columns = [
        "afos_fid",
        "afos_id",
        "search_year",
        "si_do",
        "gu_gun",
        "province_name",
        "district_name",
        "bjd_cd",
        "spot_cd",
        "sido_sgg_nm",
        "spot_nm",
        "occrrnc_cnt",
        "caslt_cnt",
        "dth_dnv_cnt",
        "se_dnv_cnt",
        "sl_dnv_cnt",
        "wnd_dnv_cnt",
        "longitude",
        "latitude",
        "coordinate_valid",
        "geom_json",
        "polygon_valid",
        "collected_at",
    ]

    remaining_columns = [
        column
        for column in processed.columns
        if column not in preferred_columns
    ]
    processed = processed.reindex(
        columns=preferred_columns + remaining_columns
    )

    return processed, quality


# ============================================================
# 8. CSV ATOMIC SAVE
# ============================================================


def write_complete_snapshot(raw_df: pd.DataFrame, run_id: str) -> Path:
    snapshot_path = (
        RAW_DIR / f"koroad_bicycle_accident_hotspots_{run_id}.csv"
    )
    temp_path = snapshot_path.with_suffix(".tmp.csv")

    raw_df.to_csv(
        temp_path,
        index=False,
        encoding="utf-8-sig",
    )

    if not temp_path.exists() or temp_path.stat().st_size == 0:
        raise PipelineError(
            f"CSV 임시파일 생성 실패: {temp_path}"
        )

    os.replace(temp_path, snapshot_path)
    return snapshot_path


def publish_canonical_csv(snapshot_path: Path) -> None:
    temp_path = CANONICAL_RAW_CSV.with_suffix(".tmp.csv")
    shutil.copyfile(snapshot_path, temp_path)
    os.replace(temp_path, CANONICAL_RAW_CSV)


# ============================================================
# 9. DATABASE
# ============================================================


def build_database_url() -> tuple[str, str]:
    explicit_url = os.getenv("DATABASE_URL")

    if explicit_url:
        db_name = os.getenv("DB_NAME") or os.getenv("POSTGRES_DB") or ""
        return explicit_url, db_name

    host = (
        os.getenv("DB_HOST")
        or os.getenv("POSTGRES_HOST")
        or "localhost"
    )
    port = (
        os.getenv("DB_PORT")
        or os.getenv("POSTGRES_PORT")
        or "5432"
    )
    db_name = (
        os.getenv("DB_NAME")
        or os.getenv("POSTGRES_DB")
        or "woosimwoonkka"
    )
    user = os.getenv("DB_USER") or os.getenv("POSTGRES_USER")
    password = os.getenv("DB_PASSWORD") or os.getenv("POSTGRES_PASSWORD")

    if not user:
        raise ValueError(
            "DB_USER 또는 POSTGRES_USER가 .env에 없습니다."
        )

    if not password:
        raise ValueError(
            "DB_PASSWORD 또는 POSTGRES_PASSWORD가 .env에 없습니다."
        )

    url = (
        "postgresql+psycopg2://"
        f"{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{db_name}"
    )
    return url, db_name


def prepare_raw_db_frame(raw_df: pd.DataFrame) -> pd.DataFrame:
    raw_db = raw_df.copy()

    def raw_value(value):
        if value is None or value is pd.NA:
            return None

        if isinstance(value, float) and math.isnan(value):
            return None

        if isinstance(value, (dict, list)):
            return json.dumps(
                value,
                ensure_ascii=False,
                separators=(",", ":"),
            )

        return str(value)

    for column in raw_db.columns:
        raw_db[column] = raw_db[column].map(raw_value)

    return raw_db


def processed_sql_types(processed_df: pd.DataFrame) -> dict:
    types = {column: Text() for column in processed_df.columns}

    for column in [
        "search_year",
        "request_page_no",
    ]:
        if column in types:
            types[column] = Integer()

    for column in [
        "occrrnc_cnt",
        "caslt_cnt",
        "dth_dnv_cnt",
        "se_dnv_cnt",
        "sl_dnv_cnt",
        "wnd_dnv_cnt",
    ]:
        if column in types:
            types[column] = BigInteger()

    for column in ["longitude", "latitude"]:
        if column in types:
            types[column] = Float()

    for column in ["coordinate_valid", "polygon_valid"]:
        if column in types:
            types[column] = Boolean()

    if "collected_at" in types:
        types["collected_at"] = DateTime(timezone=True)

    return types


def quoted_table(schema: str, table: str) -> str:
    return f'{schema}."{table}"'


def test_db_connection(engine, expected_db_name: str) -> str:
    with engine.connect() as connection:
        actual_db_name = connection.execute(
            text("SELECT current_database()")
        ).scalar_one()

    if expected_db_name and actual_db_name != expected_db_name:
        raise PipelineError(
            "DB 연결 대상 불일치: "
            f"예상={expected_db_name} / 실제={actual_db_name}"
        )

    return actual_db_name


def count_table(engine, schema: str, table: str) -> int:
    query = text(
        f"SELECT COUNT(*) FROM {quoted_table(schema, table)}"
    )

    with engine.connect() as connection:
        return int(connection.execute(query).scalar_one())


def drop_staging_tables(engine, raw_staging: str, processed_staging: str):
    with engine.begin() as connection:
        connection.execute(
            text(
                f"DROP TABLE IF EXISTS "
                f"{quoted_table(RAW_SCHEMA, raw_staging)}"
            )
        )
        connection.execute(
            text(
                f"DROP TABLE IF EXISTS "
                f"{quoted_table(PROCESSED_SCHEMA, processed_staging)}"
            )
        )


def load_database_atomic(
    engine,
    raw_df: pd.DataFrame,
    processed_df: pd.DataFrame,
    run_id: str,
) -> tuple[int, int]:
    raw_staging = f"{TABLE_NAME}_staging_{run_id}"
    processed_staging = f"{TABLE_NAME}_staging_{run_id}"
    raw_db = prepare_raw_db_frame(raw_df)
    swapped = False

    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS processed"))

    try:
        raw_db.to_sql(
            name=raw_staging,
            con=engine,
            schema=RAW_SCHEMA,
            if_exists="fail",
            index=False,
            chunksize=500,
            method="multi",
            dtype={column: Text() for column in raw_db.columns},
        )

        processed_df.to_sql(
            name=processed_staging,
            con=engine,
            schema=PROCESSED_SCHEMA,
            if_exists="fail",
            index=False,
            chunksize=500,
            method="multi",
            dtype=processed_sql_types(processed_df),
        )

        raw_count = count_table(engine, RAW_SCHEMA, raw_staging)
        processed_count = count_table(
            engine,
            PROCESSED_SCHEMA,
            processed_staging,
        )

        if raw_count != len(raw_df):
            raise PipelineError(
                "raw staging 건수 불일치: "
                f"DataFrame={len(raw_df):,} / DB={raw_count:,}"
            )

        if processed_count != len(processed_df):
            raise PipelineError(
                "processed staging 건수 불일치: "
                f"DataFrame={len(processed_df):,} / DB={processed_count:,}"
            )

        primary_key_name = f"pk_bicycle_{run_id}"
        region_index_name = f"ix_bicycle_region_{run_id}"
        spot_index_name = f"ix_bicycle_spot_{run_id}"

        # PostgreSQL DDL은 트랜잭션 안에서 처리된다.
        # 아래 교체 도중 실패하면 기존 final 테이블 삭제도 rollback된다.
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"ALTER TABLE "
                    f"{quoted_table(PROCESSED_SCHEMA, processed_staging)} "
                    f'ADD CONSTRAINT "{primary_key_name}" '
                    'PRIMARY KEY ("afos_fid")'
                )
            )
            connection.execute(
                text(
                    f'CREATE INDEX "{region_index_name}" ON '
                    f"{quoted_table(PROCESSED_SCHEMA, processed_staging)} "
                    '("search_year", "si_do", "gu_gun")'
                )
            )
            connection.execute(
                text(
                    f'CREATE INDEX "{spot_index_name}" ON '
                    f"{quoted_table(PROCESSED_SCHEMA, processed_staging)} "
                    '("spot_cd")'
                )
            )

            connection.execute(
                text(
                    f"DROP TABLE IF EXISTS "
                    f"{quoted_table(RAW_SCHEMA, TABLE_NAME)}"
                )
            )
            connection.execute(
                text(
                    f"DROP TABLE IF EXISTS "
                    f"{quoted_table(PROCESSED_SCHEMA, TABLE_NAME)}"
                )
            )
            connection.execute(
                text(
                    f"ALTER TABLE "
                    f"{quoted_table(RAW_SCHEMA, raw_staging)} "
                    f'RENAME TO "{TABLE_NAME}"'
                )
            )
            connection.execute(
                text(
                    f"ALTER TABLE "
                    f"{quoted_table(PROCESSED_SCHEMA, processed_staging)} "
                    f'RENAME TO "{TABLE_NAME}"'
                )
            )

        swapped = True
        return raw_count, processed_count

    finally:
        if not swapped:
            drop_staging_tables(
                engine,
                raw_staging,
                processed_staging,
            )


# ============================================================
# 10. OUTPUT
# ============================================================


def display_first_record(raw_df: pd.DataFrame) -> None:
    first = raw_df.iloc[0]

    def display(column):
        value = first.get(column)
        return "NULL" if pd.isna(value) else value

    print()
    print("[첫 번째 수집 데이터]")
    print(f"지역         : {display('sido_sgg_nm')}")
    print(f"지점         : {display('spot_nm')}")
    print(f"사고건수     : {display('occrrnc_cnt')}")
    print(f"사상자수     : {display('caslt_cnt')}")
    print(f"사망자수     : {display('dth_dnv_cnt')}")
    print(f"중상자수     : {display('se_dnv_cnt')}")
    print(f"경상자수     : {display('sl_dnv_cnt')}")
    print(f"부상신고     : {display('wnd_dnv_cnt')}")
    print(f"경도         : {display('lo_crd')}")
    print(f"위도         : {display('la_crd')}")
    print(
        "Polygon      : "
        + ("있음" if not pd.isna(first.get("geom_json")) else "없음")
    )


# ============================================================
# 11. MAIN
# ============================================================


def main():
    started_at = time.monotonic()
    run_id = datetime.now().strftime("%Y%m%d%H%M%S")

    if not ENV_PATH.exists():
        raise FileNotFoundError(
            ".env 파일을 찾을 수 없습니다.\n"
            f"확인 경로: {ENV_PATH}"
        )

    if not API_KEY:
        raise ValueError(
            "BICYCLE_ACCIDENT_API_KEY가 .env에 없습니다."
        )

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print("=" * 70)
    print("KoROAD 자전거 교통사고 전국·전연도 파이프라인")
    print("=" * 70)
    print(f"ENV          : {ENV_PATH}")
    print(f"코드표       : {CODELIST_PATH}")
    print(f"RAW CSV 폴더 : {RAW_DIR}")
    print("API KEY      : 확인됨")

    database_url, expected_db_name = build_database_url()
    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        future=True,
    )

    try:
        actual_db_name = test_db_connection(engine, expected_db_name)
        print(f"DB 연결      : {actual_db_name}")

        scopes, afos_id_by_year = load_request_scopes(CODELIST_PATH)
        print(
            f"수집 범위    : {min(afos_id_by_year)}~"
            f"{max(afos_id_by_year)} / {len(scopes):,}개 요청"
        )
        print()

        raw_df = collect_all_scopes(scopes, run_id)
        display_first_record(raw_df)

        processed_df, quality = prepare_processed(raw_df)

        print()
        print("[정제·품질검증]")
        print(f"RAW 건수          : {quality['raw_count']:,}건")
        print(f"PROCESSED 건수    : {quality['processed_count']:,}건")
        print(
            "동일 ID 중복 제거 : "
            f"{quality['duplicate_removed_count']:,}건"
        )
        print(
            "좌표 NULL/범위밖  : "
            f"{quality['invalid_coordinate_count']:,}건"
        )
        print(
            "Polygon NULL/오류 : "
            f"{quality['invalid_polygon_count']:,}건"
        )

        snapshot_path = write_complete_snapshot(raw_df, run_id)
        print(f"완료본 CSV        : {snapshot_path}")

        raw_db_count, processed_db_count = load_database_atomic(
            engine,
            raw_df,
            processed_df,
            run_id,
        )

        # DB 교체 성공 후에만 대표 CSV도 새 완료본으로 교체한다.
        publish_canonical_csv(snapshot_path)
        remove_checkpoint_after_success()

        elapsed_seconds = time.monotonic() - started_at

        print()
        print("=" * 70)
        print("전국·전연도 수집 및 적재 성공")
        print("=" * 70)
        print(f"요청 범위       : {len(scopes):,}개")
        print(f"수집 건수       : {len(raw_df):,}건")
        print(f"raw DB          : {raw_db_count:,}건")
        print(f"processed DB    : {processed_db_count:,}건")
        print(f"대표 RAW CSV    : {CANONICAL_RAW_CSV}")
        print(f"소요시간        : {elapsed_seconds:,.1f}초")
        print("=" * 70)

    finally:
        engine.dispose()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n사용자가 실행을 중단했습니다. 기존 DB는 교체하지 않습니다.")
        raise SystemExit(130)
    except Exception as exc:
        print()
        print("=" * 70)
        print("파이프라인 실패")
        print("기존 정상 CSV/DB 테이블은 교체하지 않습니다.")
        print(f"원인: {type(exc).__name__}: {exc}")
        print("=" * 70)
        raise
