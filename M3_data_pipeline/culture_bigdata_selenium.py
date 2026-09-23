"""문화 빅데이터 플랫폼 CSV 다운로드 및 PostgreSQL raw 적재 자동화.

``collector/.env``의 ``CULTURE_ID``와 ``CULTURE_PASSWORD``로 로그인한 뒤,
상품 상세 페이지에서 CSV 배포 파일만 선택하고 활용목적/필수약관 팝업을
처리한다. 다운로드한 원본은 수정하지 않고 보존하며, CSV 구조를 검증한 뒤
``woosimwoonkka`` 데이터베이스의 ``raw`` 스키마에 원자적으로 교체 적재하고,
SQL 기반 정제 결과를 ``processed`` 스키마에 원자적으로 교체 적재한다.

기본 실행은 설정된 상품을 차례대로 내려받는다::

    python M3_data_pipeline/bigdata_culture_selenium.py

특정 상품만 실행하려면 ``--url``을 지정한다::

    python M3_data_pipeline/bigdata_culture_selenium.py --url "https://..."
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Sequence
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv
import psycopg
from psycopg import sql
from selenium import webdriver
from selenium.common.exceptions import (
    NoAlertPresentException,
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    UnexpectedAlertPresentException,
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


BASE_URL = "https://www.bigdata-culture.kr"
LOGIN_URL = f"{BASE_URL}/bigdata/user/member/login.do"
TARGET_URLS = (
    # 수집 완료
    # f"{BASE_URL}/bigdata/user/data_market/detail.do"
    # "?id=b5880ea0-247a-4258-9f7b-79eab6751591",
    # f"{BASE_URL}/bigdata/user/data_market/detail.do"
    # "?id=c3b8fb69-307d-4ae7-ab42-d0314c89ef47",
    f"{BASE_URL}/bigdata/user/data_market/detail.do"
    "?id=615e7eb0-6e17-11ee-88b4-1384e6e2c3c9",
    f"{BASE_URL}/bigdata/user/data_market/detail.do"
    "?id=3b5399ad-88c4-43aa-a1d7-7ef6a630370b",
    f"{BASE_URL}/bigdata/user/data_market/detail.do"
    "?id=599b29a1-bb8d-41a5-8de5-400d2c8d2ba5",
    f"{BASE_URL}/bigdata/user/data_market/detail.do"
    "?id=ddd74830-f977-11eb-8e60-2bcdc8456bfb",
    f"{BASE_URL}/bigdata/user/data_market/detail.do"
    "?id=1762f1c0-2594-11eb-af9a-4b03f0a582d6",
    # 체력 측정별 운동처방 데이터
    f"{BASE_URL}/bigdata/user/data_market/detail.do"
    "?id=2b1c565d-5f37-4152-966d-5f8094f8cf33",
    # 잘못된 상품 ID: UUID 마지막 문자가 빠졌고 상세 페이지가 존재하지 않음
    # f"{BASE_URL}/bigdata/user/data_market/detail.do"
    # "?id=599b29a1-bb8d-41a5-8de5-400d2c8d2ba",
)

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
ENV_PATH = SCRIPT_DIR / "collector" / ".env"
DEFAULT_DOWNLOAD_DIR = PROJECT_ROOT / "data" / "raw" / "culture_bigdata"
DEFAULT_LOG_DIR = PROJECT_ROOT / "data" / "logs"

# 실제 페이지 DOM에서 확인한 값이다.
PURPOSE_CODES = {
    "research": "004001001",
    "startup": "004001002",
    "business": "004001003",
    "other": "004001004",
}
TEMPORARY_SUFFIXES = {".crdownload", ".part", ".tmp"}
RAW_SCHEMA = "raw"
PROCESSED_SCHEMA = "processed"
EXPECTED_DB_NAME = "woosimwoonkka"
KOREA_LATITUDE_RANGE = (33.0, 39.0)
KOREA_LONGITUDE_RANGE = (124.0, 132.0)
NULL_LITERALS = (
    "",
    "-",
    "--",
    "null",
    "none",
    "n/a",
    "na",
    "n.a.",
    "정보없음",
    "정보 없음",
    "미상",
)
NUMERIC_PATTERN = (
    r"^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$"
)
PRODUCT_TABLES = {
    "b5880ea0-247a-4258-9f7b-79eab6751591": (
        "culture_public_sports_facilities"
    ),
    "c3b8fb69-307d-4ae7-ab42-d0314c89ef47": (
        "culture_public_sports_facility_programs"
    ),
    "615e7eb0-6e17-11ee-88b4-1384e6e2c3c9": (
        "culture_sports_facility_nearby_public_transport"
    ),
    "3b5399ad-88c4-43aa-a1d7-7ef6a630370b": (
        "culture_national_sports_facility_status"
    ),
    "599b29a1-bb8d-41a5-8de5-400d2c8d2ba5": (
        "culture_location_fitness_measurement_prescriptions"
    ),
    "ddd74830-f977-11eb-8e60-2bcdc8456bfb": (
        "culture_open_school_sports_facilities"
    ),
    "1762f1c0-2594-11eb-af9a-4b03f0a582d6": (
        "culture_sports_facility_safety_inspections"
    ),
    "2b1c565d-5f37-4152-966d-5f8094f8cf33": (
        "culture_fitness_measurement_prescriptions"
    ),
}


PRODUCT_FILE_STEMS = {
    "b5880ea0-247a-4258-9f7b-79eab6751591": (
        "KS_WNTY_PUBLIC_PHSTRN_FCLTY_STTUS"
    ),
    "c3b8fb69-307d-4ae7-ab42-d0314c89ef47": (
        "KS_PUBLIC_ALSFC_PROGRM_INFO"
    ),
    "615e7eb0-6e17-11ee-88b4-1384e6e2c3c9": (
        "KS_ALSFC_NEARBY_PBTRNSP_INFO"
    ),
    "3b5399ad-88c4-43aa-a1d7-7ef6a630370b": (
        "KS_WNTY_PHSTRN_FCLTY_STTUS"
    ),
    "599b29a1-bb8d-41a5-8de5-400d2c8d2ba5": (
        "KS_LC_IFRA_FTNESS_MESURE_MVM_PRSCRPTN_INFO"
    ),
    "ddd74830-f977-11eb-8e60-2bcdc8456bfb": (
        "KS_OPN_SCHUL_ALSFC_INFO"
    ),
    "1762f1c0-2594-11eb-af9a-4b03f0a582d6": (
        "KS_ALSFC_SAFECHK_INFO"
    ),
    "2b1c565d-5f37-4152-966d-5f8094f8cf33": (
        "KS_NFA_FTNESS_MESURE_MVM_PRSCRPTN_INFO"
    ),
}

# (latitude, longitude) 순서다. required 좌표가 유효하지 않은 행만 제외하고,
# 프로그램 데이터의 선택적 인접 교통 좌표는 숫자 변환 실패 시 NULL로 둔다.
TABLE_COORDINATES = {
    "culture_public_sports_facilities": {
        "required": (("FCLTY_LA", "FCLTY_LO"),),
        "optional": (),
    },
    "culture_public_sports_facility_programs": {
        "required": (("FCLTY_LA", "FCLTY_LO"),),
        "optional": tuple(
            (f"PBTRNSP_FCLTY_{rank}R_LA", f"PBTRNSP_FCLTY_{rank}R_LO")
            for rank in range(1, 6)
        ),
    },
    "culture_sports_facility_nearby_public_transport": {
        "required": (
            ("ALSFC_LA", "ALSFC_LO"),
            ("PBTRNSP_FCLTY_LA", "PBTRNSP_FCLTY_LO"),
        ),
        "optional": (),
    },
    "culture_national_sports_facility_status": {
        "required": (("FCLTY_LA", "FCLTY_LO"),),
        "optional": (),
    },
    "culture_location_fitness_measurement_prescriptions": {
        "required": (("CNTER_LA", "CNTER_LO"),),
        "optional": (),
    },
    "culture_open_school_sports_facilities": {
        "required": (),
        "optional": (),
    },
    "culture_sports_facility_safety_inspections": {
        "required": (("FCLTY_CRDNT_LA", "FCLTY_CRDNT_LO"),),
        "optional": (),
    },
    "culture_fitness_measurement_prescriptions": {
        "required": (),
        "optional": (),
    },
}


@dataclass(frozen=True)
class DownloadedFile:
    product_id: str
    title: str
    path: Path
    size: int


@dataclass(frozen=True)
class CsvValidation:
    path: Path
    columns: tuple[str, ...]
    row_count: int
    null_count: int
    encoding: str = "utf-8-sig"


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str


@dataclass(frozen=True)
class DatabaseLoad:
    schema: str
    table: str
    csv_row_count: int
    db_row_count: int


@dataclass(frozen=True)
class ProcessedLoad:
    schema: str
    table: str
    raw_row_count: int
    processed_row_count: int
    null_conversion_count: int
    coordinate_missing_removed: int
    coordinate_range_removed: int


class AutomationError(RuntimeError):
    """자동화의 실패 단계와 원인을 함께 보존한다."""

    def __init__(self, stage: str, reason: str) -> None:
        super().__init__(reason)
        self.stage = stage
        self.reason = reason


def product_id(url: str) -> str:
    values = parse_qs(urlparse(url).query).get("id", [])
    if not values:
        raise AutomationError("arguments", f"상품 id가 없는 URL: {url}")
    return values[0]


def configure_logging(log_dir: Path) -> tuple[logging.Logger, Path]:
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"bigdata_culture_selenium_{stamp}.log"

    logger = logging.getLogger("bigdata_culture_selenium")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger, log_path


def load_credentials() -> tuple[str, str]:
    if not ENV_PATH.is_file():
        raise AutomationError("credentials", f".env 파일 없음: {ENV_PATH}")

    load_dotenv(ENV_PATH, override=False)
    login_id = os.getenv("CULTURE_ID", "").strip()
    password = os.getenv("CULTURE_PASSWORD", "")
    missing = [
        name
        for name, value in (
            ("CULTURE_ID", login_id),
            ("CULTURE_PASSWORD", password),
        )
        if not value
    ]
    if missing:
        raise AutomationError(
            "credentials",
            f".env에 값이 없는 항목: {', '.join(missing)}",
        )
    return login_id, password


def load_database_config() -> DatabaseConfig:
    """민감정보를 로그나 연결 URL 문자열로 만들지 않고 DB 설정을 읽는다."""

    if not ENV_PATH.is_file():
        raise AutomationError("db_config", f".env 파일 없음: {ENV_PATH}")
    load_dotenv(ENV_PATH, override=False)

    values = {
        name: os.getenv(name, "")
        for name in ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD")
    }
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise AutomationError(
            "db_config",
            f".env에 값이 없는 항목: {', '.join(missing)}",
        )
    if values["DB_NAME"] != EXPECTED_DB_NAME:
        raise AutomationError(
            "db_config",
            f"DB_NAME은 {EXPECTED_DB_NAME!r}이어야 함",
        )
    try:
        port = int(values["DB_PORT"])
    except ValueError as exc:
        raise AutomationError("db_config", "DB_PORT가 정수가 아님") from exc

    return DatabaseConfig(
        host=values["DB_HOST"],
        port=port,
        dbname=values["DB_NAME"],
        user=values["DB_USER"],
        password=values["DB_PASSWORD"],
    )


def create_driver(download_dir: Path, headless: bool) -> webdriver.Chrome:
    download_dir.mkdir(parents=True, exist_ok=True)
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-notifications")
    options.add_argument("--start-maximized")
    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": str(download_dir.resolve()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "profile.default_content_setting_values.automatic_downloads": 1,
            "safebrowsing.enabled": True,
        },
    )
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1600,1200")

    try:
        driver = webdriver.Chrome(options=options)
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {
                "behavior": "allow",
                "downloadPath": str(download_dir.resolve()),
            },
        )
        return driver
    except Exception as exc:
        raise AutomationError("chrome_start", f"Chrome 실행 실패: {exc}") from exc


def wait_for_document(driver: webdriver.Chrome, timeout: int = 30) -> None:
    try:
        WebDriverWait(driver, timeout).until(
            lambda browser: browser.execute_script(
                "return document.readyState"
            )
            == "complete"
        )
    except TimeoutException as exc:
        raise AutomationError(
            "page_load",
            f"{timeout}초 안에 문서 로딩이 완료되지 않음: {driver.current_url}",
        ) from exc


def accept_alert(driver: webdriver.Chrome) -> str | None:
    try:
        alert = driver.switch_to.alert
        message = alert.text
        alert.accept()
        return message
    except NoAlertPresentException:
        return None


def login_submission_finished(driver: webdriver.Chrome) -> bool:
    """top 페이지 이동, iframe 응답 완료, 경고창 중 하나를 기다린다."""

    try:
        if "/member/login.do" not in driver.current_url:
            return True
        return bool(
            driver.execute_script(
                """
                const frame = document.getElementById('hidden_iframe');
                if (!frame) return false;
                try {
                    return frame.contentWindow.location.href !== 'about:blank'
                        && frame.contentDocument.readyState === 'complete';
                } catch (error) {
                    return false;
                }
                """
            )
        )
    except UnexpectedAlertPresentException:
        return True
    except StaleElementReferenceException:
        return False


def login(
    driver: webdriver.Chrome,
    login_id: str,
    password: str,
    verification_url: str,
    logger: logging.Logger,
) -> None:
    logger.info("stage=login status=START url=%s", LOGIN_URL)
    driver.get(LOGIN_URL)
    wait_for_document(driver)
    wait = WebDriverWait(driver, 30)

    try:
        id_input = wait.until(EC.visibility_of_element_located((By.ID, "loginId")))
        password_input = wait.until(
            EC.visibility_of_element_located((By.ID, "loginPw"))
        )
        login_button = wait.until(
            EC.element_to_be_clickable((By.ID, "loginBtn"))
        )
    except TimeoutException as exc:
        raise AutomationError(
            "login_form",
            "실제 DOM의 #loginId, #loginPw 또는 #loginBtn을 찾지 못함",
        ) from exc

    id_input.clear()
    id_input.send_keys(login_id)
    password_input.clear()
    password_input.send_keys(password)
    login_button.click()

    try:
        wait.until(login_submission_finished)
    except TimeoutException:
        # 성공 여부는 아래 상세 페이지의 #islogin 값으로 최종 확인한다.
        logger.warning("stage=login_submit status=TIMEOUT verification=CONTINUE")

    alert_message = accept_alert(driver)
    if alert_message:
        raise AutomationError("login_submit", f"사이트 경고: {alert_message}")

    driver.get(verification_url)
    wait_for_document(driver)
    try:
        login_state = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.ID, "islogin"))
        ).get_attribute("value")
    except TimeoutException as exc:
        raise AutomationError(
            "login_verify",
            "상세 페이지에서 로그인 상태 요소 #islogin을 찾지 못함",
        ) from exc

    if (login_state or "").strip().lower() != "true":
        raise AutomationError(
            "login_verify",
            "로그인 요청 후 상세 페이지의 #islogin 값이 true가 아님",
        )
    logger.info("stage=login status=OK")


def find_csv_checkbox(driver: webdriver.Chrome):
    """AJAX로 생성된 배포 목록 중 icon 텍스트가 CSV인 행을 반환한다."""

    rows = driver.find_elements(By.CSS_SELECTOR, "#distributionList li.tr")
    matches = []
    for row in rows:
        icons = {
            element.text.strip().lower()
            for element in row.find_elements(By.CSS_SELECTOR, "span.icon")
        }
        checkboxes = row.find_elements(
            By.CSS_SELECTOR,
            "input.distributionChk[name='chk_goods2'][data-id]",
        )
        if "csv" in icons and checkboxes:
            matches.append(checkboxes[0])
    return matches


def select_csv_file(
    driver: webdriver.Chrome,
    logger: logging.Logger,
    item_id: str,
):
    try:
        matches = WebDriverWait(driver, 40).until(
            lambda browser: find_csv_checkbox(browser) or False
        )
    except TimeoutException as exc:
        raise AutomationError(
            "csv_selection",
            "#distributionList에서 CSV 배포 상품을 찾지 못함",
        ) from exc

    checkbox = matches[0]
    if len(matches) > 1:
        versioned_matches = []
        for candidate in matches:
            candidate_title = (
                candidate.get_attribute("data-title") or ""
            ).strip()
            version_match = re.search(
                r"\((\d{4})(0[1-9]|1[0-2])\)$",
                candidate_title,
            )
            if version_match is None:
                raise AutomationError(
                    "csv_selection",
                    "여러 CSV 중 상품명 끝의 (YYYYMM)을 확인할 수 없음: "
                    f"{candidate_title!r}",
                )
            version = int("".join(version_match.groups()))
            versioned_matches.append((version, candidate))

        latest_version = max(version for version, _candidate in versioned_matches)
        latest_matches = [
            candidate
            for version, candidate in versioned_matches
            if version == latest_version
        ]
        if len(latest_matches) != 1:
            raise AutomationError(
                "csv_selection",
                f"최신 버전 ({latest_version}) CSV가 "
                f"{len(latest_matches)}개이므로 하나를 선택할 수 없음",
            )
        checkbox = latest_matches[0]

    if (checkbox.get_attribute("data-pricecknm") or "") not in {"무료", "샘플"}:
        raise AutomationError(
            "csv_selection",
            "CSV 배포 상품이 무료/샘플이 아니므로 자동 다운로드하지 않음",
        )

    title = (checkbox.get_attribute("data-title") or "").strip()
    driver.execute_script(
        "arguments[0].scrollIntoView({block: 'center'}); arguments[0].click();",
        checkbox,
    )
    try:
        WebDriverWait(driver, 10).until(lambda _browser: checkbox.is_selected())
    except TimeoutException as exc:
        raise AutomationError(
            "csv_selection",
            "CSV 상품 체크박스 선택 상태를 확인하지 못함",
        ) from exc

    logger.info(
        "stage=csv_selection status=OK product_id=%s file_id=%s title=%r",
        item_id,
        checkbox.get_attribute("data-id"),
        title,
    )
    return checkbox, title


def click_verified(driver: webdriver.Chrome, element) -> None:
    driver.execute_script(
        "arguments[0].scrollIntoView({block: 'center'}); arguments[0].click();",
        element,
    )


def fill_consent_popup(
    driver: webdriver.Chrome,
    purpose_code: str,
    other_purpose: str | None,
    logger: logging.Logger,
) -> None:
    wait = WebDriverWait(driver, 20)
    try:
        popup = wait.until(EC.visibility_of_element_located((By.ID, "down_layer")))
        purpose = popup.find_element(
            By.CSS_SELECTOR,
            f"input[name='use_code'][value='{purpose_code}']",
        )
        purchase_consent = popup.find_element(By.ID, "select")
        security_consent = popup.find_element(By.ID, "select2")
        confirm = popup.find_element(
            By.CSS_SELECTOR,
            "input.btn.btn_ok[onclick='fnSave()']",
        )
    except (TimeoutException, NoSuchElementException) as exc:
        raise AutomationError(
            "consent_popup",
            "#down_layer의 활용목적/필수동의/확인 요소를 찾지 못함",
        ) from exc

    click_verified(driver, purpose)
    if purpose_code == PURPOSE_CODES["other"]:
        if not other_purpose:
            raise AutomationError(
                "consent_popup",
                "기타 활용목적에는 --other-purpose가 필요함",
            )
        other_input = popup.find_element(By.CSS_SELECTOR, "input[name='etc_name']")
        other_input.clear()
        other_input.send_keys(other_purpose)

    click_verified(driver, purchase_consent)
    click_verified(driver, security_consent)
    try:
        wait.until(
            lambda _browser: purpose.is_selected()
            and purchase_consent.is_selected()
            and security_consent.is_selected()
        )
    except TimeoutException as exc:
        raise AutomationError(
            "consent_popup",
            "활용목적 또는 필수약관 선택 상태를 확인하지 못함",
        ) from exc

    logger.info(
        "stage=consent_popup status=ACCEPTED purpose_code=%s",
        purpose_code,
    )
    click_verified(driver, confirm)


def snapshot_files(download_dir: Path) -> set[Path]:
    return {path.resolve() for path in download_dir.iterdir() if path.is_file()}


def wait_for_download(
    driver: webdriver.Chrome,
    download_dir: Path,
    before: set[Path],
    timeout: int,
) -> Path:
    last_observed: list[str] = []

    def completed_file(_driver: webdriver.Chrome):
        nonlocal last_observed
        alert_message = accept_alert(driver)
        if alert_message:
            raise AutomationError("download", f"사이트 경고: {alert_message}")
        if "/member/login.do" in driver.current_url:
            raise AutomationError("download", "다운로드 도중 로그인 페이지로 이동됨")

        current = snapshot_files(download_dir)
        new_files = current - before
        last_observed = sorted(path.name for path in new_files)
        temporary = {
            path
            for path in new_files
            if path.suffix.lower() in TEMPORARY_SUFFIXES
        }
        finished = [
            path
            for path in new_files - temporary
            if path.exists()
            and path.suffix.lower() == ".csv"
            and path.stat().st_size > 0
        ]
        if temporary or not finished:
            return False
        return max(finished, key=lambda path: path.stat().st_mtime_ns)

    try:
        return WebDriverWait(driver, timeout, poll_frequency=1).until(completed_file)
    except TimeoutException as exc:
        raise AutomationError(
            "download_wait",
            f"{timeout}초 안에 다운로드 완료 안 됨; 관찰 파일={last_observed}",
        ) from exc


def validate_download(path: Path) -> None:
    with path.open("rb") as downloaded:
        prefix = downloaded.read(512).lstrip().lower()
    if prefix.startswith((b"<!doctype html", b"<html")):
        raise AutomationError(
            "download_validation",
            f"다운로드 결과가 CSV 데이터가 아니라 HTML임: {path.name}",
        )


def existing_csv_files(
    directory: Path,
    logger: logging.Logger,
) -> list[DownloadedFile]:
    """Map existing CSVs only when their canonical product filename is unambiguous."""

    if not directory.is_dir():
        raise AutomationError(
            "load_only",
            f"--load-only directory does not exist: {directory}",
        )

    paths = sorted(
        path
        for path in directory.iterdir()
        if path.is_file() and path.suffix.lower() == ".csv"
    )
    if not paths:
        raise AutomationError(
            "load_only",
            f"--load-only directory has no CSV files: {directory}",
        )

    results = []
    seen_product_ids: set[str] = set()
    for path in paths:
        matches = [
            item_id
            for item_id, file_stem in PRODUCT_FILE_STEMS.items()
            if re.fullmatch(
                rf"{re.escape(file_stem)}_\d{{6}}\.csv",
                path.name,
                flags=re.IGNORECASE,
            )
        ]
        if len(matches) != 1:
            raise AutomationError(
                "db_mapping",
                f"cannot safely map CSV filename to one product: {path.name}",
            )

        item_id = matches[0]
        if item_id in seen_product_ids:
            raise AutomationError(
                "db_mapping",
                f"multiple CSV files map to product {item_id}: {path.name}",
            )
        if item_id not in PRODUCT_TABLES:
            raise AutomationError(
                "db_mapping",
                f"raw table mapping is missing for product: {item_id}",
            )

        validate_download(path)
        resolved_path = path.resolve()
        results.append(
            DownloadedFile(
                product_id=item_id,
                title=path.stem,
                path=resolved_path,
                size=path.stat().st_size,
            )
        )
        seen_product_ids.add(item_id)
        logger.info(
            "stage=load_only_mapping status=OK product_id=%s file=%s",
            item_id,
            resolved_path,
        )
    return results


def validate_csv(path: Path, logger: logging.Logger) -> CsvValidation:
    """원본을 수정하지 않고 UTF-8 CSV 구조와 실제 레코드 수를 검증한다."""

    logger.info("stage=csv_validation status=START file=%s", path.resolve())
    if not path.is_file():
        raise AutomationError("csv_validation", f"CSV 파일 없음: {path}")
    if path.suffix.lower() != ".csv":
        raise AutomationError(
            "csv_validation",
            f"CSV 확장자가 아닌 다운로드 결과: {path.name}",
        )

    csv.field_size_limit(1024 * 1024 * 1024)
    row_count = 0
    null_count = 0
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.reader(source, escapechar="\\")
            try:
                columns = tuple(next(reader))
            except StopIteration as exc:
                raise AutomationError("csv_validation", "CSV가 비어 있음") from exc

            if not columns:
                raise AutomationError("csv_validation", "CSV 헤더가 비어 있음")
            if any(column == "" for column in columns):
                raise AutomationError("csv_validation", "이름이 없는 CSV 컬럼이 있음")
            if len(set(columns)) != len(columns):
                raise AutomationError("csv_validation", "중복된 CSV 컬럼명이 있음")

            width = len(columns)
            for line_number, row in enumerate(reader, start=2):
                if len(row) != width:
                    raise AutomationError(
                        "csv_validation",
                        f"{line_number}번째 CSV 레코드의 컬럼 수가 "
                        f"헤더와 다름: expected={width}, actual={len(row)}",
                    )
                if any("\x00" in value for value in row):
                    raise AutomationError(
                        "csv_validation",
                        f"{line_number}번째 CSV 레코드에 PostgreSQL TEXT가 "
                        "허용하지 않는 NUL 문자가 있음",
                    )
                row_count += 1
                null_count += sum(value == "" for value in row)
    except UnicodeDecodeError as exc:
        raise AutomationError(
            "csv_validation",
            f"UTF-8 CSV 디코딩 실패: byte={exc.start}",
        ) from exc
    except csv.Error as exc:
        raise AutomationError(
            "csv_validation",
            f"CSV 파싱 실패: {exc}",
        ) from exc

    if row_count == 0:
        raise AutomationError("csv_validation", "헤더 외 데이터 행이 없음")

    result = CsvValidation(
        path=path.resolve(),
        columns=columns,
        row_count=row_count,
        null_count=null_count,
    )
    logger.info(
        "stage=csv_validation status=OK file=%s encoding=%s columns=%s "
        "rows=%s null_fields=%s",
        result.path,
        result.encoding,
        len(result.columns),
        result.row_count,
        result.null_count,
    )
    return result


def connect_database(config: DatabaseConfig):
    try:
        connection = psycopg.connect(
            host=config.host,
            port=config.port,
            dbname=config.dbname,
            user=config.user,
            password=config.password,
            connect_timeout=15,
            autocommit=True,
        )
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database()")
            connected_database = cursor.fetchone()[0]
        if connected_database != EXPECTED_DB_NAME:
            connection.close()
            raise AutomationError(
                "db_connection",
                f"연결된 DB가 {EXPECTED_DB_NAME!r}이 아님",
            )
        return connection
    except AutomationError:
        raise
    except Exception as exc:
        sqlstate = getattr(exc, "sqlstate", None) or "unknown"
        raise AutomationError(
            "db_connection",
            f"PostgreSQL {EXPECTED_DB_NAME} 연결 실패: "
            f"error_type={type(exc).__name__}, sqlstate={sqlstate}",
        ) from exc


def create_text_table(cursor, table_name: str, columns: Sequence[str]) -> None:
    column_definitions = sql.SQL(", ").join(
        sql.SQL("{} TEXT").format(sql.Identifier(column))
        for column in columns
    )
    cursor.execute(
        sql.SQL("CREATE TABLE {}.{} ({})").format(
            sql.Identifier(RAW_SCHEMA),
            sql.Identifier(table_name),
            column_definitions,
        )
    )


def copy_csv_rows(
    cursor,
    validation: CsvValidation,
    staging_table: str,
) -> int:
    """빈 문자열만 None으로 바꾸고 나머지 원본 문자열은 그대로 COPY한다."""

    copy_statement = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
        sql.Identifier(RAW_SCHEMA),
        sql.Identifier(staging_table),
        sql.SQL(", ").join(
            sql.Identifier(column) for column in validation.columns
        ),
    )
    copied_rows = 0
    with validation.path.open(
        "r",
        encoding=validation.encoding,
        newline="",
    ) as source:
        reader = csv.reader(source, escapechar="\\")
        next(reader)
        with cursor.copy(copy_statement) as copy:
            for row in reader:
                copy.write_row(
                    tuple(None if value == "" else value for value in row)
                )
                copied_rows += 1
    return copied_rows


def table_row_count(cursor, table_name: str) -> int:
    cursor.execute(
        sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(RAW_SCHEMA),
            sql.Identifier(table_name),
        )
    )
    return int(cursor.fetchone()[0])


def schema_table_row_count(cursor, schema_name: str, table_name: str) -> int:
    cursor.execute(
        sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )
    )
    return int(cursor.fetchone()[0])


def table_columns(cursor, schema_name: str, table_name: str) -> tuple[str, ...]:
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema_name, table_name),
    )
    return tuple(row[0] for row in cursor.fetchall())


def normalized_text_expression(column: str):
    identifier = sql.Identifier(column)
    null_literals = sql.SQL(", ").join(
        sql.Literal(value) for value in NULL_LITERALS
    )
    return sql.SQL(
        "CASE WHEN {} IS NULL OR lower(btrim({})) IN ({}) "
        "THEN NULL ELSE {} END"
    ).format(identifier, identifier, null_literals, identifier)


def numeric_coordinate_expression(column: str):
    identifier = sql.Identifier(column)
    normalized = normalized_text_expression(column)
    return sql.SQL(
        "CASE WHEN ({}) IS NOT NULL AND btrim({}) ~ {} "
        "THEN btrim({})::double precision ELSE NULL END"
    ).format(
        normalized,
        identifier,
        sql.Literal(NUMERIC_PATTERN),
        identifier,
    )


def joined_predicate(parts, operator: str, default: str):
    parts = list(parts)
    if not parts:
        return sql.SQL(default)
    return sql.SQL(f" {operator} ").join(
        sql.SQL("({})").format(part) for part in parts
    )


def load_raw_to_processed(
    table_name: str,
    config: DatabaseConfig,
    logger: logging.Logger,
) -> ProcessedLoad:
    """SQL로 NULL/좌표를 정제한 뒤 processed 테이블을 원자 교체한다."""

    try:
        coordinate_policy = TABLE_COORDINATES[table_name]
    except KeyError as exc:
        raise AutomationError(
            "processed_mapping",
            f"processed 좌표 정책이 없는 raw 테이블: {table_name}",
        ) from exc

    required_pairs = coordinate_policy["required"]
    optional_pairs = coordinate_policy["optional"]
    coordinate_columns = {
        column
        for pair in (*required_pairs, *optional_pairs)
        for column in pair
    }
    staging_table = f"{table_name}__staging"
    logger.info(
        "stage=processed_load status=START source=%s.%s target=%s.%s",
        RAW_SCHEMA,
        table_name,
        PROCESSED_SCHEMA,
        table_name,
    )

    connection = connect_database(config)
    try:
        with connection.transaction():
            with connection.cursor() as cursor:
                # raw 교체와 processed 교체 모두 같은 순서로 직렬화한다.
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"{RAW_SCHEMA}.{table_name}",),
                )
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"{PROCESSED_SCHEMA}.{table_name}",),
                )

                columns = table_columns(cursor, RAW_SCHEMA, table_name)
                if not columns:
                    raise AutomationError(
                        "processed_source",
                        f"raw 테이블이 없거나 컬럼이 없음: {RAW_SCHEMA}.{table_name}",
                    )
                missing_columns = sorted(coordinate_columns - set(columns))
                if missing_columns:
                    raise AutomationError(
                        "processed_mapping",
                        f"{RAW_SCHEMA}.{table_name}에 좌표 컬럼이 없음: "
                        f"{missing_columns}",
                    )

                null_terms = []
                for column in columns:
                    if column in coordinate_columns:
                        null_terms.append(
                            sql.SQL(
                                "CASE WHEN {} IS NOT NULL AND {} IS NULL "
                                "THEN 1 ELSE 0 END"
                            ).format(
                                sql.Identifier(column),
                                numeric_coordinate_expression(column),
                            )
                        )
                    else:
                        null_terms.append(
                            sql.SQL(
                                "CASE WHEN {} IS NOT NULL "
                                "AND lower(btrim({})) IN ({}) "
                                "THEN 1 ELSE 0 END"
                            ).format(
                                sql.Identifier(column),
                                sql.Identifier(column),
                                sql.SQL(", ").join(
                                    sql.Literal(value)
                                    for value in NULL_LITERALS
                                ),
                            )
                        )
                missing_predicate = joined_predicate(
                    (
                        sql.SQL("{} IS NULL").format(
                            numeric_coordinate_expression(column)
                        )
                        for pair in required_pairs
                        for column in pair
                    ),
                    "OR",
                    "FALSE",
                )
                range_predicate = joined_predicate(
                    (
                        sql.SQL("{} NOT BETWEEN {} AND {}").format(
                            numeric_coordinate_expression(column),
                            sql.Literal(
                                KOREA_LATITUDE_RANGE[0]
                                if index == 0
                                else KOREA_LONGITUDE_RANGE[0]
                            ),
                            sql.Literal(
                                KOREA_LATITUDE_RANGE[1]
                                if index == 0
                                else KOREA_LONGITUDE_RANGE[1]
                            ),
                        )
                        for pair in required_pairs
                        for index, column in enumerate(pair)
                    ),
                    "OR",
                    "FALSE",
                )
                cursor.execute(
                    sql.SQL(
                        "SELECT COUNT(*), COALESCE(SUM({}), 0), "
                        "COUNT(*) FILTER (WHERE {}), "
                        "COUNT(*) FILTER (WHERE NOT ({}) AND ({})) "
                        "FROM {}.{}"
                    ).format(
                        sql.SQL(" + ").join(null_terms),
                        missing_predicate,
                        missing_predicate,
                        range_predicate,
                        sql.Identifier(RAW_SCHEMA),
                        sql.Identifier(table_name),
                    )
                )
                (
                    raw_row_count,
                    null_conversion_count,
                    coordinate_missing_removed,
                    coordinate_range_removed,
                ) = (int(value) for value in cursor.fetchone())

                cursor.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                        sql.Identifier(PROCESSED_SCHEMA)
                    )
                )
                cursor.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                        sql.Identifier(PROCESSED_SCHEMA),
                        sql.Identifier(staging_table),
                    )
                )
                select_columns = [
                    sql.SQL("{} AS {}").format(
                        (
                            numeric_coordinate_expression(column)
                            if column in coordinate_columns
                            else normalized_text_expression(column)
                        ),
                        sql.Identifier(column),
                    )
                    for column in columns
                ]
                cursor.execute(
                    sql.SQL(
                        "CREATE TABLE {}.{} AS SELECT {} FROM {}.{} "
                        "WHERE NOT ({}) AND NOT ({})"
                    ).format(
                        sql.Identifier(PROCESSED_SCHEMA),
                        sql.Identifier(staging_table),
                        sql.SQL(", ").join(select_columns),
                        sql.Identifier(RAW_SCHEMA),
                        sql.Identifier(table_name),
                        missing_predicate,
                        range_predicate,
                    )
                )
                processed_row_count = schema_table_row_count(
                    cursor,
                    PROCESSED_SCHEMA,
                    staging_table,
                )
                expected_count = (
                    raw_row_count
                    - coordinate_missing_removed
                    - coordinate_range_removed
                )
                if processed_row_count != expected_count:
                    raise AutomationError(
                        "processed_row_count_validation",
                        f"processed staging 행 수 불일치: raw={raw_row_count}, "
                        f"missing_removed={coordinate_missing_removed}, "
                        f"range_removed={coordinate_range_removed}, "
                        f"expected={expected_count}, actual={processed_row_count}",
                    )

                cursor.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                        sql.Identifier(PROCESSED_SCHEMA),
                        sql.Identifier(table_name),
                    )
                )
                cursor.execute(
                    sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                        sql.Identifier(PROCESSED_SCHEMA),
                        sql.Identifier(staging_table),
                        sql.Identifier(table_name),
                    )
                )

        with connection.cursor() as cursor:
            committed_count = schema_table_row_count(
                cursor,
                PROCESSED_SCHEMA,
                table_name,
            )
        if committed_count != processed_row_count:
            raise AutomationError(
                "processed_row_count_validation",
                f"커밋 후 processed 행 수 불일치: "
                f"before={processed_row_count}, after={committed_count}",
            )
    except AutomationError:
        raise
    except Exception as exc:
        raise AutomationError(
            "processed_load",
            f"{RAW_SCHEMA}.{table_name} → {PROCESSED_SCHEMA}.{table_name} "
            f"변환 실패: {exc}",
        ) from exc
    finally:
        connection.close()

    logger.info(
        "stage=processed_load status=OK source=%s.%s target=%s.%s "
        "raw_rows=%s processed_rows=%s null_conversions=%s "
        "coordinate_missing_removed=%s coordinate_range_removed=%s",
        RAW_SCHEMA,
        table_name,
        PROCESSED_SCHEMA,
        table_name,
        raw_row_count,
        committed_count,
        null_conversion_count,
        coordinate_missing_removed,
        coordinate_range_removed,
    )
    return ProcessedLoad(
        schema=PROCESSED_SCHEMA,
        table=table_name,
        raw_row_count=raw_row_count,
        processed_row_count=committed_count,
        null_conversion_count=null_conversion_count,
        coordinate_missing_removed=coordinate_missing_removed,
        coordinate_range_removed=coordinate_range_removed,
    )


def load_csv_to_raw(
    validation: CsvValidation,
    table_name: str,
    config: DatabaseConfig,
    logger: logging.Logger,
) -> DatabaseLoad:
    """스테이징 적재와 행 수 검증 후 대상 raw 테이블을 원자적으로 교체한다."""

    staging_table = f"{table_name}__staging"
    logger.info(
        "stage=db_load status=START target=%s.%s csv_rows=%s file=%s",
        RAW_SCHEMA,
        table_name,
        validation.row_count,
        validation.path,
    )

    connection = connect_database(config)
    try:
        with connection.transaction():
            with connection.cursor() as cursor:
                # 동일 테이블에 대한 동시 실행을 직렬화한다.
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"{RAW_SCHEMA}.{table_name}",),
                )
                cursor.execute(
                    sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                        sql.Identifier(RAW_SCHEMA)
                    )
                )
                cursor.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                        sql.Identifier(RAW_SCHEMA),
                        sql.Identifier(staging_table),
                    )
                )
                create_text_table(cursor, staging_table, validation.columns)
                copied_rows = copy_csv_rows(cursor, validation, staging_table)
                staging_count = table_row_count(cursor, staging_table)

                if copied_rows != validation.row_count:
                    raise AutomationError(
                        "db_copy_validation",
                        f"CSV 검증 행 수와 COPY 처리 행 수 불일치: "
                        f"csv={validation.row_count}, copied={copied_rows}",
                    )
                if staging_count != validation.row_count:
                    raise AutomationError(
                        "db_copy_validation",
                        f"CSV와 staging DB 행 수 불일치: "
                        f"csv={validation.row_count}, db={staging_count}",
                    )

                # 기존 대상은 새 스테이징 검증이 끝난 뒤 같은 트랜잭션에서만 교체한다.
                cursor.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                        sql.Identifier(RAW_SCHEMA),
                        sql.Identifier(table_name),
                    )
                )
                cursor.execute(
                    sql.SQL("ALTER TABLE {}.{} RENAME TO {}").format(
                        sql.Identifier(RAW_SCHEMA),
                        sql.Identifier(staging_table),
                        sql.Identifier(table_name),
                    )
                )
                final_count = table_row_count(cursor, table_name)
                if final_count != validation.row_count:
                    raise AutomationError(
                        "db_row_count_validation",
                        f"CSV와 최종 DB 행 수 불일치: "
                        f"csv={validation.row_count}, db={final_count}",
                    )

        # 커밋 이후 별도 조회로 최종 가시 행 수도 다시 확인한다.
        with connection.cursor() as cursor:
            committed_count = table_row_count(cursor, table_name)
        if committed_count != validation.row_count:
            raise AutomationError(
                "db_row_count_validation",
                f"커밋 후 CSV와 DB 행 수 불일치: "
                f"csv={validation.row_count}, db={committed_count}",
            )
    except AutomationError:
        raise
    except Exception as exc:
        raise AutomationError(
            "db_load",
            f"{RAW_SCHEMA}.{table_name} 적재 실패: {exc}",
        ) from exc
    finally:
        connection.close()

    logger.info(
        "stage=db_row_count_validation status=OK target=%s.%s "
        "csv_rows=%s db_rows=%s",
        RAW_SCHEMA,
        table_name,
        validation.row_count,
        committed_count,
    )
    return DatabaseLoad(
        schema=RAW_SCHEMA,
        table=table_name,
        csv_row_count=validation.row_count,
        db_row_count=committed_count,
    )


def download_csv_product(
    driver: webdriver.Chrome,
    url: str,
    download_dir: Path,
    purpose_code: str,
    other_purpose: str | None,
    timeout: int,
    logger: logging.Logger,
) -> DownloadedFile:
    item_id = product_id(url)
    logger.info("stage=detail_page status=START product_id=%s url=%s", item_id, url)
    driver.get(url)
    wait_for_document(driver)

    try:
        login_state = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "islogin"))
        ).get_attribute("value")
    except TimeoutException as exc:
        raise AutomationError(
            "detail_page",
            "상세 페이지에서 #islogin을 찾지 못함",
        ) from exc
    if (login_state or "").strip().lower() != "true":
        raise AutomationError("detail_page", "상세 페이지에서 로그인 세션이 확인되지 않음")

    _checkbox, title = select_csv_file(driver, logger, item_id)
    before = snapshot_files(download_dir)

    try:
        download_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.ID, "download"))
        )
    except TimeoutException as exc:
        raise AutomationError(
            "download_button",
            "실제 DOM의 다운로드 버튼 #download를 클릭할 수 없음",
        ) from exc
    click_verified(driver, download_button)
    fill_consent_popup(
        driver=driver,
        purpose_code=purpose_code,
        other_purpose=other_purpose,
        logger=logger,
    )

    path = wait_for_download(
        driver=driver,
        download_dir=download_dir,
        before=before,
        timeout=timeout,
    )
    validate_download(path)
    size = path.stat().st_size
    logger.info(
        "stage=download status=OK product_id=%s title=%r file=%s bytes=%s",
        item_id,
        title,
        path.resolve(),
        size,
    )
    return DownloadedFile(item_id, title, path.resolve(), size)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        action="append",
        dest="urls",
        help="대상 상세 URL. 여러 번 지정 가능하며 기본값은 요청된 두 상품",
    )
    parser.add_argument(
        "--download-dir",
        type=Path,
        default=DEFAULT_DOWNLOAD_DIR,
        help="원본 보존용 실행별 하위 디렉터리를 만들 기준 경로",
    )
    parser.add_argument(
        "--load-only",
        type=Path,
        help="skip Selenium and load existing CSV files from this directory",
    )
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument(
        "--purpose",
        choices=sorted(PURPOSE_CODES),
        default="business",
        help="데이터 활용목적(기본: business/업무활용)",
    )
    parser.add_argument("--other-purpose")
    parser.add_argument(
        "--download-timeout",
        type=int,
        default=3600,
        help="상품별 다운로드 완료 대기 초(기본: 3600)",
    )
    parser.add_argument("--headless", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    urls = tuple(args.urls or TARGET_URLS)
    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    run_download_dir = args.load_only or (args.download_dir / run_stamp)
    logger, log_path = configure_logging(args.log_dir)
    logger.info(
        "stage=run status=START mode=%s products=%s download_dir=%s log=%s",
        "load_only" if args.load_only else "download",
        "existing" if args.load_only else len(urls),
        run_download_dir.resolve(),
        log_path.resolve(),
    )

    driver: webdriver.Chrome | None = None
    try:
        if args.load_only and args.urls:
            raise AutomationError(
                "arguments",
                "--load-only cannot be combined with --url",
            )
        if not args.load_only and args.purpose == "other" and not args.other_purpose:
            raise AutomationError(
                "arguments",
                "--purpose other에는 --other-purpose가 필요함",
            )
        database_config = load_database_config()
        if args.load_only:
            results = existing_csv_files(args.load_only, logger)
            logger.info(
                "stage=load_only_discovery status=OK files=%s",
                len(results),
            )
        else:
            login_id, password = load_credentials()
            driver = create_driver(run_download_dir, args.headless)
            logger.info("stage=chrome_start status=OK headless=%s", args.headless)
            login(driver, login_id, password, urls[0], logger)

            results = [
                download_csv_product(
                    driver=driver,
                    url=url,
                    download_dir=run_download_dir,
                    purpose_code=PURPOSE_CODES[args.purpose],
                    other_purpose=args.other_purpose,
                    timeout=args.download_timeout,
                    logger=logger,
                )
                for url in urls
            ]
            for result in results:
                print(
                    f"DOWNLOAD_OK product_id={result.product_id} "
                    f"bytes={result.size} path={result.path}"
                )
            logger.info(
                "stage=download_pipeline status=OK downloaded=%s",
                len(results),
            )
            driver.quit()
            driver = None

        # 다운로드 완료 이후 Chrome을 닫아 원본 파일 핸들을 확실히 해제한다.
        validations = [validate_csv(result.path, logger) for result in results]
        database_loads = []
        for result, validation in zip(results, validations, strict=True):
            try:
                table_name = PRODUCT_TABLES[result.product_id]
            except KeyError as exc:
                raise AutomationError(
                    "db_mapping",
                    f"raw 테이블 매핑이 없는 상품: {result.product_id}",
                ) from exc
            database_loads.append(
                load_csv_to_raw(
                    validation=validation,
                    table_name=table_name,
                    config=database_config,
                    logger=logger,
                )
            )

        for loaded in database_loads:
            print(
                f"DB_LOAD_OK table={loaded.schema}.{loaded.table} "
                f"csv_rows={loaded.csv_row_count} db_rows={loaded.db_row_count}"
            )

        
        logger.info(
            "stage=run status=OK downloaded=%s raw_loaded_tables=%s",
            len(results),
            len(database_loads),
        )
        return 0
    except AutomationError as exc:
        logger.error("stage=%s status=FAILED reason=%r", exc.stage, exc.reason)
        return 2
    except Exception as exc:
        logger.exception("stage=unexpected status=FAILED reason=%r", str(exc))
        return 1
    finally:
        if driver is not None:
            driver.quit()


if __name__ == "__main__":
    sys.exit(main())
