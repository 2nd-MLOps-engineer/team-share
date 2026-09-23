"""문화 빅데이터 플랫폼 배포 파일 다운로드 자동화.

이 collector는 기존 파이프라인과 독립적으로 실행된다. 사이트가 요구하는 로그인
세션과 "데이터 활용목적" 팝업을 Selenium으로 처리하고, 성공 여부는 HTTP 200이
아니라 브라우저 다운로드 디렉터리에 실제 파일이 생성됐는지로 판단한다.

최초 실행 준비::

    python -m pip install selenium

비로그인 상태/현재 배포 파일만 점검::

    python culture_bigdata_downloader.py --probe-only

전용 Chrome 프로필에 로그인 세션을 저장하는 최초 대화형 실행::

    python culture_bigdata_downloader.py --login-wait-seconds 300

같은 ``--user-data-dir``를 계속 사용하면 이후 실행에서 저장된 세션을 재사용할 수
있다. 아이디/비밀번호는 코드나 로그에 저장하지 않는다.
"""

from __future__ import annotations

import argparse
import http.cookiejar
import logging
import re
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Sequence


BASE_URL = "https://www.bigdata-culture.kr"
DETAIL_URLS = (
    f"{BASE_URL}/bigdata/user/data_market/detail.do"
    "?id=b5880ea0-247a-4258-9f7b-79eab6751591",
    f"{BASE_URL}/bigdata/user/data_market/detail.do"
    "?id=c3b8fb69-307d-4ae7-ab42-d0314c89ef47",
    # 체력 측정별 운동처방 데이터
    f"{BASE_URL}/bigdata/user/data_market/detail.do"
    "?id=2b1c565d-5f37-4152-966d-5f8094f8cf33",
)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DOWNLOAD_DIR = PROJECT_ROOT / "data" / "raw" / "culture_bigdata"
DEFAULT_LOG_DIR = PROJECT_ROOT / "data" / "logs"
DEFAULT_PROFILE_DIR = PROJECT_ROOT / "data" / "browser_profiles" / "culture_bigdata"

PURPOSES = {
    "research": "004001001",
    "startup": "004001002",
    "business": "004001003",
    "other": "004001004",
}
TEMP_DOWNLOAD_SUFFIXES = {".crdownload", ".part", ".tmp"}


@dataclass(frozen=True)
class DistributionFile:
    file_id: str
    title: str
    price_label: str
    icons: tuple[str, ...] = field(default_factory=tuple)

    @property
    def format(self) -> str:
        for icon in self.icons:
            if icon.lower() in {"csv", "json", "xml", "zip", "xlsx"}:
                return icon.lower()
        return "unknown"


class DistributionListParser(HTMLParser):
    """distributionList 조각 HTML에서 파일 메타데이터만 추출한다."""

    def __init__(self) -> None:
        super().__init__()
        self.files: list[DistributionFile] = []
        self._row: dict[str, object] | None = None
        self._capture_icon = False
        self._icon_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {key: value or "" for key, value in attrs}
        classes = set(attributes.get("class", "").split())

        if tag == "li" and "tr" in classes:
            self._row = {"icons": []}
            return

        if self._row is None:
            return

        if tag == "input" and "distributionChk" in classes:
            self._row.update(
                file_id=attributes.get("data-id", ""),
                title=attributes.get("data-title", ""),
                price_label=attributes.get("data-pricecknm", ""),
            )
        elif tag == "span" and "icon" in classes:
            self._capture_icon = True
            self._icon_text = []

    def handle_data(self, data: str) -> None:
        if self._capture_icon:
            self._icon_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "span" and self._capture_icon:
            icon = " ".join("".join(self._icon_text).split())
            if icon and self._row is not None:
                icons = self._row["icons"]
                assert isinstance(icons, list)
                icons.append(icon)
            self._capture_icon = False
            self._icon_text = []
            return

        if tag == "li" and self._row is not None:
            file_id = str(self._row.get("file_id", ""))
            if file_id:
                icons = tuple(str(value) for value in self._row.get("icons", []))
                self.files.append(
                    DistributionFile(
                        file_id=file_id,
                        title=str(self._row.get("title", "")),
                        price_label=str(self._row.get("price_label", "")),
                        icons=icons,
                    )
                )
            self._row = None


class BlockedAutomation(RuntimeError):
    def __init__(self, stage: str, reason: str) -> None:
        super().__init__(reason)
        self.stage = stage
        self.reason = reason


def product_id(detail_url: str) -> str:
    query = urllib.parse.parse_qs(urllib.parse.urlparse(detail_url).query)
    values = query.get("id", [])
    if not values:
        raise ValueError(f"상품 ID가 없는 URL입니다: {detail_url}")
    return values[0]


def setup_logging(log_dir: Path) -> tuple[logging.Logger, Path]:
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"culture_bigdata_download_{timestamp}.log"

    logger = logging.getLogger("culture_bigdata_downloader")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger, log_path


def build_http_opener() -> urllib.request.OpenerDirector:
    cookie_jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
    opener.addheaders = [
        (
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 Chrome/140 Safari/537.36",
        )
    ]
    return opener


def fetch_distribution_files(
    opener: urllib.request.OpenerDirector,
    item_id: str,
) -> list[DistributionFile]:
    body = urllib.parse.urlencode(
        {
            "TP": "distributionList",
            "id": item_id,
            "currentPage": "1",
            "maxRows": "50",
        }
    ).encode("ascii")
    request = urllib.request.Request(
        f"{BASE_URL}/bigdata/user/data_market/process.ajax.do",
        data=body,
    )
    with opener.open(request, timeout=30) as response:
        html = response.read().decode("utf-8")

    parser = DistributionListParser()
    parser.feed(html)
    return parser.files


def probe_without_login(urls: Sequence[str], logger: logging.Logger) -> bool:
    """공개 상세/목록 접근과 비로그인 직접 다운로드의 차단을 확인한다."""

    all_ok = True
    opener = build_http_opener()

    for detail_url in urls:
        item_id = product_id(detail_url)
        logger.info("stage=detail_probe status=START product_id=%s", item_id)
        try:
            with opener.open(detail_url, timeout=30) as response:
                detail_html = response.read().decode("utf-8")
                logger.info(
                    "stage=detail_probe status=OK product_id=%s http_status=%s bytes=%s",
                    item_id,
                    response.status,
                    len(detail_html.encode("utf-8")),
                )

            files = fetch_distribution_files(opener, item_id)
            if not files:
                raise RuntimeError("배포 파일 목록을 찾지 못했습니다")

            for file_info in files:
                logger.info(
                    "stage=distribution_probe status=FOUND product_id=%s "
                    "file_id=%s format=%s title=%r price=%r metadata=%r",
                    item_id,
                    file_info.file_id,
                    file_info.format,
                    file_info.title,
                    file_info.price_label,
                    file_info.icons,
                )

            first_file = files[0]
            query = urllib.parse.urlencode(
                {
                    "type": "distribution",
                    "re_id": item_id,
                    "title": first_file.title,
                    "file_id": first_file.file_id,
                    "use_name": "업무활용",
                    "use_common_code": PURPOSES["business"],
                }
            )
            download_url = (
                f"{BASE_URL}/bigdata/user/data_market/process.file.do?{query}"
            )
            request = urllib.request.Request(download_url, headers={"Range": "bytes=0-8191"})
            with opener.open(request, timeout=30) as response:
                prefix = response.read(8192)
                content_type = response.headers.get("Content-Type", "")
                disposition = response.headers.get("Content-Disposition", "")

            is_html = "text/html" in content_type.lower() or prefix.lstrip().startswith(b"<")
            if is_html:
                decoded = prefix.decode("utf-8", errors="replace")
                login_marker = (
                    "/bigdata/user/member/login.do" in decoded
                    or "로그인" in decoded
                )
                logger.warning(
                    "stage=direct_download_probe status=BLOCKED_AUTH product_id=%s "
                    "http_status=200 content_type=%r content_disposition=%r "
                    "login_marker=%s reason=%r",
                    item_id,
                    content_type,
                    disposition,
                    login_marker,
                    "파일 대신 로그인 안내 HTML 응답",
                )
            else:
                logger.warning(
                    "stage=direct_download_probe status=UNEXPECTED_FILE product_id=%s "
                    "content_type=%r content_disposition=%r",
                    item_id,
                    content_type,
                    disposition,
                )
        except Exception as exc:  # 네트워크/사이트 변경도 로그에 정확히 남긴다.
            all_ok = False
            logger.exception(
                "stage=probe status=FAILED product_id=%s reason=%r",
                item_id,
                str(exc),
            )

    return all_ok


def create_driver(
    download_dir: Path,
    user_data_dir: Path,
    headless: bool,
    logger: logging.Logger,
):
    try:
        from selenium import webdriver
    except ImportError as exc:
        raise BlockedAutomation(
            "driver_init",
            "selenium 미설치: python -m pip install selenium",
        ) from exc

    download_dir.mkdir(parents=True, exist_ok=True)
    user_data_dir.mkdir(parents=True, exist_ok=True)

    options = webdriver.ChromeOptions()
    options.add_argument(f"--user-data-dir={user_data_dir.resolve()}")
    options.add_argument("--disable-notifications")
    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": str(download_dir.resolve()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        },
    )
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--window-size=1600,1200")

    logger.info(
        "stage=driver_init status=START browser=chrome headless=%s profile=%s",
        headless,
        user_data_dir.resolve(),
    )
    try:
        driver = webdriver.Chrome(options=options)
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {"behavior": "allow", "downloadPath": str(download_dir.resolve())},
        )
    except Exception as exc:
        raise BlockedAutomation("driver_init", f"Chrome 시작 실패: {exc}") from exc

    logger.info("stage=driver_init status=OK")
    return driver


def is_logged_in(driver) -> bool:
    from selenium.webdriver.common.by import By

    elements = driver.find_elements(By.CSS_SELECTOR, "#islogin")
    if not elements:
        return False
    return (elements[0].get_attribute("value") or "").strip().lower() == "true"


def wait_for_manual_login(
    driver,
    detail_url: str,
    seconds: int,
    logger: logging.Logger,
) -> None:
    if seconds <= 0:
        raise BlockedAutomation(
            "authentication",
            "로그인 세션 없음. --login-wait-seconds 또는 로그인된 --user-data-dir 필요",
        )

    if "--headless" in " ".join(driver.capabilities.get("goog:chromeOptions", {}).get("args", [])):
        raise BlockedAutomation(
            "authentication",
            "headless 모드에서는 수동 로그인 대기를 사용할 수 없습니다",
        )

    login_url = f"{BASE_URL}/bigdata/user/member/login.do"
    logger.warning(
        "stage=authentication status=WAITING seconds=%s action=%r",
        seconds,
        "열린 Chrome에서 사용자가 직접 로그인",
    )
    driver.get(login_url)
    deadline = time.monotonic() + seconds

    while time.monotonic() < deadline:
        current = urllib.parse.urlparse(driver.current_url)
        returned_to_platform = (
            current.hostname == "www.bigdata-culture.kr"
            and "/member/login.do" not in current.path
        )
        if returned_to_platform:
            driver.get(detail_url)
            if is_logged_in(driver):
                logger.info("stage=authentication status=OK method=manual_or_saved_session")
                return
            driver.get(login_url)
        time.sleep(2)

    raise BlockedAutomation("authentication", f"{seconds}초 안에 로그인이 확인되지 않음")


def accept_alert_if_present(driver) -> str | None:
    from selenium.common.exceptions import NoAlertPresentException

    try:
        alert = driver.switch_to.alert
        message = alert.text
        alert.accept()
        return message
    except NoAlertPresentException:
        return None


def wait_for_downloads(
    driver,
    download_dir: Path,
    previous_files: set[Path],
    expected_count: int,
    timeout_seconds: int,
    logger: logging.Logger,
) -> list[Path]:
    deadline = time.monotonic() + timeout_seconds

    while time.monotonic() < deadline:
        alert_message = accept_alert_if_present(driver)
        if alert_message:
            if "로그인" in alert_message:
                raise BlockedAutomation("download", f"사이트 로그인 경고: {alert_message}")
            raise BlockedAutomation("download", f"사이트 경고창: {alert_message}")

        if "/member/login.do" in driver.current_url:
            raise BlockedAutomation("download", "다운로드 중 로그인 페이지로 이동됨")

        current_files = {path for path in download_dir.iterdir() if path.is_file()}
        new_files = current_files - previous_files
        temporary = {
            path for path in new_files if path.suffix.lower() in TEMP_DOWNLOAD_SUFFIXES
        }
        completed = sorted(new_files - temporary)

        if len(completed) >= expected_count and not temporary:
            return completed
        time.sleep(1)

    current_files = {path for path in download_dir.iterdir() if path.is_file()}
    new_files = current_files - previous_files
    partial_names = sorted(
        path.name
        for path in new_files
        if path.suffix.lower() in TEMP_DOWNLOAD_SUFFIXES
    )
    logger.error(
        "stage=download_wait status=TIMEOUT expected=%s observed=%s partial=%r",
        expected_count,
        len(new_files),
        partial_names,
    )
    raise BlockedAutomation(
        "download_wait",
        f"{timeout_seconds}초 안에 다운로드 완료 안 됨; partial={partial_names}",
    )


def download_product(
    driver,
    detail_url: str,
    download_dir: Path,
    purpose_code: str,
    other_purpose: str | None,
    login_wait_seconds: int,
    download_timeout_seconds: int,
    logger: logging.Logger,
) -> list[Path]:
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as ec
    from selenium.webdriver.support.ui import WebDriverWait

    item_id = product_id(detail_url)
    logger.info("stage=page_open status=START product_id=%s url=%s", item_id, detail_url)
    driver.get(detail_url)
    WebDriverWait(driver, 30).until(
        lambda browser: browser.execute_script("return document.readyState") == "complete"
    )
    logger.info("stage=page_open status=OK product_id=%s", item_id)

    if not is_logged_in(driver):
        wait_for_manual_login(driver, detail_url, login_wait_seconds, logger)

    checkboxes = WebDriverWait(driver, 30).until(
        ec.presence_of_all_elements_located(
            (By.CSS_SELECTOR, "input.distributionChk[data-id]")
        )
    )
    free_checkboxes = [
        checkbox
        for checkbox in checkboxes
        if (checkbox.get_attribute("data-pricecknm") or "") in {"무료", "샘플"}
    ]
    if not free_checkboxes:
        raise BlockedAutomation("file_selection", "무료/샘플 배포 파일을 찾지 못함")
    if len(free_checkboxes) > 10:
        raise BlockedAutomation("file_selection", "사이트 멀티 다운로드 제한(10개) 초과")

    selected_metadata = []
    for checkbox in free_checkboxes:
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", checkbox)
        if not checkbox.is_selected():
            driver.execute_script("arguments[0].click();", checkbox)
        selected_metadata.append(
            {
                "file_id": checkbox.get_attribute("data-id"),
                "title": checkbox.get_attribute("data-title"),
                "price": checkbox.get_attribute("data-pricecknm"),
            }
        )
    logger.info(
        "stage=file_selection status=OK product_id=%s count=%s files=%r",
        item_id,
        len(free_checkboxes),
        selected_metadata,
    )

    previous_files = {path for path in download_dir.iterdir() if path.is_file()}
    download_button = WebDriverWait(driver, 20).until(
        ec.element_to_be_clickable((By.CSS_SELECTOR, "#download"))
    )
    driver.execute_script("arguments[0].click();", download_button)

    popup = WebDriverWait(driver, 20).until(
        ec.visibility_of_element_located((By.CSS_SELECTOR, "#down_layer"))
    )
    logger.info("stage=consent_popup status=OPEN product_id=%s", item_id)

    purpose_selector = (
        f"#down_layer input[name='use_code'][value='{purpose_code}']"
    )
    purpose_radio = popup.find_element(By.CSS_SELECTOR, purpose_selector)
    driver.execute_script("arguments[0].click();", purpose_radio)

    if purpose_code == PURPOSES["other"]:
        if not other_purpose:
            raise BlockedAutomation(
                "consent_popup",
                "other 목적에는 --other-purpose 문구가 필요함",
            )
        other_input = popup.find_element(By.CSS_SELECTOR, "input[name='etc_name']")
        other_input.clear()
        other_input.send_keys(other_purpose)

    for checkbox_id in ("select", "select2"):
        consent = popup.find_element(By.CSS_SELECTOR, f"#{checkbox_id}")
        if not consent.is_selected():
            driver.execute_script("arguments[0].click();", consent)

    logger.info(
        "stage=consent_popup status=ACCEPTED product_id=%s purpose_code=%s",
        item_id,
        purpose_code,
    )
    confirm_button = popup.find_element(By.CSS_SELECTOR, "input.btn_ok[onclick*='fnSave']")
    driver.execute_script("arguments[0].click();", confirm_button)

    completed = wait_for_downloads(
        driver=driver,
        download_dir=download_dir,
        previous_files=previous_files,
        expected_count=len(free_checkboxes),
        timeout_seconds=download_timeout_seconds,
        logger=logger,
    )
    for path in completed:
        logger.info(
            "stage=download status=OK product_id=%s file=%s bytes=%s",
            item_id,
            path.resolve(),
            path.stat().st_size,
        )
    return completed


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        action="append",
        dest="urls",
        help="대상 상세 URL. 여러 번 지정 가능(기본값: 설정된 상품)",
    )
    parser.add_argument(
        "--probe-only",
        action="store_true",
        help="Selenium 없이 공개 목록과 비로그인 직접 다운로드 차단만 점검",
    )
    parser.add_argument("--headless", action="store_true", help="Chrome headless 실행")
    parser.add_argument(
        "--login-wait-seconds",
        type=int,
        default=0,
        help="세션이 없을 때 사용자의 브라우저 로그인을 기다릴 시간(기본 0)",
    )
    parser.add_argument(
        "--download-timeout-seconds",
        type=int,
        default=1800,
        help="각 상품 다운로드 완료 대기 시간(기본 1800초)",
    )
    parser.add_argument(
        "--purpose",
        choices=sorted(PURPOSES),
        default="business",
        help="활용목적(기본 business/업무활용)",
    )
    parser.add_argument(
        "--other-purpose",
        help="--purpose other일 때 사이트에 입력할 활용목적",
    )
    parser.add_argument(
        "--download-dir",
        type=Path,
        default=DEFAULT_DOWNLOAD_DIR,
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=DEFAULT_LOG_DIR,
    )
    parser.add_argument(
        "--user-data-dir",
        type=Path,
        default=DEFAULT_PROFILE_DIR,
        help="로그인 세션을 재사용할 Chrome 전용 프로필 경로",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    urls = tuple(args.urls or DETAIL_URLS)
    logger, log_path = setup_logging(args.log_dir)
    logger.info("stage=run status=START log=%s products=%s", log_path.resolve(), len(urls))

    if args.purpose == "other" and not args.other_purpose:
        logger.error(
            "stage=arguments status=BLOCKED reason=%r",
            "--purpose other에는 --other-purpose가 필요함",
        )
        return 2
    if args.headless and args.login_wait_seconds > 0:
        logger.error(
            "stage=arguments status=BLOCKED reason=%r",
            "--headless와 수동 로그인 대기는 함께 사용할 수 없음",
        )
        return 2

    if args.probe_only:
        ok = probe_without_login(urls, logger)
        logger.info("stage=run status=%s mode=probe_only", "OK" if ok else "FAILED")
        return 0 if ok else 1

    driver = None
    try:
        driver = create_driver(
            download_dir=args.download_dir,
            user_data_dir=args.user_data_dir,
            headless=args.headless,
            logger=logger,
        )
        for detail_url in urls:
            try:
                download_product(
                    driver=driver,
                    detail_url=detail_url,
                    download_dir=args.download_dir,
                    purpose_code=PURPOSES[args.purpose],
                    other_purpose=args.other_purpose,
                    login_wait_seconds=args.login_wait_seconds,
                    download_timeout_seconds=args.download_timeout_seconds,
                    logger=logger,
                )
            except BlockedAutomation as exc:
                logger.error(
                    "stage=%s status=BLOCKED product_id=%s reason=%r",
                    exc.stage,
                    product_id(detail_url),
                    exc.reason,
                )
                return 2
            except Exception as exc:
                logger.exception(
                    "stage=product status=FAILED product_id=%s reason=%r",
                    product_id(detail_url),
                    str(exc),
                )
                return 1
    except BlockedAutomation as exc:
        logger.error("stage=%s status=BLOCKED reason=%r", exc.stage, exc.reason)
        return 2
    finally:
        if driver is not None:
            driver.quit()

    logger.info("stage=run status=OK downloaded_products=%s", len(urls))
    return 0


if __name__ == "__main__":
    sys.exit(main())
