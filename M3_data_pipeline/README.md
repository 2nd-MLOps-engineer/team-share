# 🛡️ Production PostgreSQL Data Reliability Pipeline & DQ Audit Engine

[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15%2B-336791.svg?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![PostGIS](https://img.shields.io/badge/PostGIS-Spatial-006400.svg?logo=postgis&logoColor=white)](https://postgis.net/)
[![Selenium](https://img.shields.io/badge/Selenium-Automation-43B02A.svg?logo=selenium&logoColor=white)](https://www.selenium.dev/)
[![Data Quality](https://img.shields.io/badge/DQ_Audit-16_Rules-success.svg)](https://github.com/baikAnalyst/data-reliability-pipeline)
[![Unexplained Loss](https://img.shields.io/badge/Unexplained_Loss-0_Rows-brightgreen.svg)](https://github.com/baikAnalyst/data-reliability-pipeline)

> **"If data cannot be audited, it is not reliable."**  
> 대규모 공공/문화 빅데이터(408만+ 건)를 대상으로 **수집 장애 격리, 체크포인트 복구, 16개 DQ 감사 규칙, 행 단위 대조(Reconciliation) 및 일 2회 자동 운영 보고**를 수행하는 프로덕션 레벨 데이터 신뢰성 엔지니어링(DRE) 파이프라인입니다.

---

## 📌 Executive Summary (핵심 성과 지표)

| 지표 | 수치 | 엔지니어링 의미 |
| :--- | :---: | :--- |
| **전체 처리 데이터셋** | **19개 Dataset** | 기상 실황/특보, 대기질, AED, 전국체육시설, 두루누비 등 이종(Heterogeneous) 데이터 |
| **수집 원천 데이터 (RAW)** | **4,088,065 건** | API 병렬 수집 및 Selenium 크롤링을 통한 PostgreSQL RAW 적재 |
| **정제/가공 데이터 (PROCESSED)** | **4,009,597 건** | 좌표 검증(대한민국 영역), 결측 표준화, 자료형 정규화 완료 |
| **설명 불가능한 유실 (Unexplained Loss)** | **0 건 (0.00%)** | **전체 78,468건의 정제 제외 데이터에 대해 100% 명확한 탈락 사유 소명 완료** |
| **장애 사전 방어 사례** | **25,070 건 방어** | 잘못된 명세 변환으로 인한 대규모 NULL 변환 이슈 사전 탐지 및 원천 차단 |
| **무중단 운영 자동화** | **일 2회 정기 브리핑** | 09:15(야간 배치 요약), 17:00(주간 운영 현황) Webhook 자동 발송 |

---

## 🏗️ 1. Architecture & Multi-Schema Isolation

데이터의 무결성을 보장하기 위해 데이터베이스 내부 스키마를 논리적/물리적으로 엄격하게 분리 운영합니다.

```text
 [공공데이터 API]                [문화빅데이터 플랫폼]
        │                                 │
   (Collector)                   (Selenium Headless)
        │                                 │
  응답코드/포맷 검증                CSV 스트리밍 다운로드
        │                                 │
        ▼                                 ▼ (COPY Bulk Load)
┌────────────────────────────────────────────────────────┐
│                   PostgreSQL DB                        │
│                                                        │
│  ┌────────────────────────┐  ┌──────────────────────┐  │
│  │   schema: raw          │  │ schema: processed    │  │
│  │   - 원천 불변 보존     │  │ - 16개 DQ 감사 통과  │  │
│  │   - 재처리/감사 기준선 │  │ - 서비스 서빙 최적화 │  │
│  └───────────┬────────────┘  └──────────▲───────────┘  │
│              │                          │              │
│              ▼                          │              │
│     [ Cleaning & Transformation ]       │              │
│              │                          │              │
│              ▼                          │              │
│     [ 16 DQ Audit & Reconciliation ] ───┘              │
│                                                        │
│  ┌────────────────────────┐  ┌──────────────────────┐  │
│  │   schema: monitoring   │  │ schema: derived      │  │
│  │   - pipeline_history   │  │ - PostGIS 공간 파생  │  │
│  └────────────────────────┘  └──────────────────────┘  │
└────────────────────────────────────────────────────────┘
```

| Schema | 역할 및 격리 정책 |
| :--- | :--- |
| **`raw`** | 외부 수집 데이터를 어떠한 변형 없이 원본 그대로 보존 (재처리 및 감사 기준선) |
| **`processed`** | 16개 DQ 감사 및 비즈니스 룰을 100% 통과한 정제 완료 데이터 (서비스 서빙용) |
| **`monitoring`** | 파이프라인 실행 이력, 실행 시간, 단계별 장애 원인, DQ Audit JSON 결과 영구 아카이빙 |
| **`derived`** | PostGIS 공간 함수(`ST_Distance`) 등을 통해 생성된 공간/통계 파생 데이터 영역 |
| **`interaction`** | 추천/검색 등 서비스 이용 과정에서 발생하는 사용자 상호작용 데이터 영역 |

---

## ⚡ 2. High-Performance Ingestion & Bulk Load

- **API 수집 검증**: HTTP 200 상태코드만으로 정상 수집을 판단하지 않고, 데이터 소스별 결과코드(Result Code) 및 JSON/XML 페이로드를 검증하여 정상/오류를 완벽히 격리.
- **중간 파일 의존성 제거**: 기존 `API → CSV → DB`의 비효율성을 제거하고 메모리 버퍼에서 `raw` 스키마로 직접 스트리밍 적재.
- **PostgreSQL `COPY` 기반 초고속 대용량 적재**: 수십만 건의 문화빅데이터 적재 시 행 단위 `INSERT` 대신 PostgreSQL 바이너리 스트리밍 `COPY` 명령어를 사용하여 적재 병목 90% 이상 해소.

---

## 🛡️ 3. Fault-Tolerance & Safe Recovery

수집 및 적재 과정에서 발생하는 모든 실패에 대해 회복 탄력성(Resilience)을 보장합니다.

```text
 오류 감지 ➔ 오류 유형 분류 ➔ Retry / Timeout ➔ 필요 시 Checkpoint-Resume
                                                      │
                                                      ▼
                      [ 임시 STAGING 적재 & Row Count 검증 ]
                                      │
               ┌──────────────────────┴──────────────────────┐
               ▼ (FAILED)                                    ▼ (PASSED)
   STAGING 폐기 & 기존 RAW 유지                  신규 RAW 원자적 교체 반영
   (불완전 적재로 인한 오염 차단)                 (Zero Downtime Safe Swap)
```

1. **지능형 재시도 (Retry & Timeout)**:
   - 일시적 네트워크 타임아웃은 Exponential Backoff로 재시도.
   - API 일일 쿼터 초과 등 재시도가 무의미한 에러는 즉시 Fail-fast 처리하여 리소스 낭비 방지.
2. **체크포인트 복구 (Checkpoint / Resume)**:
   - 대용량 수집 중단 시 완료된 작업 단위(Offset)를 저장하여 장애 발생 지점부터 즉시 이어받기 지원.
3. **기존 정상 RAW 보호 (Staging-to-Atomic Swap)**:
   - 신규 데이터는 Staging 버퍼에 임시 적재 후 Row Count와 무결성이 검증된 경우에만 원자적으로 교체. 수집 실패 시 기존 정상 RAW를 손상 없이 영구 보존.

---

## 🔍 4. 16-Rule Data Quality (DQ) Audit & Reconciliation

파이프라인의 성공(`Pipeline SUCCESS`)과 데이터 품질 통과(`Data Quality PASS`)를 완전히 분리하여 다룹니다.

### 4.1 Row-Level Reconciliation (행 단위 정합성 대조)
전체 행 수만 단순 비교하면 유실된 행과 새로 추가된 행이 상쇄되어 문제를 놓칠 수 있습니다.
- 개별 행에 `_dq_row_id`를 부여하여 수집부터 정제까지 생애주기 전수 추적.
- **소명 불가능한 유실 0건 달성**:
  - `RAW (4,088,065건) - 정제 제외 (78,468건) = PROCESSED (4,009,597건)`
  - 정제 과정에서 탈락한 78,468건(1.92%)에 대해 중복 키, 범위 외 좌표 등 100% 명확한 탈락 사유를 기록.

### 4.2 Column-Level Profile & State Audit
RAW와 PROCESSED의 컬럼 상태 변화를 전수 관찰하여 의도치 않은 손실 탐지:
- `dtype`, `null_rate`, `unique_rate`, `numeric min/max/mean`, `string length` 전후 비교.

### 4.3 실제 장애 방어 사례 (Data Loss Prevention)
- **사례**: 체력 측정별 운동처방 데이터의 `MESURE_TME`(측정회차) 컬럼 명세 변환 오류로 인해 **25,070건의 정상 데이터가 NULL로 증발하는 결함**을 DQ 컬럼 프로파일러가 사전 탐지.
- **조치**: 원천 명세를 재분석하여 파싱 로직을 정규화하고, 데이터 손실률 0%를 복구한 후 배포 승인.

---

## 📊 5. Audit Evidence (DB 실측 감사 증적)

파이프라인 실행 시마다 `monitoring.pipeline_run_history` 테이블에 실행 이력과 함께 **DQ 결과 JSON 리포트**가 영구 보존됩니다.

```sql
SELECT run_id, dataset, run_status, dq_status, jsonb_pretty(dq_results) 
FROM monitoring.pipeline_run_history 
ORDER BY started_at DESC LIMIT 1;
```

```json
{
  "weather_warning": {
    "rows": {
      "raw_rows": 186,
      "actual_final": 128,
      "removed_rows": 58,
      "removal_reasons": {
        "weather_warning_duplicate_stn_tmfc_tmseq": 58
      },
      "row_count_matches": true,
      "unexplained_row_loss": 0,
      "explained_removed_rows": 58
    },
    "validation_summary": [
      {"rule": "row_tracking", "status": "PASS"},
      {"rule": "row_reconciliation", "status": "PASS", "expected": 128, "observed": 128}
    ]
  }
}
```

---

## ⏱️ 6. Automated Operations & Webhook Ops Digest

APScheduler를 활용하여 데이터 소스의 갱신 주기에 맞춘 최적화 스케줄링을 운영하며, **하루 2회 정기 운영 리포트**를 발송합니다.

| Dataset | Schedule | 갱신 주기 고려 사항 |
| :--- | :--- | :--- |
| **`weather`** | 매시 10분, 40분 | 기상청 초단기 예보 업데이트 주기 동기화 |
| **`air_quality`** | 매시 15분 | 에어코리아 시간대별 확정 데이터 수집 |
| **`weather_warning`** | 10분마다 | 기상특보 실시간 감시 |
| **`facility / AED`** | 매월 1일 새벽 | 월간 변경분 배치 동기화 |

- **장애 즉시 알림**: 파이프라인 FAILED 또는 DQ CHECK_FAILED 발생 시 상세 원인 Webhook 즉시 발송.
- **정기 브리핑 (Daily 2x Ops Digest)**:
  - **09:15**: 야간 배치 실행 요약 및 시스템 헬스체크 보고
  - **17:00**: 일간 수집 현황, 누적 처리량 및 이상 징후 종합 보고

---

## 📂 7. Repository Structure

```text
.
├── collector/                       # 공공데이터 API 수집기 (Validation Layer 내장)
├── culture_bigdata_selenium.py      # 대용량 문화빅데이터 크롤링 & COPY 벌크 적재기
├── dataset_processors.py            # 데이터셋별 16개 정제 및 결측치 정규화 엔진
├── dq_profiler.py                   # RAW↔PROCESSED 무결성 검증 & DQ Audit 엔진
├── pipeline_elt.py                  # Multi-Schema 트랜잭션 ELT 오케스트레이터
├── pipeline_metadata.py             # 데이터셋별 파이프라인 메타데이터 및 스키마 명세
├── pipeline_monitoring.py           # 실행 이력 로깅, 상태 집계 및 브리핑 생성기
├── pipeline_scheduler.py            # APScheduler 기반 무중단 스케줄러
├── webhook_notifier.py              # Webhook Incident & Daily Digest 전송기
├── tests/                           # 무결성 검증, 회귀 테스트 및 감사 증적 테스트
│   ├── test_audit_evidence.py
│   ├── test_collector_checkpoint_recovery.py
│   └── test_dq_profiler.py
└── requirements.txt                 # 의존성 라이브러리 목록
```
