# M3 데이터 파이프라인

공공데이터 API와 문화빅데이터 웹 데이터를 수집·정제하여 PostgreSQL에 적재하는 데이터 파이프라인입니다.

**원본 보존과 변환 손실 추적을 기반으로 데이터 품질을 검증하고, 공공데이터의 신뢰성과 활용성을 높이는 데이터 파이프라인을 구축했습니다.** 수집·정제 과정의 행 수와 결측치 변화를 추적하는 데이터 품질 관리 체계와 실행 이력·로그·알림을 구성했으며, API와 CSV 원천의 특성에 따라 처리 방식을 분리해 불필요한 중간 파일 의존성을 줄이고 대용량 데이터 처리까지 고려한 구조로 설계했습니다.

---

## Architecture

```text
공공데이터 API
      │
      ▼
  Collectors ─────────────────────────┐
      │                               │
      │ direct load                   │
      ▼                               │
PostgreSQL raw (Bronze)               │
      │                               │
      │                        문화빅데이터 웹
      │                               │
      │                        Selenium 수집
      │                               │
      │                           원본 CSV
      │                               │
      │                      PostgreSQL COPY 적재
      │                               │
      ◀───────────────────────────────┘
      │
      ▼
dataset_processors.py
      │
      ▼
Data Quality Validation
      │
      ▼
PostgreSQL processed (Silver)
      │
      ├──────────→ Backend / Service
      │
      ▼
derived
Feature Engineering / Join / Aggregation

Pipeline execution
      │
      ├──────────→ monitoring.pipeline_run_history
      ├──────────→ Rotating Logs
      └──────────→ Discord Alert / Operations Summary
```

### `raw` — Bronze

수집한 원천 데이터를 보존하는 계층입니다.

정제 과정에서 원본 데이터를 직접 수정하지 않고 `raw`와 `processed`를 분리하여 원본 추적과 재처리가 가능하도록 구성했습니다.

일반 API 데이터는 수집 후 불필요한 중간 CSV를 생성하지 않고 `raw`에 직접 적재합니다.

### `processed` — Silver

`raw` 데이터를 기반으로 데이터셋별 정제·표준화 규칙을 적용한 결과를 저장합니다.

주요 처리 항목은 다음과 같습니다.

- 대표 결측값의 `NULL` 표준화
- 숫자 및 날짜 데이터 타입 변환
- 유효 키 기준 중복 처리
- 데이터셋별 운영 상태 및 품질 조건 적용
- 위도·경도 숫자형 변환
- 좌표 결측값 및 0 좌표 제거
- 대한민국 범위를 벗어난 좌표 제거
- 완전 결측 컬럼 탐지
- 검토가 필요한 행 식별

### `derived`

피처 엔지니어링, 조인, 집계, 점수 산출 등 원본·정제 데이터로부터 생성되는 파생 데이터를 위한 계층입니다.

서비스 조회용 정제 데이터와 분석·추천을 위한 파생 데이터를 분리할 수 있도록 구성했습니다.

### `monitoring`

파이프라인 실행 결과와 데이터 품질 검사 결과를 운영 데이터와 분리하여 관리합니다.

`monitoring.pipeline_run_history`에 실행 상태, DQ 상태, 단계별 처리시간, 실패 정보 등을 기록하여 개별 실행의 결과를 추적할 수 있도록 구성했습니다.

---

## Architecture Decisions

### 1. RAW와 PROCESSED 분리

정제된 데이터만 남기면 변환 과정에서 값이 잘못 제거되거나 변환되었을 때 원인을 추적하기 어렵습니다.

따라서 원천 데이터는 `raw`에 보존하고, 정제 결과는 `processed`에 별도로 적재하도록 구성했습니다.

```text
Source → RAW → Cleaning / Validation → PROCESSED
```

이를 통해 원본과 최종 결과를 비교하고 동일한 RAW 데이터에서 정제 로직을 다시 실행할 수 있습니다.

### 2. API 중간 CSV 의존성 제거

API 데이터에 대해

```text
API → CSV → pandas → DB
```

형태의 중간 파일을 반복적으로 생성하면 파일시스템 의존성과 불필요한 I/O가 증가합니다.

운영 대상 API 수집기는 다음 흐름을 사용하도록 정리했습니다.

```text
API → Collector → PostgreSQL RAW → Processing → PROCESSED
```

CSV는 더 이상 일반 API 데이터의 적재 중간 단계로 사용하지 않습니다.

단, 자전거 사고다발지역의 checkpoint 및 실패 진단 파일처럼 **복구·진단을 위한 파일은 데이터 적재용 중간 파일과 구분하여 유지**합니다.

### 3. 문화빅데이터 CSV는 별도 처리

문화빅데이터는 웹에서 CSV 파일 자체를 제공하므로 일반 API 데이터와 동일하게 처리하지 않았습니다.

```text
Website
   ↓
Selenium
   ↓
per-run CSV
   ↓
streaming validation
   ↓
PostgreSQL COPY
   ↓
RAW
```

대용량 CSV 전체를 한 번에 메모리에 적재하지 않고 스트리밍 검증과 PostgreSQL `COPY`를 사용합니다.

RAW 적재가 정상적으로 검증된 경우 운영용 다운로드 파일을 정리하고, 적재 실패 시 기존 RAW 데이터와 원본 CSV를 보존하여 재처리가 가능하도록 구성했습니다.

### 4. 다중 테이블 적재의 원자성

날씨, 기상특보, 두루누비, 문화빅데이터처럼 하나의 논리적 데이터셋이 여러 테이블로 구성되는 경우 일부 테이블만 갱신되면 서로 다른 시점의 데이터가 섞일 수 있습니다.

이를 방지하기 위해 staging 및 row-count 검증 후 관련 테이블을 하나의 transaction 단위로 교체하도록 구성했습니다.

```text
Load staging tables
        ↓
Validate row counts
        ↓
BEGIN
        ↓
Replace related tables
        ↓
Validate
        ↓
COMMIT
```

실패 시 기존 RAW 데이터를 유지하여 부분 갱신을 방지합니다.

---

## Data Quality Management

파이프라인의 정상 종료 여부와 데이터 자체의 정상 여부를 별도로 확인합니다.

단순히 "`NULL`이 몇 개인가"만 확인하는 것이 아니라 **정상 값이 변환 과정에서 사라졌는지, 제거된 행이 정제 규칙으로 설명 가능한지**를 추적하는 것을 목표로 했습니다.

### Row Tracking & Reconciliation

RAW에서 PROCESSED로 이동하는 동안 행에 추적 ID를 부여하여 다음을 확인합니다.

- RAW / PROCESSED 행 수
- 제거된 행
- 설명 가능한 제거 행
- 설명되지 않는 행 손실
- tracking ID 누락
- 알 수 없는 tracking ID
- tracking ID 중복

최종 행 수가 정제 규칙으로 설명되지 않으면 DQ 실패로 판정할 수 있도록 구성했습니다.

### NULL Transition

컬럼별로 처리 전후 결측치 변화를 추적합니다.

```text
RAW NULL
   ↓
NULL normalization
   ↓
type conversion
   ↓
PROCESSED NULL
```

이를 통해 원래 존재하던 결측치와 정제·타입 변환 과정에서 새로 발생한 결측치를 구분할 수 있습니다.

### Column Profile

RAW와 PROCESSED의 컬럼별 상태를 비교할 수 있도록 프로파일을 생성합니다.

주요 관찰 항목:

- dtype
- row count
- NULL count / rate
- non-null count
- unique count / rate
- numeric min / max / mean / median
- infinite value
- datetime min / max
- string min / max length
- empty string count

Column Profile은 현재 **관찰 지표**이며 자체적으로 DQ 실패를 발생시키는 규칙으로 사용하지 않습니다.

### Human Audit Report

DQ 결과를 사람이 빠르게 검토할 수 있도록 다음 영역으로 구성된 audit report를 출력합니다.

1. Validation Summary
2. Row Transformation
3. Missing Value Profile
4. Column Profile — RAW ↔ PROCESSED
5. Duplicate / Needs Review / Issues

### DQ Status

현재 `CHECK_PASSED` / `CHECK_FAILED` 판정은 핵심 데이터 무결성 조건을 기준으로 합니다.

```text
Row Tracking
      +
Row Count Reconciliation
      ↓
CHECK_PASSED / CHECK_FAILED
```

결측치 수, 중복 관찰값, `needs_review`, Column Profile 등은 현재 상태를 관찰하고 원인을 분석하기 위한 지표이며 별도의 threshold 기반 실패 규칙은 적용하지 않았습니다.

---

## Incident → Fix

### 타입 변환으로 인한 정상 값 손실

정제 코드가 오류 없이 실행되더라도 잘못된 타입 변환으로 기존 값이 `NULL`로 바뀌는 문제를 확인했습니다.

이를 계기로 단순 실행 성공 여부가 아니라 RAW와 PROCESSED 사이의 **행 수와 결측치 변화 자체를 검증하는 DQ 구조**를 추가했습니다.

```text
Pipeline SUCCESS
        ≠
Data Quality PASS
```

### 장시간 API 수집과 호출 제한

전국 단위 데이터를 순차 수집할 경우 실행시간이 길어지고, API 일일 호출 제한에 도달하면 처음부터 다시 수집해야 하는 문제가 있었습니다.

자전거 사고다발지역 수집기에 다음 구조를 적용했습니다.

- `ThreadPoolExecutor` 기반 bounded concurrency
- scope 단위 checkpoint
- 중단 후 resume
- API 결과코드 분류
- retry / timeout
- 실패 및 미지원 scope 기록

이를 통해 I/O 대기시간을 줄이고 장시간 수집 작업의 복구 가능성을 높였습니다.

### 파일 기반 중간 단계 의존성

일반 API 수집 과정의 중간 CSV는 운영 환경에서 파일시스템 의존성과 추가 I/O를 만들었습니다.

API 데이터는 RAW DB 직접 적재 방식으로 변경하고, CSV 자체가 원천 데이터인 문화빅데이터만 별도 lifecycle을 적용하도록 구조를 분리했습니다.

---

## Reliability & Observability

### Retry & Error Handling

데이터 소스별 특성에 맞게 다음과 같은 오류를 구분하여 처리합니다.

- HTTP 오류
- timeout
- API 결과코드 오류
- JSON / XML 응답 형식 오류
- 일일 API quota 초과
- DB 적재 오류

일시적인 오류에는 제한된 retry를 적용하고, 일일 quota 초과처럼 즉시 재시도해도 해결되지 않는 오류는 불필요한 반복 호출을 방지하도록 처리합니다.

### Persistent Logging

파이프라인 실행 로그는 `RotatingFileHandler`를 이용해 분리합니다.

```text
logs/
├── pipeline.log
├── error.log
└── dq_audit.log
```

일반 실행 로그, 오류 로그, DQ audit 로그를 구분하여 운영 중 문제 발생 시 원인을 추적할 수 있도록 구성했습니다.

### Pipeline Run History

각 실행의 구조화된 결과를 `monitoring.pipeline_run_history`에 기록합니다.

주요 기록 항목:

- run ID
- job / dataset
- source type
- pipeline run status
- DQ status
- stage durations
- failure stage
- error type / message
- start / finish time
- elapsed time
- DQ result

Raw log와 구조화된 실행 이력을 분리하여 운영 로그 전체를 DB에 중복 저장하지 않도록 했습니다.

### Discord Notification

운영 상태를 확인할 수 있도록 Discord Webhook 기반 알림 기능을 구성했습니다.

- Pipeline Failure Alert
- DQ Failure Alert
- Operations Summary

운영 요약은 `pipeline_run_history`를 집계하여 생성하도록 설계했습니다.

---

## Scheduling & Orchestration

`pipeline_scheduler.py`가 수집 → RAW → 정제 → DQ → PROCESSED → monitoring 흐름을 관리합니다.

데이터의 갱신 특성에 따라 실시간성 데이터는 짧은 주기로, 변경 빈도가 낮은 데이터는 주간·월간 단위로 실행합니다.

| Dataset | Schedule |
| --- | --- |
| 기상 실황 / 초단기예보 | 매시 10분, 40분 |
| 대기질 | 매시 15분 |
| 기상특보 | 10분마다 |
| 버스정류장 | 매월 28일 03:00 |
| 두루누비 | 매주 월요일 04:00 |
| 체육시설 | 매월 1일 04:00 |
| 공공개방시설 | 매월 1일 05:00 |
| AED | 매월 2일 03:00 |
| 자전거 사고다발지역 | 매월 3일 02:00 |
| 문화빅데이터 | 매월 1일 07:00 |

---

## Data Sources

- 기상 실황 및 초단기예보
- 기상특보
- 실시간 대기질
- 버스정류장
- AED (자동심장충격기 설치정보)
- 공공체육시설
- 공공개방시설
- 두루누비 산책·둘레길
- 자전거 사고다발지역
- 문화빅데이터 기반 체육시설·프로그램·교통·안전·체력측정 데이터

---

## Project Structure

```text
M3_data_pipeline/
├── collector/                       # 공공데이터 API 수집기
├── culture_bigdata_selenium.py      # 문화빅데이터 수집 및 적재
├── dataset_processors.py            # 데이터셋별 정제 규칙
├── dq_profiler.py                   # 데이터 품질 추적 및 Audit
├── pipeline_elt.py                  # RAW / PROCESSED 공통 ELT
├── pipeline_metadata.py             # 데이터셋 및 파이프라인 메타데이터
├── pipeline_monitoring.py           # 실행 이력 및 운영 집계
├── pipeline_scheduler.py            # 통합 스케줄링 / orchestration
├── webhook_notifier.py              # Discord 알림
├── pytest.ini
├── tests/
└── requirements.txt
```

---

## Verification

리팩터링 후 독립된 Python 3.13 가상환경에서 전체 단위 테스트를 실행했습니다.

```text
78 passed
15 subtests passed
0 failed
```

추가 정적 검증:

```text
python -m compileall -q .
git diff --check
```

모두 오류 없이 통과했습니다.

실제 데이터 수집 과정에서는 자전거 사고다발지역 수집의 checkpoint/resume, API quota 처리와 버스정류장 전국 수집 및 RAW/PROCESSED 행 수 검증 등 주요 실행 경로를 확인했습니다.

> 최근 구조 변경 이후 모든 외부 API 및 DB 경로를 대상으로 전체 E2E를 다시 실행한 것은 아니므로, 단위 테스트 검증과 실제 운영 데이터 검증 범위를 구분하여 기록합니다.

---

## Limitations & Next Steps

현재 구조는 교육 프로젝트 환경에서 데이터 파이프라인의 신뢰성·재처리 가능성·운영 가시성을 높이는 데 초점을 맞췄습니다.

향후 개선 항목:

- 데이터셋별 기준을 적용한 데이터 품질 이상 자동 판정
- 신규 데이터 수집기 추가 시 스케줄링·정제·품질검사 자동 연결
- `derived` 계층을 활용한 추천용 파생 데이터 및 Feature 생성
---

## Data Flow

```text
Source
  ↓
Collection
  ↓
RAW
  ↓
Cleaning / Transformation
  ↓
Data Quality Audit
  ↓
PROCESSED
  ↓
Backend
```
