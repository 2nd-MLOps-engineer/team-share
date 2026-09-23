# M3 데이터 파이프라인

공공데이터 API와 문화빅데이터 웹 데이터를 수집하여 PostgreSQL에 적재하고,
데이터셋별 정제·검증 규칙을 적용해 서비스에서 사용할 수 있는 형태로 변환하는
통합 데이터 파이프라인입니다.

## Architecture

```text
공공데이터 API ──→ Collector ─────┐
                                  ├─→ PostgreSQL raw (Bronze)
문화빅데이터 ──→ Selenium ─────────┘
                                         ↓
                                dataset_processors.py
                                         ↓
                              Data Quality Validation
                                         ↓
                           PostgreSQL processed (Silver)
                                         ↓
                                  Backend / Service
```

### Bronze — `raw`

API 및 Selenium을 통해 수집한 원본 데이터를 보존하는 계층입니다.

원본 데이터의 추적과 재처리가 가능하도록 수집 데이터를 `raw` 스키마에 유지하고,
정제 과정에서 원본 데이터를 직접 수정하지 않습니다.

### Silver — `processed`

`raw` 데이터를 기반으로 데이터셋별 정제·검증 규칙을 적용한 결과를 저장합니다.

주요 처리 항목은 다음과 같습니다.

- 대표 결측값의 `NULL` 표준화
- 숫자 및 날짜 데이터 타입 변환
- 유효 키 기준 중복 제거
- 데이터셋별 운영 상태 및 품질 조건 적용
- 위도·경도 숫자형 변환
- 좌표 결측값 및 0 좌표 제거
- 대한민국 범위를 벗어난 좌표 제거
- 완전 결측 컬럼 탐지 및 안전한 제거
- 검토가 필요한 행 식별

## Pipeline Components

### Data Collection

공공데이터 API를 이용한 데이터 수집과 Selenium 기반 문화빅데이터 수집을 지원합니다.

각 데이터 소스의 응답 형식과 오류 특성에 맞게 예외처리 및 재시도 로직을 적용하여
수집 실패가 전체 파이프라인에 미치는 영향을 줄였습니다.

### Data Processing

`dataset_processors.py`에서 데이터셋별 정제 규칙을 중앙 관리합니다.

원본 데이터는 `raw`에 보존하고 정제된 결과만 `processed`에 반영하여,
수집과 정제 책임을 분리하고 데이터 변환 과정을 추적할 수 있도록 구성했습니다.

### Data Quality

정제 과정에서 데이터 손실이나 비정상적인 변환을 확인할 수 있도록
데이터 품질 지표를 함께 기록합니다.

1. RAW / PROCESSED 행 수
2. 제거 행 수 및 데이터 유지율
3. RAW / PROCESSED 결측치 수와 비율
4. 완전 결측 컬럼 탐지 및 안전한 제거
5. 검토 필요 행 수 및 비율
6. 데이터셋별 정제 처리시간
7. RAW 조회 · 정제 · PROCESSED 적재 단계별 처리시간
8. 전체 파이프라인 처리시간

이를 통해 타입 변환이나 정제 규칙으로 인해 발생할 수 있는 예상치 못한
데이터 손실을 확인할 수 있도록 했습니다.

### Scheduling & Orchestration

`pipeline_scheduler.py`를 중심으로 데이터 수집과 정제 작업의 실행 흐름을 관리합니다.

데이터 특성에 따라 실시간성 데이터는 짧은 주기로,
변경 빈도가 낮은 데이터는 주간·월간 단위로 수집하도록 구성했습니다.

현재 통합 스케줄러에는 다음 작업이 등록되어 있습니다.

| Dataset | Schedule |
| --- | --- |
| 기상 실황 / 초단기예보 | 매시 10분, 40분 |
| 대기질 | 매시 15분 |
| 기상특보 | 10분마다 |
| 버스정류장 | 매주 일요일 03:00 |
| 두루누비 | 매주 월요일 04:00 |
| 체육시설 | 매월 1일 04:00 |
| 공공개방시설 | 매월 1일 05:00 |
| AED (자동심장충격기 설치정보) | 매월 2일 03:00 |
| 자전거 사고다발지역 | 매월 3일 02:00 |
| 문화빅데이터 | 매월 1일 07:00 |

문화빅데이터 역시 통합 스케줄러에서 관리하며,
Selenium 수집 후 `raw`에 적재된 데이터를 공통 정제 단계와 연결하여
`processed`까지 처리하도록 구성했습니다.

## Data Sources

파이프라인에는 다음과 같은 데이터가 포함됩니다.

- 기상 실황 및 초단기예보
- 기상특보
- 실시간 대기질
- 버스정류장
- AED (자동심장충격기 설치정보)
- 공공체육시설
- 공공개방시설
- 두루누비 산책·둘레길
- 자전거 사고다발지역
- 문화빅데이터 기반 체육시설·프로그램·교통·안전·체력측정 관련 데이터

## Project Structure

```text
M3_data_pipeline/
├── collector/                  # API 데이터 수집
├── culture_bigdata_selenium.py # Selenium 기반 문화빅데이터 수집
├── dataset_processors.py       # 데이터셋별 정제 및 품질 규칙
├── pipeline_common.py          # 공통 DB 적재 및 파이프라인 처리
├── pipeline_scheduler.py       # 통합 스케줄링 및 실행 흐름 관리
├── tests/                      # 파이프라인 테스트
└── requirements.txt
```

## Data Flow

```text
Source
  ↓
Collection
  ↓
RAW (Bronze)
  ↓
Cleaning / Validation
  ↓
PROCESSED (Silver)
  ↓
Backend / Service
```

수집 원본과 서비스용 정제 데이터를 분리하여
원본 보존, 재처리 가능성, 데이터 품질 검증을 함께 고려한 구조로 설계했습니다.
