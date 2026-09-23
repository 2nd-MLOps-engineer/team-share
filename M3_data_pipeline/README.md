# M3 데이터 파이프라인

각 수집기는 API 전체 페이지를 로컬 `data/raw/<dataset>/` CSV로 저장합니다.
통합 스케줄러는 CSV 원문을 PostgreSQL `raw` 스키마에 그대로 적재합니다.
`processed`에는 데이터셋별 기존 정제 규칙(숫자·날짜 변환, 중복 제거,
운영 상태 필터 등)을 적용하며, 좌표 데이터는 숫자 변환 후 결측/0/대한민국
범위 밖 좌표를 제거합니다. 공통 적재 대상의 정제 CSV는
`data/processed/<dataset>/`에도 저장합니다. 수집이나 검증이 실패하면
해당 job은 실패 처리됩니다.

## 설치와 환경변수

```powershell
cd M3_data_pipeline
python -m pip install -r requirements.txt
if (-not (Test-Path collector/.env)) { Copy-Item .env.example collector/.env }
```

`collector/.env`에 API 키와 PostgreSQL 접속 정보를 입력합니다. 이 파일은
비밀값을 포함하므로 Git에 커밋하지 않습니다.

## 실행

```powershell
# 등록 작업과 실제 적용 cron 확인
python pipeline_scheduler.py --list

# 작업 하나 즉시 실행(API -> CSV -> raw/processed)
python pipeline_scheduler.py --run-once bicycle_accident

# API와 CSV만 시험하고 공통 DB 적재 생략(자체 DB 작업 제외)
python pipeline_scheduler.py --run-once weather --skip-db

# 모든 작업을 한 번씩 실행
python pipeline_scheduler.py --run-all-once

# APScheduler 상시 실행
python pipeline_scheduler.py
```

문화 빅데이터는 이미 완성된 기존 동작을 보존하기 위해 통합 대상에서
제외했습니다. 별도 프로세스로 다음과 같이 실행합니다.

```powershell
python culture_bigdata_scheduler.py
```

## 기본 주기 (Asia/Seoul)

| 작업 | 주기 |
|---|---|
| 기상 실황/초단기예보 | 매시 10분, 40분 |
| 대기질 | 매시 15분 |
| 기상특보 | 10분마다 |
| 버스정류장 | 매주 일요일 03:00 |
| 두루누비 | 매주 월요일 04:00 |
| 체육시설 | 매월 1일 04:00 |
| 공공개방시설 | 매월 1일 05:00 |
| AED | 매월 2일 03:00 |
| 자전거 사고다발지역 | 매월 3일 02:00 |

각 주기는 `.env`의 `<JOB_ID>_CRON` 값으로 변경할 수 있습니다. 예를 들어
`BICYCLE_ACCIDENT_CRON=0 1 1 */3 *`는 분기 첫날 01:00 실행입니다.

운영에서는 프로세스가 중단되어도 다시 시작되도록 Windows 작업 스케줄러,
NSSM 또는 컨테이너 재시작 정책으로 `pipeline_scheduler.py`를 감싸는 것을
권장합니다.
