# bus_stop 제거 및 복구 메모

2026-09-29 작업본 기준. 운영 등록은 제거했으며 아래 SQL은 실행하지 않았다.
비밀키·credential 값은 보관하지 않는다.

## 출처와 수집 방식

- 공공데이터포털 국토교통부 TAGO 버스정류소정보: 기존 base URL은
  `http://apis.data.go.kr/1613000/BusSttnInfoInqireService`.
- `getCtyCodeList`로 도시 목록을 얻고 `getSttnNoList`에 `cityCode`,
  `pageNo`, `numOfRows=100`, `_type=json`, `serviceKey`를 전달했다.
- 인증은 `collector/.env`의 `BUSSTOP_API_KEY`를 읽어 `unquote` 후 사용했다.
  복구 시 별도로 발급·설정하고 URL/응답 로그에 키를 남기지 않는다.
- 도시별 최대 8개 병렬 작업, 스레드별 HTTP session 재사용. 도시 내 페이지는 순차 처리,
  페이지 간 0.2초 대기, 요청 timeout 30초, 최대 3회 요청 및 재시도 간 2초 대기.
  API 성공 코드는 `00`. 단일 item 객체를 목록으로 정규화하고 totalCount 불일치는 실패 처리했다.
- `node_id` 없는 레코드를 제외하고 `(city_code, node_id)` 중복의 마지막 값을 유지한 후
  같은 키 순서로 정렬했다. 일부 도시 실패 또는 전체 0건이면 기존 RAW snapshot을 유지했다.
- `data/raw/bus_stop/bus_stop_checkpoint.jsonl`에 도시별 success/no_data 및 레코드를 저장했다.
  version=1, dataset, 페이지 크기, 도시 수·도시 키 fingerprint를 검증하고 완료 도시는 재사용했다.
  RAW 저장 성공 후에만 체크포인트를 삭제했다. 공통 `collector/jsonl_checkpoint.py`는 유지한다.

## 테이블·정제·DQ

| 대상 | 컬럼 / 계약 |
| --- | --- |
| `raw.bus_stop` | `city_code`(문자열), `city_name`, `node_id` ← nodeid, `node_name` ← nodenm, `node_no` ← nodeno, `latitude` ← gpslati, `longitude` ← gpslong |
| `processed.bus_stop` | 같은 업무 컬럼, 위도·경도 숫자 변환 및 정제 결과 |

고정 DDL/type 선언은 없고 pandas `to_sql`이 frame dtype으로 SQL 타입을 추론했다.
위 표는 코드 기준이며 실제 DB 스키마 조회는 수행하지 않았다. 전용 PK·index·validator는 없었고
`SourceKind.RAW_DATABASE`, `allow_empty=False`였다.

수집기는 `replace_raw_dataset_group({"bus_stop": frame})`으로 RAW snapshot을 교체했다.
ELT는 RAW를 읽고 `process_bus_stop`으로 NULL 정규화 → 대한민국 좌표 범위
(위도 33.0–39.5, 경도 124.0–132.0, 양끝 포함) 밖/NULL 좌표 행 제거 →
유효한 `(city_code, node_id)`만 중복 제거(마지막 유지, NULL 키 행은 보존) → index 재설정을 했다.
공통 staging 적재/건수 검증/교체 및 DQ audit를 사용했다. `_dq_row_id`는 공통 DQ 내부 추적용이다.
전용 DQ 규칙은 없었으며 기존 processor는 좌표·중복 제거 사유를 명시적으로 전달하지 않았다.

## 기존 등록 위치와 스케줄

- `collector/busstop_api.py`: 수집기(삭제).
- `dataset_processors.py`: `process_bus_stop`, `PROCESSORS["bus_stop"]`(삭제).
- `pipeline_metadata.py`: `STATIC_DATASETS`의 DatasetSpec/TableSpec(삭제).
  cron=`0 3 28 * *`(매월 28일 03:00, Asia/Seoul), run_order=1000,
  run_last=True, default_attempts=3. 전체 일회 실행에서는 다른 작업 성공 후 마지막 단계였다.
- `pipeline_scheduler.py`: `JOB_SPECS = dict(get_dataset_specs())`로 자동 등록;
  `BUS_STOP_CRON`, `BUS_STOP_MAX_ATTEMPTS`로 override 가능했다.
  과거 `.env.example`의 주간 cron 예제는 metadata의 실제 월간 기본값과 달랐다.
- `pipeline_elt.py`와 `dq_profiler.py`는 공통 metadata/processor 경로이므로 수정하지 않았다.
- `tests/test_busstop_api.py`(삭제): 병렬 병합/정렬, 부분 실패 저장 방지, 체크포인트 재개,
  session 분리, 순차 pagination 및 건수 불일치 검증.
  metadata의 마지막 단계 기대값을 빈 목록으로 수정했고 scheduler 공통 테스트는
  `final_dataset` 가상 fixture로 유지했다.
- `.env.example`, 로컬 `collector/.env`, README의 전용 항목과
  `load_policy_check.txt`의 오래된 전용 파일 검색 결과를 제거했다.

## 보존 범위 및 DROP 후보

monitoring 실행/audit history와 `logs/`는 그대로 보존한다. 기존 RAW/PROCESSED 데이터 및
체크포인트 등 수집 산출물도 이번 작업에서 삭제하지 않는다.
`collector/tago_bus.py`, `tests/experiments/tago_bus_api_test.py`는 별도
`ArvlInfoInqireService` 도착정보 실험이며 이 데이터셋의 등록/테이블 의존성이 없어 유지했다.
다른 데이터셋의 정류장 관련 필드·값도 유지한다.

아래 두 테이블만 삭제 후보이며 별도 승인·의존성 확인 후 운영자가 실행할 SQL이다.
CASCADE 및 monitoring 삭제 SQL은 포함하지 않는다.

```sql
DROP TABLE IF EXISTS processed.bus_stop;
DROP TABLE IF EXISTS raw.bus_stop;
```

## 복구 절차

1. 삭제 전 버전의 전용 수집기·테스트·processor를 복원한다. 참고 Git HEAD는
   `1d0d0d24e11fd131b4694b61952ec5fa31d6515d`이며 제거 당시 수집기/테스트에는
   미커밋 체크포인트 변경이 있었다. Git 버전만 복원하면 위 재개 동작이 없을 수 있으므로
   이 문서와 공통 checkpoint helper 계약에 맞춰 보완한다. 공통 파일 전체를 되돌리지 않는다.
2. metadata에 위 DatasetSpec/TableSpec과 processor registry를 재등록하고 설정 예제·README를 갱신한다.
   scheduler/ELT는 이를 자동으로 사용한다. run_last를 복원하면 다른 마지막 단계와 충돌하지 않는지 확인한다.
3. 키를 안전하게 별도 설정하고 기존 체크포인트의 fingerprint·schema 호환성을 검증한다.
   DB 테이블이 남아 있으면 재사용 가능 여부를 확인하고, 삭제됐다면 정상 snapshot 적재로 재생성한다.
4. API·DB를 mock한 수집/정제/metadata/ELT/scheduler 테스트와 compile 검증을 먼저 수행한다.
   운영 API 호출·DB 적재·스케줄 활성화는 별도 승인된 복구 작업에서 수행한다.
