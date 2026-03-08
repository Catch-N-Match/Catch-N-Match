# Catch-N-Match

GA4 ecommerce 샘플 데이터를 Airflow로 수집·분석·ML 예측하는 파이프라인.
시각화: Looker Studio ← BigQuery T1~T8 테이블

---

## DAG 구조

### DAG1: `daily_ga4_ingestion`
- 스케줄: `0/10 * * * *` (10분마다, GA4 1일치 처리)
- 흐름: `compute_ga4_date` → `bq_create_raw_table` → `bq_export_to_gcs` → `download_from_gcs` → `merge_csv_files`
- 출력: BQ `raw_data_{date}` 테이블 + 로컬 `/opt/airflow/data/raw/raw_data_{date}.csv`

### DAG2: `daily_bq_update`
- 스케줄: `5/10 * * * *` (DAG1보다 5분 뒤)
- `ExternalTaskSensor`로 DAG1 `merge_csv_files` 완료 대기 (최대 2시간)
- 흐름: `wait_for_dag1` → `compute_ga4_date` → `append_to_stored_data` → T1~T6 갱신
- DAG1과 독립: DAG3 영향 없음

### DAG3: `daily_ml_pipeline`
- 스케줄: `5/10 * * * *` (DAG2와 동일)
- `ExternalTaskSensor`로 DAG1 `merge_csv_files` 완료 대기 (최대 2시간)
- 흐름: `wait_for_dag1` → `compute_ga4_date` → `prepare_ml_input` → `ml_predict` → `upload_to_gcs` → T7/T8 BQ 업로드
- DAG1과 독립: DAG2 영향 없음

---

## 날짜 시뮬레이션

| 배포 후 인터벌 | GA4 날짜 | 설명 |
|---|---|---|
| 0번째 (첫 실행) | 2021-01-17 | `AIRFLOW_DEPLOY_DATETIME` 기준 |
| 1번째 (+10분) | 2021-01-18 | |
| ... | ... | |
| 14번째 (+140분) | 2021-01-31 | 마지막 실행 |

- GA4 데이터 범위: 2021-01-17 ~ 2021-01-31 (15일)
- `end_date` = `AIRFLOW_DEPLOY_DATETIME` + 145분 (DAG2/3 기준)
- 현재 시각이 `end_date`를 초과하면 task skip → DAG success with no tasks 발생

---

## 실패 동작

| 실패 위치 | 동작 |
|---|---|
| DAG1 task 실패 | retries 없음 → 하위 task `upstream_failed` → DAG2/3 Sensor 2시간 대기 후 timeout |
| DAG2 task 실패 | DAG3 영향 없음 (독립) |
| DAG3 task 실패 | DAG2 영향 없음 (독립) |

---

## 재시작 절차

`docker compose up` 이후 시간이 지나 `end_date`가 만료되면 DAG가 실행되지 않는다.
완전 재시작 시 `.env`의 `AIRFLOW_DEPLOY_DATETIME`을 **`up -d` 직전 현재 KST 시각**으로 반드시 업데이트할 것.

```bash
# 1. 컨테이너 및 볼륨 제거
docker compose -f docker/compose.yml --env-file .env down -v

# 2. 로그 정리
sudo rm -rf docker/logs/*

# 3. .env 의 AIRFLOW_DEPLOY_DATETIME 을 현재 KST 시각으로 수정
#    예: AIRFLOW_DEPLOY_DATETIME=2026-03-08T14:30:00
vi .env

# 4. 재시작
docker compose -f docker/compose.yml --env-file .env up -d
```

> `.env`의 `AIRFLOW_DEPLOY_DATETIME` 형식: `YYYY-MM-DDTHH:MM:SS` (KST 기준)

---

## 환경 설정

`.env.example`을 복사하여 `.env` 파일 생성 후 값 입력:

```bash
cp .env.example .env
```

| 변수 | 설명 |
|---|---|
| `GCP_PROJECT_ID` | GCP 프로젝트 ID |
| `BQ_DATASET` | BigQuery 데이터셋 ID |
| `GCS_BUCKET` | GCS 버킷 이름 |
| `AIRFLOW_DEPLOY_DATETIME` | Airflow 배포 시각 (KST, `YYYY-MM-DDTHH:MM:SS`) |

GCP 서비스 계정 키 파일을 `credentials/service_account.json`에 위치시킬 것.

---

## 주요 경로

| 경로 | 설명 |
|---|---|
| `docker/dags/` | Airflow DAG 파일 |
| `docker/dags/sql/` | BigQuery SQL 파일 |
| `src/ML/ml_predict.py` | ML 예측 스크립트 |
| `src/ML/models/purchase_model.pkl` | ML 모델 |
| `data/raw/` | 로컬 raw CSV 파일 |
| `output/` | ML 예측 결과 CSV |
| `credentials/` | GCP 서비스 계정 키 (gitignore) |
