import os
import glob
import pendulum
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

# ---------------------------------------------------------------------------
# 환경변수 (docker/compose.yml → .env 에서 주입)
# ---------------------------------------------------------------------------
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET     = os.getenv("BQ_DATASET")
GCS_BUCKET     = os.getenv("GCS_BUCKET")
LOCAL_DATA_DIR = "/opt/airflow/data/raw"

# ---------------------------------------------------------------------------
# 날짜 시뮬레이션 기준
# 배포일(start_date) 기준 Day 1 = 2021-01-17, Day 2 = 2021-01-18, ...
# ---------------------------------------------------------------------------
GA4_SIMULATION_START = pendulum.datetime(2021, 1, 17, tz="UTC")

# ---------------------------------------------------------------------------
# SQL 로딩
# ---------------------------------------------------------------------------
_SQL_PATH = os.path.join(os.path.dirname(__file__), "sql", "raw_data_daily.sql")
with open(_SQL_PATH) as f:
    BQ_QUERY = f.read()


# ---------------------------------------------------------------------------
# Python callable: GA4 날짜 계산 (실행일 → 시뮬레이션 GA4 날짜)
# ---------------------------------------------------------------------------
def compute_ga4_date(execution_date, **context):
    dag_start       = context["dag"].start_date
    # 10분 인터벌 단위로 delta 계산 (600초 = 10분 = GA4 1일)
    # @daily 대신 0/10 * * * * 스케줄 사용 시, 같은 날 여러 실행이 서로 다른 ga4_date를 가짐
    delta_seconds   = int((execution_date - dag_start).total_seconds())
    delta_intervals = delta_seconds // 600          # 600초 = 10분 = 1 GA4 일
    ga4_date        = GA4_SIMULATION_START.add(days=delta_intervals)
    result          = ga4_date.format("YYYYMMDD")
    print(f"execution_date={execution_date}  delta={delta_intervals}일  →  ga4_date={result}")
    return result  # XCom으로 자동 push


# ---------------------------------------------------------------------------
# Python callable: GCS → 로컬 다운로드
# ---------------------------------------------------------------------------
def download_gcs_files(ga4_date: str, **_):
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    prefix = f"raw_data/{ga4_date}/"

    blobs = list(bucket.list_blobs(prefix=prefix))
    if not blobs:
        raise ValueError(f"GCS에서 파일을 찾을 수 없습니다: gs://{GCS_BUCKET}/{prefix}")

    local_dir = os.path.join(LOCAL_DATA_DIR, ga4_date)
    os.makedirs(local_dir, exist_ok=True)

    for blob in blobs:
        filename = os.path.basename(blob.name)
        dest = os.path.join(local_dir, filename)
        blob.download_to_filename(dest)
        print(f"Downloaded: gs://{GCS_BUCKET}/{blob.name} → {dest}")


# ---------------------------------------------------------------------------
# Python callable: split CSV → 단일 파일로 병합
# ---------------------------------------------------------------------------
def merge_csv_files(ga4_date: str, **_):
    local_dir   = os.path.join(LOCAL_DATA_DIR, ga4_date)
    output_path = os.path.join(LOCAL_DATA_DIR, f"raw_data_{ga4_date}.csv")

    pattern = os.path.join(local_dir, f"raw_data_{ga4_date}_*.csv")
    files   = sorted(glob.glob(pattern))

    if not files:
        raise ValueError(f"병합할 CSV 파일이 없습니다: {pattern}")

    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    df.to_csv(output_path, index=False)
    print(f"Merged {len(files)} files → {output_path}  ({len(df):,} rows)")


# ---------------------------------------------------------------------------
# DAG 정의
# ---------------------------------------------------------------------------
_XCOM_GA4_DATE = "{{ ti.xcom_pull(task_ids='compute_ga4_date') }}"

with DAG(
    dag_id="daily_ga4_ingestion",
    start_date=pendulum.datetime(2026, 3, 6, 0, 0, 0, tz="UTC"),  # 배포 시점 고정 (재로드 시 delta_days 틀어짐 방지)
    schedule_interval="0/10 * * * *",  # 매시 00/10/20/30/40/50분 실행 (테스트용, 실제 배포 시 @daily로 복구)
    catchup=False,
    max_active_runs=3,
    tags=["ga4", "ingestion"],
) as dag:

    # ------------------------------------------------------------------
    # Task 0: 실행일 → GA4 날짜 매핑 (XCom push)
    # ------------------------------------------------------------------
    get_ga4_date = PythonOperator(
        task_id="compute_ga4_date",
        python_callable=compute_ga4_date,
    )

    # ------------------------------------------------------------------
    # Task 1: BigQuery events_YYYYMMDD → raw_data_YYYYMMDD 테이블 생성
    # ------------------------------------------------------------------
    bq_create_raw_table = BigQueryInsertJobOperator(
        task_id="bq_create_raw_table",
        configuration={
            "query": {
                "query": BQ_QUERY,
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BQ_DATASET,
                    "tableId":   f"raw_data_{_XCOM_GA4_DATE}",
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition":  "WRITE_TRUNCATE",
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # ------------------------------------------------------------------
    # Task 2: BQ 테이블 → GCS split CSV 내보내기
    # ------------------------------------------------------------------
    bq_export_to_gcs = BigQueryToGCSOperator(
        task_id="bq_export_to_gcs",
        source_project_dataset_table=(
            f"{GCP_PROJECT_ID}.{BQ_DATASET}.raw_data_{_XCOM_GA4_DATE}"
        ),
        destination_cloud_storage_uris=[
            f"gs://{GCS_BUCKET}/raw_data/{_XCOM_GA4_DATE}/raw_data_{_XCOM_GA4_DATE}_*.csv"
        ],
        export_format="CSV",
        print_header=True,
        gcp_conn_id="google_cloud_default",
    )

    # ------------------------------------------------------------------
    # Task 3: GCS split CSV → 로컬 다운로드
    # ------------------------------------------------------------------
    download_from_gcs = PythonOperator(
        task_id="download_from_gcs",
        python_callable=download_gcs_files,
        op_kwargs={"ga4_date": _XCOM_GA4_DATE},
    )

    # ------------------------------------------------------------------
    # Task 4: split CSV → raw_data_YYYYMMDD.csv 단일 파일 병합
    # ------------------------------------------------------------------
    merge_csv = PythonOperator(
        task_id="merge_csv_files",
        python_callable=merge_csv_files,
        op_kwargs={"ga4_date": _XCOM_GA4_DATE},
    )

    # ------------------------------------------------------------------
    # 의존성
    # ------------------------------------------------------------------
    get_ga4_date >> bq_create_raw_table >> bq_export_to_gcs >> download_from_gcs >> merge_csv
