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
# SQL 로딩
# ---------------------------------------------------------------------------
_SQL_PATH = os.path.join(os.path.dirname(__file__), "sql", "raw_data_daily.sql")
with open(_SQL_PATH) as f:
    BQ_QUERY = f.read()


# ---------------------------------------------------------------------------
# Python callable: GCS → 로컬 다운로드
# ---------------------------------------------------------------------------
def download_gcs_files(ds_nodash: str, **_):
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    prefix = f"raw_data/{ds_nodash}/"

    blobs = list(bucket.list_blobs(prefix=prefix))
    if not blobs:
        raise ValueError(f"GCS에서 파일을 찾을 수 없습니다: gs://{GCS_BUCKET}/{prefix}")

    local_dir = os.path.join(LOCAL_DATA_DIR, ds_nodash)
    os.makedirs(local_dir, exist_ok=True)

    for blob in blobs:
        filename = os.path.basename(blob.name)
        dest = os.path.join(local_dir, filename)
        blob.download_to_filename(dest)
        print(f"Downloaded: gs://{GCS_BUCKET}/{blob.name} → {dest}")


# ---------------------------------------------------------------------------
# Python callable: split CSV → 단일 파일로 병합
# ---------------------------------------------------------------------------
def merge_csv_files(ds_nodash: str, **_):
    local_dir   = os.path.join(LOCAL_DATA_DIR, ds_nodash)
    output_path = os.path.join(LOCAL_DATA_DIR, f"raw_data_{ds_nodash}.csv")

    pattern = os.path.join(local_dir, f"raw_data_{ds_nodash}_*.csv")
    files   = sorted(glob.glob(pattern))

    if not files:
        raise ValueError(f"병합할 CSV 파일이 없습니다: {pattern}")

    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    df.to_csv(output_path, index=False)
    print(f"Merged {len(files)} files → {output_path}  ({len(df):,} rows)")


# ---------------------------------------------------------------------------
# DAG 정의
# ---------------------------------------------------------------------------
with DAG(
    dag_id="daily_ga4_ingestion",
    start_date=pendulum.datetime(2021, 1, 17, tz="UTC"),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=3,
    tags=["ga4", "ingestion"],
) as dag:

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
                    "tableId":   "raw_data_{{ ds_nodash }}",
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
            f"{GCP_PROJECT_ID}.{BQ_DATASET}.raw_data_{{{{ ds_nodash }}}}"
        ),
        destination_cloud_storage_uris=[
            f"gs://{GCS_BUCKET}/raw_data/{{{{ ds_nodash }}}}"
            f"/raw_data_{{{{ ds_nodash }}}}_*.csv"
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
        op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
    )

    # ------------------------------------------------------------------
    # Task 4: split CSV → raw_data_YYYYMMDD.csv 단일 파일 병합
    # ------------------------------------------------------------------
    merge_csv = PythonOperator(
        task_id="merge_csv_files",
        python_callable=merge_csv_files,
        op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
    )

    # ------------------------------------------------------------------
    # 의존성
    # ------------------------------------------------------------------
    bq_create_raw_table >> bq_export_to_gcs >> download_from_gcs >> merge_csv
