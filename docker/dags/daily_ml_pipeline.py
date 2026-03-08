"""
DAG3: daily_ml_pipeline
========================
목적:
    DAG1(daily_ga4_ingestion) 완료 후, 로컬에 저장된 raw_data_{ga4_date}.csv를
    ml_predict.py에 전달하여 구매 예측(T7)과 트렌드 요약(T8) CSV를 생성하고,
    GCS를 거쳐 BigQuery에 APPEND 업로드한다.

태스크 흐름:
    wait_for_dag1
        └─► prepare_ml_input
                └─► ml_predict
                        └─► upload_to_gcs
                                ├─► upload_t7_to_bq (병렬)
                                └─► upload_t8_to_bq (병렬)

출력 파일:
    - 로컬: /opt/airflow/data/raw/T7_{ga4_date}.csv
            /opt/airflow/data/raw/T8_{ga4_date}.csv
    - GCS:  gs://{GCS_BUCKET}/ml_output/T7_{ga4_date}.csv
            gs://{GCS_BUCKET}/ml_output/T8_{ga4_date}.csv
    - BQ:   {project}.{dataset}.T7_prediction_result (WRITE_APPEND)
            {project}.{dataset}.T8_prediction_trend  (WRITE_APPEND)

환경변수 (.env → compose.yml → Airflow 컨테이너):
    GCP_PROJECT_ID, BQ_DATASET, GCS_BUCKET
"""

import os
import shutil
import subprocess
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sensors.external_task import ExternalTaskSensor

# ---------------------------------------------------------------------------
# 환경변수 (docker/compose.yml → .env 에서 주입)
# ---------------------------------------------------------------------------
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET     = os.getenv("BQ_DATASET")
GCS_BUCKET     = os.getenv("GCS_BUCKET")

# DAG1 merge_csv_files가 생성하는 로컬 CSV 디렉터리
LOCAL_DATA_DIR = "/opt/airflow/data/raw"

# ml_predict.py의 OUTPUT_DIR (/app/output) - 스케줄러 컨테이너에서 /app = 레포 루트
OUTPUT_DIR = "/app/output"

# ml_predict.py 스크립트 경로 (스케줄러 컨테이너에서 /app = 레포 루트)
ML_SCRIPT_PATH = "/app/src/ML/ml_predict.py"

# ml_predict.py가 읽는 고정 입력 파일 경로
ML_INPUT_PATH = "/app/data/raw/raw_data_with_gmt.csv"

# ---------------------------------------------------------------------------
# GA4 시뮬레이션 시작 날짜 (DAG1과 동일)
# DAG3는 start_date가 5분 오프셋이므로 cross-DAG XCom 대신 자체 계산
# ---------------------------------------------------------------------------
GA4_SIMULATION_START = pendulum.datetime(2021, 1, 17, tz="Asia/Seoul")

# ---------------------------------------------------------------------------
# XCom 템플릿: 동일 DAG의 compute_ga4_date 태스크가 push한 ga4_date 값을 참조
# ---------------------------------------------------------------------------
_GA4_DATE = "{{ ti.xcom_pull(task_ids='compute_ga4_date') }}"


# ---------------------------------------------------------------------------
# Python Callable 함수 정의
# ---------------------------------------------------------------------------

def compute_ga4_date(execution_date, **context):
    """
    현재 DAG의 execution_date와 start_date를 기반으로 GA4 시뮬레이션 날짜를 계산한다.

    DAG3의 schedule이 5/10 * * * * (DAG1보다 5분 오프셋)이므로,
    cross-DAG XCom pull 대신 동일 공식으로 자체 계산하여 run_id 불일치 문제를 방지한다.

    Args:
        execution_date: Airflow가 자동 주입하는 현재 run의 execution_date
    Returns:
        ga4_date 문자열 (예: '20210117') - XCom으로 자동 push됨
    """
    dag_start       = context["dag"].start_date
    delta_seconds   = int((execution_date - dag_start).total_seconds())
    delta_intervals = delta_seconds // 600          # 600초 = 10분 = 1 GA4 일
    ga4_date        = GA4_SIMULATION_START.add(days=delta_intervals)
    result          = ga4_date.format("YYYYMMDD")
    print(f"execution_date={execution_date}  delta={delta_intervals}일  →  ga4_date={result}")
    return result  # XCom으로 자동 push


def prepare_ml_input(ga4_date: str, **_):
    """
    DAG1이 생성한 raw_data_{ga4_date}.csv를 ml_predict.py의 입력 경로로 복사한다.

    ml_predict.py는 INPUT_PATH = "/app/data/raw/raw_data_with_gmt.csv"로 고정되어 있어
    날짜별 파일명을 인식하지 못하므로, 해당 날짜 파일을 고정 경로로 복사하여 전달한다.

    Args:
        ga4_date: GA4 시뮬레이션 날짜 (예: '20210117'), XCom에서 수신
    """
    src = os.path.join(LOCAL_DATA_DIR, f"raw_data_{ga4_date}.csv")
    dst = ML_INPUT_PATH

    if not os.path.exists(src):
        raise FileNotFoundError(f"ML 입력 파일이 없습니다: {src}")

    # 목적지 디렉터리가 없으면 생성
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copy2(src, dst)
    print(f"ML 입력 파일 복사 완료: {src} → {dst}")


def run_ml_predict(ga4_date: str, **_):
    """
    ml_predict.py를 subprocess로 실행하여 T7/T8 CSV를 생성하고,
    날짜 suffix가 붙은 파일명으로 rename한다.

    ml_predict.py 실행 결과:
        /app/output/t7_prediction_result.csv → T7_{ga4_date}.csv
        /app/output/t8_prediction_trend.csv  → T8_{ga4_date}.csv

    Args:
        ga4_date: GA4 시뮬레이션 날짜 (예: '20210117'), XCom에서 수신
    """
    # 출력 디렉터리 생성
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ml_predict.py 실행 (ga4_date를 인수로 전달하여 prediction_date를 시뮬레이션 날짜로 저장)
    print(f"ml_predict.py 실행 시작 (ga4_date={ga4_date})")
    result = subprocess.run(
        ["python", ML_SCRIPT_PATH, ga4_date],
        capture_output=True,
        text=True,
    )

    # 실행 로그 출력
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)

    # 실패 시 에러 발생
    if result.returncode != 0:
        raise RuntimeError(f"ml_predict.py 실행 실패 (returncode={result.returncode})")

    # 출력 파일을 날짜 suffix 파일명으로 rename
    # 덮어쓰기 방지 및 날짜별 이력 보존을 위해 T7_{date}.csv 형식으로 저장
    file_map = {
        "t7_prediction_result.csv": f"T7_{ga4_date}.csv",
        "t8_prediction_trend.csv":  f"T8_{ga4_date}.csv",
    }
    for src_name, dst_name in file_map.items():
        src = os.path.join(OUTPUT_DIR, src_name)
        dst = os.path.join(OUTPUT_DIR, dst_name)
        if not os.path.exists(src):
            raise FileNotFoundError(f"ml_predict.py 출력 파일이 없습니다: {src}")
        os.rename(src, dst)
        print(f"파일 rename 완료: {src_name} → {dst_name}")


def upload_to_gcs(ga4_date: str, **_):
    """
    로컬에 생성된 T7/T8 CSV 파일을 GCS에 업로드한다.

    업로드 경로: gs://{GCS_BUCKET}/ml_output/T7_{ga4_date}.csv
                 gs://{GCS_BUCKET}/ml_output/T8_{ga4_date}.csv

    Args:
        ga4_date: GA4 시뮬레이션 날짜 (예: '20210117'), XCom에서 수신
    """
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)

    # T7, T8 각각 업로드
    for prefix in ["T7", "T8"]:
        filename   = f"{prefix}_{ga4_date}.csv"
        local_path = os.path.join(OUTPUT_DIR, filename)
        gcs_path   = f"ml_output/{filename}"

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"GCS 업로드 대상 파일이 없습니다: {local_path}")

        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(local_path)
        print(f"GCS 업로드 완료: {local_path} → gs://{GCS_BUCKET}/{gcs_path}")


# ---------------------------------------------------------------------------
# DAG start_date: DAG1과 동일하게 AIRFLOW_DEPLOY_DATETIME env 변수로 관리
# ExternalTaskSensor가 execution_date 단위로 매칭하므로, DAG1과 start_date가 반드시 일치해야 함
# ---------------------------------------------------------------------------
_DEPLOY_DT_STR = os.getenv("AIRFLOW_DEPLOY_DATETIME")
if not _DEPLOY_DT_STR:
    raise ValueError(
        "AIRFLOW_DEPLOY_DATETIME 환경변수가 설정되지 않았습니다. "
        ".env 파일에 AIRFLOW_DEPLOY_DATETIME=YYYY-MM-DDTHH:MM:SS 를 추가하세요. (KST 기준)"
    )
_DAG_START_DATE = pendulum.parse(_DEPLOY_DT_STR, tz="Asia/Seoul").add(minutes=5)
# DAG1보다 5분 뒤 첫 인터벌 시작 → execution_delta=5min과 함께 DAG1 run을 정확히 매칭

# GA4 데이터 범위: 2021-01-17 ~ 2021-01-31 (15일) → DAG1과 동일하게 end_date 설정
_DAG_END_DATE = _DAG_START_DATE.add(minutes=14 * 10)

# ---------------------------------------------------------------------------
# DAG 정의
# ---------------------------------------------------------------------------
with DAG(
    dag_id="daily_ml_pipeline",
    start_date=_DAG_START_DATE,        # .env의 AIRFLOW_DEPLOY_DATE (DAG1과 동일)
    end_date=_DAG_END_DATE,            # GA4 2021-01-31 (15번째 인터벌)까지만 실행
    schedule_interval="5/10 * * * *",  # 매시 05/15/25/35/45/55분 실행 (DAG1보다 5분 뒤)
    catchup=False,                     # DAG1과 동일하게 False: 백로그 없이 10분마다 1개씩 실행
    max_active_runs=1,                  # 동시 실행 제한
    tags=["ml", "prediction"],
) as dag:

    # -----------------------------------------------------------------------
    # Task 0: DAG1 완료 대기
    # daily_ga4_ingestion의 merge_csv_files 완료 시 로컬 CSV가 준비된 것으로 간주
    # -----------------------------------------------------------------------
    wait_for_dag1 = ExternalTaskSensor(
        task_id="wait_for_dag1",
        external_dag_id="daily_ga4_ingestion",
        external_task_id="merge_csv_files",   # DAG1의 마지막 태스크
        timeout=7200,                          # 2시간 대기 후 실패
        poke_interval=60,                      # 60초마다 완료 여부 확인
        mode="reschedule",                     # 대기 중 worker 슬롯 반환
        execution_delta=timedelta(minutes=5),  # DAG3 execution_date - 5분 = DAG1 execution_date
    )

    # -----------------------------------------------------------------------
    # Task 0-1: GA4 시뮬레이션 날짜 계산
    # DAG3 schedule이 5/10 * * * * (DAG1보다 5분 오프셋)이므로 cross-DAG XCom 대신
    # 동일 공식으로 자체 계산 → run_id 불일치로 인한 None 반환 문제 방지
    # -----------------------------------------------------------------------
    compute_ga4_date_task = PythonOperator(
        task_id="compute_ga4_date",
        python_callable=compute_ga4_date,
    )

    # -----------------------------------------------------------------------
    # Task 1: ML 입력 파일 준비
    # raw_data_{ga4_date}.csv → raw_data_with_gmt.csv (고정 경로) 복사
    # -----------------------------------------------------------------------
    prepare_input = PythonOperator(
        task_id="prepare_ml_input",
        python_callable=prepare_ml_input,
        op_kwargs={"ga4_date": _GA4_DATE},  # XCom에서 수신한 날짜 전달
    )

    # -----------------------------------------------------------------------
    # Task 2: ML 예측 실행
    # ml_predict.py 실행 → T7_{date}.csv, T8_{date}.csv 생성
    # -----------------------------------------------------------------------
    ml_predict = PythonOperator(
        task_id="ml_predict",
        python_callable=run_ml_predict,
        op_kwargs={"ga4_date": _GA4_DATE},
    )

    # -----------------------------------------------------------------------
    # Task 3: GCS 업로드
    # T7/T8 CSV → gs://{GCS_BUCKET}/ml_output/ 업로드
    # -----------------------------------------------------------------------
    upload_gcs = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={"ga4_date": _GA4_DATE},
    )

    # -----------------------------------------------------------------------
    # Task 4-1: T7 BQ 업로드 (병렬)
    # GCS → BQ T7_prediction_result WRITE_APPEND
    # autodetect=True: CSV 헤더로 스키마 자동 감지 (첫 실행 시 테이블 자동 생성)
    # -----------------------------------------------------------------------
    upload_t7_to_bq = BigQueryInsertJobOperator(
        task_id="upload_t7_to_bq",
        configuration={
            "load": {
                "sourceUris": [
                    f"gs://{GCS_BUCKET}/ml_output/T7_{_GA4_DATE}.csv"
                ],
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BQ_DATASET,
                    "tableId":   "T7_prediction_result",
                },
                "createDisposition": "CREATE_IF_NEEDED",  # 테이블 없으면 자동 생성
                "writeDisposition":  "WRITE_APPEND",      # 기존 데이터 유지, 신규 행 추가
                "sourceFormat":      "CSV",
                "skipLeadingRows":   1,                   # 헤더 행 스킵
                "autodetect":        True,                # 스키마 자동 감지
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # -----------------------------------------------------------------------
    # Task 4-2: T8 BQ 업로드 (병렬)
    # GCS → BQ T8_prediction_trend WRITE_APPEND
    # -----------------------------------------------------------------------
    upload_t8_to_bq = BigQueryInsertJobOperator(
        task_id="upload_t8_to_bq",
        configuration={
            "load": {
                "sourceUris": [
                    f"gs://{GCS_BUCKET}/ml_output/T8_{_GA4_DATE}.csv"
                ],
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BQ_DATASET,
                    "tableId":   "T8_prediction_trend",
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition":  "WRITE_APPEND",
                "sourceFormat":      "CSV",
                "skipLeadingRows":   1,
                "autodetect":        True,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # -----------------------------------------------------------------------
    # 태스크 의존성 정의
    #
    # wait_for_dag1 → compute_ga4_date → prepare_ml_input → ml_predict → upload_to_gcs
    #                                                                          ├─► upload_t7_to_bq (병렬)
    #                                                                          └─► upload_t8_to_bq (병렬)
    # -----------------------------------------------------------------------
    wait_for_dag1 >> compute_ga4_date_task >> prepare_input >> ml_predict >> upload_gcs >> [upload_t7_to_bq, upload_t8_to_bq]
