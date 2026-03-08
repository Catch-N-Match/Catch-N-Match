"""
DAG2: daily_bq_update
======================
목적:
    DAG1(daily_ga4_ingestion) 완료 후, 해당 날짜의 raw_data_{ga4_date}를
    기존 누적 테이블(stored-data-77days)에 APPEND하고,
    T1~T6 BigQuery 분석 테이블을 갱신한다.

갱신 방식:
    - T1 / T2(×2) / T3 / T4: WRITE_APPEND (증분 - 새 날짜 데이터만 INSERT)
    - T5 / T6: CREATE OR REPLACE (전체 재계산 - 누적 집계 특성상 불가피)

태스크 흐름:
    wait_for_dag1
        └─► append_to_stored_data
                ├─► t1_append          (병렬)
                ├─► t2_order_append    (병렬)
                ├─► t2_detail_append   (병렬)
                └─► t3_session_funnel_append  (병렬)
                        ├─► t4_funnel_daily_append    (병렬)
                        └─► t5_device_channel_replace (병렬)
                                └─► t6_user_feature_replace

환경변수 (.env → compose.yml → Airflow 컨테이너):
    GCP_PROJECT_ID, BQ_DATASET, GCS_BUCKET
"""

import os
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

# ---------------------------------------------------------------------------
# GA4 시뮬레이션 시작 날짜 (DAG1과 동일)
# DAG2/DAG3는 start_date가 5분 오프셋이므로 cross-DAG XCom 대신 자체 계산
# ---------------------------------------------------------------------------
GA4_SIMULATION_START = pendulum.datetime(2021, 1, 17, tz="Asia/Seoul")

# ---------------------------------------------------------------------------
# XCom 템플릿: 동일 DAG의 compute_ga4_date 태스크가 push한 ga4_date 값을 참조
# Airflow의 Jinja 렌더링 시점에 실제 날짜 문자열(예: '20210117')로 치환됨
# ---------------------------------------------------------------------------
_GA4_DATE = "{{ ti.xcom_pull(task_ids='compute_ga4_date') }}"


def compute_ga4_date(execution_date, **context):
    """
    현재 DAG의 execution_date와 start_date를 기반으로 GA4 시뮬레이션 날짜를 계산한다.

    DAG2의 schedule이 5/10 * * * * (DAG1보다 5분 오프셋)이므로,
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


def load_sql(filename: str) -> str:
    """
    SQL 파일을 읽어서 플레이스홀더를 실제 값으로 치환한다.

    플레이스홀더 규칙:
        {project}   → GCP_PROJECT_ID (환경변수, DAG 로드 시점에 치환)
        {dataset}   → BQ_DATASET     (환경변수, DAG 로드 시점에 치환)
        {ga4_date}  → Jinja 템플릿   (Airflow 런타임 시점에 XCom에서 치환)

    Args:
        filename: sql/ 디렉터리 기준 SQL 파일명 (예: 't1_daily_summary_append.sql')

    Returns:
        플레이스홀더가 치환된 SQL 문자열
    """
    sql_path = os.path.join(os.path.dirname(__file__), "sql", filename)
    with open(sql_path) as f:
        sql = f.read()

    return (
        sql
        .replace("{project}",  GCP_PROJECT_ID)
        .replace("{dataset}",  BQ_DATASET)
        .replace("{ga4_date}", _GA4_DATE)   # Jinja 템플릿으로 치환 → Airflow가 런타임에 렌더링
    )


# ---------------------------------------------------------------------------
# DAG start_date: DAG1과 동일하게 AIRFLOW_DEPLOY_DATE env 변수로 관리
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
    dag_id="daily_bq_update",
    start_date=_DAG_START_DATE,        # .env의 AIRFLOW_DEPLOY_DATE (DAG1과 동일)
    end_date=_DAG_END_DATE,            # GA4 2021-01-31 (15번째 인터벌)까지만 실행
    schedule_interval="5/10 * * * *",  # 매시 05/15/25/35/45/55분 실행 (DAG1보다 5분 뒤)
    catchup=False,                     # DAG1과 동일하게 False: 백로그 없이 10분마다 1개씩 실행
    max_active_runs=1,                  # 동시 실행 제한 (DAG1과 순서 보장)
    tags=["analytics", "bigquery"],
) as dag:

    # -----------------------------------------------------------------------
    # Task 0: DAG1 완료 대기
    # ExternalTaskSensor로 daily_ga4_ingestion의 merge_csv_files 태스크 완료를 감지
    # timeout: 최대 2시간 대기 후 실패 처리
    # mode="reschedule": 대기 중 worker 슬롯을 점유하지 않음 (리소스 효율)
    # -----------------------------------------------------------------------
    wait_for_dag1 = ExternalTaskSensor(
        task_id="wait_for_dag1",
        external_dag_id="daily_ga4_ingestion",
        external_task_id="merge_csv_files",   # DAG1의 마지막 태스크
        timeout=7200,                          # 2시간 대기 후 실패
        poke_interval=60,                      # 60초마다 완료 여부 확인
        mode="reschedule",                     # 대기 중 worker 슬롯 반환
        execution_delta=timedelta(minutes=5),  # DAG2 execution_date - 5분 = DAG1 execution_date
    )

    # -----------------------------------------------------------------------
    # Task 0-1: GA4 시뮬레이션 날짜 계산
    # DAG2 schedule이 5/10 * * * * (DAG1보다 5분 오프셋)이므로 cross-DAG XCom 대신
    # 동일 공식으로 자체 계산 → run_id 불일치로 인한 None 반환 문제 방지
    # -----------------------------------------------------------------------
    compute_ga4_date_task = PythonOperator(
        task_id="compute_ga4_date",
        python_callable=compute_ga4_date,
    )

    # -----------------------------------------------------------------------
    # Task 1: stored-data-77days에 신규 날짜 데이터 APPEND
    # raw_data_{ga4_date} → stored-data-77days (INSERT SELECT *)
    # 컬럼 구조가 동일하므로 SELECT * 로 단순 APPEND
    # -----------------------------------------------------------------------
    append_to_stored_data = BigQueryInsertJobOperator(
        task_id="append_to_stored_data",
        configuration={
            "query": {
                # INSERT INTO 대상 테이블명의 하이픈(-)은 BigQuery 백틱으로 처리
                "query": (
                    f"INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET}.stored-data-77days` "
                    f"SELECT * FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.raw_data_{_GA4_DATE}`"
                ),
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # -----------------------------------------------------------------------
    # Task 2-1: T1_daily_summary_table 증분 APPEND (병렬 실행 가능)
    # 하루치 일별 요약 지표 1행 INSERT
    # -----------------------------------------------------------------------
    t1_append = BigQueryInsertJobOperator(
        task_id="t1_append",
        configuration={
            "query": {
                "query":        load_sql("t1_daily_summary_append.sql"),
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # -----------------------------------------------------------------------
    # Task 2-2: T2_order_table 증분 APPEND (병렬 실행 가능)
    # 신규 트랜잭션 행 INSERT
    # -----------------------------------------------------------------------
    t2_order_append = BigQueryInsertJobOperator(
        task_id="t2_order_append",
        configuration={
            "query": {
                "query":        load_sql("t2_order_append.sql"),
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # -----------------------------------------------------------------------
    # Task 2-3: T2_detail_order_item_table 증분 APPEND (병렬 실행 가능)
    # 신규 구매 아이템 상세 행 INSERT
    # -----------------------------------------------------------------------
    t2_detail_append = BigQueryInsertJobOperator(
        task_id="t2_detail_append",
        configuration={
            "query": {
                "query":        load_sql("t2_detail_order_item_append.sql"),
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # -----------------------------------------------------------------------
    # Task 2-4: T3_session_funnel_table 증분 APPEND (병렬 실행 가능)
    # 신규 세션 퍼널 행 INSERT
    # -----------------------------------------------------------------------
    t3_session_funnel_append = BigQueryInsertJobOperator(
        task_id="t3_session_funnel_append",
        configuration={
            "query": {
                "query":        load_sql("t3_session_funnel_append.sql"),
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # -----------------------------------------------------------------------
    # Task 3-1: T4_funnel_daily_table 증분 APPEND
    # T3 APPEND 완료 후 실행 (T3 데이터 의존)
    # 신규 날짜의 퍼널 단계별 집계 6행 INSERT
    # -----------------------------------------------------------------------
    t4_funnel_daily_append = BigQueryInsertJobOperator(
        task_id="t4_funnel_daily_append",
        configuration={
            "query": {
                "query":        load_sql("t4_funnel_daily_append.sql"),
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # -----------------------------------------------------------------------
    # Task 3-2: T5_device_channel_table 전체 재계산 (병렬 실행 가능)
    # T3 APPEND 완료 후 실행 (T3 전체 데이터 의존)
    # CREATE OR REPLACE로 누적 집계 재계산
    # -----------------------------------------------------------------------
    t5_device_channel_replace = BigQueryInsertJobOperator(
        task_id="t5_device_channel_replace",
        configuration={
            "query": {
                "query":        load_sql("t5_device_channel.sql"),
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # -----------------------------------------------------------------------
    # Task 4: T6_user_feature_table 전체 재계산
    # T4, T5 완료 후 실행 (stored-data-77days + T3 전체 의존)
    # CREATE OR REPLACE로 유저별 피처 재계산
    # -----------------------------------------------------------------------
    t6_user_feature_replace = BigQueryInsertJobOperator(
        task_id="t6_user_feature_replace",
        configuration={
            "query": {
                "query":        load_sql("t6_user_feature.sql"),
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
    )

    # -----------------------------------------------------------------------
    # 태스크 의존성 정의
    #
    # wait_for_dag1
    #     └─► compute_ga4_date
    #             └─► append_to_stored_data
    #                     ├─► t1_append          (병렬)
    #                     ├─► t2_order_append    (병렬)
    #                     ├─► t2_detail_append   (병렬)
    #                     └─► t3_session_funnel_append (병렬)
    #                             ├─► t4_funnel_daily_append    (병렬)
    #                             └─► t5_device_channel_replace (병렬)
    #                                     └─► t6_user_feature_replace
    # -----------------------------------------------------------------------
    wait_for_dag1 >> compute_ga4_date_task >> append_to_stored_data

    # stored-data-77days APPEND 완료 후 T1~T3 병렬 실행
    append_to_stored_data >> [t1_append, t2_order_append, t2_detail_append, t3_session_funnel_append]

    # T3 완료 후 T4, T5 병렬 실행
    t3_session_funnel_append >> [t4_funnel_daily_append, t5_device_channel_replace]

    # T4, T5 완료 후 T6 실행
    [t4_funnel_daily_append, t5_device_channel_replace] >> t6_user_feature_replace
