-- =============================================================================
-- T1_daily_summary_table 증분 APPEND
-- =============================================================================
-- 목적: DAG1이 생성한 raw_data_{ga4_date} 테이블에서
--       해당 날짜 하루치 일별 요약 지표를 집계하여 T1에 INSERT
-- 실행 주기: 매일 (DAG2 daily_bq_update 내 t1_append 태스크)
-- 쓰기 방식: WRITE_APPEND (기존 데이터 유지, 신규 날짜 1행 추가)
-- 플레이스홀더: {project}, {dataset}, {ga4_date} → DAG Python 코드에서 치환
-- =============================================================================

INSERT INTO `{project}.{dataset}.T1_daily_summary_table`

WITH session_base AS (
  -- 세션 단위 중복 제거: (user, session_id) 기준으로 유니크 세션 집계
  SELECT
    event_date,
    user_pseudo_id,
    param_ga_session_id
  FROM `{project}.{dataset}.raw_data_{ga4_date}`
  WHERE param_ga_session_id IS NOT NULL
  GROUP BY 1, 2, 3
),

purchase_base AS (
  -- 트랜잭션 단위 중복 제거: items UNNEST로 인해 transaction당 여러 행 → 1행으로 집약
  SELECT
    event_date,
    transaction_id,
    MAX(purchase_revenue) AS purchase_revenue
  FROM `{project}.{dataset}.raw_data_{ga4_date}`
  WHERE event_name = 'purchase'
    AND transaction_id IS NOT NULL
  GROUP BY 1, 2
),

shipping_base AS (
  -- 배송 유형별 이벤트 카운트 (add_shipping_info 기준)
  SELECT
    event_date,
    COUNTIF(param_shipping_tier = 'Ground')                              AS ground_shipping_cnt,
    COUNTIF(param_shipping_tier IN ('Next Day Air', '2 Day Air'))         AS air_shipping_cnt
  FROM `{project}.{dataset}.raw_data_{ga4_date}`
  WHERE event_name = 'add_shipping_info'
  GROUP BY 1
),

daily_sessions AS (
  -- 일별 세션 수 및 유저 수 집계
  SELECT
    event_date,
    COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(param_ga_session_id AS STRING))) AS daily_sessions,
    COUNT(DISTINCT user_pseudo_id)                                                AS daily_users
  FROM session_base
  GROUP BY 1
),

daily_orders AS (
  -- 일별 매출 및 거래 건수 집계
  SELECT
    event_date,
    SUM(purchase_revenue)          AS daily_revenue,
    COUNT(DISTINCT transaction_id) AS daily_transactions
  FROM purchase_base
  GROUP BY 1
)

SELECT
  ds.event_date,
  ds.daily_sessions,
  ds.daily_users,
  -- 매출 관련 지표 (거래 없는 날은 0으로 채움)
  COALESCE(do.daily_revenue, 0)                                                  AS daily_revenue,
  COALESCE(do.daily_transactions, 0)                                              AS daily_transactions,
  -- 평균 주문 금액 = 매출 / 거래 건수
  COALESCE(do.daily_revenue, 0) / NULLIF(COALESCE(do.daily_transactions, 0), 0)  AS daily_avg_order_value,
  -- 전환율 = 거래 건수 / 세션 수
  COALESCE(do.daily_transactions, 0) / NULLIF(ds.daily_sessions, 0)              AS daily_conversion_rate,
  -- 배송 유형별 카운트
  COALESCE(sb.ground_shipping_cnt, 0)                                             AS ground_shipping_cnt,
  COALESCE(sb.air_shipping_cnt, 0)                                                AS air_shipping_cnt,
  -- 프로모션 기간 레이블 분류
  CASE
    WHEN ds.event_date BETWEEN '20201123' AND '20201130' THEN 'black_friday'
    WHEN ds.event_date BETWEEN '20201207' AND '20201218' THEN 'christmas'
    ELSE 'normal'
  END AS period_label

FROM daily_sessions ds
LEFT JOIN daily_orders  do ON ds.event_date = do.event_date
LEFT JOIN shipping_base sb ON ds.event_date = sb.event_date
