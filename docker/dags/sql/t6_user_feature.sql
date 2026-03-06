-- =============================================================================
-- T6_user_feature_table 전체 재계산 (CREATE OR REPLACE)
-- =============================================================================
-- 목적: stored-data-77days(누적 원시 데이터)와 T3_session_funnel_table(세션 집계)를 기반으로
--       유저별 전체 관측 기간 피처를 계산하여 ML 모델 입력 및 분석에 활용
-- 실행 주기: 매일 (DAG2 daily_bq_update 내 t6_user_feature_replace 태스크)
-- 쓰기 방식: CREATE OR REPLACE (전체 재계산)
-- 전체 재계산 이유: 유저 피처는 전체 관측 기간(77일+누적)을 기준으로 계산되므로
--                  새 날짜 데이터가 추가될 때마다 기존 유저의 집계값이 변경됨
--                  → 증분 APPEND 불가, 전체 재계산 필요
-- 기준일: 2021-01-16 (관측 기간 종료 기준 날짜)
-- 플레이스홀더: {project}, {dataset} → DAG Python 코드에서 치환
-- =============================================================================

CREATE OR REPLACE TABLE `{project}.{dataset}.T6_user_feature_table` AS

WITH

-- ① 관측 기간 이벤트 단위 집계 (stored-data-77days 기반)
obs_event_agg AS (
  SELECT
    user_pseudo_id,
    -- 이벤트 유형별 카운트
    COUNTIF(event_name = 'page_view')                                         AS total_page_views,
    COUNTIF(event_name = 'view_item')                                          AS view_item_count,
    COUNTIF(event_name = 'add_to_cart')                                        AS add_to_cart_count,
    COUNTIF(event_name = 'begin_checkout')                                     AS begin_checkout_count,
    COUNTIF(event_name = 'add_shipping_info')                                  AS add_shipping_info_count,
    COUNTIF(event_name = 'add_payment_info')                                   AS add_payment_info_count,
    COUNTIF(event_name = 'purchase')                                           AS purchase_count,
    COUNTIF(event_name = 'scroll')                                             AS scroll_count,
    COUNTIF(event_name = 'user_engagement')                                    AS user_engagement_count,
    -- 세션 및 활동 지표
    COUNT(DISTINCT param_ga_session_id)                                        AS total_sessions,
    SUM(param_engagement_time_msec)                                            AS total_active_time,
    COUNT(*)                                                                   AS total_events,
    COUNT(DISTINCT CASE WHEN event_name = 'view_item' THEN item_name END)      AS unique_items_viewed,
    -- 실제 구매 트랜잭션 수 (is_purchaser, is_repeat_purchaser 판별용)
    COUNT(DISTINCT CASE
      WHEN event_name = 'purchase' AND transaction_id IS NOT NULL
      THEN transaction_id
    END)                                                                       AS distinct_purchase_count
  FROM `{project}.{dataset}.stored-data-77days`
  GROUP BY user_pseudo_id
),

-- ② 관측 기간 세션 단위 집계 (T3 기반)
obs_session_agg AS (
  SELECT
    user_pseudo_id,
    MIN(session_date)                                                          AS first_visit_date,
    MAX(session_date)                                                          AS last_visit_date,
    COUNT(DISTINCT session_date)                                               AS days_active,
    DATE_DIFF(MAX(session_date), MIN(session_date), DAY)                      AS visit_span_days,
    -- 가장 많이 사용한 기기/유입 채널 추출
    APPROX_TOP_COUNT(session_device, 1)[OFFSET(0)].value                      AS main_device_category,
    APPROX_TOP_COUNT(session_traffic_source, 1)[OFFSET(0)].value              AS main_traffic_source,
    -- 최초 세션의 유입 채널 (session_date, ga_session_id 오름차순 기준 첫 번째 값)
    ARRAY_AGG(session_traffic_source ORDER BY session_date ASC, ga_session_id ASC LIMIT 1)[SAFE_OFFSET(0)] AS first_traffic_source
  FROM `{project}.{dataset}.T3_session_funnel_table`
  GROUP BY user_pseudo_id
),

-- ③ 마지막/직전 세션 퍼널 단계 파악 (최근 여정 패턴 분석용)
session_ranked AS (
  SELECT
    user_pseudo_id,
    session_funnel_step,
    session_funnel_stage,
    -- 최신 세션부터 역순 번호 부여 (1=가장 최근, 2=직전)
    ROW_NUMBER() OVER (
      PARTITION BY user_pseudo_id
      ORDER BY session_date DESC, ga_session_id DESC
    ) AS rn
  FROM `{project}.{dataset}.T3_session_funnel_table`
),

obs_journey AS (
  -- 최근 2개 세션의 퍼널 단계 추출
  SELECT
    user_pseudo_id,
    MAX(CASE WHEN rn = 1 THEN session_funnel_stage END)  AS funnel_stage,       -- 마지막 세션 퍼널 단계
    MAX(CASE WHEN rn = 1 THEN session_funnel_step  END)  AS last_funnel_step,
    MAX(CASE WHEN rn = 2 THEN session_funnel_stage END)  AS prev_funnel_stage,  -- 직전 세션 퍼널 단계
    MAX(CASE WHEN rn = 2 THEN session_funnel_step  END)  AS prev_funnel_step
  FROM session_ranked
  WHERE rn <= 2
  GROUP BY user_pseudo_id
),

-- ④ 관측 기간 유저 목록 (left join 기준)
obs_users AS (
  SELECT DISTINCT user_pseudo_id
  FROM `{project}.{dataset}.stored-data-77days`
)

SELECT
  -- 유저 식별자
  u.user_pseudo_id,

  -- 타겟 변수 (기준일 2021-01-16 기준)
  IF(e.distinct_purchase_count > 0, 1, 0)                                      AS is_purchaser,        -- 구매 여부
  IF(e.distinct_purchase_count >= 2, 1, 0)                                     AS is_repeat_purchaser,  -- 재구매 여부
  IF(DATE_DIFF(DATE '2021-01-16', s.last_visit_date, DAY) > 15, 1, 0)         AS is_churned,           -- 이탈 여부 (마지막 방문 후 15일 초과)

  -- 퍼널 상태 (관측 기간 내 마지막 세션 기준)
  j.funnel_stage,
  j.last_funnel_step                                                           AS funnel_max_step,

  -- 퍼널 행동 지표
  e.total_page_views,
  e.view_item_count,
  e.add_to_cart_count,
  e.begin_checkout_count,
  e.add_shipping_info_count,
  e.add_payment_info_count,
  e.purchase_count,
  e.scroll_count,
  e.user_engagement_count,

  -- 전환율 / 이탈 패턴 파생 지표
  ROUND(e.add_to_cart_count  / NULLIF(e.view_item_count,   0), 4)              AS view_to_cart_rate,       -- 상품 조회 → 장바구니 전환율
  ROUND(e.purchase_count      / NULLIF(e.add_to_cart_count, 0), 4)              AS cart_to_purchase_rate,   -- 장바구니 → 구매 전환율
  e.begin_checkout_count - e.purchase_count                                     AS checkout_abandonment,     -- 결제 이탈 횟수

  -- 세션 / 시간 지표
  e.total_sessions,
  e.total_active_time,
  ROUND(e.total_active_time / NULLIF(e.total_sessions, 0), 2)                  AS avg_engagement_time_msec,  -- 세션당 평균 체류 시간
  DATE_DIFF(DATE '2021-01-16', s.first_visit_date, DAY)                        AS days_since_first_visit,    -- 첫 방문 이후 경과일
  DATE_DIFF(DATE '2021-01-16', s.last_visit_date,  DAY)                        AS days_since_last_visit,     -- 마지막 방문 이후 경과일
  ROUND(e.total_events / NULLIF(e.total_sessions, 0), 2)                       AS event_per_session,         -- 세션당 평균 이벤트 수
  ROUND(s.days_active  / NULLIF(s.visit_span_days, 0), 4)                      AS visit_frequency,           -- 방문 빈도 (활성일 / 총 기간)

  -- 퍼널 여정 패턴
  j.prev_funnel_stage,
  COALESCE(j.last_funnel_step, 0) - COALESCE(j.prev_funnel_step, 0)           AS funnel_improvement,        -- 퍼널 진행도 변화
  e.unique_items_viewed,
  s.main_device_category,
  s.main_traffic_source,
  s.first_traffic_source   -- 최초 유입 채널

FROM obs_users               AS u
LEFT JOIN obs_event_agg      AS e ON u.user_pseudo_id = e.user_pseudo_id
LEFT JOIN obs_session_agg    AS s ON u.user_pseudo_id = s.user_pseudo_id
LEFT JOIN obs_journey        AS j ON u.user_pseudo_id = j.user_pseudo_id
