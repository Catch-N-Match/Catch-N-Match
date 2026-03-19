-- =============================================================================
-- T3_session_funnel_table 증분 APPEND
-- =============================================================================
-- 목적: DAG1이 생성한 raw_data_{ga4_date} 테이블에서
--       해당 날짜의 신규 세션 퍼널 데이터를 T3_session_funnel_table에 INSERT
-- 실행 주기: 매일 (DAG2 daily_bq_update 내 t3_session_funnel_append 태스크)
-- 쓰기 방식: WRITE_APPEND (기존 데이터 유지, 신규 세션 행 추가)
-- 플레이스홀더: {project}, {dataset}, {ga4_date} → DAG Python 코드에서 치환
-- 주의: churn_session_flag는 LEAD(next_session_date)를 당일 데이터 내에서만 계산하므로
--       일내 마지막 세션의 이탈 여부는 미래 데이터가 없어 항상 0으로 처리됨 (허용된 한계)
-- =============================================================================

INSERT INTO `{project}.{dataset}.T3_session_funnel_table`

WITH base AS (
  -- 세션 단위 집계: (user_pseudo_id, param_ga_session_id, event_date) 기준
  SELECT
    user_pseudo_id,
    param_ga_session_id                                              AS ga_session_id,

    -- 세션 날짜 / 시간 파생 (타임존 오프셋 적용)
    PARSE_DATE('%Y%m%d', event_date)                                 AS session_date,
    EXTRACT(HOUR FROM
      TIMESTAMP_ADD(
        TIMESTAMP_MICROS(MIN(event_timestamp)),
        INTERVAL ANY_VALUE(offset_minutes) MINUTE
      )
    )                                                                AS session_hour,
    FORMAT_DATE('%A', PARSE_DATE('%Y%m%d', event_date))              AS session_weekday,

    -- 이벤트 유형별 카운트
    COUNTIF(event_name = 'page_view')                                AS session_page_views,
    COUNTIF(event_name = 'view_item')                                AS session_view_item_count,
    COUNTIF(event_name = 'add_to_cart')                              AS session_add_to_cart_count,
    COUNTIF(event_name = 'begin_checkout')                           AS session_checkout_count,
    COUNTIF(event_name = 'add_shipping_info')                        AS session_add_shipping_info_count,
    COUNTIF(event_name = 'add_payment_info')                         AS session_add_payment_info_count,
    COUNTIF(event_name = 'purchase')                                 AS session_purchase_count,
    COUNTIF(event_name = 'scroll')                                   AS session_scroll_count,
    COUNT(*)                                                         AS session_event_count,

    -- 세션 체류 시간 (밀리초 합산)
    SUM(param_engagement_time_msec)                                  AS session_engagement_time,

    -- 디바이스 / 유입 채널 (세션 내 대표값 1개 선택)
    ANY_VALUE(category)                                              AS session_device,
    ANY_VALUE(source)                                                AS session_traffic_source,
    ANY_VALUE(medium)                                                AS session_traffic_medium,

    -- 세션 번호 (해당 유저의 몇 번째 세션인지)
    ANY_VALUE(param_ga_session_number)                               AS session_number,

    -- 프로모션 기간 레이블 분류
    CASE
      WHEN event_date BETWEEN '20201123' AND '20201130' THEN 'black_friday'
      WHEN event_date BETWEEN '20201207' AND '20201218' THEN 'christmas'
      ELSE 'normal'
    END AS period_label

  FROM `{project}.{dataset}.raw_data_{ga4_date}`
  WHERE param_ga_session_id IS NOT NULL
  GROUP BY
    user_pseudo_id,
    param_ga_session_id,
    event_date
),

funnel AS (
  -- 퍼널 단계 및 구매/바운스 여부 파생
  SELECT
    *,
    -- 퍼널 단계 숫자 (6단계: 발견 → 흥미 → 욕망 → 구매 시도 → 구매 임박 → 구매 완료)
    CASE
      WHEN session_purchase_count > 0                                                THEN 6
      WHEN session_add_payment_info_count > 0 OR session_add_shipping_info_count > 0 THEN 5
      WHEN session_checkout_count > 0                                                THEN 4
      WHEN session_add_to_cart_count > 0                                             THEN 3
      WHEN session_view_item_count > 0                                               THEN 2
      ELSE 1
    END AS session_funnel_step,

    -- 구매 완료 여부 플래그
    IF(session_purchase_count > 0, 1, 0)                              AS purchase_flag,

    -- 바운스 여부: 페이지뷰 1개 이하이면서 구매 없는 세션
    IF(session_page_views <= 1 AND session_purchase_count = 0, 1, 0)  AS bounce_flag

  FROM base
),

churn AS (
  -- 이탈 세션 판별: 같은 유저의 다음 세션까지 15일 초과 시 이탈로 간주
  -- 주의: 당일 데이터만 보므로 일내 마지막 세션의 next_session_date는 NULL
  --       → COALESCE(NULL, session_date) = session_date → DATE_DIFF = 0 → churn_session_flag = 0
  SELECT
    *,
    LEAD(session_date) OVER (
      PARTITION BY user_pseudo_id
      ORDER BY session_date, ga_session_id
    ) AS next_session_date
  FROM funnel
)

SELECT
  * EXCEPT(next_session_date),

  -- 퍼널 단계 텍스트 레이블
  CASE session_funnel_step
    WHEN 6 THEN '구매 완료'
    WHEN 5 THEN '구매 임박'
    WHEN 4 THEN '구매 시도'
    WHEN 3 THEN '욕망'
    WHEN 2 THEN '흥미'
    ELSE        '발견'
  END AS session_funnel_stage,

  -- 이탈 세션 여부: 다음 세션까지 15일 초과이면 1
  -- next_session_date가 NULL(당일 마지막 세션)이면 session_date 자신으로 대체 → diff = 0 → 항상 0
  IF(
    DATE_DIFF(
      COALESCE(next_session_date, session_date),
      session_date,
      DAY
    ) > 15, 1, 0
  ) AS churn_session_flag

FROM churn
