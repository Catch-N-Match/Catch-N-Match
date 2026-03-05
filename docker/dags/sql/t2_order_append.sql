-- =============================================================================
-- T2_order_table 증분 APPEND
-- =============================================================================
-- 목적: DAG1이 생성한 raw_data_{ga4_date} 테이블에서
--       해당 날짜의 신규 주문(트랜잭션)을 T2_order_table에 INSERT
-- 실행 주기: 매일 (DAG2 daily_bq_update 내 t2_order_append 태스크)
-- 쓰기 방식: WRITE_APPEND (기존 데이터 유지, 신규 트랜잭션 행 추가)
-- 플레이스홀더: {project}, {dataset}, {ga4_date} → DAG Python 코드에서 치환
-- =============================================================================

INSERT INTO `{project}.{dataset}.T2_order_table`

SELECT
  transaction_id,
  user_pseudo_id,
  event_date,
  -- 날짜 형식 파생: YYYYMMDD → DATE / YYYY-MM
  PARSE_DATE('%Y%m%d', event_date)                         AS order_date,
  FORMAT_DATE('%Y-%m', PARSE_DATE('%Y%m%d', event_date))   AS order_month,

  -- 주문 총액 = 아이템별 (단가 × 수량) 합산
  -- items UNNEST로 인해 트랜잭션당 여러 행이 존재 → GROUP BY로 1행으로 집약
  SUM(price * quantity)                                    AS order_revenue,
  COUNT(DISTINCT item_name)                                AS item_count,
  -- 번들 주문 여부: 서로 다른 상품 2종 이상이면 번들로 분류
  IF(COUNT(DISTINCT item_name) >= 2, 1, 0)                 AS is_bundle_order,

  -- 프로모션 기간 레이블 분류
  CASE
    WHEN event_date BETWEEN '20201123' AND '20201130' THEN 'black_friday'
    WHEN event_date BETWEEN '20201207' AND '20201218' THEN 'christmas'
    ELSE 'normal'
  END AS period_label

FROM `{project}.{dataset}.raw_data_{ga4_date}`
WHERE event_name     = 'purchase'
  AND transaction_id IS NOT NULL
GROUP BY
  transaction_id,
  user_pseudo_id,
  event_date
