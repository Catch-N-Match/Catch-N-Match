-- =============================================================================
-- T2_detail_order_item_table 증분 APPEND
-- =============================================================================
-- 목적: DAG1이 생성한 raw_data_{ga4_date} 테이블에서
--       해당 날짜의 구매 아이템 상세 행을 T2_detail_order_item_table에 INSERT
-- 실행 주기: 매일 (DAG2 daily_bq_update 내 t2_detail_append 태스크)
-- 쓰기 방식: WRITE_APPEND (기존 데이터 유지, 신규 아이템 행 추가)
-- 플레이스홀더: {project}, {dataset}, {ga4_date} → DAG Python 코드에서 치환
-- 참고: t2_order_append.sql과 달리 GROUP BY 없이 아이템 행 단위로 INSERT
-- =============================================================================

INSERT INTO `{project}.{dataset}.T2_detail_order_item_table`

SELECT
  transaction_id,
  user_pseudo_id,
  event_date,

  -- 상품 정보 (raw_data에서 items UNNEST로 이미 행 확장된 상태)
  item_name,
  item_category,

  -- 아이템 단위 매출 = 단가 × 수량
  price * quantity                                          AS item_revenue,
  quantity                                                  AS item_quantity,
  -- 월 파생 컬럼 (월별 집계 시 활용)
  FORMAT_DATE('%Y-%m', PARSE_DATE('%Y%m%d', event_date))    AS event_month,

  -- 프로모션 기간 레이블 분류
  CASE
    WHEN event_date BETWEEN '20201123' AND '20201130' THEN 'black_friday'
    WHEN event_date BETWEEN '20201207' AND '20201218' THEN 'christmas'
    ELSE 'normal'
  END AS period_label

FROM `{project}.{dataset}.raw_data_{ga4_date}`
WHERE event_name     = 'purchase'
  AND transaction_id IS NOT NULL
  AND item_name      IS NOT NULL  -- 아이템명 없는 행 제외
