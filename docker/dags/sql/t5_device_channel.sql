-- =============================================================================
-- T5_device_channel_table 전체 재계산 (CREATE OR REPLACE)
-- =============================================================================
-- 목적: T3_session_funnel_table 전체를 기준으로 기기 × 채널 조합별 집계 지표를 계산
-- 실행 주기: 매일 (DAG2 daily_bq_update 내 t5_device_channel_replace 태스크)
-- 쓰기 방식: CREATE OR REPLACE (전체 재계산)
-- 전체 재계산 이유: device × channel 조합의 누적 합산(total_sessions 등)은
--                  증분 APPEND 시 기존 집계값에 오늘치가 중복 추가되므로
--                  T3 전체를 읽어 매일 새로 계산하는 방식 사용
-- 플레이스홀더: {project}, {dataset} → DAG Python 코드에서 치환
-- =============================================================================

CREATE OR REPLACE TABLE `{project}.{dataset}.T5_device_channel_table` AS

SELECT
  -- 집계 기준: 기기 카테고리 × 트래픽 소스/미디엄 조합
  session_device,
  session_traffic_source,
  session_traffic_medium,

  -- 세션 수 집계
  COUNT(*)                                                         AS total_sessions,
  SUM(purchase_flag)                                               AS purchase_sessions,
  SUM(bounce_flag)                                                 AS bounce_sessions,
  SUM(churn_session_flag)                                          AS churn_sessions,

  -- 전환율 / 바운스율 / 이탈률
  ROUND(SUM(purchase_flag)       / NULLIF(COUNT(*), 0), 4)         AS conversion_rate,
  ROUND(SUM(bounce_flag)         / NULLIF(COUNT(*), 0), 4)         AS bounce_rate,
  ROUND(SUM(churn_session_flag)  / NULLIF(COUNT(*), 0), 4)         AS churn_session_rate,

  -- 퍼널 / 행동 지표 평균
  ROUND(AVG(session_funnel_step),        2)                        AS avg_funnel_depth,
  ROUND(AVG(session_page_views),         2)                        AS avg_page_views,
  ROUND(AVG(session_engagement_time),    2)                        AS avg_engagement_time

FROM `{project}.{dataset}.T3_session_funnel_table`
GROUP BY
  session_device,
  session_traffic_source,
  session_traffic_medium
ORDER BY
  total_sessions DESC
