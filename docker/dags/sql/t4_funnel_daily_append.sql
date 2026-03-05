-- =============================================================================
-- T4_funnel_daily_table 증분 APPEND
-- =============================================================================
-- 목적: T3_session_funnel_table에서 해당 날짜({ga4_date}) 세션만 필터링하여
--       일별 퍼널 단계별 집계 결과를 T4_funnel_daily_table에 INSERT
-- 실행 주기: 매일 (DAG2 daily_bq_update 내 t4_funnel_daily_append 태스크)
-- 의존성: t3_session_funnel_append 완료 후 실행 (DAG2 내 순차 의존)
-- 쓰기 방식: WRITE_APPEND (기존 데이터 유지, 신규 날짜 6행 추가 - 퍼널 단계당 1행)
-- 플레이스홀더: {project}, {dataset}, {ga4_date} → DAG Python 코드에서 치환
-- =============================================================================

INSERT INTO `{project}.{dataset}.T4_funnel_daily_table`

WITH funnel_steps AS (
  -- 각 퍼널 단계(1~6)별 세션 수 집계
  -- session_funnel_step >= step_num 조건으로 해당 단계 이상 도달한 세션만 카운트
  SELECT
    session_date                                      AS event_date,
    period_label,
    step_num,
    stage_name,
    COUNT(*)                                          AS session_cnt
  FROM `{project}.{dataset}.T3_session_funnel_table`,
    -- 퍼널 6단계를 UNNEST로 펼쳐서 각 단계별 행 생성
    UNNEST([
      STRUCT(1 AS step_num, '발견'     AS stage_name),
      STRUCT(2,              '흥미'                 ),
      STRUCT(3,              '욕망'                 ),
      STRUCT(4,              '구매 시도'             ),
      STRUCT(5,              '구매 임박'             ),
      STRUCT(6,              '구매 완료'             )
    ]) AS s
  -- 해당 날짜의 세션만 필터링 (증분 처리 핵심 조건)
  WHERE session_funnel_step >= step_num
    AND session_date = PARSE_DATE('%Y%m%d', '{ga4_date}')
  GROUP BY
    session_date,
    period_label,
    step_num,
    stage_name
),

total_sessions AS (
  -- 일별 전체 세션 수 = 발견 단계(step=1) 세션 수
  SELECT
    event_date,
    session_cnt AS total_sessions
  FROM funnel_steps
  WHERE step_num = 1
),

with_next AS (
  -- LEAD로 다음 단계 세션 수를 가져와 이탈(drop_off) 계산
  SELECT
    fs.event_date,
    fs.period_label,
    fs.step_num                                           AS funnel_step,
    fs.stage_name                                         AS funnel_stage,
    fs.session_cnt,
    ts.total_sessions,
    -- 다음 단계 세션 수 (마지막 단계는 0)
    COALESCE(
      LEAD(fs.session_cnt) OVER (
        PARTITION BY fs.event_date ORDER BY fs.step_num
      ), 0
    )                                                     AS next_session_cnt
  FROM funnel_steps AS fs
  JOIN total_sessions AS ts
    ON fs.event_date = ts.event_date
)

SELECT
  event_date,
  period_label,
  funnel_step,
  funnel_stage,
  session_cnt,
  -- 이탈 세션 수: 현재 단계 - 다음 단계
  (session_cnt - next_session_cnt)                        AS drop_off_cnt,
  -- 단계별 이탈률 = 이탈 수 / 현재 단계 세션 수
  ROUND(
    (session_cnt - next_session_cnt) / NULLIF(session_cnt, 0), 4
  )                                                       AS drop_off_rate,
  -- 누적 통과율 = 현재 단계 세션 수 / 전체 세션 수
  ROUND(
    session_cnt / NULLIF(total_sessions, 0), 4
  )                                                       AS cumulative_pass_rate

FROM with_next
ORDER BY
  event_date,
  funnel_step
