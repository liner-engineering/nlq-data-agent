"""
성공한 BigQuery 쿼리 모음

LLM이 참고할 수 있는 실제 작동하는 쿼리들
각 분석 타입별로 구성
"""

SUCCESSFUL_QUERIES = {
    'sector_retention_d7': {
        'description': '섹터별 D+7 리텐션',
        'use_case': '어떤 섹터의 리텐션이 높을까?',
        'sql': """
-- 섹터별 D+7 리텐션 분석
WITH first_events AS (
  -- 사용자의 첫 make_chat 이벤트
  SELECT
    user_id,
    JSON_EXTRACT_SCALAR(event_properties, '$.query') as query_text,
    DATE(event_time) as first_date,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time) as event_rank
  FROM `liner-219011.analysis.EVENTS_296805`
  WHERE event_type = 'make_chat'
    AND DATE(event_time) BETWEEN '2026-03-01' AND '2026-04-01'
),
first_events_unique AS (
  SELECT * FROM first_events WHERE event_rank = 1
),
retention_check AS (
  SELECT
    fe.user_id,
    fe.query_text,
    fe.first_date,
    MAX(CASE
      WHEN DATE(e.event_time) BETWEEN DATE_ADD(fe.first_date, INTERVAL 7 DAY)
                                   AND DATE_ADD(fe.first_date, INTERVAL 13 DAY)
      THEN 1 ELSE 0
    END) as retained_d7
  FROM first_events_unique fe
  LEFT JOIN `liner-219011.analysis.EVENTS_296805` e
    ON fe.user_id = e.user_id
    AND e.event_type = 'make_chat'
    AND DATE(e.event_time) != fe.first_date
  GROUP BY fe.user_id, fe.query_text, fe.first_date
)
SELECT
  query_text as sector,
  COUNT(DISTINCT user_id) as total_users,
  SUM(retained_d7) as retained_users,
  ROUND(100 * SUM(retained_d7) / COUNT(DISTINCT user_id), 2) as retention_rate_pct
FROM retention_check
GROUP BY query_text
HAVING total_users >= 10
ORDER BY retention_rate_pct DESC
"""
    },

    'sector_conversion': {
        'description': '섹터별 구독 전환율',
        'use_case': '어떤 섹터가 가장 구독 전환율이 높을까?',
        'sql': """
-- 섹터별 구독 전환율
WITH make_chat_users AS (
  SELECT
    user_id,
    JSON_EXTRACT_SCALAR(event_properties, '$.query') as query_text,
    DATE(event_time) as event_date,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time) as rn
  FROM `liner-219011.analysis.EVENTS_296805`
  WHERE event_type = 'make_chat'
    AND DATE(event_time) BETWEEN '2026-03-01' AND '2026-04-30'
),
first_make_chat AS (
  SELECT * FROM make_chat_users WHERE rn = 1
),
converted_users AS (
  SELECT DISTINCT user_id
  FROM `liner-219011.light.fct_moon_subscription`
  WHERE DATE(start_date) BETWEEN '2026-03-01' AND '2026-04-30'
)
SELECT
  fmc.query_text as sector,
  COUNT(DISTINCT fmc.user_id) as users_with_query,
  COUNT(DISTINCT cu.user_id) as converted_users,
  ROUND(100 * COUNT(DISTINCT cu.user_id) / COUNT(DISTINCT fmc.user_id), 2) as conversion_rate_pct
FROM first_make_chat fmc
LEFT JOIN converted_users cu ON fmc.user_id = cu.user_id
GROUP BY fmc.query_text
HAVING users_with_query >= 10
ORDER BY conversion_rate_pct DESC
"""
    },

    'event_sequence_conversion': {
        'description': '이벤트 시퀀스별 구독 전환율',
        'use_case': '어떤 이벤트 조합이 구독 전환율이 높을까?',
        'sql': """
-- 이벤트 시퀀스별 구독 전환율 (상위 50개)
WITH user_events AS (
  SELECT
    user_id,
    ARRAY_AGG(event_type ORDER BY event_time LIMIT 10) as event_sequence,
    MIN(DATE(event_time)) as first_event_date,
    MAX(DATE(event_time)) as last_event_date
  FROM `liner-219011.analysis.EVENTS_296805`
  WHERE DATE(event_time) BETWEEN '2026-03-01' AND '2026-04-30'
  GROUP BY user_id
),
converted_users AS (
  SELECT DISTINCT user_id
  FROM `liner-219011.light.fct_moon_subscription`
  WHERE DATE(start_date) BETWEEN '2026-03-01' AND '2026-04-30'
)
SELECT
  ARRAY_TO_STRING(ue.event_sequence, ' → ') as event_path,
  COUNT(DISTINCT ue.user_id) as users,
  COUNT(DISTINCT cu.user_id) as converted,
  ROUND(100 * COUNT(DISTINCT cu.user_id) / COUNT(DISTINCT ue.user_id), 2) as conversion_rate_pct
FROM user_events ue
LEFT JOIN converted_users cu ON ue.user_id = cu.user_id
GROUP BY event_sequence
HAVING users >= 5
ORDER BY users DESC, conversion_rate_pct DESC
LIMIT 50
"""
    },

    'journey_pattern': {
        'description': '사용자 여정 (상위 이벤트 시퀀스)',
        'use_case': '사용자들의 주요 패턴 및 저니맵은?',
        'sql': """
-- 상위 20개 사용자 여정 패턴
WITH user_journeys AS (
  SELECT
    user_id,
    ARRAY_AGG(
      STRUCT(event_type, DATE(event_time) as event_date),
      ORDER BY event_time
      LIMIT 10
    ) as journey_events
  FROM `liner-219011.analysis.EVENTS_296805`
  WHERE DATE(event_time) BETWEEN '2026-04-01' AND '2026-05-06'
  GROUP BY user_id
),
journey_paths AS (
  SELECT
    (SELECT ARRAY_AGG(DISTINCT e.event_type ORDER BY OFFSET(e) LIMIT 10)
     FROM UNNEST(journey_events) as e WITH OFFSET) as event_sequence,
    COUNT(DISTINCT user_id) as user_count
  FROM user_journeys
  GROUP BY journey_paths
)
SELECT
  ARRAY_TO_STRING(event_sequence, ' → ') as journey_path,
  user_count,
  ROUND(100 * user_count / (SELECT COUNT(*) FROM user_journeys), 2) as pct
FROM journey_paths
ORDER BY user_count DESC
LIMIT 20
"""
    },

    'subscriber_pattern': {
        'description': '구독자의 주요 활동 패턴',
        'use_case': '구독자들의 주요 패턴은?',
        'sql': """
-- 구독자 vs 비구독자의 활동 비교
WITH subscriber_status AS (
  SELECT
    DISTINCT e.user_id,
    MAX(CASE WHEN s.user_id IS NOT NULL THEN 1 ELSE 0 END) as is_subscriber
  FROM `liner-219011.analysis.EVENTS_296805` e
  LEFT JOIN `liner-219011.light.fct_moon_subscription` s
    ON e.user_id = s.user_id
    AND DATE(e.event_time) >= s.start_date
    AND (s.end_date IS NULL OR DATE(e.event_time) <= s.end_date)
  WHERE DATE(e.event_time) BETWEEN '2026-03-01' AND '2026-05-06'
  GROUP BY e.user_id
),
event_stats AS (
  SELECT
    ss.is_subscriber,
    e.event_type,
    COUNT(DISTINCT e.user_id) as user_count,
    COUNT(*) as event_count
  FROM subscriber_status ss
  JOIN `liner-219011.analysis.EVENTS_296805` e ON ss.user_id = e.user_id
  WHERE DATE(e.event_time) BETWEEN '2026-03-01' AND '2026-05-06'
  GROUP BY ss.is_subscriber, e.event_type
)
SELECT
  CASE WHEN is_subscriber = 1 THEN 'Subscriber' ELSE 'Non-Subscriber' END as user_type,
  event_type,
  user_count,
  event_count,
  ROUND(event_count / user_count, 2) as avg_events_per_user
FROM event_stats
ORDER BY is_subscriber DESC, user_count DESC
"""
    },

    'heavy_user_pattern': {
        'description': '헤비 유저 (활동도 상위 10%)의 주요 패턴',
        'use_case': '헤비 유저의 주요 패턴은?',
        'sql': """
-- 헤비 유저 분석 (활동도 상위 10%)
WITH user_activity AS (
  SELECT
    user_id,
    COUNT(*) as total_events,
    COUNT(DISTINCT DATE(event_time)) as active_days,
    COUNT(DISTINCT event_type) as unique_event_types,
    PERCENTILE_CONT(COUNT(*), 0.9) OVER () as p90_events
  FROM `liner-219011.analysis.EVENTS_296805`
  WHERE DATE(event_time) BETWEEN '2026-03-01' AND '2026-05-06'
  GROUP BY user_id
),
heavy_users AS (
  SELECT user_id FROM user_activity
  WHERE total_events >= p90_events
),
heavy_user_events AS (
  SELECT
    e.event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT e.user_id) as user_count
  FROM `liner-219011.analysis.EVENTS_296805` e
  INNER JOIN heavy_users hu ON e.user_id = hu.user_id
  WHERE DATE(e.event_time) BETWEEN '2026-03-01' AND '2026-05-06'
  GROUP BY e.event_type
)
SELECT
  event_type,
  event_count,
  user_count,
  ROUND(event_count / user_count, 2) as avg_frequency
FROM heavy_user_events
ORDER BY event_count DESC
"""
    },

    'cohort_retention': {
        'description': '코호트별 리텐션 곡선',
        'use_case': '월별 신규 사용자의 리텐션 추이는?',
        'sql': """
-- 월별 코호트 D+0, D+7, D+14, D+30 리텐션
WITH first_events AS (
  SELECT
    user_id,
    DATE(event_time) as first_event_date,
    DATE_TRUNC(DATE(event_time), MONTH) as cohort_month
  FROM `liner-219011.analysis.EVENTS_296805`
  WHERE event_type = 'make_chat'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time) = 1
),
retention_days AS (
  SELECT
    fe.cohort_month,
    fe.first_event_date,
    COUNTIF(DATE_DIFF(DATE(e.event_time), fe.first_event_date, DAY) BETWEEN 0 AND 0) as d0_users,
    COUNTIF(DATE_DIFF(DATE(e.event_time), fe.first_event_date, DAY) BETWEEN 7 AND 13) as d7_users,
    COUNTIF(DATE_DIFF(DATE(e.event_time), fe.first_event_date, DAY) BETWEEN 14 AND 30) as d14_users,
    COUNTIF(DATE_DIFF(DATE(e.event_time), fe.first_event_date, DAY) BETWEEN 30 AND 60) as d30_users,
    COUNT(DISTINCT fe.user_id) as cohort_size
  FROM first_events fe
  LEFT JOIN `liner-219011.analysis.EVENTS_296805` e ON fe.user_id = e.user_id
  WHERE fe.cohort_month >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 120 DAY), MONTH)
  GROUP BY fe.cohort_month, fe.first_event_date
)
SELECT
  cohort_month,
  cohort_size,
  ROUND(100 * d0_users / cohort_size, 1) as d0_retention_pct,
  ROUND(100 * d7_users / cohort_size, 1) as d7_retention_pct,
  ROUND(100 * d14_users / cohort_size, 1) as d14_retention_pct,
  ROUND(100 * d30_users / cohort_size, 1) as d30_retention_pct
FROM retention_days
GROUP BY cohort_month, cohort_size
ORDER BY cohort_month
"""
    }
}

# 쿼리 가이드
QUERY_GUIDELINES = """
## BigQuery 쿼리 작성 가이드

### 기본 원칙
1. **항상 GROUP BY 사용**: 1개 행의 결과는 의미 없음
2. **시간 범위 명확히**: WHERE DATE(event_time) BETWEEN '...' AND '...'
3. **분석 용도 명확히**: 리텐션? 전환율? 패턴?

### 리텐션 계산
- D+7: 첫 이벤트 후 7-13일 사이에 재활동
- D+30: 첫 이벤트 후 30-60일 사이에 재활동
- ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time) = 1로 첫 이벤트만 추출

### 구독 전환율 계산
- make_chat 사용자 = 분모
- fct_moon_subscription 등록자 = 분자
- 시간 범위 일치 확인 필수

### 이벤트 시퀀스 추출
- ARRAY_AGG(event_type ORDER BY event_time) 사용
- LIMIT 10으로 최대 10개 이벤트만
- ARRAY_TO_STRING으로 가독성 있게 표현
"""
