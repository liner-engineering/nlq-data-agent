"""
성공한 BigQuery 쿼리 모음

LLM이 참고할 수 있는 실제 작동하는 쿼리들
각 분석 타입별로 구성

중요: 사용자 세그먼트는 query 내용으로 판별 (예: 교육/취업 관련 키워드)
"""

SUCCESSFUL_QUERIES = {
    'keyword_based_segmentation': {
        'description': '키워드 기반 사용자 세그먼트 분석',
        'use_case': '특정 주제(예: 교육, 취업)에 관심 있는 사용자 수',
        'sql': """
-- 취업/이력서 관련 쿼리를 한 사용자 수
SELECT
  COUNT(DISTINCT user_id) as career_interested_users,
  ROUND(100.0 * COUNT(DISTINCT user_id) /
    (SELECT COUNT(DISTINCT user_id) FROM `liner-219011.analysis.EVENTS_296805`
     WHERE event_type = 'make_chat' AND DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)), 2) as percentage
FROM `liner-219011.analysis.EVENTS_296805`
WHERE event_type = 'make_chat'
  AND DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND (LOWER(JSON_EXTRACT_SCALAR(event_properties, '$.query')) LIKE '%이력서%'
    OR LOWER(JSON_EXTRACT_SCALAR(event_properties, '$.query')) LIKE '%취업%'
    OR LOWER(JSON_EXTRACT_SCALAR(event_properties, '$.query')) LIKE '%면접%'
    OR LOWER(JSON_EXTRACT_SCALAR(event_properties, '$.query')) LIKE '%resume%'
    OR LOWER(JSON_EXTRACT_SCALAR(event_properties, '$.query')) LIKE '%career%')
        """
    },

    'query_category_distribution': {
        'description': '사용자들의 쿼리 카테고리별 분포',
        'use_case': '어떤 주제의 쿼리가 가장 많은지 파악',
        'sql': """
-- 쿼리 주제별 사용자 수
SELECT
  CASE
    WHEN LOWER(query_text) LIKE '%이력서%' OR LOWER(query_text) LIKE '%resume%' THEN 'Career: Resume'
    WHEN LOWER(query_text) LIKE '%취업%' OR LOWER(query_text) LIKE '%면접%' THEN 'Career: Job/Interview'
    WHEN LOWER(query_text) LIKE '%영어%' OR LOWER(query_text) LIKE '%영문%' THEN 'Language'
    WHEN LOWER(query_text) LIKE '%report%' OR LOWER(query_text) LIKE '%레포트%' THEN 'Academic: Report'
    ELSE 'Other'
  END as query_category,
  COUNT(DISTINCT user_id) as user_count,
  COUNT(*) as total_queries
FROM (
  SELECT
    user_id,
    JSON_EXTRACT_SCALAR(event_properties, '$.query') as query_text
  FROM `liner-219011.analysis.EVENTS_296805`
  WHERE event_type = 'make_chat'
    AND DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
)
WHERE query_text IS NOT NULL
GROUP BY query_category
ORDER BY user_count DESC
        """
    },

    'user_segment_retention': {
        'description': '쿼리 주제별 사용자의 D+7 리텐션',
        'use_case': '어떤 관심사의 사용자들이 더 오래 머물러 있을까?',
        'sql': """
-- 취업 관심 사용자 vs 다른 사용자의 리텐션 비교
WITH user_segments AS (
  SELECT DISTINCT
    user_id,
    CASE
      WHEN LOWER(query_text) LIKE '%이력서%' OR LOWER(query_text) LIKE '%취업%'
        OR LOWER(query_text) LIKE '%면접%' THEN 'Career'
      ELSE 'Other'
    END as segment
  FROM (
    SELECT
      user_id,
      JSON_EXTRACT_SCALAR(event_properties, '$.query') as query_text
    FROM `liner-219011.analysis.EVENTS_296805`
    WHERE event_type = 'make_chat'
      AND DATE(event_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 37 DAY)
                              AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  )
),
user_activity AS (
  SELECT
    user_id,
    MIN(DATE(event_time)) as first_date,
    COUNT(*) as total_events
  FROM `liner-219011.analysis.EVENTS_296805`
  WHERE event_type = 'make_chat'
  GROUP BY user_id
),
retention_check AS (
  SELECT
    us.segment,
    us.user_id,
    ua.first_date,
    MAX(CASE
      WHEN DATE(e.event_time) BETWEEN DATE_ADD(ua.first_date, INTERVAL 7 DAY)
                                   AND DATE_ADD(ua.first_date, INTERVAL 13 DAY)
      THEN 1 ELSE 0
    END) as retained_d7
  FROM user_segments us
  JOIN user_activity ua ON us.user_id = ua.user_id
  LEFT JOIN `liner-219011.analysis.EVENTS_296805` e ON us.user_id = e.user_id
    AND e.event_type = 'make_chat'
    AND DATE(e.event_time) != ua.first_date
  GROUP BY us.segment, us.user_id, ua.first_date
)
SELECT
  segment,
  COUNT(DISTINCT user_id) as total_users,
  SUM(retained_d7) as retained_users,
  ROUND(100.0 * SUM(retained_d7) / COUNT(DISTINCT user_id), 2) as retention_rate_pct
FROM retention_check
GROUP BY segment
        """
    },

    'power_user_identification': {
        'description': '활발한 사용자 식별 (쿼리 빈도 기준)',
        'use_case': '가장 많이 사용하는 사용자 그룹 파악',
        'sql': """
-- 쿼리 빈도별 사용자 분류
SELECT
  CASE
    WHEN query_count >= 50 THEN 'Power User (50+)'
    WHEN query_count >= 20 THEN 'Active User (20-49)'
    WHEN query_count >= 5 THEN 'Regular User (5-19)'
    ELSE 'Casual User (1-4)'
  END as user_tier,
  COUNT(DISTINCT user_id) as user_count,
  ROUND(AVG(query_count), 1) as avg_queries_per_user,
  MIN(query_count) as min_queries,
  MAX(query_count) as max_queries
FROM (
  SELECT
    user_id,
    COUNT(*) as query_count
  FROM `liner-219011.analysis.EVENTS_296805`
  WHERE event_type = 'make_chat'
    AND DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  GROUP BY user_id
)
GROUP BY user_tier
ORDER BY user_count DESC
        """
    }
}

# 중요 주석: context_builder에서 사용할 설명
CONTEXT_NOTES = {
    'critical': [
        'make_chat은 사용자가 query를 남긴 행동 기록이다',
        'query 내용(event_properties.$.query)으로 사용자 의도를 파악한다',
        '예: 이력서/취업 관련 쿼리 → "취업 관심 사용자" 라고 분류',
        'user_id는 like.dim_user와 조인 가능 (사용자 속성 추가)',
    ],
    'anti_patterns': [
        '❌ 틀림: "sector라는 컬럼에서 professional 값 찾기" → sector 컬럼 없음',
        '❌ 틀림: "event_properties.sector" → 없는 필드',
        '✓ 올바름: "query 내용의 키워드로 사용자 분류"',
        '✓ 올바름: "LIKE 또는 REGEX로 query_text 필터링"',
    ]
}
