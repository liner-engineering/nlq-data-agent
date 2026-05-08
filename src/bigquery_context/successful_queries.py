"""
성공한 BigQuery 쿼리 모음

LLM이 참고할 수 있는 실제 작동하는 쿼리들
각 분석 타입별로 구성

중요: 사용자 세그먼트는 query 내용으로 판별 (예: 교육/취업 관련 키워드)
"""

SUCCESSFUL_QUERIES = {
    'daily_active_users': {
        'description': '일별 활성 사용자 수 (DAU) 추이',
        'use_case': '지난 30일 일별 활성 사용자 추이 확인',
        'critical_note': '반드시 GROUP BY DATE(event_time)이 필요함! 없으면 30일 누적값이 됨',
        'sql': """
SELECT
  DATE(event_time) AS report_date,
  COUNT(DISTINCT user_id) AS dau
FROM `liner-219011.analysis.EVENTS_296805`
WHERE DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND event_type = 'make_chat'
GROUP BY DATE(event_time)
ORDER BY report_date DESC
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
    },

    'scholar_user_credit_by_plan': {
        'description': 'Scholar 사용자의 플랜별 credit 사용량 분석',
        'use_case': 'Scholar free/pro/max 유저별로 credit을 얼마나 사용했는지 조회',
        'sql': """
-- Scholar 사용자의 subscription 플랜 정보와 credit 사용량 조인
SELECT
  du.user_id,
  s.plan_id AS scholar_plan,
  COUNT(DISTINCT DATE(acul.used_at)) AS usage_days,
  COUNT(*) AS usage_count,
  SUM(ABS(acul.delta_amount)) AS total_credit_used,
  ROUND(AVG(ABS(acul.delta_amount)), 2) AS avg_credit_per_use
FROM `liner-219011.cdc_service_db_new_liner.agent_credit_usage_log` acul
INNER JOIN `liner-219011.like.dim_user` du
  ON acul.user_id = du.user_id
LEFT JOIN `liner-219011.like.fct_moon_subscription` s
  ON du.user_id = s.user_id
  AND DATE(acul.used_at) >= DATE(s.subscription_start_at)
  AND (s.subscription_ended_at IS NULL OR DATE(acul.used_at) <= DATE(s.subscription_ended_at))
WHERE acul.delta_amount < 0  -- 사용 기록만 (충전은 제외)
  AND acul.service = 'scholar'  -- Scholar 서비스만
GROUP BY user_id, scholar_plan
ORDER BY total_credit_used DESC
        """
    },

    'write_user_credit_usage': {
        'description': 'Write 서비스 사용자의 credit 사용량 (최적화된 버전)',
        'use_case': 'Write 유저 중 credit을 가장 많이 사용한 사람 TOP 10',
        'comment': """
-- 의사결정 경로:
-- 1. "write 유저" = EVENTS_296805 + liner_product='write' 필터
-- 2. "credit 사용" = agent_credit_usage_log (delta_amount < 0만)
-- 3. 파티션 필터: DATE(event_time) 범위 지정 (BigQuery 비용 절감)
-- 4. 최적화: base CTE로 한 번에 처리 (중복 스캔 제거)
-- 5. 필터 순서: WHERE에서 조기 필터링, HAVING으로 0값 제거
        """,
        'sql': """
-- base CTE: 한 번에 필요한 컬럼만 추출 (파티션 필터 포함)
WITH base AS (
  SELECT
    SAFE_CAST(user_id AS INT64) AS user_id,
    JSON_EXTRACT_SCALAR(event_properties, '$.liner_product') AS liner_product
  FROM `liner-219011.analysis.EVENTS_296805`
  WHERE DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)  -- 파티션 필터 (event_time으로 파티셔닝됨)
    AND event_type = 'make_chat'
)

-- write_users: base CTE에서 write 사용자 추출
SELECT
  acul.user_id,
  SUM(-acul.delta_amount) AS total_credit_used
FROM `liner-219011.cdc_service_db_new_liner.agent_credit_usage_log` acul
WHERE acul.user_id IN (SELECT DISTINCT user_id FROM base WHERE liner_product = 'write')
  AND acul.delta_amount < 0
GROUP BY acul.user_id
HAVING total_credit_used > 0
ORDER BY total_credit_used DESC
LIMIT 10
        """
    }
}

# 중요 주석: context_builder에서 사용할 설명
CONTEXT_NOTES = {
    'critical': [
        'make_chat은 사용자가 query를 남긴 행동 기록이다',
        'user_segment 분석: LIKE 매칭 금지, 사전 분류 mart 테이블 사용',
        'mart 테이블이 없으면 그 사실을 명시하고 작업 중단',
        'user_id는 like.dim_user와 조인 가능 (사용자 속성 추가)',
    ],
    'anti_patterns': [
        '❌ 틀림: "sector라는 컬럼에서 professional 값 찾기" → sector 컬럼 없음',
        '❌ 틀림: "event_properties.sector" → 없는 필드',
        '❌ 틀림: "LIKE로 query_text 필터링하여 segment 판별" → 부정확, mart 테이블 사용',
        '✓ 올바름: "사전 분류된 mart 테이블 JOIN으로 segment 분류"',
    ]
}
