"""
분석 템플릿 - 실제 스키마 기반으로 재작성

주의:
- event_time (TIMESTAMP) 사용, event_date 아님
- JSON_EXTRACT_SCALAR(event_properties, '$.query')로 쿼리 텍스트 추출
- sector는 query 키워드로 CASE WHEN으로 분류
- 모든 f-string이 query 파라미터를 반영함
"""

from dataclasses import dataclass
from typing import Callable


@dataclass
class AnalysisTemplate:
    """분석 템플릿"""
    name: str  # 분석 이름 (예: "전환율")
    description: str  # 설명
    keywords: list[str]  # 키워드 (자연어 매칭용)
    sql_generator: Callable[[str], str]  # SQL 생성 함수
    interpretation: Callable[[dict], str]  # 결과 해석 함수


def _build_sector_case_when(keyword_map: dict) -> str:
    """CASE WHEN 문으로 sector 분류 로직 생성

    Args:
        keyword_map: {sector: [keywords]}

    Returns:
        CASE WHEN ... THEN ... END 문자열
    """
    cases = []
    for sector, keywords in keyword_map.items():
        conditions = " OR ".join([
            f"LOWER(query_text) LIKE '%{kw}%'"
            for kw in keywords
        ])
        cases.append(f"WHEN {conditions} THEN '{sector}'")

    return f"CASE {' '.join(cases)} ELSE 'Other' END"


# 세크터 분류 키워드 (domain_knowledge와 일치)
SECTOR_KEYWORDS = {
    'career': ['이력서', 'resume', 'cv', '취업', '면접', 'interview', 'job'],
    'education': ['영문', '과제', 'report', '레포트', '논문', 'essay'],
    'professional': ['컨설팅', '법률', 'business', '계약', '제안'],
    'content': ['글쓰기', '콘텐츠', 'content', 'writing'],
}

# 템플릿 매칭용 키워드 (구체적인 어구 우선)
QUERY_VOLUME_KEYWORDS = ["쿼리볼륨", "쿼리 볼륨", "query volume", "dau", "mau", "일일쿼리", "일별쿼리", "추이", "trend"]
SECTOR_DISTRIBUTION_KEYWORDS = ["섹터별", "sector", "카테고리별", "관심사별", "영역별", "분포"]
RETENTION_KEYWORDS = ["리텐션", "retention", "d+7", "d7", "재방문", "재활동", "지속률", "유지율"]
POWER_USER_KEYWORDS = ["파워사용자", "파워 사용자", "활동빈도", "활동 빈도", "활발한", "자주"]
CHURN_KEYWORDS = ["이탈", "churn", "이탈사용자", "비활성", "inactive", "휴면", "떠난"]
QUERY_TYPE_KEYWORDS = ["쿼리유형", "쿼리 유형", "길이", "복잡도", "쿼리길이"]


# 기본 템플릿들
QUERY_VOLUME_TEMPLATE = AnalysisTemplate(
    name="쿼리 볼륨 분석",
    description="시간대별 쿼리 수",
    keywords=QUERY_VOLUME_KEYWORDS,
    sql_generator=lambda query: f"""
SELECT
  DATE_TRUNC(DATE(event_time), DAY) as date,
  COUNT(*) as query_count,
  COUNT(DISTINCT user_id) as unique_users
FROM `liner-219011.analysis.EVENTS_296805`
WHERE event_type = 'make_chat'
  AND DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY date
ORDER BY date DESC
    """,
    interpretation=lambda data: (
        f"평균 일일 쿼리: {data.get('query_count', 0):.0f}\n"
        f"활성 사용자: {data.get('unique_users', 0):.0f}"
    )
)

SECTOR_DISTRIBUTION_TEMPLATE = AnalysisTemplate(
    name="섹터별 분포",
    description="사용자 관심사 분포",
    keywords=SECTOR_DISTRIBUTION_KEYWORDS,
    sql_generator=lambda query: f"""
WITH events_with_query AS (
  SELECT
    user_id,
    JSON_EXTRACT_SCALAR(event_properties, '$.query') as query_text
  FROM `liner-219011.analysis.EVENTS_296805`
  WHERE event_type = 'make_chat'
    AND DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND JSON_EXTRACT_SCALAR(event_properties, '$.query') IS NOT NULL
),
classified_queries AS (
  SELECT
    user_id,
    query_text,
    {_build_sector_case_when(SECTOR_KEYWORDS)} as sector
  FROM events_with_query
)
SELECT
  sector,
  COUNT(DISTINCT user_id) as user_count,
  COUNT(*) as query_count,
  ROUND(100.0 * COUNT(DISTINCT user_id) / (SELECT COUNT(DISTINCT user_id) FROM classified_queries), 2) as user_percentage
FROM classified_queries
GROUP BY sector
ORDER BY user_count DESC
    """,
    interpretation=lambda data: (
        f"주도 섹터: {data.get('sector', 'Unknown')}\n"
        f"사용자: {data.get('user_count', 0):.0f}명"
    )
)

SECTOR_RETENTION_TEMPLATE = AnalysisTemplate(
    name="섹터별 리텐션",
    description="섹터별 D+7 리텐션율",
    keywords=RETENTION_KEYWORDS,
    sql_generator=lambda query: f"""
WITH events_with_query AS (
  SELECT
    user_id,
    event_time,
    JSON_EXTRACT_SCALAR(event_properties, '$.query') as query_text
  FROM `liner-219011.analysis.EVENTS_296805`
  WHERE event_type = 'make_chat'
    AND DATE(event_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 37 DAY)
                             AND DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    AND JSON_EXTRACT_SCALAR(event_properties, '$.query') IS NOT NULL
),
first_events AS (
  SELECT
    user_id,
    DATE(MIN(event_time)) as first_date,
    {_build_sector_case_when(SECTOR_KEYWORDS)} as sector
  FROM events_with_query
  GROUP BY user_id, sector
),
retention_check AS (
  SELECT
    fe.sector,
    fe.user_id,
    MAX(CASE
      WHEN DATE(e.event_time) BETWEEN DATE_ADD(fe.first_date, INTERVAL 7 DAY)
                                   AND DATE_ADD(fe.first_date, INTERVAL 13 DAY)
      THEN 1 ELSE 0
    END) as retained_d7
  FROM first_events fe
  LEFT JOIN `liner-219011.analysis.EVENTS_296805` e
    ON fe.user_id = e.user_id
    AND e.event_type = 'make_chat'
    AND DATE(e.event_time) != fe.first_date
  GROUP BY fe.sector, fe.user_id
)
SELECT
  sector,
  COUNT(DISTINCT user_id) as first_time_users,
  SUM(retained_d7) as day7_retained,
  ROUND(100.0 * SUM(retained_d7) / COUNT(DISTINCT user_id), 2) as retention_rate_pct
FROM retention_check
WHERE sector != 'Other'
GROUP BY sector
ORDER BY retention_rate_pct DESC
    """,
    interpretation=lambda data: (
        f"D+7 리텐션: {data.get('retention_rate_pct', 0):.1f}%\n"
        f"재활동자: {data.get('day7_retained', 0):.0f}명"
    )
)

POWER_USER_TEMPLATE = AnalysisTemplate(
    name="파워 사용자",
    description="활동 빈도별 사용자 분류",
    keywords=POWER_USER_KEYWORDS,
    sql_generator=lambda query: f"""
SELECT
  CASE
    WHEN query_count >= 50 THEN 'Power User (50+)'
    WHEN query_count >= 20 THEN 'Active User (20-49)'
    WHEN query_count >= 5 THEN 'Regular User (5-19)'
    ELSE 'Casual User (1-4)'
  END as user_tier,
  COUNT(DISTINCT user_id) as user_count,
  ROUND(AVG(query_count), 1) as avg_queries,
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
ORDER BY
  CASE user_tier
    WHEN 'Power User (50+)' THEN 1
    WHEN 'Active User (20-49)' THEN 2
    WHEN 'Regular User (5-19)' THEN 3
    ELSE 4
  END
    """,
    interpretation=lambda data: (
        f"파워 사용자: {data.get('power_user', 0):.0f}명\n"
        f"평균 쿼리: {data.get('avg_queries', 0):.1f}"
    )
)

CHURN_ANALYSIS_TEMPLATE = AnalysisTemplate(
    name="이탈 분석",
    description="비활성 사용자 식별",
    keywords=CHURN_KEYWORDS,
    sql_generator=lambda query: f"""
WITH last_activity AS (
  SELECT
    user_id,
    MAX(DATE(event_time)) as last_active_date,
    COUNT(*) as total_events
  FROM `liner-219011.analysis.EVENTS_296805`
  WHERE event_type = 'make_chat'
    AND DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  GROUP BY user_id
)
SELECT
  CASE
    WHEN DATE_DIFF(CURRENT_DATE(), last_active_date, DAY) > 30 THEN 'Churned (30+ days)'
    WHEN DATE_DIFF(CURRENT_DATE(), last_active_date, DAY) > 14 THEN 'At Risk (14+ days)'
    ELSE 'Active'
  END as status,
  COUNT(DISTINCT user_id) as user_count,
  ROUND(AVG(total_events), 1) as avg_events
FROM last_activity
GROUP BY status
ORDER BY
  CASE status
    WHEN 'Churned (30+ days)' THEN 1
    WHEN 'At Risk (14+ days)' THEN 2
    ELSE 3
  END
    """,
    interpretation=lambda data: (
        f"이탈자: {data.get('churned', 0):.0f}명\n"
        f"위험군: {data.get('at_risk', 0):.0f}명"
    )
)

QUERY_TYPE_ANALYSIS_TEMPLATE = AnalysisTemplate(
    name="쿼리 유형 분석",
    description="쿼리 길이/복잡도 분석",
    keywords=QUERY_TYPE_KEYWORDS,
    sql_generator=lambda query: f"""
SELECT
  CASE
    WHEN query_length < 50 THEN 'Short (< 50 chars)'
    WHEN query_length < 200 THEN 'Medium (50-200 chars)'
    WHEN query_length < 500 THEN 'Long (200-500 chars)'
    ELSE 'Very Long (500+ chars)'
  END as query_length_category,
  COUNT(*) as query_count,
  COUNT(DISTINCT user_id) as user_count,
  ROUND(AVG(query_length), 0) as avg_length
FROM (
  SELECT
    user_id,
    JSON_EXTRACT_SCALAR(event_properties, '$.query') as query_text,
    LENGTH(JSON_EXTRACT_SCALAR(event_properties, '$.query')) as query_length
  FROM `liner-219011.analysis.EVENTS_296805`
  WHERE event_type = 'make_chat'
    AND DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND JSON_EXTRACT_SCALAR(event_properties, '$.query') IS NOT NULL
)
GROUP BY query_length_category
ORDER BY avg_length
    """,
    interpretation=lambda data: (
        f"중간 길이 쿼리: {data.get('medium', 0):.0f}개\n"
        f"평균 길이: {data.get('avg_length', 0):.0f} chars"
    )
)


TEMPLATE_REGISTRY = {
    "volume": QUERY_VOLUME_TEMPLATE,
    "sector": SECTOR_DISTRIBUTION_TEMPLATE,
    "retention": SECTOR_RETENTION_TEMPLATE,
    "power_user": POWER_USER_TEMPLATE,
    "churn": CHURN_ANALYSIS_TEMPLATE,
    "query_type": QUERY_TYPE_ANALYSIS_TEMPLATE,
}


def find_template(query: str, min_score: int = 1) -> AnalysisTemplate | None:
    """자연어 쿼리에서 적절한 템플릿 찾기 (엄격한 매칭)

    점수 시스템:
    - 매칭 키워드 수로 점수 계산
    - 최소 min_score개 이상 키워드 매칭 필요 (기본: 1개, 명확한 의도)
    - 동점이 있으면 모호함 → 자유 쿼리로 폴백 (LLM 사용)

    Args:
        query: 자연어 쿼리
        min_score: 최소 매칭 키워드 수 (기본값: 1)

    Returns:
        AnalysisTemplate 또는 None (자유 쿼리로 폴백)
    """
    query_lower = query.lower()

    # 각 템플릿별 매칭 점수 계산
    scored = []
    for template in TEMPLATE_REGISTRY.values():
        score = sum(1 for kw in template.keywords if kw in query_lower)
        if score > 0:
            scored.append((score, template))

    if not scored:
        return None

    # 점수 높은 순으로 정렬
    scored.sort(reverse=True, key=lambda x: x[0])
    top_score, top_template = scored[0]

    # 동점이 있으면 모호함 → 자유 쿼리로 폴백
    if len(scored) > 1 and scored[0][0] == scored[1][0]:
        return None

    # 점수가 최소값 미만이면 자유 쿼리로 폴백
    if top_score < min_score:
        return None

    return top_template
