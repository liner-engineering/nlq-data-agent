"""
분석 템플릿

전환율, 리텐션, 이탈 등 기본 분석 템플릿을 정의합니다.
"""

from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class AnalysisTemplate:
    """분석 템플릿"""
    name: str  # 분석 이름 (예: "전환율")
    description: str  # 설명
    keywords: list[str]  # 키워드 (자연어 매칭용)
    sql_generator: Callable[[str], str]  # SQL 생성 함수
    interpretation: Callable[[dict], str]  # 결과 해석 함수


# 기본 분석 템플릿들
CONVERSION_TEMPLATE = AnalysisTemplate(
    name="전환율 분석",
    description="특정 기간의 사용자별 전환율 및 변화",
    keywords=["전환율", "conversion", "전환", "구매율", "가입율", "구독율"],
    sql_generator=lambda query: f"""
    SELECT
        DATE_TRUNC(event_date, DAY) as date,
        COUNT(DISTINCT user_id) as total_users,
        COUNT(DISTINCT CASE WHEN event_type LIKE '%purchase%' OR event_type LIKE '%subscribe%' THEN user_id END) as converted_users,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN event_type LIKE '%purchase%' OR event_type LIKE '%subscribe%' THEN user_id END) / COUNT(DISTINCT user_id), 2) as conversion_rate_pct
    FROM `liner-219011.analysis.EVENTS_296805`
    WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY date
    ORDER BY date DESC
    """,
    interpretation=lambda data: (
        f"평균 전환율: {data.get('conversion_rate_pct', 0):.2f}%\n"
        f"최근 전환자: {data.get('converted_users', 0):.0f}명\n"
        f"총 사용자: {data.get('total_users', 0):.0f}명"
    )
)

RETENTION_TEMPLATE = AnalysisTemplate(
    name="리텐션 분석",
    description="사용자 재활동 및 유지율",
    keywords=["리텐션", "retention", "유지", "재활동", "재방문", "D+7"],
    sql_generator=lambda query: f"""
    WITH first_event AS (
        SELECT
            user_id,
            MIN(DATE(event_time)) as first_date,
            MIN(event_properties.sector) as sector
        FROM `liner-219011.analysis.EVENTS_296805`
        WHERE DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        GROUP BY user_id
    )
    SELECT
        sector,
        COUNT(DISTINCT fe.user_id) as first_time_users,
        COUNT(DISTINCT CASE
            WHEN DATE(e.event_time) BETWEEN DATE_ADD(fe.first_date, INTERVAL 7 DAY)
                 AND DATE_ADD(fe.first_date, INTERVAL 13 DAY)
            THEN fe.user_id
        END) as day7_retained,
        ROUND(100.0 * COUNT(DISTINCT CASE
            WHEN DATE(e.event_time) BETWEEN DATE_ADD(fe.first_date, INTERVAL 7 DAY)
                 AND DATE_ADD(fe.first_date, INTERVAL 13 DAY)
            THEN fe.user_id
        END) / COUNT(DISTINCT fe.user_id), 2) as retention_rate_pct
    FROM first_event fe
    LEFT JOIN `liner-219011.analysis.EVENTS_296805` e ON fe.user_id = e.user_id
    GROUP BY sector
    ORDER BY retention_rate_pct DESC
    """,
    interpretation=lambda data: (
        f"D+7 리텐션율: {data.get('retention_rate_pct', 0):.2f}%\n"
        f"재활동자: {data.get('day7_retained', 0):.0f}명\n"
        f"신규 사용자: {data.get('first_time_users', 0):.0f}명"
    )
)

CHURN_TEMPLATE = AnalysisTemplate(
    name="이탈 분석",
    description="이탈 사용자의 특징 및 패턴",
    keywords=["이탈", "churn", "이탈율", "이탈자", "취소", "unsubscribe"],
    sql_generator=lambda query: f"""
    WITH user_activity AS (
        SELECT
            user_id,
            MAX(DATE(event_time)) as last_active_date,
            COUNT(DISTINCT event_type) as event_type_count,
            COUNT(*) as total_events
        FROM `liner-219011.analysis.EVENTS_296805`
        WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        GROUP BY user_id
    )
    SELECT
        CASE
            WHEN DATE_DIFF(CURRENT_DATE(), last_active_date, DAY) > 30 THEN 'Churned (30+ days)'
            WHEN DATE_DIFF(CURRENT_DATE(), last_active_date, DAY) > 14 THEN 'At Risk (14+ days)'
            ELSE 'Active'
        END as churn_status,
        COUNT(*) as user_count,
        ROUND(AVG(total_events), 1) as avg_events,
        ROUND(AVG(event_type_count), 1) as avg_event_types
    FROM user_activity
    GROUP BY churn_status
    ORDER BY user_count DESC
    """,
    interpretation=lambda data: (
        f"이탈자: {data.get('churned', 0):.0f}명\n"
        f"위험군: {data.get('at_risk', 0):.0f}명\n"
        f"활성: {data.get('active', 0):.0f}명"
    )
)

REVENUE_TEMPLATE = AnalysisTemplate(
    name="매출 분석",
    description="사용자별/세그먼트별 매출 분석",
    keywords=["매출", "revenue", "결제", "수익", "구매금액", "ARPU"],
    sql_generator=lambda query: f"""
    SELECT
        DATE_TRUNC(event_date, MONTH) as month,
        COUNT(DISTINCT user_id) as paying_users,
        COUNT(*) as transaction_count,
        ROUND(SUM(CAST(JSON_EXTRACT_SCALAR(event_properties, '$.amount') as FLOAT64)), 2) as total_revenue,
        ROUND(AVG(CAST(JSON_EXTRACT_SCALAR(event_properties, '$.amount') as FLOAT64)), 2) as avg_transaction
    FROM `liner-219011.analysis.EVENTS_296805`
    WHERE event_type LIKE '%purchase%' OR event_type LIKE '%payment%'
    AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    GROUP BY month
    ORDER BY month DESC
    """,
    interpretation=lambda data: (
        f"월 매출: ${data.get('total_revenue', 0):,.2f}\n"
        f"결제 사용자: {data.get('paying_users', 0):.0f}명\n"
        f"평균 거래액: ${data.get('avg_transaction', 0):.2f}"
    )
)

USER_SEGMENT_TEMPLATE = AnalysisTemplate(
    name="사용자 세그먼트 분석",
    description="사용자를 활동 수준, 이벤트 유형 등으로 분할",
    keywords=["세그먼트", "segment", "분류", "그룹", "유형", "카테고리"],
    sql_generator=lambda query: f"""
    SELECT
        CASE
            WHEN event_count >= 50 THEN 'Power User'
            WHEN event_count >= 20 THEN 'Active User'
            WHEN event_count >= 5 THEN 'Regular User'
            ELSE 'Casual User'
        END as user_segment,
        COUNT(*) as user_count,
        ROUND(AVG(event_count), 1) as avg_events,
        ROUND(AVG(day_span), 1) as avg_active_days
    FROM (
        SELECT
            user_id,
            COUNT(*) as event_count,
            DATE_DIFF(MAX(DATE(event_time)), MIN(DATE(event_time)), DAY) as day_span
        FROM `liner-219011.analysis.EVENTS_296805`
        WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        GROUP BY user_id
    )
    GROUP BY user_segment
    ORDER BY avg_events DESC
    """,
    interpretation=lambda data: (
        f"Power User: {data.get('power_user', 0):.0f}명\n"
        f"Active User: {data.get('active_user', 0):.0f}명\n"
        f"Regular User: {data.get('regular_user', 0):.0f}명"
    )
)

FEATURE_USAGE_TEMPLATE = AnalysisTemplate(
    name="기능 사용 분석",
    description="가장 많이 사용되는 기능 및 사용 패턴",
    keywords=["기능", "feature", "사용", "usage", "인기", "이벤트"],
    sql_generator=lambda query: f"""
    SELECT
        event_type,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_id) as user_count,
        ROUND(100.0 * COUNT(DISTINCT user_id) / (SELECT COUNT(DISTINCT user_id) FROM `liner-219011.analysis.EVENTS_296805` WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)), 2) as adoption_rate_pct,
        ROUND(AVG(TIMESTAMP_DIFF(event_timestamp, LAG(event_timestamp) OVER (PARTITION BY user_id ORDER BY event_timestamp), SECOND)), 0) as avg_interval_seconds
    FROM `liner-219011.analysis.EVENTS_296805`
    WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY event_type
    ORDER BY event_count DESC
    LIMIT 20
    """,
    interpretation=lambda data: (
        f"상위 기능 사용: {data.get('event_type', 'N/A')}\n"
        f"사용 횟수: {data.get('event_count', 0):.0f}회\n"
        f"채택률: {data.get('adoption_rate_pct', 0):.2f}%"
    )
)

TEMPLATE_REGISTRY = {
    "conversion": CONVERSION_TEMPLATE,
    "retention": RETENTION_TEMPLATE,
    "churn": CHURN_TEMPLATE,
    "revenue": REVENUE_TEMPLATE,
    "segment": USER_SEGMENT_TEMPLATE,
    "feature": FEATURE_USAGE_TEMPLATE,
}


def find_template(query: str) -> AnalysisTemplate | None:
    """
    자연어 쿼리에서 적절한 템플릿 찾기

    Args:
        query: 자연어 쿼리

    Returns:
        AnalysisTemplate 또는 None
    """
    query_lower = query.lower()

    for template in TEMPLATE_REGISTRY.values():
        for keyword in template.keywords:
            if keyword in query_lower:
                return template

    return None
