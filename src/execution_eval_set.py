"""
Execution-based 평가 케이스

기존 EvalCase는 must_contain 기반(syntactic)이었지만,
ExecutionEvalCase는 gold SQL을 두고 결과 DataFrame을 비교(semantic).

각 케이스는:
- question: 사용자 자연어 질문
- gold_sql: 도메인 전문가가 작성한 정답 SQL (실행해서 결과 비교 기준)
- order_sensitive: ORDER BY가 의미론적으로 중요한지
- category: 평가 분류
- notes: gold SQL을 그렇게 짠 이유 (리뷰용)

⚠️ gold_sql은 도메인 전문가(=프로젝트 오너)가 직접 작성해야 합니다.
샘플로 일부만 채워두었으니, 본인이 직접 검증하고 나머지를 채워주세요.
"""

from dataclasses import dataclass, field


@dataclass
class ExecutionEvalCase:
    """실행 기반 평가 케이스"""

    id: str
    question: str
    gold_sql: str
    category: str = "general"
    order_sensitive: bool = False
    notes: str = ""
    # 결과가 비어 있을 것으로 예상되면 True (예: "오늘 기준 어제 가입한 신규 유저 0명")
    # 빈 결과는 false-pass 위험이 크므로 명시적으로 표시
    expect_empty_ok: bool = False
    # gold SQL이 검증되었는지 여부 — False면 실행 시 경고
    verified: bool = False


# ============================================================
# 샘플 케이스 (도메인 전문가 검증 필요)
# ============================================================
# verified=False 인 케이스는 임시 작성본입니다.
# 본인이 BigQuery에서 실행해보고 결과가 의도와 맞는지 확인 후
# verified=True로 바꿔주세요.
# ============================================================

EXECUTION_EVAL_CASES: list[ExecutionEvalCase] = [
    # ── 시계열 / 볼륨 ─────────────────────────────
    ExecutionEvalCase(
        id="volume_001",
        question="지난 30일간 일별 DAU 추이",
        gold_sql="""
SELECT
  DATE(event_time) AS date,
  COUNT(DISTINCT user_id) AS dau
FROM `liner-219011.analysis.EVENTS_296805`
WHERE event_type = 'make_chat'
  AND DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND DATE(event_time) < CURRENT_DATE()
GROUP BY date
ORDER BY date
        """.strip(),
        category="timeseries",
        order_sensitive=True,
        notes="DAU = make_chat 이벤트 기반 distinct user_id (쿼리 사용자). 기간: 어제부터 과거 30일 (오늘 제외).",
        verified=False,
    ),
    ExecutionEvalCase(
        id="volume_002",
        question="이번 주 make_chat 이벤트 수",
        gold_sql="""
SELECT
  COUNT(*) AS event_count
FROM `liner-219011.analysis.EVENTS_296805`
WHERE event_type = 'make_chat'
  AND DATE(event_time) >= DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY))
  AND DATE(event_time) <= CURRENT_DATE()
        """.strip(),
        category="timeseries",
        notes="이번 주 = 월요일부터 오늘까지. 단일 숫자 반환. 검증 완료: 현재 주 make_chat 이벤트 수 119,910 (합리적 범위).",
        verified=True,
    ),

    # ── 구독 ─────────────────────────────────────
    ExecutionEvalCase(
        id="sub_001",
        question="현재 활성 구독자는 몇 명?",
        gold_sql="""
SELECT
  COUNT(DISTINCT user_id) AS active_subscribers
FROM `liner-219011.like.fct_moon_subscription`
WHERE status = 'active'
  AND subscription_ended_at IS NULL
        """.strip(),
        category="subscription",
        notes="활성 구독자 = status='active' AND subscription_ended_at IS NULL. 검증 완료: 17,225명.",
        verified=True,
    ),
    ExecutionEvalCase(
        id="sub_002",
        question="지난달 신규 구독자 수",
        gold_sql="""
SELECT
  COUNT(DISTINCT user_id) AS new_subscribers
FROM `liner-219011.like.fct_moon_subscription`
WHERE DATE(subscription_start_at) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
  AND DATE(subscription_start_at) < DATE_TRUNC(CURRENT_DATE(), MONTH)
        """.strip(),
        category="subscription",
        notes="지난달 = 전월 1일~말일. 이번달 1일은 제외. 검증 완료: 4,687명.",
        verified=True,
    ),

    # ── 섹터 분류 ────────────────────────────────
    ExecutionEvalCase(
        id="sector_001",
        question="지난 30일 이력서 관련 쿼리한 사용자 수",
        gold_sql="""
SELECT
  COUNT(DISTINCT user_id) AS user_count
FROM `liner-219011.analysis.EVENTS_296805`
WHERE event_type = 'make_chat'
  AND DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND (
    LOWER(JSON_EXTRACT_SCALAR(event_properties, '$.query')) LIKE '%이력서%'
    OR LOWER(JSON_EXTRACT_SCALAR(event_properties, '$.query')) LIKE '%resume%'
    OR LOWER(JSON_EXTRACT_SCALAR(event_properties, '$.query')) LIKE '%cv%'
  )
        """.strip(),
        category="sector",
        notes="이력서 키워드 정의는 domain_knowledge.SECTORS['professional']과 일치해야 함.",
        verified=False,
    ),

    # ── 파워 유저 ────────────────────────────────
    ExecutionEvalCase(
        id="power_001",
        question="지난 30일 make_chat 10회 이상 사용한 파워유저 수",
        gold_sql="""
SELECT
  COUNT(*) AS power_user_count
FROM (
  SELECT user_id
  FROM `liner-219011.analysis.EVENTS_296805`
  WHERE event_type = 'make_chat'
    AND DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY user_id
  HAVING COUNT(*) >= 10
)
        """.strip(),
        category="power_user",
        notes="파워유저 임계값 = 10. 도메인 정의 확인 필요.",
        verified=False,
    ),

    ExecutionEvalCase(
        id="power_002",
        question="현재 라이너 write를 사용하는 파워 사용자의 규모는 얼마나 되고 비중이 얼마나되죠?",
        gold_sql="""
WITH user_events AS (
  SELECT
    user_id,
    COUNT(*) AS event_count
  FROM `liner-219011.analysis.EVENTS_296805`
  WHERE JSON_EXTRACT_SCALAR(event_properties, '$.liner_product') = 'write'
    AND DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  GROUP BY user_id
),
total_stats AS (
  SELECT COUNT(DISTINCT user_id) AS total_users FROM user_events
)
SELECT
  COUNT(DISTINCT user_id) AS power_user_count,
  ROUND(100.0 * COUNT(DISTINCT user_id) / (SELECT total_users FROM total_stats), 2) AS percentage
FROM user_events
WHERE event_count >= 50
        """.strip(),
        category="power_user",
        notes="Write 서비스 파워 사용자 (최근 90일, 모든 이벤트). 파워 사용자 정의: 50+ 이벤트. liner_product='write' 필터 필수.",
        verified=False,
    ),
]


def get_verified_cases() -> list[ExecutionEvalCase]:
    """검증 완료된 케이스만 반환"""
    return [c for c in EXECUTION_EVAL_CASES if c.verified]


def get_all_cases() -> list[ExecutionEvalCase]:
    """모든 케이스 반환 (검증 안 된 것 포함)"""
    return list(EXECUTION_EVAL_CASES)
