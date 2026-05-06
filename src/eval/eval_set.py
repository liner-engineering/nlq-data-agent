"""
NL2SQL 평가셋 — 변경마다 회귀 테스트

각 케이스는 (질문, 기대하는 SQL 특성)으로 구성.
정확히 일치하는 SQL을 요구하지 않고, 핵심 패턴이 들어있는지만 확인.
"""

from dataclasses import dataclass, field


@dataclass
class EvalCase:
    question: str
    expected_tables: list[str]
    must_contain: list[str] = field(default_factory=list)      # 반드시 포함
    must_not_contain: list[str] = field(default_factory=list)  # 절대 포함 X
    category: str = "general"
    gold_sql: str = ""  # Execution-based eval용 정답 SQL (선택사항)


EVAL_CASES = [
    # === 카테고리 1: 시계열/볼륨 ===
    EvalCase(
        question="지난 30일간 일별 DAU 추이",
        expected_tables=["EVENTS_296805"],
        must_contain=["DATE(event_time)", "GROUP BY", "DISTINCT user_id"],
        must_not_contain=["sector", "JSON_EXTRACT"],
        category="timeseries",
    ),
    EvalCase(
        question="이번 주 make_chat 이벤트 수",
        expected_tables=["EVENTS_296805"],
        must_contain=["make_chat", "COUNT"],
        must_not_contain=["sector"],
        category="timeseries",
    ),
    EvalCase(
        question="일별 쿼리 수 추이",
        expected_tables=["EVENTS_296805"],
        must_contain=["DATE", "COUNT(*)"],
        must_not_contain=["sector"],
        category="timeseries",
    ),

    # === 카테고리 2: 구독 관련 ===
    EvalCase(
        question="지난달 신규 구독자 수",
        expected_tables=["fct_moon_subscription"],
        must_contain=["start_date"],
        must_not_contain=["EVENTS_296805"],
        category="subscription",
    ),
    EvalCase(
        question="현재 활성 구독자는 몇 명?",
        expected_tables=["fct_moon_subscription"],
        must_contain=["end_date IS NULL"],
        category="subscription",
    ),
    EvalCase(
        question="구독 전환율 — make_chat 후 구독한 비율",
        expected_tables=["EVENTS_296805", "fct_moon_subscription"],
        must_contain=["JOIN", "make_chat"],
        category="subscription",
    ),

    # === 카테고리 3: 섹터 분류 ===
    EvalCase(
        question="섹터별 사용자 분포",
        expected_tables=["EVENTS_296805"],
        must_contain=["JSON_EXTRACT_SCALAR", "CASE WHEN"],
        category="sector",
    ),
    EvalCase(
        question="이력서 관련 쿼리한 사용자는 몇 명?",
        expected_tables=["EVENTS_296805"],
        must_contain=["JSON_EXTRACT_SCALAR", "이력서"],
        category="sector",
    ),
    EvalCase(
        question="가장 관심 많은 주제는?",
        expected_tables=["EVENTS_296805"],
        must_contain=["JSON_EXTRACT_SCALAR"],
        category="sector",
    ),

    # === 카테고리 4: 리텐션 ===
    EvalCase(
        question="섹터별 D+7 리텐션",
        expected_tables=["EVENTS_296805"],
        must_contain=["DATE_ADD", "INTERVAL 7 DAY"],
        category="retention",
    ),
    EvalCase(
        question="D+7 지속률을 계산해 줘",
        expected_tables=["EVENTS_296805"],
        must_contain=["DATE_ADD", "INTERVAL"],
        category="retention",
    ),

    # === 카테고리 5: 메시지 텍스트 분석 ===
    EvalCase(
        question="가장 자주 등장하는 메시지 카테고리",
        expected_tables=["fct_question_answer_binding_message"],
        must_contain=["category", "GROUP BY"],
        category="messages",
    ),
    EvalCase(
        question="사용자가 입력한 메시지는?",
        expected_tables=["fct_question_answer_binding_message"],
        must_contain=["message_text"],
        category="messages",
    ),

    # === 카테고리 6: 사용자 활동도 ===
    EvalCase(
        question="지난 30일 make_chat 10회 이상 사용한 파워유저 수",
        expected_tables=["EVENTS_296805"],
        must_contain=["HAVING", "COUNT"],
        category="power_user",
    ),
    EvalCase(
        question="활발한 사용자는 누가 있나?",
        expected_tables=["EVENTS_296805"],
        must_contain=["COUNT"],
        category="power_user",
    ),

    # === 카테고리 7: 이탈 분석 ===
    EvalCase(
        question="14일 이상 활동 없는 사용자는?",
        expected_tables=["EVENTS_296805"],
        must_contain=["DATE_DIFF", "INTERVAL"],
        category="churn",
    ),
    EvalCase(
        question="휴면 사용자 분석",
        expected_tables=["EVENTS_296805"],
        must_contain=["CASE WHEN"],
        category="churn",
    ),
]
