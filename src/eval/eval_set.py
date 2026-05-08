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
    must_be_under_bytes: int = 0  # 비용 제약: 이 바이트 이하로 스캔해야 함 (0=제약 없음)


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

    # === 카테고리 7: Glossary 준수 (도메인 용어 → 올바른 SQL 매핑) ===
    EvalCase(
        question="라이너 스칼라를 사용한 사람들 중에서 pro/max 유저의 크레딧 사용량",
        expected_tables=["EVENTS_296805", "fct_moon_subscription", "agent_credit_usage_log"],
        must_contain=[
            "researcher",  # Scholar = researcher (스칼라 필터)
            "plan_id",     # pro/max는 plan_id에서 찾아야 함
            "agent_credit_usage_log",  # 크레딧 데이터
            "delta_amount",  # 크레딧 사용량
        ],
        must_not_contain=[
            "liner_product') IN ('pro'",  # pro/max는 liner_product가 아님
            "liner_product') = 'pro'",
            "liner_product') = 'scholar'",  # scholar는 사용자 표현, 내부값은 researcher
        ],
        category="glossary_compliance",
    ),
    EvalCase(
        question="Pro 구독자의 월별 활동 추이",
        expected_tables=["fct_moon_subscription", "EVENTS_296805"],
        must_contain=["plan_id", "'pro'"],
        must_not_contain=["liner_product') = 'pro'"],
        category="glossary_compliance",
    ),
    EvalCase(
        question="Write 서비스 사용자들의 월별 DAU",
        expected_tables=["EVENTS_296805"],
        must_contain=["liner_product", "'write'", "COUNT(DISTINCT", "GROUP BY"],
        must_not_contain=["'Write'"],  # 올바른 값은 'write' (소문자)
        category="glossary_compliance",
    ),
    EvalCase(
        question="Scholar 사용자 중 활성 구독자의 credit 사용량",
        expected_tables=["EVENTS_296805", "fct_moon_subscription", "agent_credit_usage_log"],
        must_contain=["researcher", "status = 'active'", "delta_amount"],
        must_not_contain=["liner_product') = 'scholar'"],
        category="glossary_compliance",
    ),
    EvalCase(
        question="Max 유료 구독자의 평균 활동 강도",
        expected_tables=["fct_moon_subscription", "EVENTS_296805"],
        must_contain=["plan_id", "'max'", "make_chat"],
        must_not_contain=["liner_product'),  IN ('max'"],
        category="glossary_compliance",
    ),

    # === 카테고리 8: SQL 최적화 (비용 절감) ===
    # 다음 평가 케이스들은 비용 제약을 포함합니다.
    # must_be_under_bytes를 초과하면 실패합니다. (dry-run 스캔 바이트 기준)

    EvalCase(
        question="Write 서비스 사용자의 4월 크레딧 사용량",
        expected_tables=["EVENTS_296805", "agent_credit_usage_log"],
        must_contain=[
            "liner_product", "'write'",  # Write 필터
            "DATE(event_time)",  # 파티션 필터
            "agent_credit_usage_log",  # 크레딧 데이터
            "delta_amount",  # 크레딧 사용량
        ],
        must_not_contain=[
            "BETWEEN '2024'",  # 잘못된 연도
        ],
        category="optimization_cost",
        must_be_under_bytes=107_374_182_400,  # 100GB 제약 (3TB 방지)
    ),

    EvalCase(
        question="라이너 스칼라(Scholar) Pro/Max 구독자의 크레딧 사용량 분석",
        expected_tables=["EVENTS_296805", "fct_moon_subscription", "agent_credit_usage_log"],
        must_contain=[
            "researcher",  # Scholar = researcher
            "product_category", "pro", "max",  # 구독 필터
            "agent_credit_usage_log",  # 크레딧
            "delta_amount",  # 사용량
        ],
        must_not_contain=[
            "UNION",  # 같은 테이블 중복 읽기 금지
            "product_category IN ('pro', 'max')",  # UNION 패턴
        ],
        category="optimization_cost",
        must_be_under_bytes=10_737_418_240,  # 10GB 제약 (좁은 세트 먼저 전략)
    ),

    EvalCase(
        question="지난 30일 EVENTS_296805에서 전체 이벤트 수",
        expected_tables=["EVENTS_296805"],
        must_contain=[
            "DATE(event_time)",  # 파티션 필터
            "COUNT",
        ],
        must_not_contain=[
            "2024",  # 과거 연도 금지
            "WHERE event_type",  # 파티션 필터가 먼저 와야 함
        ],
        category="optimization_cost",
        must_be_under_bytes=1_073_741_824,  # 1GB 제약 (30일 필터 효과 검증)
    ),

    EvalCase(
        question="Pro 구독자들의 최근 30일 활동",
        expected_tables=["fct_moon_subscription", "EVENTS_296805"],
        must_contain=[
            "product_category", "'pro'",  # 구독 필터
            "DATE(event_time)",  # 파티션 필터
            "JOIN",  # 명시적 조인
        ],
        must_not_contain=[
            "UNION",  # 좁은 세트 먼저 패턴 필수
        ],
        category="optimization_cost",
        must_be_under_bytes=10_737_418_240,  # 10GB 제약
    ),
]
