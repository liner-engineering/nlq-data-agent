"""
도메인 용어 → 데이터 소스 라우팅 규칙 (Glossary)

사용자가 사용하는 자연어 용어(credit, 구독, DAU 등)를
정확한 SQL 라우팅으로 변환하기 위한 도메인 사전.

각 용어는:
1. primary_source: 정답 테이블/이벤트
2. secondary_source: 보조 지표 (선택)
3. anti_patterns: 절대 금지 SQL 패턴 (규칙 기반 lint)
4. routing_rule: LLM이 따를 의사결정 경로
"""

GLOSSARY = {
    'credit': {
        'alternative_terms': ['크레딧', '사용량', '구매', 'usage'],
        'description': '크레딧: Write/Research 서비스에서 사용자가 소비하는 단위. 구매(payment)와 사용(usage)을 구분.',
        'primary_source': [
            '사용량: agent_credit_usage_log (직접)',
            '구매액: payment_v2_item_purchase (구독과 분리)'
        ],
        'secondary_source': 'EVENTS_296805 (의도/퍼널 보조, 쿼리 내용은 신뢰X)',
        'anti_patterns': [
            "LIKE '%credit%' in JSON_EXTRACT(event_properties, '$.query')",
            "event_type = 'make_chat' 기반 필터링 (쿼리 텍스트 검색)",
            "fct_moon_subscription만 사용 (구독≠크레딧 사용)"
        ],
        'routing_rule': """
        credit 질문 →
          1. "사용량 TOP" → agent_credit_usage_log 직접 조회
          2. "구매/결제" → payment_v2_item_purchase 조회
          3. "추이/시계열" → usage_log 또는 payment 시계열
          4. EVENTS는 "어떤 제품을 사용 중인가?"의 보조만
        """,
        'example_queries': [
            '("credit을 가장 많이 사용한 사용자", primary_source),',
            '("write 유저의 평균 credit 사용량", agent_credit_usage_log)',
        ]
    },

    'dau': {
        'alternative_terms': ['DAU', '일별활성사용자', '활동', '사용자 수'],
        'description': 'DAU = Daily Active User. Liner에서는 make_chat 이벤트 기반 (쿼리를 입력한 사용자).',
        'primary_source': 'EVENTS_296805: event_type = \'make_chat\' + DATE(event_time)',
        'anti_patterns': [
            'event_type 필터 없이 모든 이벤트 포함 (값이 5배 이상 커짐)',
            'query 텍스트 필터링만으로 세그멘트화'
        ],
        'routing_rule': """
        DAU 계산 →
          WHERE event_type = 'make_chat'
            AND DATE(event_time) >= ...
          GROUP BY DATE(event_time), user_id (distinct 확보)
        """
    },

    'write': {
        'alternative_terms': ['Write', '라이너 라이트', '라이터'],
        'description': 'Liner의 문서 작성 서비스. liner_product = \'write\' 필터로 분리.',
        'primary_source': 'EVENTS_296805: JSON_EXTRACT_SCALAR(event_properties, \'$.liner_product\') = \'write\'',
        'anti_patterns': [
            'liner_product 필터 없음 (모든 제품 포함)',
            '다른 필드명 사용 (service, product_name 등은 없음)'
        ],
        'routing_rule': """
        write 관련 질문 →
          WHERE JSON_EXTRACT_SCALAR(event_properties, '$.liner_product') = 'write'
          + 필요시 date/user 필터
        """
    },

    'subscription': {
        'alternative_terms': ['구독', '구독상태', '활성구독자', '신규구독'],
        'description': '구독 정보: 사용자의 구독 시작/종료 기록.',
        'primary_source': 'like.fct_moon_subscription (start_at, ended_at, status)',
        'secondary_source': 'EVENTS 구독 이벤트 (view_subscription_wall 등) - 퍼널용',
        'anti_patterns': [
            'start_date/end_date 필드명 사용 (정답: subscription_start_at, subscription_ended_at)',
            'status만 사용하고 ended_at IS NULL 조건 누락',
            '불필요한 JOIN (dim_user, EVENTS 등)'
        ],
        'routing_rule': """
        구독 관련 →
          활성 구독자: WHERE status = 'active' AND subscription_ended_at IS NULL
          신규 구독: WHERE DATE(subscription_start_at) >= ... AND DATE(...) < ...
          기간 계산: DATE() 변환 필수 (TIMESTAMP 필드)
        """
    },

    'scholar': {
        'alternative_terms': ['Scholar', '스칼라', 'researcher', 'research'],
        'description': 'Scholar = 논문 검색/요약 서비스. liner_product = \'researcher\' (주의: 필드명과 다름)',
        'primary_source': 'EVENTS_296805: JSON_EXTRACT_SCALAR(event_properties, \'$.liner_product\') = \'researcher\'',
        'anti_patterns': [
            'liner_product = \'scholar\' (틀림, \'researcher\' 사용)',
        ],
        'routing_rule': 'scholar 질문 → liner_product = \'researcher\' 필터'
    },

    'ai_search': {
        'alternative_terms': ['AI Search', '통합검색', 'ai_search'],
        'description': '통합 검색 서비스.',
        'primary_source': 'EVENTS_296805: JSON_EXTRACT_SCALAR(event_properties, \'$.liner_product\') = \'ai_search\'',
    },
}


def get_glossary_section_for_prompt() -> str:
    """프롬프트에 삽입할 glossary 섹션 생성 (마크다운)"""
    lines = [
        "## 도메인 용어 사전 (Glossary)\n",
        "다음 용어가 질문에 등장하면, 반드시 지정된 소스를 우선 조회하세요.\n",
        "anti_patterns에 나열된 SQL은 절대 금지합니다.\n",
    ]

    for term, info in sorted(GLOSSARY.items()):
        lines.append(f"\n### {term}")
        lines.append(f"**정의**: {info.get('description', '')}")
        if 'alternative_terms' in info:
            lines.append(f"**동의어**: {', '.join(info['alternative_terms'])}")

        primary = info.get('primary_source', '')
        if isinstance(primary, list):
            lines.append(f"**정답 소스**:")
            for src in primary:
                lines.append(f"  - {src}")
        else:
            lines.append(f"**정답 소스**: {primary}")

        if 'anti_patterns' in info:
            lines.append(f"\n**금지 패턴** (절대 사용 금지):")
            for pattern in info['anti_patterns']:
                lines.append(f"  - ❌ {pattern}")

        if 'routing_rule' in info:
            lines.append(f"\n**의사결정 경로**:")
            lines.append("```")
            lines.append(info['routing_rule'].strip())
            lines.append("```")

    return "\n".join(lines)


# 체크리스트: SQL이 glossary 규칙을 따르는지 검증
def get_anti_patterns_for_validation() -> dict[str, list[str]]:
    """
    validator.py에서 사용: 질문 키워드 → anti-pattern 체크

    Returns:
        {
            'credit': ['LIKE \'%credit%\'', 'event_type = \'make_chat\''],
            'subscription': ['start_date', 'end_date'],
            ...
        }
    """
    result = {}
    for term, info in GLOSSARY.items():
        if 'anti_patterns' in info:
            result[term] = info['anti_patterns']
    return result
