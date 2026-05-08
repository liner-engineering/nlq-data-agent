"""
도메인 용어 → 데이터 소스 라우팅 규칙 (Glossary)

사용자가 사용하는 자연어 용어(credit, 구독, DAU 등)를
정확한 SQL 라우팅으로 변환하기 위한 도메인 사전.

각 용어는:
1. primary_source: 정답 테이블/이벤트
2. secondary_source: 보조 지표 (선택)
3. anti_patterns: 절대 금지 SQL 패턴 (규칙 기반 lint)
4. routing_rule: LLM이 따를 의사결정 경로

⚠️ CRITICAL: 스키마 타입 주의!
테이블마다 user_id 타입이 다르므로 조인 시 CAST 필수:
- EVENTS_296805.user_id = STRING (⚠️)
- fct_moon_subscription.user_id = INTEGER
- agent_credit_usage_log.user_id = INTEGER

타입 불일치 에러: "No matching signature for operator =" 발생 시
→ CAST(events.user_id AS INT64) = subscription.user_id 사용

올바른 패턴:
- JOIN: ON CAST(e.user_id AS INT64) = s.user_id
- IN: WHERE CAST(e.user_id AS INT64) IN (SELECT user_id FROM ...)
"""

GLOSSARY = {
    'credit': {
        'alternative_terms': ['크레딧', '사용량', '구매', 'usage'],
        'description': '크레딧: Write/Research 서비스에서 사용자가 소비하는 단위. 사용(usage)은 agent_credit_usage_log에서 delta_amount < 0으로 추적.',
        'primary_source': [
            '사용량: cdc_service_db_new_liner.agent_credit_usage_log (delta_amount < 0, user_id는 INTEGER)',
            '제품 필터: EVENTS_296805에서 make_chat + liner_product로 제품별 사용자 정의 (user_id는 STRING → 타입 캐스팅 필수!)'
        ],
        'secondary_source': 'agent_credit_item (상세 항목 정보, 필요시)',
        'time_range': '⚠️ 기간 제한 불가: credit은 누적 지표이므로 기간 없음 권장. 시계열이 필요하면 명시.',
        'anti_patterns': [
            "LIKE '%credit%' in JSON_EXTRACT(event_properties, '$.query')",
            "event_type = 'complete_use_credit' 사용 (EVENTS의 크레딧 이벤트는 신뢰X)",
            "agent_credit_usage_log만 사용해서 제품 정보 손실 (EVENTS 조인 필요)",
            "자동으로 DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 필터 추가 (누적 지표는 전체 기간)",
            "EVENTS_296805 (STRING user_id)과 agent_credit_usage_log (INTEGER user_id) 조인 시 CAST 누락 → 타입 오류"
        ],
        'routing_rule': """
        credit 질문 →
          1. "Write/Scholar 사용자의 credit 사용" →
             Step 1: EVENTS_296805 + (event_type='make_chat' + liner_product) 정의
             Step 2: cdc_service_db_new_liner.agent_credit_usage_log JOIN (delta_amount < 0)
             Step 3: SUM(-delta_amount) 집계
          2. "사용량 TOP" → GROUP BY user_id + ORDER BY DESC + LIMIT 1
          3. "기간 제한" → date_trunc(created_at, 'Asia/Seoul') 사용
          4. ⚠️ 기간이 없으면 전체 데이터 사용 (30일 자동 추가 금지!)
          5. BigQuery 최적화:
             - EVENTS 쿼리: DATE(_PARTITIONTIME) 또는 DATE(event_time) 범위 반드시 지정
             - CTE 구조: base CTE로 한 번에 필요한 컬럼만 추출 (중복 스캔 금지)
             - 필터 순서: WHERE에서 조기 필터링, HAVING 최소화
        """,
        'example_queries': [
            '("credit을 가장 많이 사용한 사용자", write_users CTE + agent_credit_usage_log 조인 + ORDER BY DESC LIMIT 1)',
            '("write 유저의 credit 사용량", EVENTS 제품 필터 → agent_credit_usage_log SUM)',
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

    'pro/max 구독자': {
        'alternative_terms': ['프로 유저', '맥스 유저', '유료 구독', '프로/맥스', 'pro user', 'max user', 'pro', 'max'],
        'description': 'Pro/Max 유료 구독자. product_category가 \'pro\' 또는 \'max\'인 사용자.',
        'primary_source': 'like.fct_moon_subscription: product_category IN (\'pro\', \'max\')',
        'anti_patterns': [
            'liner_product에서 pro/max 찾기 (틀림, liner_product는 제품명)',
            'plan_id에 \'pro\'/\'max\' 직접 필터링 (실제 plan_id는 Stripe ID)',
            'status만 사용하고 product_category 조건 누락'
        ],
        'routing_rule': """
        pro/max 구독자 질문 →
          WHERE product_category IN ('pro', 'max')
          + 필요시 구독 기간 조건 (subscription_start_at, subscription_ended_at)
          + 필요시 파트너 제외: is_from_partnership = FALSE
        """,
        'forbidden_in_columns': [
            {
                'wrong_column': 'liner_product',
                'wrong_values': ['pro', 'max', 'Pro', 'Max'],
                'reason': 'pro/max는 구독 플랜 정보이지, 제품(liner_product)이 아님',
                'correct': 'fct_moon_subscription.product_category IN (\'pro\', \'max\')'
            }
        ]
    },

    'scholar': {
        'alternative_terms': ['Scholar', '스칼라', 'researcher', 'research'],
        'description': 'Scholar = 논문 검색/요약 서비스. liner_product = \'researcher\' (주의: 필드명과 다름)',
        'primary_source': 'EVENTS_296805: JSON_EXTRACT_SCALAR(event_properties, \'$.liner_product\') = \'researcher\'',
        'anti_patterns': [
            'liner_product = \'scholar\' (틀림, \'researcher\' 사용)',
        ],
        'routing_rule': 'scholar 질문 → liner_product = \'researcher\' 필터',
        'forbidden_in_columns': [
            {
                'wrong_column': 'liner_product',
                'wrong_values': ['scholar', 'Scholar', 'SCHOLAR'],
                'reason': 'Scholar의 내부 값은 \'researcher\'이지, \'scholar\'가 아님',
                'correct': 'liner_product = \'researcher\''
            }
        ]
    },

    'ai_search': {
        'alternative_terms': ['AI Search', '통합검색', 'ai_search'],
        'description': '통합 검색 서비스.',
        'primary_source': 'EVENTS_296805: JSON_EXTRACT_SCALAR(event_properties, \'$.liner_product\') = \'ai_search\'',
    },

    'segment': {
        'alternative_terms': ['세그먼트', '카테고리', '분류', 'category', 'group'],
        'description': '사용자를 특정 그룹으로 분류 (예: 교육/취업/비즈니스). LIKE 매칭으로는 부정확함.',
        'primary_source': '사전 분류된 mart 테이블 (user_category, user_segment 등) - 테이블명은 회사 데이터 팀에 확인',
        'anti_patterns': [
            '쿼리 텍스트에 LIKE 매칭으로 segment 판별 (부정확, 유지보수 어려움)',
            'WHERE LOWER(JSON_EXTRACT_SCALAR(event_properties, \'$.query\')) LIKE \'%키워드%\' 패턴 금지',
            'CASE WHEN ... LIKE ... 로 수동 분류하기'
        ],
        'routing_rule': """
        segment/category 질문 →
          1. segment mart 테이블이 존재하는가? 확인하기
             - 존재한다면: INNER JOIN <mart_table> ON user_id
             - 존재하지 않는다면: "segment 분류 mart 테이블이 필요합니다. 데이터 팀 확인 필요" 응답
          2. LIKE 매칭으로 분류하기 금지 (정확도 낮음)
          3. 대신 사전 분류 테이블 JOIN
        """
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
