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

    'referral': {
        'alternative_terms': ['추천', '친구초대', 'referral program', '친구 초대', '초대'],
        'description': '추천 프로그램: 기존 사용자(inviter)가 신규 사용자(invitee)를 초대하면 양쪽 모두 추천 크레딧 획득.',
        'primary_source': 'EVENTS_296805: event_type IN (\'complete_signup\', \'complete_provide_credit\') WHERE entry_type = \'referral_invited\' (신청자 기준)',
        'secondary_source': 'user_properties.inviter_user_id / invitee_user_id (event_properties)',
        'anti_patterns': [
            '기간 없이 전체 분석 (프로그램 시작일 2026-04-09, 안정화: 2026-04-10부터)',
            '초대자 신청자 양쪽을 한 번에 세기 (각각 WHERE 조건 구분 필수)',
            'complete_provide_credit 누락 (크레딧 부여 추적 필요)'
        ],
        'routing_rule': """
        추천 관련 질문 →
          WHERE entry_type = 'referral_invited' (신청자 기준)
          데이터 시작: 2026-04-10 (안정화)
          그룹별 분석:
            - 신청자: event_type = 'complete_signup' + entry_type = 'referral_invited'
            - 초대자: event_type = 'complete_provide_credit' + trigger_type = 'inviter_invite'
            - 크레딧 사용: use_research_agent_credit + promotion_type = 'referral_signup_reward'
        """
    },

    'experiment': {
        'alternative_terms': ['A/B 테스트', 'AB 테스트', 'statsig', 'statsig experiment', '실험'],
        'description': 'Statsig로 관리되는 A/B 실험. 노출 이벤트는 EVENTS_296805의 statsig::experiment_exposure.',
        'primary_source': 'EVENTS_296805: event_type = \'statsig::experiment_exposure\' (user_id 타입: UUID 또는 numeric)',
        'secondary_source': 'user_properties.stable_id (UUID 해석 시)',
        'anti_patterns': [
            'UUID user_id 직접 사용 (stable_id로 해석 필수)',
            '예시처리된 user_properties.user_id 사용 (NULL, 사용 불가)',
            '기노출 이벤트 포함 (first_exposure_at 이후 필터링 필수)',
            'experimentGroupName = NULL인 rows (Cache:Unrecognized, 제외)'
        ],
        'routing_rule': """
        실험 분석 →
          1. user_id 타입 판별: UUID → stable_id 해석, numeric → 직접 사용
          2. 최초 노출일: MIN(event_time) per user
          3. 행동 필터: 노출 이후의 이벤트만 (행동 오염 방지)
          4. experimentGroupName NULL 제외
          JSON key: '$."metadata.config"', '$."metadata.experimentGroupName"' (dot-notation, 중첩 X)
        """
    },

    'people_search': {
        'alternative_terms': ['인물검색', '사람검색', 'people', 'person search'],
        'description': 'People Search 기능: 쿼리로 관련 인물을 exa API를 통해 검색.',
        'primary_source': 'langfuse_data.observations: name = \'handle_people_search\' (2026-04-20 이후)',
        'secondary_source': 'metadata 필드: exa_call_count, exa_r1_count, exa_r2_count, raw_card_count, final_card_count',
        'anti_patterns': [
            '2026-04-20 이전 데이터 사용 (metadata NULL, 신뢰성 낮음)',
            'langfuse_data.traces에서 조회 (observations에만 존재)',
            'handle_people_search가 아닌 다른 name 값 사용'
        ],
        'routing_rule': """
        People Search 분석 →
          WHERE name = 'handle_people_search' (2026-04-20+)
          주요 지표:
            - exa_call_count: 총 exa API 호출 수
            - raw_card_count: 필터링 전 후보 카드 수
            - final_card_count: 사용자에게 반환된 카드 수
          시간: start_time (UTC, KST는 -9시간)
        """
    },

    'platform': {
        'alternative_terms': ['결제수단', '결제플랫폼', 'payment platform', 'pg', '스트라이프', '토스', '패들'],
        'description': '결제 플랫폼: Stripe (USD, global) / Toss (KRW, Korea) / Paddle (MOR) / PayPal / Apple / Google 등',
        'primary_source': 'like.fct_subscription_revenue_financial (platform별 별도 int 모델에서 통합)',
        'anti_patterns': [
            'Paddle VAT 처리 오류 (MOR이므로 금액이 VAT-excluded)',
            '플랫폼별 금액 단위 오류 (Paddle USD는 센트 단위, /100 필요)',
            'Paddle plan_change 트랜잭션 누락 (subscription_update rows 포함)'
        ],
        'routing_rule': """
        결제 플랫폼별 분석 →
          기본: where platform IN ('stripe', 'tosspayments', 'paddle', 'paypal', 'apple', 'google')
          Paddle 특수:
            - 금액 단위: KRW는 원 (use as-is), USD는 센트 (/100.0)
            - VAT: Paddle이 공제하고 정산 (fct /1.1 적용)
            - Plan change: origin = 'subscription_update' (line_item quantity >0이 새 플랜)
          Type 필터: 'payment', 'refund', 'dispute', 'reversal' (failure 제외)
        """
    },

    'business_model': {
        'alternative_terms': ['비즈니스모델', '매출분류', 'business', '사업모델'],
        'description': '매출 분류: 8개 Business Model (Mutually Exclusive) - ads, api, subscription, credit, partnership, contract, b2b_ax, gov_grant',
        'primary_source': 'like.int_revenue_daily_by_business_model (인식매출) / like.int_revenue_daily_by_business_model_received (수취매출)',
        'anti_patterns': [
            '하나의 매출을 여러 Business Model에 할당 (반드시 1개만)',
            'custom 분류 생성 (반드시 8개 enum 중 1개만 사용)',
            '인식매출과 수취매출을 혼용 (질문에 따라 1가지만 선택)'
        ],
        'routing_rule': """
        Business Model 분석 →
          1. ads: Ad Manager + Keyword Ad (인식=수취)
          2. api: Adot API 사용료 (인식=수취)
          3. subscription: 구독료 (인식: 기간 분배 + VAT, 수취: 결제 시점)
          4. credit: 크레딧 구매 (인식: earned+breakage accrual, 수취: booking 시점)
          5. partnership: 파트너 계약 (인식: 계약기간 분배, 수취: 시작일 일시)
          6. contract: 일반 계약 (인식: 계약기간 분배, 수취: 시작일 일시)
          7. b2b_ax: B2B AX 계약 (인식: 계약기간 분배, 수취: 시작일 일시)
          8. gov_grant: 정부지원 (인식: 계약기간 분배, 수취: 시작일 일시)
        """
    },

    'revenue_recognition': {
        'alternative_terms': ['인식매출', '발생매출', 'accrual', 'earned revenue'],
        'description': '인식매출: 발생주의 기반 매출 (언제 서비스가 제공되었는가). 구독은 기간 분배, 크레딧은 사용/만료.',
        'primary_source': 'like.fct_subscription_revenue_recognition (구독) / like.met_credit_revenue_daily_summary (크레딧)',
        'secondary_source': 'like.fct_contracted_revenue_recognition (계약형)',
        'anti_patterns': [
            '결제 시점 매출과 혼용 (수취매출과 별도)',
            '구독 기간 분배 없이 단순 구독 금액 집계'
        ],
        'routing_rule': """
        인식매출 분석 →
          Subscription: fct_subscription_revenue_recognition (구독 기간 일별 분배 + VAT 조정)
          Credit: met_credit_revenue_daily_summary (earned_revenue_usd/krw + breakage_revenue_usd/krw)
          Contract형: fct_contracted_revenue_recognition (start~end 일별 분배)
          Ad/API: 인식매출 = 수취매출 동일
        """
    },

    'payment_received': {
        'alternative_terms': ['수취매출', '현금매출', 'cash basis', 'bookings'],
        'description': '수취매출: 현금주의 기반 매출 (실제 언제 돈을 받았는가). 구독은 결제 시점, 계약형은 계약 시작일.',
        'primary_source': 'like.fct_subscription_revenue_financial (구독) / like.met_credit_purchase_bookings (크레딧)',
        'secondary_source': 'like.fct_contracted_revenue_recognition (계약형, start_date 기준 SUM)',
        'anti_patterns': [
            '발생 시점 매출과 혼용 (인식매출과 별도)',
            'failure type 포함 (결제 미발생 건, 제외 필수)'
        ],
        'routing_rule': """
        수취매출 분석 →
          Subscription: fct_subscription_revenue_financial (type NOT IN (\'failure\') - net basis)
          Credit: met_credit_purchase_bookings (booking 시점 결제액)
          Contract형: GROUP BY start_date 하여 전체 금액 일시 인식
          필터: type IN (\'payment\', \'refund\', \'dispute\', \'reversal\') (failure 제외)
        """
    },

    'arr': {
        'alternative_terms': ['ARR', '연간경상수익', 'MRR', '월간경상수익'],
        'description': 'ARR = Annual Recurring Revenue (연간화). MRR = Monthly Recurring Revenue. 개인 구독과 팀 구독 별도 추적.',
        'primary_source': 'like.met_individual_subscription_arr_ltm_daily (개인) / like.met_team_subscription_arr_ltm_daily (팀)',
        'anti_patterns': [
            '개인과 팀 ARR을 단순 합산 (별도로 추적)',
            '구독 중인 수가 아닌 "누적 매출" 집계 (ARR과 혼동)',
            'LTM과 NTM 혼용 (각각 다른 필터 조건)'
        ],
        'routing_rule': """
        ARR 분석 →
          개인 ARR: like.met_individual_subscription_arr_ltm_daily (LTM 필터)
          팀 ARR: like.met_team_subscription_arr_ltm_daily (LTM 필터)
          NTM (다음 12개월): _ntm_daily 테이블 사용
          Trial: _trial_no_card_daily 테이블 (별도 추적)
        """
    },

    'credit_allocation': {
        'alternative_terms': ['크레딧할당', '월간할당', '기본크레딧'],
        'description': '플랜별 월간 기본 크레딧: Free=100, Pro=1000, Max=2500 (일부 지역 Free=30)',
        'primary_source': '설정값 (비즈니스 로직, 쿼리에서는 상수값으로 처리)',
        'anti_patterns': [
            '할당값을 쿼리로 추출 (고정 상수, 코드 상수로 관리)',
            '실제 사용량과 할당량 혼용'
        ],
        'routing_rule': """
        크레딧 할당 분석 →
          Free 플랜: 100 credits/month (PH,IN,IR,EG,DZ,PK,VN은 30)
          Pro 플랜: 1,000 credits/month
          Max 플랜: 2,500 credits/month
          실제 사용/남은 크레딧은 agent_credit_usage_log에서 조회
        """
    },

    'referral_pnl': {
        'alternative_terms': ['추천손익', '친구초대손익', 'referral profit'],
        'description': '추천 프로그램 P&L = 수익 - 초대자AI비용 - 신청자AI비용 - 신청자 외 AI비용',
        'primary_source': 'EVENTS_296805 (credit_grant + credit_usage) / fct_langfuse_traces (AI 비용)',
        'anti_patterns': [
            '초대자와 신청자 크레딧을 합산 (각각 별도 계산)',
            'llm_cost 필드를 찾을 수 없을 때 0으로 처리 (estimation: actual_credit * 0.007)',
            '프로그램 시작 전 데이터 포함 (2026-04-10 이후만 안정)'
        ],
        'routing_rule': """
        추천 P&L 분석 →
          전체 P&L = Lifetime Revenue (신청자 결제)
                   - 초대자 AI 비용 (inviter_invite 크레딧 → AI cost)
                   - 신청자 AI 비용 (invitee_signup 크레딧 → AI cost)
                   - 신청자 일반 AI 비용 (non-referral credit 사용)
          AI 비용 출처:
            1. llm_cost (event_properties) - 우선
            2. actual_credit * 0.007 (fallback)
            3. fct_langfuse_traces.total_cost (검증용)
          초대자 비용 분배: grant-weighted proration
            inviter_per_invitee = inviter_total_usage * (inviter_grant_per_invitee / inviter_total_grant)
        """
    },

    'langfuse': {
        'alternative_terms': ['langfuse', 'traces', 'observations', 'LLM cost', 'AI비용'],
        'description': 'Langfuse: LLM 호출 추적 시스템. traces는 전체 플로우, observations는 개별 작업단위.',
        'primary_source': 'langfuse_data.traces (전체 쿼리) / langfuse_data.observations (people_search, 각 API 호출)',
        'anti_patterns': [
            'traces와 observations 중 잘못된 테이블 선택 (필요한 작업 단위 확인)',
            'handle_people_search 이전 데이터 사용 (metadata 없음, 2026-04-20+만 사용)'
        ],
        'routing_rule': """
        Langfuse 분석 →
          전체 쿼리 AI 비용: langfuse_data.traces + trace_id join
          People Search 상세: langfuse_data.observations WHERE name='handle_people_search'
          비용 필드: total_cost (USD), 또는 input_cost + output_cost 합산
          시간: start_time (UTC, KST는 -9시간)
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
