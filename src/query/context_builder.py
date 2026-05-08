"""
LLM 프롬프트 컨텍스트 빌더

BigQuery 스키마, 성공 사례, 금지 패턴을 LLM 프롬프트로 조합하여
LLM에 최적화된 프롬프트를 생성합니다.
"""

from typing import Any

from src.bigquery_context import (
    ANTIPATTERNS,
    BIGQUERY_SCHEMA,
    SAMPLE_EVENTS,
    SECTORS,
    SUCCESSFUL_QUERIES,
)
from src.bigquery_context.glossary import get_glossary_section_for_prompt, GLOSSARY
from src.exceptions import ContextBuildingError
from src.query.example_selector import get_selector


class ContextBuilder:
    """LLM 프롬프트 컨텍스트 생성기

    BigQuery 스키마, 성공 사례, 안티패턴, 비즈니스 규칙을
    구조화된 프롬프트로 조합합니다.

    Example:
        builder = ContextBuilder()
        prompt = builder.build_prompt("섹터별 D+7 리텐션이 뭐야?")
    """

    SYSTEM_PROMPT = """당신은 liner의 BigQuery 데이터 분석 전문가입니다.

사용자의 자연어 쿼리를 정확한 BigQuery SQL로 변환하는 것이 목표입니다.

## ⚠️ 시간 범위: 선택적 (분석 유형에 따라)

시간 범위는 **질문에 명시된 경우만** 사용합니다. 명시되지 않은 경우:
- **시계열/추이 분석** ("DAU 추이", "주간 이벤트"): 최근 30일 권장
- **누적 최대값 분석** ("가장 많이 사용한 사람", "TOP 10"): 기간 없음 (전체 기간)
- **구독/결제 분석**: 기간 없음

### 명시된 기간 해석 규칙

- **"지난 30일"**: `WHERE DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND DATE(event_time) < CURRENT_DATE()`
  (오늘 제외, 완성된 30일)

- **"이번 주"**: `WHERE DATE(event_time) >= DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) AND DATE(event_time) <= CURRENT_DATE()`
  (월요일부터 오늘까지)

- **"어제"**: `WHERE DATE(event_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`

- **"지난달"**: `WHERE DATE(event_time) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AND DATE(event_time) < DATE_TRUNC(CURRENT_DATE(), MONTH)`
  (전월 1일~말일, 이번달 1일 제외)

❌ 틀린 예: `DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)` (7일 전, "이번 주"가 아님!)

## ⚠️ CRITICAL: 제품(Product) 필터링 — 정확한 매핑 필수

**제품명 → liner_product 매핑 (매우 중요!)**:

| 사용자가 말한 제품 | liner_product 값 | 설명 |
|---|---|---|
| "Scholar" | `'researcher'` | Scholar 제품 (Scholar Free/Pro/Max 모두 포함) |
| "Write" | `'write'` | Write 제품 |
| "AI Search" | `'ai_search'` | AI Search 제품 |

```sql
-- ✓ 올바른 예: Scholar 사용자 필터링
WHERE JSON_EXTRACT_SCALAR(event_properties, '$.liner_product') = 'researcher'

-- ✓ 올바른 예: Write 사용자 필터링
WHERE JSON_EXTRACT_SCALAR(event_properties, '$.liner_product') = 'write'

-- ❌ 틀린 예: 이런 필드명은 없음
WHERE '$.service' = 'scholar'  -- 필드 없음!
WHERE '$.product' = 'scholar'  -- 필드 없음!
```

**중요: 구독 플랜 (pro/max) vs 제품 (Scholar/Write) 혼동하지 말 것**:
- "Scholar 유저" = `liner_product = 'researcher'` (EVENTS_296805)
- "Pro 구독자" = `plan_id = 'pro'` (fct_moon_subscription)
- "Scholar pro 유저" = 둘 다 조건 필요 (두 테이블 조인 또는 IN 서브쿼리)

**⚠️ 타입 변환 필수: 테이블 조인 시 user_id 타입 일치**:
- EVENTS_296805.user_id = **STRING** (⚠️ 주의!)
- fct_moon_subscription.user_id = **INTEGER**
- agent_credit_usage_log.user_id = **INTEGER**

```sql
-- ✓ 올바른 방법 1: EVENTS_296805 + fct_moon_subscription 조인
FROM `liner-219011.analysis.EVENTS_296805` e
INNER JOIN `liner-219011.like.fct_moon_subscription` s
  ON CAST(e.user_id AS INT64) = s.user_id  -- STRING을 INT64로 캐스팅!
  AND s.product_category IN ('pro', 'max')

-- ✓ 올바른 방법 2: EVENTS_296805 + agent_credit_usage_log
FROM `liner-219011.analysis.EVENTS_296805` e
INNER JOIN `liner-219011.cdc_service_db_new_liner.agent_credit_usage_log` acu
  ON CAST(e.user_id AS INT64) = acu.user_id  -- STRING을 INT64로 캐스팅!

-- ❌ 틀린 예: 타입 캐스팅 없음 → "No matching signature for operator ="
FROM EVENTS_296805 e
WHERE e.user_id IN (
  SELECT user_id FROM fct_moon_subscription  -- STRING IN INTEGER 타입 오류!
)

-- ✓ 올바른 IN 서브쿼리 (CAST 필수)
FROM EVENTS_296805 e
WHERE CAST(e.user_id AS INT64) IN (
  SELECT user_id FROM fct_moon_subscription WHERE product_category IN ('pro', 'max')
)
```

## ⚠️ CRITICAL: 사용자 세그먼트 분류 방법

**중요**: Liner의 사용자 분류는 "쿼리 내용"으로 한다!

- make_chat 이벤트의 query 텍스트를 분석한다
- 예: "이력서", "취업", "면접" 키워드 → "취업 관심 사용자"
- 예: "영문", "수료증" 키워드 → "교육 관심 사용자"
- 예: "컨설팅", "법률" 키워드 → "비즈니스 사용자"

## ⚠️ CRITICAL: DAU는 make_chat 기반

**DAU (Daily Active User) = "쿼리를 입력한 사용자" = make_chat 이벤트 기반**

```sql
-- ✓ 올바른 DAU 계산
SELECT COUNT(DISTINCT user_id)
FROM EVENTS_296805
WHERE event_type = 'make_chat'  -- 필수!
  AND DATE(event_time) >= ...
```

사용자가 "DAU를 구해줘"라고 하면 반드시 `event_type = 'make_chat'` 필터를 포함하세요!

## ⚠️ CRITICAL: Credit은 agent_credit_usage_log에서만

**Credit 사용량은 EVENTS_296805에 없습니다. 반드시 별도 테이블에서 조회하세요!**

```sql
-- ❌ 금지: EVENTS에서 credit 추출 시도
SELECT JSON_EXTRACT_SCALAR(event_properties, '$.credit_used')  -- 필드 없음!

-- ✓ 올바른 방법: agent_credit_usage_log 사용
SELECT
  user_id,
  SUM(-delta_amount) AS total_credit
FROM `liner-219011.cdc_service_db_new_liner.agent_credit_usage_log`
WHERE delta_amount < 0  -- 음수만 = 사용량
```

**Credit 관련 질문 처리 (매우 중요!)**:

예시: "Scholar 사용자들이 credit을 얼마나 사용했는지?"

```sql
-- ✓ 올바른 방법: agent_credit_usage_log 직접 조회 (가장 간단, 가장 빠름)
SELECT
  user_id,
  SUM(ABS(delta_amount)) AS total_credit_used,
  COUNT(*) AS usage_count,
  MIN(used_at) AS first_usage,
  MAX(used_at) AS last_usage
FROM `liner-219011.cdc_service_db_new_liner.agent_credit_usage_log`
WHERE delta_amount < 0  -- ★ 사용량만 조회 (음수)
  AND service = 'scholar'  -- ★ 서비스 필터
GROUP BY user_id
HAVING total_credit_used > 0
ORDER BY total_credit_used DESC
```

**핵심 포인트**:
1. agent_credit_usage_log는 credit 데이터의 원천 (가장 정확함)
2. delta_amount < 0 필터는 필수 (음수 = 사용, 양수 = 충전)
3. SUM(ABS(delta_amount))로 사용량 합계 (음수 부호 제거)
4. service = 'scholar' 또는 'write' 등으로 제품 필터링
5. EVENTS_296805는 불필요 (credit은 agent_credit_usage_log에만 있음)

## 핵심 SQL 패턴

1. **make_chat 이벤트에서 쿼리 추출**:
   ```sql
   JSON_EXTRACT_SCALAR(event_properties, '$.query') as query_text
   ```

2. **사용자 segment/category 분석**:

   ⚠️ **중요**: 쿼리 텍스트에 LIKE 매칭을 하지 마세요!

   - 대신 사전 분류된 mart 테이블을 사용합니다
   - mart 테이블이 없으면 그 사실을 명시하고 작업을 중단합니다

   ```sql
   -- ✓ 올바른 방법: 사전 분류 테이블 JOIN
   SELECT
     uc.category,
     COUNT(DISTINCT e.user_id) as user_count
   FROM `liner-219011.analysis.EVENTS_296805` e
   JOIN `<mart_table>.user_category` uc
     ON e.user_id = uc.user_id
   WHERE e.event_type = 'make_chat'
     AND DATE(e.event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
   GROUP BY uc.category

   -- ❌ 금지: LIKE 매칭으로 category 판별
   -- WHERE LOWER(query_text) LIKE '%이력서%' -- 이런 방식 금지!
   ```

3. **구독 데이터 쿼리 패턴**:
   ```sql
   -- 활성 구독자 (현재)
   WHERE status = 'active' AND subscription_ended_at IS NULL

   -- 특정 기간 신규 구독자
   WHERE DATE(subscription_start_at) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
     AND DATE(subscription_start_at) < DATE_TRUNC(CURRENT_DATE(), MONTH)
   ```

4. **JSON 필드의 안전한 타입 변환**:
   ```sql
   -- JSON 필드는 STRING이므로 타입 변환 필요
   -- ✓ 올바른 예: SAFE_CAST로 null-safe 변환
   SAFE_CAST(JSON_EXTRACT_SCALAR(event_properties, '$.amount') AS INT64) as amount

   -- ✗ 위험한 예: CAST는 null string 처리 오류 위험
   CAST(JSON_EXTRACT_SCALAR(event_properties, '$.amount') AS INT64) -- 금지
   ```

5. **사용자 마스터 테이블 조인** (선택):
   ```sql
   FROM `liner-219011.analysis.EVENTS_296805` e
   JOIN `liner-219011.like.dim_user` u ON e.user_id = u.user_id
   ```

## 규칙

1. **DISTINCT 사용**: 조인 후 중복 제거
2. **GROUP BY 필수**: 집계 함수 사용 시
3. **COUNT 함수 사용**:
   - 전체 행 수: `COUNT(*)` 사용 (NULL 포함)
   - 특정 컬럼 수: `COUNT(컬럼명)` 사용 (NULL 제외) — 단, user_id는 거의 항상 null이 아님
   - "이벤트 수"를 세면 `COUNT(*)` 사용 (행 = 이벤트)
4. **날짜 형식**: YYYY-MM-DD (따옴표 포함)
5. **테이블 전체 경로**: liner-219011.analysis.EVENTS_296805
6. **구독 테이블 필터링**:
   - 활성 구독자: `WHERE status = 'active' AND subscription_ended_at IS NULL` (두 조건 모두 필수)
   - 날짜 필터: TIMESTAMP 필드는 DATE() 변환 후 비교 (`DATE(subscription_start_at) >= ...`)
   - 절대 불필요한 테이블을 JOIN하지 말 것 (구독 테이블만으로 충분)

## ⚠️ CRITICAL: EVENTS_296805 파티션 필터 (BigQuery 비용 절감)

**EVENTS_296805는 event_time 컬럼으로 파티셔닝됨. 반드시 파티션 필터를 WHERE절에 추가하세요!**

```sql
-- ✓ 올바른 예: 파티션 필터 추가 (DATE() 적용하면 partition pruning 동작)
WHERE DATE(event_time) BETWEEN '2026-04-01' AND '2026-05-07'
  OR WHERE DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)

-- ❌ 금지: 파티션 필터 없음 (전체 스캔, 비용 폭발)
WHERE event_type = 'make_chat'
```

**주의**: DATE(event_time)을 사용하면 BigQuery가 자동으로 partition pruning을 적용합니다 (monotonic function 최적화).

## ⚠️ CRITICAL: 두 테이블 조인 시 IN 서브쿼리 금지 - JOIN 권장

**같은 테이블을 여러 번 읽는 CTE 구조 금지. 단일 패스 처리로 통합하세요!**

**특히 EVENTS_296805와 fct_moon_subscription을 조인할 때:**
- ❌ 금지: `WHERE user_id IN (SELECT user_id FROM fct_moon_subscription ...)`
  → BigQuery 타입 불일치 에러 (INT64 vs STRING)
- ✓ 권장: JOIN으로 직접 연결
  → 타입 안전, 성능 우수

```sql
-- ❌ 나쁜 예: IN 서브쿼리 (타입 불일치)
WITH scholar_users AS (
  SELECT user_id
  FROM `liner-219011.analysis.EVENTS_296805`
  WHERE liner_product = 'researcher'
)
SELECT ... FROM agent_credit_usage_log
WHERE user_id IN (SELECT user_id FROM scholar_users)
  AND user_id IN (SELECT user_id FROM fct_moon_subscription WHERE plan_id IN ('pro', 'max'))
  -- ↑ 에러: INT64 (events.user_id) vs STRING (fct_moon_subscription.user_id)

-- ✓ 좋은 예: JOIN 사용
SELECT ...
FROM agent_credit_usage_log acu
JOIN (
  SELECT DISTINCT SAFE_CAST(user_id AS INT64) AS user_id
  FROM fct_moon_subscription
  WHERE plan_id IN ('pro', 'max')
) subs ON acu.user_id = subs.user_id
WHERE acu.user_id IN (
  SELECT user_id FROM EVENTS_296805
  WHERE liner_product = 'researcher'
)
```

## ⚠️ CRITICAL: CTE 중복 스캔 제거

**같은 테이블을 여러 번 읽는 CTE 구조 금지. 단일 패스 처리로 통합하세요!**

```sql
-- ❌ 나쁜 예: 같은 테이블 3번 스캔
WITH scholar_users AS (
  SELECT user_id FROM EVENTS_296805 WHERE liner_product='researcher'  -- 스캔 1
)
SELECT ... FROM EVENTS_296805 WHERE user_id IN (SELECT ...)  -- 스캔 2 (IN 서브쿼리)
  AND user_id IN (SELECT user_id FROM scholar_users)  -- 스캔 3

-- ✓ 좋은 예: 단일 패스 (base CTE)
WITH base AS (
  SELECT
    user_id,
    event_type,
    JSON_EXTRACT_SCALAR(event_properties, '$.liner_product') AS liner_product,
    ... 필요한 모든 컬럼
  FROM EVENTS_296805
  WHERE DATE(_PARTITIONTIME) BETWEEN '2026-04-01' AND '2026-05-07'  -- 파티션 필터
)
SELECT
  user_id,
  SUM(IF(event_type='make_chat', credit_used, 0)) AS total_credit
FROM base
WHERE user_id IN (SELECT DISTINCT user_id FROM base WHERE liner_product='researcher')
  AND credit_used IS NOT NULL
GROUP BY user_id
HAVING total_credit > 0  -- 0 값 제거
ORDER BY total_credit DESC
LIMIT 10
```

**핵심:**
- 파티션 필터로 스캔 범위 좁히기 (필수)
- base CTE로 한 번에 필요한 컬럼만 추출
- WHERE에서 조기 필터링 (HAVING이 아니라 WHERE)
- HAVING으로 0 값 제거

## ⚠️ 비즈니스 도메인 규칙

### Business Model (매출 분류) — 8가지, Mutually Exclusive

매출은 **정확히 1개 Business Model에만** 속합니다:
1. **ads**: Ad Manager + Keyword Ad
2. **api**: Adot API 사용료
3. **subscription**: 구독료 (인식 vs 수취 구분)
4. **credit**: 크레딧 구매/사용 (인식 vs 수취 구분)
5. **partnership**: 파트너 계약
6. **contract**: 일반 계약
7. **b2b_ax**: B2B AX 계약
8. **gov_grant**: 정부 지원사업

### Revenue Recognition (인식매출) vs Payment Received (수취매출)

| 항목 | 인식매출 | 수취매출 |
|------|--------|--------|
| **Subscription** | fct_subscription_revenue_recognition (기간 분배 + VAT) | fct_subscription_revenue_financial (결제 시점, type≠failure) |
| **Credit** | met_credit_revenue_daily_summary (earned + breakage) | met_credit_purchase_bookings (booking 시점) |
| **Contract형** | fct_contracted_revenue_recognition (기간 분배) | GROUP BY start_date SUM (일시 인식) |
| **Ads/API** | 양쪽 동일 | 양쪽 동일 |

**중요**: "매출"이라고 하면 인식매출/수취매출 중 어느 것인지 확인하고, 명시되지 않으면 질문해야 함.

### Credit 시스템 (중요!)

**Plan별 월간 기본 할당**:
- Free: 100 credits/month (일부 지역 30)
- Pro: 1,000 credits/month
- Max: 2,500 credits/month

**시스템 마이그레이션** (2025-10-27):
- Legacy: user_agent_credit_usage_modification_log (양수 = 사용) — 폐기됨
- Current: agent_credit_usage_log (양수 = 충전, 음수 = 사용) ← **현재 사용**

### ARR/MRR (구독)

- **개인 ARR**: like.met_individual_subscription_arr_ltm_daily (LTM 필터)
- **팀 ARR**: like.met_team_subscription_arr_ltm_daily (LTM 필터)
- **NTM** (다음 12개월): _ntm_daily 테이블 사용
- **Trial**: _trial_no_card_daily (카드 없는 체험판)

### Referral Program (추천 프로그램)

**프로그램**: 초대자와 신청자 모두 크레딧 획득 → P&L = 수익 - AI비용들

**데이터**:
- 신청자: `entry_type = 'referral_invited'` + `event_type = 'complete_signup'`
- 초대자: `trigger_type = 'inviter_invite'` + `event_type = 'complete_provide_credit'`
- 크레딧 사용: `promotion_type = 'referral_signup_reward'` + `use_research_agent_credit`
- AI 비용: llm_cost (event_properties) 또는 estimation: actual_credit * 0.007
- 안정화 시작: **2026-04-10** (04-09는 품질 낮음)
- 초대자 비용 분배: grant-weighted proration

### Statsig A/B 실험

**ID 타입 판별** (매우 중요!):
- UUID (stableID): `user_properties.stable_id` JOIN 필수
- Numeric (Liner user_id): 직접 사용 (추가 해석 불필요)

**Cohort 구성**:
1. 최초 노출일: MIN(event_time) per user
2. 행동 필터: 노출 **이후** 이벤트만 포함 (행동 오염 방지)
3. experimentGroupName = NULL 제외 (Cache:Unrecognized)
4. JSON key: `'$."metadata.config"'`, `'$."metadata.experimentGroupName"'` (dot-notation, 중첩 X)

### People Search (인물검색)

**데이터**: langfuse_data.observations
- 필터: `name = 'handle_people_search'` (2026-04-20 이후만 가능)
- 메타데이터: exa_call_count, exa_r1_count, exa_r2_count, raw_card_count, final_card_count
- 시간: start_time (UTC, KST는 -9시간)

### Payment Platform (결제 플랫폼)

**지원 플랫폼**: stripe, tosspayments, paddle, paypal, apple, google

**Paddle 특수성** (MOR - Merchant of Record):
- VAT: Paddle이 공제하고 정산 (fct /1.1 적용)
- 금액 단위: KRW 원 단위, USD 센트 단위 (/100.0)
- Plan change: origin = 'subscription_update' (line_item quantity>0 = 새 플랜)
- 데이터 시작: 2026-03-31

**Transaction Type**: payment, refund, dispute, reversal (failure 제외)

## 응답 형식

SQL 코드블록만 반환. 설명 없음.

```sql
SELECT ...
```
"""

    def __init__(self) -> None:
        """초기화"""
        self.schema = BIGQUERY_SCHEMA
        self.success_queries = SUCCESSFUL_QUERIES
        self.antipatterns = ANTIPATTERNS
        self.sample_events = SAMPLE_EVENTS
        self.sectors = SECTORS

    # ========== P3: 동적 테이블/항목 선택 (토큰 효율) ==========

    def _get_relevant_tables(self, user_query: str) -> list[str]:
        """사용자 쿼리와 관련된 테이블만 선택

        키워드 매칭으로 필요한 테이블 선택:
        - 'credit', '크레딧', '사용량' → EVENTS_296805 + agent_credit_usage_log
        - '구독', '활성' → fct_moon_subscription
        - '메시지', '답변' → fct_question_answer_binding_message
        - 'DAU', '활성사용자' → EVENTS_296805
        """
        query_lower = user_query.lower()
        relevant = []

        # 모든 테이블은 기본으로 포함 (선택적 - 충분히 관련도 높음)
        # 하지만 우선순위는: 명시된 테이블을 먼저

        # Credit 관련 → agent_credit_usage_log 필수
        if any(kw in query_lower for kw in ['credit', '크레딧', '사용량', 'usage', '비용']):
            relevant.append('analysis.EVENTS_296805')  # 제품 필터링용
            relevant.append('cdc_service_db_new_liner.agent_credit_usage_log')  # 메인 테이블

        # 구독 관련
        if any(kw in query_lower for kw in ['구독', '활성', '신규', '구독자']):
            relevant.append('like.fct_moon_subscription')

        # 메시지 관련
        if any(kw in query_lower for kw in ['메시지', '답변', '질문', '문답']):
            relevant.append('like.fct_question_answer_binding_message')

        # DAU, 이벤트, 쿼리, 제품 관련 → EVENTS는 항상
        if any(kw in query_lower for kw in ['dau', 'event', 'make_chat', '이벤트', '쿼리',
                                               'write', 'scholar', '제품', 'product', 'liner']):
            relevant.append('analysis.EVENTS_296805')

        # 기본값: 명시 키워드 없으면 EVENTS 포함 (대부분의 쿼리가 EVENTS 사용)
        if not relevant:
            relevant.append('analysis.EVENTS_296805')

        # user 관련
        if any(kw in query_lower for kw in ['사용자', 'user', '속성', 'attribute']):
            if 'like.dim_user' not in relevant:
                relevant.append('like.dim_user')

        return list(set(relevant))  # 중복 제거

    def _get_relevant_glossary_terms(self, user_query: str) -> list[str]:
        """사용자 쿼리와 관련된 glossary 항목만 선택"""
        query_lower = user_query.lower()
        relevant_terms = []

        for term in GLOSSARY.keys():
            term_info = GLOSSARY[term]

            # 기본 용어명 매칭
            if term in query_lower:
                relevant_terms.append(term)
                continue

            # 동의어 매칭
            if 'alternative_terms' in term_info:
                for alt in term_info['alternative_terms']:
                    if alt.lower() in query_lower:
                        relevant_terms.append(term)
                        break

        return list(set(relevant_terms))

    def build_prompt(self, user_query: str) -> str:
        """
        P3 개선: 단일 진입점. 동적으로 관련 정보만 포함하여 프롬프트 생성.

        구성:
        1. 시스템 프롬프트
        2. 관련 테이블만의 스키마 (동적 선택)
        3. 관련 glossary 항목만
        4. 동적 예시 (이미 구현됨)
        5. 관련 안티패턴만
        6. 비즈니스 규칙

        Args:
            user_query: 사용자의 자연어 쿼리

        Returns:
            LLM에 전달할 프롬프트

        Raises:
            ContextBuildingError: 프롬프트 구성 실패
        """
        try:
            self._user_query = user_query
            parts = [
                self.SYSTEM_PROMPT,
                "\n" + "=" * 80 + "\n",
                self._build_dynamic_schema_section(user_query),  # P3: 동적
                "\n" + "=" * 80 + "\n",
                self._build_relevant_glossary_section(user_query),  # P3: 동적
                "\n" + "=" * 80 + "\n",
                self._build_success_examples_section(),
                "\n" + "=" * 80 + "\n",
                self._build_relevant_antipatterns_section(user_query),  # P3: 동적
                "\n" + "=" * 80 + "\n",
                self._build_business_rules_section(),
                "\n" + "=" * 80 + "\n\n",
                f"## 사용자 쿼리\n\n{user_query}\n\n",
                "위 규칙과 예시를 참고하여 BigQuery SQL을 작성하세요.",
            ]

            return "".join(parts)

        except Exception as e:
            raise ContextBuildingError(f"프롬프트 생성 실패: {str(e)}") from e

    def _build_dynamic_schema_section(self, user_query: str) -> str:
        """P3: 관련 테이블만의 스키마 섹션 (토큰 절감)

        사용자 쿼리와 관련된 테이블만 포함하여 불필요한 정보 제외.
        """
        parts = ["/* Given the following BigQuery schema: */\n\n"]
        relevant_tables = self._get_relevant_tables(user_query)

        for table_key, table_info in self.schema.items():
            # P3: 관련 테이블만 포함
            full_name = table_info.get("full_name", table_key)
            if full_name not in relevant_tables and table_key not in relevant_tables:
                continue

            # 이하는 기존 _build_schema_section 로직
            if "partitioning" in table_info:
                p = table_info["partitioning"]
                parts.append(f"-- ⚠️ 파티션 컬럼: {p['column']} (필수 필터)\n")
                parts.append(f"-- {p['note']}\n\n")

            parts.append(f"CREATE TABLE `{full_name}` (\n")

            col_lines = []
            for col_name, col_info in table_info.get("columns", {}).items():
                col_type = col_info["type"]
                nullable = "" if col_info.get("nullable", True) else " NOT NULL"

                comment_parts = []
                if "role" in col_info:
                    comment_parts.append(f"[{col_info['role']}]")
                comment_parts.append(col_info.get("description", ""))

                if "examples" in col_info and col_info["examples"]:
                    examples = [str(e) for e in col_info["examples"][:3] if e is not None]
                    if examples:
                        comment_parts.append(f"예: {', '.join(examples)}")

                if col_info.get("note"):
                    comment_parts.append(f"★ {col_info['note']}")

                comment = " | ".join(comment_parts)
                col_lines.append(f"  {col_name} {col_type}{nullable},  -- {comment}")

            if col_lines:
                col_lines[-1] = col_lines[-1].rstrip(",")

            parts.append("\n".join(col_lines))
            parts.append(f"\n);\n-- {table_info['description']}\n")

            # JSON 컬럼의 important_keys 펼치기
            for col_name, col_info in table_info.get("columns", {}).items():
                if "important_keys" in col_info:
                    parts.append(f"-- JSON 컬럼 '{col_name}'의 주요 키:\n")
                    for key_name, key_info in col_info["important_keys"].items():
                        key_desc = key_info.get("description", "")
                        parts.append(f"--   $.{key_name}: {key_desc}")

                        if key_info.get("note"):
                            parts.append(f" ★ {key_info['note']}")

                        if key_info.get("extraction"):
                            parts.append(f"\n--     추출: {key_info['extraction']}")

                        if key_info.get("examples"):
                            examples = key_info['examples'][:2]
                            parts.append(f"\n--     예: {examples}")

                        parts.append("\n")
                    parts.append("\n")

            # 파티션 필터 예시
            if "partitioning" in table_info:
                col = table_info["partitioning"]["column"]
                parts.append(f"-- 필수 파티션 필터 예시:\n")
                parts.append(f"--   WHERE DATE({col}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)\n")
                parts.append(f"--   또는: WHERE DATE({col}) BETWEEN '2026-04-01' AND '2026-05-07'\n\n")

            # 금지사항
            if "not_for" in table_info:
                parts.append("-- 주의: 다음 용도로는 이 테이블 사용 금지:\n")
                for item in table_info["not_for"]:
                    parts.append(f"--   - {item}\n")
            parts.append("\n")

        return "".join(parts)

    def _build_schema_section(self) -> str:
        """(레거시) 모든 테이블의 스키마 섹션. build_prompt에서는 _build_dynamic_schema_section 사용."""
        parts = ["/* Given the following BigQuery schema: */\n\n"]

        for table_key, table_info in self.schema.items():
            full_name = table_info.get("full_name", table_key)

            # 파티션 정보가 있으면 강조
            if "partitioning" in table_info:
                p = table_info["partitioning"]
                parts.append(f"-- ⚠️ 파티션 컬럼: {p['column']} (필수 필터)\n")
                parts.append(f"-- {p['note']}\n\n")

            parts.append(f"CREATE TABLE `{full_name}` (\n")

            col_lines = []
            for col_name, col_info in table_info["columns"].items():
                col_type = col_info["type"]
                nullable = "" if col_info.get("nullable", True) else " NOT NULL"

                # 기본 주석: 역할 + 설명
                comment_parts = []
                if "role" in col_info:
                    comment_parts.append(f"[{col_info['role']}]")
                comment_parts.append(col_info.get("description", ""))

                # 예시 추가
                if "examples" in col_info and col_info["examples"]:
                    examples = [str(e) for e in col_info["examples"][:3] if e is not None]
                    if examples:
                        comment_parts.append(f"예: {', '.join(examples)}")

                # ★ 마크된 중요 가이드
                if col_info.get("note"):
                    comment_parts.append(f"★ {col_info['note']}")

                comment = " | ".join(comment_parts)
                col_lines.append(f"  {col_name} {col_type}{nullable},  -- {comment}")

            if col_lines:
                col_lines[-1] = col_lines[-1].rstrip(",")

            parts.append("\n".join(col_lines))
            parts.append(f"\n);\n-- {table_info['description']}\n")

            # JSON 컬럼의 important_keys 펼치기
            for col_name, col_info in table_info["columns"].items():
                if "important_keys" in col_info:
                    parts.append(f"-- JSON 컬럼 '{col_name}'의 주요 키:\n")
                    for key_name, key_info in col_info["important_keys"].items():
                        key_desc = key_info.get("description", "")
                        parts.append(f"--   $.{key_name}: {key_desc}")

                        # ★ 마크된 가이드
                        if key_info.get("note"):
                            parts.append(f" ★ {key_info['note']}")

                        # 추출 패턴
                        if key_info.get("extraction"):
                            parts.append(f"\n--     추출: {key_info['extraction']}")

                        # 예시
                        if key_info.get("examples"):
                            examples = key_info['examples'][:2]
                            parts.append(f"\n--     예: {examples}")

                        parts.append("\n")
                    parts.append("\n")

            # 파티션 필터 예시
            if "partitioning" in table_info:
                col = table_info["partitioning"]["column"]
                parts.append(f"-- 필수 파티션 필터 예시:\n")
                parts.append(f"--   WHERE DATE({col}) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)\n")
                parts.append(f"--   또는: WHERE DATE({col}) BETWEEN '2026-04-01' AND '2026-05-07'\n\n")

            # 금지사항 추가 — LLM이 잘못된 테이블 선택 안 하도록
            if "not_for" in table_info:
                parts.append("-- 주의: 다음 용도로는 이 테이블 사용 금지:\n")
                for item in table_info["not_for"]:
                    parts.append(f"--   - {item}\n")
            parts.append("\n")

        return "".join(parts)

    def _build_success_examples_section(self) -> str:
        """
        성공한 쿼리 예시 섹션 (의미론적 유사도 기반 동적 선택 - DAIL-SQL 패턴)

        Returns:
            마크다운 형식의 쿼리 예시
        """
        parts = ["## 성공한 쿼리 예시\n"]

        # 동적 예시 선택 (사용자 쿼리와 의미론적 유사도 기반)
        user_query = self._user_query if hasattr(self, '_user_query') else ""
        if not user_query:
            # fallback: 처음 3개 예시
            selected = list(self.success_queries.values())[:3]
        else:
            try:
                selector = get_selector()
                selected = selector.select_examples(user_query, top_k=3)
            except Exception:
                # 임베딩 오류 시 fallback
                selected = list(self.success_queries.values())[:3]

        # 마크다운 생성
        for i, info in enumerate(selected, 1):
            similarity = f" (유사도: {info.get('similarity_score', 0):.2f})" \
                if "similarity_score" in info else ""
            parts.append(f"\n### 예시 {i}: {info['description']}{similarity}\n")
            parts.append(f"**사용 사례**: {info['use_case']}\n\n")
            parts.append(f"```sql\n{info['sql'].strip()}\n```\n")

        return "".join(parts)

    def _build_relevant_glossary_section(self, user_query: str) -> str:
        """P3: 관련 glossary 항목만 (토큰 절감)"""
        relevant_terms = self._get_relevant_glossary_terms(user_query)

        # 관련 항목이 없으면 모든 항목 포함 (폴백)
        if not relevant_terms:
            return get_glossary_section_for_prompt()

        lines = [
            "## 도메인 용어 사전 (Glossary)\n",
            "다음 용어가 질문에 등장하면, 반드시 지정된 소스를 우선 조회하세요.\n",
            "anti_patterns에 나열된 SQL은 절대 금지합니다.\n",
        ]

        for term in sorted(relevant_terms):
            if term not in GLOSSARY:
                continue

            info = GLOSSARY[term]
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

    def _build_relevant_antipatterns_section(self, user_query: str) -> str:
        """P3: 관련 안티패턴만 (토큰 절감)"""
        # 모든 안티패턴을 포함 (주요 규칙이므로 스킵하지 않음)
        # 필요시 glossary와 유사하게 동적 필터링 가능
        return self._build_antipatterns_section()

    def _build_antipatterns_section(self) -> str:
        """
        금지 패턴 섹션

        Returns:
            마크다운 형식의 안티패턴 정보
        """
        parts = ["## 반드시 피할 패턴\n"]

        for i, pattern in enumerate(self.antipatterns[:5], 1):
            parts.append(f"\n### {i}. {pattern['problem']}\n")
            parts.append(f"**잘못됨**: `{pattern['pattern']}`\n")
            parts.append(f"**해결**: {pattern['fix']}\n")

        return "".join(parts)

    def _build_business_rules_section(self) -> str:
        """
        비즈니스 규칙 섹션

        Returns:
            마크다운 형식의 비즈니스 규칙
        """
        parts = ["## 비즈니스 규칙\n"]

        # 리텐션 정의
        parts.append("\n### 리텐션 정의\n")
        parts.append("- **D+7**: 첫 이벤트 후 7~13일 사이 재활동\n")
        parts.append("- 같은 날짜 제외 (다른 날짜에 재활동)\n")

        # 섹터
        parts.append("\n### 섹터 분류\n")
        for sector, info in self.sectors.items():
            keywords = ", ".join(info["keywords"][:3])
            parts.append(f"- **{sector}**: {info['description']} (예: {keywords})\n")

        # 시간대
        parts.append("\n### 시간대\n")
        parts.append("- event_time: UTC 기준\n")
        parts.append("- 한국 시간: UTC + 9시간\n")

        # 샘플 크기
        parts.append("\n### 샘플 크기\n")
        parts.append("- **100+**: 통계적 신뢰성 높음\n")
        parts.append("- **10-100**: 주의 필요\n")
        parts.append("- **<10**: 결과 신뢰 불가\n")
        parts.append("- `HAVING COUNT(DISTINCT user_id) >= 10`으로 제한\n")

        return "".join(parts)

    def get_system_prompt(self) -> str:
        """시스템 프롬프트만 반환"""
        return self.SYSTEM_PROMPT

    def get_schema_only(self) -> str:
        """스키마 정보만 반환"""
        return self._build_schema_section()

    def get_examples_only(self) -> str:
        """성공 사례만 반환"""
        return self._build_success_examples_section()
