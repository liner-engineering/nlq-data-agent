"""
피해야 할 BigQuery 쿼리 패턴들

LLM이 엉뚱한 쿼리를 짜지 않도록 경고
"""

ANTIPATTERNS = [
    {
        'pattern': 'SELECT * FROM EVENTS_296805 WHERE user_id = "user_123"',
        'problem': '특정 사용자 1명만 조회 → 의미 없는 분석',
        'why_wrong': '개인 행동만 보면 일반화 불가능',
        'fix': 'GROUP BY를 사용해서 집계하거나, 여러 사용자와 비교',
        'correct_example': 'SELECT user_id, COUNT(*) as event_count FROM ... GROUP BY user_id'
    },

    {
        'pattern': 'WHERE event_type IN ("make_chat", "view_pricing")',
        'problem': 'event_type만으로 섹터 판단 → 너무 단순',
        'why_wrong': 'event_type은 행동이고, 섹터는 쿼리 내용 (event_properties.query)에 있음',
        'fix': 'JSON_EXTRACT_SCALAR(event_properties, "$.query")로 텍스트 분석',
        'correct_example': 'JSON_EXTRACT_SCALAR(event_properties, "$.query") LIKE "%이력서%"'
    },

    {
        'pattern': 'DATE_DIFF(event_time, NOW(), DAY) < 7',
        'problem': '현재 시점 기준 리텐션 → 시간이 지나면 부정확',
        'why_wrong': '오늘 기준으로 계산하면 내일은 다시 쓸 수 없음',
        'fix': '특정 시작일 기준으로 정확히 계산',
        'correct_example': '''
    DATE_DIFF(DATE(e.event_time), DATE(fe.first_event_date), DAY) BETWEEN 7 AND 13
    '''
    },

    {
        'pattern': 'LEFT JOIN subscription WHERE user_id = user_id',
        'problem': '구독 여부만 조인 → 구독 기간 무시',
        'why_wrong': 'user_id만 맞춰도 구독 기간이 안 맞을 수 있음',
        'fix': 'event_time이 start_date ~ end_date 사이인지 확인',
        'correct_example': '''
    LEFT JOIN subscription s
    ON e.user_id = s.user_id
    AND DATE(e.event_time) >= s.start_date
    AND (s.end_date IS NULL OR DATE(e.event_time) <= s.end_date)
    '''
    },

    {
        'pattern': 'SELECT retention_rate FROM pre_calculated_table',
        'problem': '이미 계산된 테이블 찾기 → 테이블이 없을 수 있음',
        'why_wrong': '우리 데이터 웨어하우스에는 미리 계산된 리텐션 테이블이 없음',
        'fix': '원본 EVENTS_296805에서 직접 계산',
        'correct_example': '''
    WITH first_events AS (...),
    retention_check AS (...)
    SELECT COUNT(DISTINCT user_id) FROM retention_check WHERE retained = 1
    '''
    },

    {
        'pattern': 'GROUP BY event_type ORDER BY COUNT(*) ASC',
        'problem': '오름차순 정렬 → 가장 적은 것부터 (의미 없음)',
        'why_wrong': '보통 가장 많은 것부터 보고 싶음',
        'fix': 'ORDER BY COUNT(*) DESC',
        'correct_example': 'ORDER BY COUNT(*) DESC'
    },

    {
        'pattern': 'WHERE DATE(event_time) BETWEEN "2026-03-01" AND "2026-03-01"',
        'problem': '같은 날짜로 필터링 → 1일 데이터만',
        'why_wrong': '분석 기간이 너무 짧으면 의미 없는 결과',
        'fix': '최소 30일 이상의 기간 설정',
        'correct_example': 'WHERE DATE(event_time) BETWEEN "2026-03-01" AND "2026-04-30"'
    },

    {
        'pattern': 'CASE WHEN event_type LIKE "%chat%" THEN "engagement"',
        'problem': 'LIKE로 event_type 패턴 매칭 → 불안정',
        'why_wrong': 'event_type은 정확한 문자열이므로 IN() 사용',
        'fix': 'WHERE event_type IN ("make_chat", "view_chat")',
        'correct_example': 'event_type IN ("make_chat")'
    }
]

ANTIPATTERN_SUMMARY = """
## 절대 하면 안 되는 것들 (TOP 5)

1. **특정 1명 사용자만 조회**
   → 반드시 GROUP BY로 집계하세요

2. **event_type만으로 sector 판단**
   → event_properties.query 텍스트 분석 필수

3. **현재 시점 기준 리텐션 계산**
   → 특정 날짜 기준으로 정확하게 계산

4. **구독 조인 시 시간 범위 무시**
   → start_date ~ end_date 확인 필수

5. **이미 계산된 지표 테이블 찾기**
   → 원본 테이블에서 직접 계산

---

## 자주 하는 실수들

- ORDER BY 없이 결과 확인
- LIMIT 없이 백만 개 행 조회
- NULL 처리 누락 (COALESCE 사용)
- 중복 제거 누락 (DISTINCT 필요할 때 안 함)
- 시간대 차이 (UTC vs 로컬)
"""
