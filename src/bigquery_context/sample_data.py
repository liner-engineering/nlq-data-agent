"""
실제 BigQuery 데이터의 샘플

LLM이 데이터 구조와 실제 값들을 이해할 수 있도록 제공
"""

SAMPLE_EVENTS = [
    # 사용자 1: 이력서 관심 → 구독
    {
        'user_id': 'user_001',
        'event_type': 'make_chat',
        'event_properties': {'query': '이력서 첨삭 부탁합니다', 'status': 'completed'},
        'event_time': '2026-04-01 10:30:00',
        'session_id': 'sess_001',
        'amplitude_id': 'amp_001'
    },
    {
        'user_id': 'user_001',
        'event_type': 'view_contents_pricing_page',
        'event_properties': None,
        'event_time': '2026-04-01 10:35:00',
        'session_id': 'sess_001',
        'amplitude_id': 'amp_001'
    },
    {
        'user_id': 'user_001',
        'event_type': 'view_subscription_wall',
        'event_properties': None,
        'event_time': '2026-04-01 10:36:00',
        'session_id': 'sess_001',
        'amplitude_id': 'amp_001'
    },
    {
        'user_id': 'user_001',
        'event_type': 'subscription_request_result_exposed',
        'event_properties': None,
        'event_time': '2026-04-01 10:37:00',
        'session_id': 'sess_001',
        'amplitude_id': 'amp_001'
    },
    # D+7 확인: 4월 8일에 재활동
    {
        'user_id': 'user_001',
        'event_type': 'make_chat',
        'event_properties': {'query': '자기소개서 작성법', 'status': 'completed'},
        'event_time': '2026-04-08 14:20:00',
        'session_id': 'sess_002',
        'amplitude_id': 'amp_001'
    },

    # 사용자 2: 블로그 관심 → 구독 안 함
    {
        'user_id': 'user_002',
        'event_type': 'make_chat',
        'event_properties': {'query': '블로그 글쓰기 팁', 'status': 'completed'},
        'event_time': '2026-04-05 09:00:00',
        'session_id': 'sess_003',
        'amplitude_id': 'amp_002'
    },
    {
        'user_id': 'user_002',
        'event_type': 'view_contents_pricing_page',
        'event_properties': None,
        'event_time': '2026-04-05 09:05:00',
        'session_id': 'sess_003',
        'amplitude_id': 'amp_002'
    },
    # D+7 확인: 4월 12일 재활동 없음

    # 사용자 3: 제안서 관심 → 구독
    {
        'user_id': 'user_003',
        'event_type': 'make_chat',
        'event_properties': {'query': '사업 제안서 작성', 'status': 'completed'},
        'event_time': '2026-04-10 15:45:00',
        'session_id': 'sess_004',
        'amplitude_id': 'amp_003'
    },
    {
        'user_id': 'user_003',
        'event_type': 'view_subscription_wall',
        'event_properties': None,
        'event_time': '2026-04-10 15:50:00',
        'session_id': 'sess_004',
        'amplitude_id': 'amp_003'
    },

    # 사용자 4: 비로그인 사용자 (user_id NULL)
    {
        'user_id': None,
        'event_type': 'make_chat',
        'event_properties': {'query': '논문 작성법', 'status': 'completed'},
        'event_time': '2026-04-15 11:20:00',
        'session_id': 'sess_005',
        'amplitude_id': 'amp_004'
    }
]

SAMPLE_SUBSCRIPTIONS = [
    # 사용자 1: 2026-04-01부터 구독 (진행 중)
    {
        'user_id': 'user_001',
        'subscription_id': 'sub_001',
        'start_date': '2026-04-01',
        'end_date': None,  # 현재 구독 중
        'plan_type': 'premium'
    },
    # 사용자 3: 2026-04-10부터 구독 (진행 중)
    {
        'user_id': 'user_003',
        'subscription_id': 'sub_003',
        'start_date': '2026-04-10',
        'end_date': None,
        'plan_type': 'basic'
    },
    # 사용자 5: 이전에 구독했다가 취소
    {
        'user_id': 'user_005',
        'subscription_id': 'sub_005',
        'start_date': '2026-03-01',
        'end_date': '2026-04-01',  # 1달 후 취소
        'plan_type': 'premium'
    }
]

SAMPLE_MESSAGES = [
    {
        'user_id': 'user_001',
        'message_id': 'msg_001',
        'message_text': '이력서 첨삭 부탁합니다',
        'created_at': '2026-04-01 10:30:00',
        'category': 'professional'
    },
    {
        'user_id': 'user_002',
        'message_id': 'msg_002',
        'message_text': '블로그 글쓰기 팁',
        'created_at': '2026-04-05 09:00:00',
        'category': 'content'
    },
    {
        'user_id': 'user_003',
        'message_id': 'msg_003',
        'message_text': '사업 제안서 작성',
        'created_at': '2026-04-10 15:45:00',
        'category': 'business'
    }
]

# 샘플 데이터 설명 (LLM 학습용)
SAMPLE_DATA_EXPLANATION = """
## 샘플 데이터 설명

### EVENTS_296805 샘플
- user_001: 4월 1일 "이력서 첨삭" 쿼리 작성 → 가격 페이지 → 구독 벽 → 4월 8일 재활동 ✓
- user_002: 4월 5일 "블로그 글쓰기" 쿼리 작성 → 가격 페이지 → 재활동 없음 ✗
- user_003: 4월 10일 "제안서" 쿼리 작성 → 구독 벽 조회
- user_004: 비로그인 사용자 (user_id=NULL, amplitude_id 사용)

### fct_moon_subscription 샘플
- user_001: 4월 1일부터 구독 중 (end_date NULL)
- user_003: 4월 10일부터 구독 중
- user_005: 3월 1일부터 4월 1일까지만 구독 (취소됨)

### 분석 예시
리텐션 = user_001 (D+7 재활동 있음)
전환율 = user_001, user_003 (구독함) / user_001, user_002 (쿼리 작성)
"""
