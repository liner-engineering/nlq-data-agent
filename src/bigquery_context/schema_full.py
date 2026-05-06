"""
liner-219011 BigQuery 프로젝트의 완전한 스키마 정의

LLM이 정확한 쿼리를 작성할 수 있도록 상세한 테이블 정보 제공
"""

BIGQUERY_SCHEMA = {
    'analysis.EVENTS_296805': {
        'full_name': 'liner-219011.analysis.EVENTS_296805',
        'description': 'Amplitude 이벤트 데이터 (메인 테이블)',
        'row_count_estimate': '~500M (2025년 기준)',
        'update_frequency': 'Real-time',
        'date_range': '2024-01-01 ~ present',
        'columns': {
            'user_id': {
                'type': 'STRING',
                'nullable': True,
                'description': '사용자 ID (nullable - 비로그인 사용자)',
                'examples': ['user_abc123', 'user_def456', None],
                'note': 'amplitude_id와 함께 사용자 식별'
            },
            'event_type': {
                'type': 'STRING',
                'nullable': False,
                'description': '이벤트 타입 (사용자 행동)',
                'examples': [
                    'make_chat',                                # 사용자가 쿼리 작성
                    'click_component_settings_page',            # 설정 페이지 클릭
                    'view_contents_pricing_page',               # 가격 페이지 조회
                    'view_subscription_wall',                   # 구독 벽 조회
                    'subscription_request_result_exposed',      # 구독 결과 노출
                    'view_defensive_confirm_cancellation_modal' # 취소 모달 조회
                ],
                'note': 'event_type만으로는 분석 불충분 → event_properties 함께 사용'
            },
            'event_properties': {
                'type': 'JSON',
                'nullable': True,
                'description': '이벤트 속성 (JSON 형식)',
                'common_keys': {
                    'query': {
                        'type': 'string',
                        'description': '사용자가 입력한 쿼리/질문',
                        'example': '이력서 첨삭 부탁합니다'
                    },
                    'status': {
                        'type': 'string',
                        'description': '쿼리 처리 상태',
                        'examples': ['completed', 'pending', 'failed']
                    },
                    'liner_product': {
                        'type': 'string',
                        'description': '사용된 제품',
                        'examples': ['chat', 'knowledge_base']
                    },
                    'path': {
                        'type': 'string',
                        'description': '앱 내 경로',
                        'example': '/chat'
                    }
                },
                'extraction_example': 'JSON_EXTRACT_SCALAR(event_properties, "$.query")'
            },
            'event_time': {
                'type': 'TIMESTAMP',
                'nullable': False,
                'description': '이벤트 발생 시간',
                'format': 'YYYY-MM-DD HH:MM:SS.fff (UTC)',
                'note': '리텐션/코호트 분석 시 DATE(event_time)로 일자 추출'
            },
            'session_id': {
                'type': 'STRING',
                'nullable': True,
                'description': '세션 ID (같은 세션 내 이벤트들)'
            },
            'amplitude_id': {
                'type': 'STRING',
                'nullable': False,
                'description': 'Amplitude ID (user_id가 null일 때 사용)',
                'note': 'user_id와 amplitude_id 중 하나 이상은 항상 존재'
            }
        },
        'common_queries': [
            'date 범위로 필터링: WHERE DATE(event_time) BETWEEN "2026-03-01" AND "2026-04-30"',
            '특정 이벤트만: WHERE event_type IN ("make_chat", "view_subscription_wall")',
            'user_id별 집계: GROUP BY user_id (리텐션, 활동도 분석)',
            'JSON 추출: JSON_EXTRACT_SCALAR(event_properties, "$.query")'
        ]
    },

    'light.fct_moon_subscription': {
        'full_name': 'liner-219011.light.fct_moon_subscription',
        'description': '구독 정보 (언제 누가 구독/취소했는가)',
        'row_count_estimate': '~500K',
        'date_range': '2023-01-01 ~ present',
        'columns': {
            'user_id': {
                'type': 'STRING',
                'nullable': False,
                'description': '사용자 ID'
            },
            'subscription_id': {
                'type': 'STRING',
                'nullable': False,
                'description': '구독 ID (unique)'
            },
            'start_date': {
                'type': 'DATE',
                'nullable': False,
                'description': '구독 시작 날짜'
            },
            'end_date': {
                'type': 'DATE',
                'nullable': True,
                'description': '구독 종료 날짜 (NULL = 현재 구독 중)',
                'note': 'end_date가 NULL이면 활성 구독자'
            },
            'plan_type': {
                'type': 'STRING',
                'nullable': False,
                'description': '요금제',
                'examples': ['premium', 'basic', 'trial']
            }
        },
        'common_queries': [
            '특정 기간 신규 구독자: WHERE DATE(start_date) BETWEEN ... AND ...',
            '현재 활성 구독자: WHERE end_date IS NULL',
            '이벤트와 구독 조인: ON events.user_id = sub.user_id AND DATE(events.event_time) >= sub.start_date'
        ]
    },

    'light.fct_question_answer_binding_message': {
        'full_name': 'liner-219011.light.fct_question_answer_binding_message',
        'description': '사용자 메시지/쿼리 데이터',
        'row_count_estimate': '~10M',
        'columns': {
            'user_id': {
                'type': 'STRING',
                'nullable': False,
                'description': '사용자 ID'
            },
            'message_id': {
                'type': 'STRING',
                'nullable': False,
                'description': '메시지 ID'
            },
            'message_text': {
                'type': 'STRING',
                'nullable': False,
                'description': '사용자가 입력한 쿼리/메시지 텍스트',
                'note': '텍스트 분석으로 sector 분류 가능'
            },
            'created_at': {
                'type': 'TIMESTAMP',
                'nullable': False,
                'description': '메시지 작성 시간'
            },
            'category': {
                'type': 'STRING',
                'nullable': True,
                'description': '이미 분류된 카테고리'
            }
        }
    }
}

COMMON_JOINS = {
    'events_to_subscription': """
    LEFT JOIN `liner-219011.light.fct_moon_subscription` s
        ON events.user_id = s.user_id
        AND DATE(events.event_time) >= s.start_date
        AND (s.end_date IS NULL OR DATE(events.event_time) <= s.end_date)
    """,

    'events_to_messages': """
    LEFT JOIN `liner-219011.light.fct_question_answer_binding_message` m
        ON events.user_id = m.user_id
        AND TIMESTAMP_DIFF(events.event_time, m.created_at, MINUTE) BETWEEN 0 AND 60
    """
}

# 스키마 요약 (LLM 프롬프트용)
SCHEMA_SUMMARY = """
## BigQuery 테이블 요약

### 1. EVENTS_296805 (Amplitude 이벤트)
- 사용자 행동 (make_chat, view_pricing, subscription_wall 등)
- 약 500M 행, 실시간 업데이트
- 주요 컬럼: user_id, event_type, event_properties (JSON), event_time

### 2. fct_moon_subscription (구독 정보)
- 구독 시작/종료 날짜
- 약 500K 행
- 주요 컬럼: user_id, start_date, end_date (NULL = 활성), plan_type

### 3. fct_question_answer_binding_message (메시지)
- 사용자가 입력한 쿼리 텍스트
- 약 10M 행
- 주요 컬럼: user_id, message_text, created_at

## 자주 하는 작업

1. 섹터별 리텐션: EVENTS_296805에서 event_type='make_chat'인 첫 이벤트 찾고, 일주일 후 재활동 여부 확인
2. 구독 전환율: EVENTS_296805와 fct_moon_subscription 조인, 이벤트 후 7일 내 구독 여부
3. 이벤트 시퀀스: 사용자별로 event_type을 시간 순서대로 나열하기
4. 텍스트 분석: message_text에서 키워드로 sector 분류
"""
