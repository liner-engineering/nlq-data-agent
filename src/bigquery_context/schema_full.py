"""
liner-219011 BigQuery 프로젝트의 완전한 스키마 정의

LLM이 정확한 쿼리를 작성할 수 있도록 상세한 테이블 정보 제공
"""

BIGQUERY_SCHEMA = {
    'analysis.EVENTS_296805': {
        'full_name': 'liner-219011.analysis.EVENTS_296805',
        'description': 'Amplitude 이벤트 데이터 - 사용자 행동 기록',
        'row_count_estimate': '~500M (2025년 기준)',
        'update_frequency': 'Real-time',
        'date_range': '2024-01-01 ~ present',
        'not_for': [
            '결제 금액/구독 상태 분석 — like.fct_moon_subscription 사용',
            '메시지 텍스트 분석 — light.fct_question_answer_binding_message 사용',
        ],
        'columns': {
            'user_id': {
                'type': 'INTEGER',
                'nullable': False,
                'role': 'ENTITY',
                'description': '사용자 ID (like.dim_user와 조인 가능)',
                'examples': [20915617, 20932418, 20954117],
                'note': 'like.dim_user의 user_id와 JOIN'
            },
            'event_type': {
                'type': 'STRING',
                'nullable': False,
                'role': 'DIMENSION',
                'description': '이벤트 타입 (사용자 행동)',
                'examples': [
                    'make_chat',                                # 사용자가 쿼리 작성 ← 검색 의도 분석에 사용
                    'click_component_settings_page',
                    'view_contents_pricing_page',
                    'view_subscription_wall',
                    'subscription_request_result_exposed',
                    'view_defensive_confirm_cancellation_modal'
                ],
                'note': 'make_chat과 event_properties.query를 함께 사용'
            },
            'event_properties': {
                'type': 'JSON',
                'nullable': True,
                'role': 'SEMI_STRUCTURED',
                'description': '이벤트 속성 (JSON 형식)',
                'important_keys': {
                    'query': {
                        'type': 'string',
                        'description': '사용자가 입력한 쿼리 텍스트 (검색 키워드)',
                        'examples': [
                            '이력서 첨삭 부탁합니다',
                            '영문 자기소개서 작성법',
                            '취업 면접 팁',
                            '이력서 레이아웃'
                        ],
                        'use_case': '쿼리 내용으로 사용자를 분류 (교육/취업/카테고리별)'
                    },
                    'status': {
                        'type': 'string',
                        'description': '쿼리 처리 상태',
                        'examples': ['completed', 'pending', 'failed']
                    },
                    'liner_product': {
                        'type': 'string',
                        'description': '★ 반드시 사용하는 제품 필드 (MANDATORY)',
                        'examples': ['write', 'researcher', 'ai_search', 'browser_extension'],
                        'note': '제품 필터링은 항상 이 필드를 사용. "service" 필드는 없음!',
                        'extraction': 'JSON_EXTRACT_SCALAR(event_properties, "$.liner_product")'
                    }
                },
                'extraction_example': 'JSON_EXTRACT_SCALAR(event_properties, "$.query")',
                'critical_note': 'query 내용으로 사용자 세그멘트 판별. 제품 필터링은 반드시 liner_product 필드만 사용!'
            },
            'event_time': {
                'type': 'TIMESTAMP',
                'nullable': False,
                'role': 'TIME',
                'description': '이벤트 발생 시간 (UTC)',
                'format': 'YYYY-MM-DD HH:MM:SS.fff',
                'note': 'DATE(event_time) 또는 DATE_TRUNC로 일자 추출'
            },
            'session_id': {
                'type': 'STRING',
                'nullable': True,
                'role': 'ATTRIBUTE',
                'description': '세션 ID'
            },
            'amplitude_id': {
                'type': 'STRING',
                'nullable': True,
                'role': 'ATTRIBUTE',
                'description': 'Amplitude ID'
            }
        },
        'critical_pattern': {
            'description': 'make_chat 이벤트로 사용자 세그먼트 분석하는 방법',
            'example_query': '''
SELECT
  COUNT(DISTINCT user_id) as user_count,
  ROUND(100 * COUNT(DISTINCT user_id) / (SELECT COUNT(DISTINCT user_id)
    FROM `liner-219011.analysis.EVENTS_296805`
    WHERE event_type = 'make_chat' AND DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)), 2) as percentage
FROM `liner-219011.analysis.EVENTS_296805`
WHERE event_type = 'make_chat'
  AND DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND LOWER(JSON_EXTRACT_SCALAR(event_properties, '$.query'))
    LIKE '%이력서%' OR LOWER(JSON_EXTRACT_SCALAR(event_properties, '$.query')) LIKE '%취업%'
            ''',
            'description': '교육/취업 관련 쿼리를 한 사용자 수 계산'
        }
    },

    'like.dim_user': {
        'full_name': 'liner-219011.like.dim_user',
        'description': '사용자 마스터 테이블 - 사용자 속성 정보',
        'row_count_estimate': '~13.5M',
        'not_for': [
            '이벤트 기반 분석 (행동 추적) — EVENTS_296805 사용',
            '메시지 텍스트 분석 — light.fct_question_answer_binding_message 사용',
        ],
        'columns': {
            'user_id': {
                'type': 'INTEGER',
                'nullable': False,
                'role': 'ENTITY',
                'description': '사용자 ID (EVENTS_296805과 조인 가능)',
                'note': '기본 조인 키'
            },
            'user_created_at': {
                'type': 'TIMESTAMP',
                'nullable': False,
                'role': 'TIME',
                'description': '사용자 가입 시간'
            },
            'signup_at': {
                'type': 'TIMESTAMP',
                'nullable': True,
                'role': 'TIME',
                'description': '회원 가입 완료 시간'
            },
            'initial_liner_service': {
                'type': 'STRING',
                'nullable': True,
                'role': 'ATTRIBUTE',
                'description': '초기 Liner 서비스 카테고리',
                'note': '사용자의 최초 관심 분야'
            },
            'last_touch_acquisition_type': {
                'type': 'STRING',
                'nullable': True,
                'role': 'ATTRIBUTE',
                'description': '마지막 터치 포인트의 유입 경로'
            }
        },
        'join_note': 'EVENTS_296805.user_id = like.dim_user.user_id로 사용자 속성 추가 가능'
    },

    'like.fct_moon_subscription': {
        'full_name': 'liner-219011.like.fct_moon_subscription',
        'description': '구독 정보 (언제 누가 구독/취소했는가)',
        'row_count_estimate': '~500K',
        'date_range': '2023-01-01 ~ present',
        'not_for': [
            '사용자 행동/이벤트 분석 — EVENTS_296805 사용',
            '메시지 텍스트 분석 — like.fct_question_answer_binding_message 사용',
        ],
        'columns': {
            'user_id': {
                'type': 'INTEGER',
                'nullable': False,
                'role': 'ENTITY',
                'description': '사용자 ID'
            },
            'subscription_id': {
                'type': 'STRING',
                'nullable': False,
                'role': 'ENTITY',
                'description': '구독 ID (unique)'
            },
            'status': {
                'type': 'STRING',
                'nullable': False,
                'role': 'DIMENSION',
                'description': '구독 상태',
                'examples': ['active', 'canceled']
            },
            'subscription_start_at': {
                'type': 'TIMESTAMP',
                'nullable': False,
                'role': 'TIME',
                'description': '구독 시작 시간 (필터링 시 DATE() 변환 필수)',
                'note': '날짜 비교: DATE(subscription_start_at)'
            },
            'subscription_ended_at': {
                'type': 'TIMESTAMP',
                'nullable': True,
                'role': 'TIME',
                'description': '구독 종료 시간 (NULL = 현재 구독 중)',
                'note': '활성 구독자 조건: subscription_ended_at IS NULL'
            },
            'plan_id': {
                'type': 'STRING',
                'nullable': False,
                'role': 'DIMENSION',
                'description': '요금제 ID'
            }
        },
        'common_queries': [
            '특정 기간 신규 구독자: WHERE DATE(subscription_start_at) BETWEEN ... AND ...',
            '현재 활성 구독자: WHERE status = "active" AND subscription_ended_at IS NULL',
            '이벤트와 구독 조인: ON events.user_id = sub.user_id AND DATE(events.event_time) >= DATE(sub.subscription_start_at)'
        ]
    },

    'like.fct_question_answer_binding_message': {
        'full_name': 'liner-219011.like.fct_question_answer_binding_message',
        'description': '사용자 메시지/쿼리 데이터',
        'row_count_estimate': '~10M',
        'not_for': [
            '사용자 행동/이벤트 분석 — EVENTS_296805 사용',
            '구독 정보/전환 분석 — like.fct_moon_subscription 사용',
        ],
        'columns': {
            'user_id': {
                'type': 'STRING',
                'nullable': False,
                'role': 'ENTITY',
                'description': '사용자 ID'
            },
            'message_id': {
                'type': 'STRING',
                'nullable': False,
                'role': 'ENTITY',
                'description': '메시지 ID'
            },
            'message_text': {
                'type': 'STRING',
                'nullable': False,
                'role': 'SEMI_STRUCTURED',
                'description': '사용자가 입력한 쿼리/메시지 텍스트',
                'note': '텍스트 분석으로 sector 분류 가능'
            },
            'created_at': {
                'type': 'TIMESTAMP',
                'nullable': False,
                'role': 'TIME',
                'description': '메시지 작성 시간'
            },
            'category': {
                'type': 'STRING',
                'nullable': True,
                'role': 'DIMENSION',
                'description': '이미 분류된 카테고리'
            }
        }
    }
}

COMMON_JOINS = {
    'events_to_subscription': """
    LEFT JOIN `liner-219011.like.fct_moon_subscription` s
        ON CAST(events.user_id AS STRING) = s.user_id
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

### 2. like.fct_moon_subscription (구독 정보)
- 구독 시작/종료 날짜 (현재: pro, max만 서비스)
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
