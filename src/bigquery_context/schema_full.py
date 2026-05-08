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
        'partitioning': {
            'column': 'event_time',
            'type': 'DAY',
            'required': True,
            'note': '파티션 필터 필수! 없으면 ~500M rows 전체 스캔 (비용 폭발, 3TB+ 바이트 청구)'
        },
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
                'note': '★ JSON 필드는 JSON_EXTRACT_SCALAR() 필수. SAFE_CAST 사용 권장.',
                'important_keys': {
                    'liner_product': {
                        'type': 'string',
                        'description': '★ 제품명 (CRITICAL: 제품 필터링 필수 필드)',
                        'examples': ['write', 'researcher', 'ai_search', 'browser_extension'],
                        'note': '★ 제품 필터링은 반드시 이 필드 사용. "service", "product" 필드는 없음!',
                        'extraction': 'JSON_EXTRACT_SCALAR(event_properties, \'$.liner_product\')'
                    },
                    'query': {
                        'type': 'string',
                        'description': '사용자가 입력한 쿼리 텍스트 (검색 키워드/의도 분석용)',
                        'examples': [
                            '이력서 첨삭 부탁합니다',
                            '영문 자기소개서 작성법',
                            '취업 면접 팁'
                        ],
                        'note': '★ 쿼리 텍스트로 user segment 분류하려면 mart 테이블 사용. LIKE 매칭 금지!',
                        'extraction': 'JSON_EXTRACT_SCALAR(event_properties, \'$.query\')'
                    },
                    'status': {
                        'type': 'string',
                        'description': '쿼리 처리 상태',
                        'examples': ['completed', 'pending', 'failed'],
                        'extraction': 'JSON_EXTRACT_SCALAR(event_properties, \'$.status\')'
                    }
                },
                'critical_note': '제품 필터링 = liner_product 필드 필수. 세그먼트 분류 = mart 테이블 사용 (LIKE 금지).'
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
            'method': '''
⚠️ 사용자 segment 분석은 반드시 사전 분류된 mart 테이블을 사용하세요!

1. LIKE 매칭 금지: query 텍스트에 LIKE를 사용하면 안 됩니다
2. mart 테이블 필수: user_category, user_segment 등 사전 분류 테이블 JOIN
3. mart 테이블 없으면: 그 사실을 명시하고 작업 중단

마트 테이블이 없으면 다음과 같이 응답하세요:
"사용자 세그먼트 분석을 위해서는 <테이블명> mart 테이블이 필요합니다.
현재 스키마에서 사용 가능한 segment 정보가 없으므로,
데이터 팀에 문의하거나 mart 테이블 생성을 요청하세요."
            ''',
            'pattern_note': '쿼리 텍스트 기반 segment 분석 = LIKE 매칭으로는 부정확함 → mart 테이블 사용'
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
    },

    'cdc_service_db_new_liner.agent_credit_usage_log': {
        'full_name': 'liner-219011.cdc_service_db_new_liner.agent_credit_usage_log',
        'description': 'AI Agent Credit 사용 로그 (Scholar, Write 등)',
        'row_count_estimate': '~50M 이상',
        'update_frequency': 'Real-time',
        'date_range': '2024년부터 present',
        'not_for': [
            '이벤트 기반 분석 — EVENTS_296805 사용',
            '구독자 분석 — like.fct_moon_subscription 사용',
        ],
        'columns': {
            'id': {
                'type': 'INTEGER',
                'nullable': False,
                'role': 'ENTITY',
                'description': 'Primary key',
            },
            'user_id': {
                'type': 'INTEGER',
                'nullable': False,
                'role': 'ENTITY',
                'description': '사용자 ID (like.dim_user와 조인 가능)',
            },
            'team_id': {
                'type': 'INTEGER',
                'nullable': True,
                'role': 'ENTITY',
                'description': '팀/조직 ID',
            },
            'credit_item_id': {
                'type': 'INTEGER',
                'nullable': True,
                'role': 'ENTITY',
                'description': '크레딧 아이템 ID',
            },
            'delta_amount': {
                'type': 'INTEGER',
                'nullable': False,
                'role': 'MEASURE',
                'description': '크레딧 변화량 (음수 = 사용, 양수 = 충전)',
                'note': '★ 사용량 조회 시 delta_amount < 0 필터 필수. SUM(ABS(delta_amount))으로 합산',
                'examples': [-100, -50, 1000],
            },
            'reason': {
                'type': 'STRING',
                'nullable': True,
                'role': 'ATTRIBUTE',
                'description': '크레딧 변화 사유 (api_call, model_usage 등)',
                'examples': ['api_call', 'model_usage', 'refund'],
            },
            'agent_name': {
                'type': 'STRING',
                'nullable': True,
                'role': 'ATTRIBUTE',
                'description': 'AI 에이전트/모델명 (gpt-4, claude-3, gemini 등)',
                'examples': ['gpt-4', 'claude-3', 'gemini'],
            },
            'resource_id': {
                'type': 'STRING',
                'nullable': True,
                'role': 'ATTRIBUTE',
                'description': '리소스 ID',
            },
            'cost_metadata': {
                'type': 'STRING',
                'nullable': True,
                'role': 'SEMI_STRUCTURED',
                'description': '비용 메타데이터 (JSON 형식)',
            },
            'created_at': {
                'type': 'TIMESTAMP',
                'nullable': False,
                'role': 'TIME',
                'description': '기록 생성 시간 (UTC) — 모든 시간 필터링은 이 컬럼 사용',
            },
            'datastream_metadata': {
                'type': 'RECORD',
                'nullable': True,
                'role': 'SEMI_STRUCTURED',
                'description': 'Datastream 메타데이터',
            }
        },
        'critical_pattern': {
            'description': '사용자별 credit 사용량 조회',
            'method': '''
★ 중요: agent_credit_usage_log는 모든 credit 변화를 기록합니다.
사용자의 credit 사용량을 집계할 때:

1. delta_amount < 0으로 필터링 (사용 기록만, 음수 = 사용)
2. SUM(ABS(delta_amount))으로 사용량 합계 (절대값으로 양수 반환)
3. 기간 필터: DATE(created_at) >= '...' 를 항상 추가 (비용 절감)
4. 사용자별 또는 agent_name별로 그룹화

예시:
SELECT
  user_id,
  SUM(ABS(delta_amount)) AS total_credit_used,
  COUNT(*) AS usage_count,
  MIN(created_at) AS first_usage,
  MAX(created_at) AS last_usage
FROM `liner-219011.cdc_service_db_new_liner.agent_credit_usage_log`
WHERE delta_amount < 0  -- 사용 기록만
  AND DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)  -- 기간 필터
GROUP BY user_id
ORDER BY total_credit_used DESC
LIMIT 100
            '''
        }
    },

    'like.fct_credit_transaction': {
        'full_name': 'liner-219011.like.fct_credit_transaction',
        'description': '크레딧 거래 모델 (발행, 사용, 만료)',
        'row_count_estimate': '~100M+',
        'date_range': '2025-08-05 ~ present',
        'columns': {
            'credit_item_id': {'type': 'INTEGER', 'description': '크레딧 아이템 ID'},
            'user_id': {'type': 'INTEGER', 'description': '사용자 ID'},
            'event_type': {'type': 'STRING', 'description': "'issued', 'spent', 'expired'"},
            'amount': {'type': 'INTEGER', 'description': '크레딧 금액 (양수)'},
            'source_type': {'type': 'STRING', 'description': "'purchase', 'grant', 'subscription' 등"},
            'event_date': {'type': 'DATE', 'description': '발생 날짜'},
        },
        'critical_pattern': {
            'description': '크레딧 발행/사용/만료 집계',
            'method': 'SUM(amount) GROUP BY event_type (issued/spent/expired)'
        }
    },

    'like.fct_subscription_revenue_recognition': {
        'full_name': 'liner-219011.like.fct_subscription_revenue_recognition',
        'description': '인식매출 (발생주의): 구독 기간 일별 분배 + VAT 조정',
        'row_count_estimate': '~50M+',
        'columns': {
            'dt': {'type': 'DATE', 'description': '인식 날짜'},
            'user_id': {'type': 'INTEGER', 'description': '사용자 ID'},
            'revenue_recognition_krw': {'type': 'NUMERIC', 'description': 'KRW 인식매출'},
            'revenue_recognition_usd': {'type': 'NUMERIC', 'description': 'USD 인식매출'},
            'plan_type': {'type': 'STRING', 'description': "'pro', 'max'"},
        },
        'usage': '구독 관련 인식매출 집계'
    },

    'like.fct_subscription_revenue_financial': {
        'full_name': 'liner-219011.like.fct_subscription_revenue_financial',
        'description': '수취매출 (현금주의): 실제 결제 시점의 금액',
        'row_count_estimate': '~5M+',
        'columns': {
            'dt': {'type': 'DATE', 'description': '결제 날짜'},
            'user_id': {'type': 'INTEGER', 'description': '사용자 ID'},
            'revenue_krw': {'type': 'NUMERIC', 'description': 'KRW 수취매출'},
            'revenue_usd': {'type': 'NUMERIC', 'description': 'USD 수취매출'},
            'type': {'type': 'STRING', 'description': "'payment', 'refund', 'dispute', 'reversal' (failure 제외)"},
            'platform': {'type': 'STRING', 'description': "'stripe', 'tosspayments', 'paddle' 등"},
        },
        'critical_pattern': {
            'description': '수취매출 필터링',
            'method': "WHERE type IN ('payment', 'refund', 'dispute', 'reversal') -- failure 제외"
        }
    },

    'like.met_credit_revenue_daily_summary': {
        'full_name': 'liner-219011.like.met_credit_revenue_daily_summary',
        'description': '크레딧 매출 요약: 인식매출(earned+breakage) + 수취매출(booking) + 이연매출',
        'columns': {
            'dt': {'type': 'DATE', 'description': '날짜'},
            'earned_revenue_usd': {'type': 'NUMERIC', 'description': 'USD 실현매출'},
            'earned_revenue_krw': {'type': 'NUMERIC', 'description': 'KRW 실현매출'},
            'breakage_revenue_usd': {'type': 'NUMERIC', 'description': 'USD 만료 매출'},
            'breakage_revenue_krw': {'type': 'NUMERIC', 'description': 'KRW 만료 매출'},
            'booking_amount_usd': {'type': 'NUMERIC', 'description': 'USD 결제액'},
            'booking_amount_krw': {'type': 'NUMERIC', 'description': 'KRW 결제액'},
            'deferred_revenue_usd': {'type': 'NUMERIC', 'description': '미실현 이연매출 (USD)'},
            'deferred_revenue_krw': {'type': 'NUMERIC', 'description': '미실현 이연매출 (KRW)'},
        }
    },

    'like.int_revenue_daily_by_business_model': {
        'full_name': 'liner-219011.like.int_revenue_daily_by_business_model',
        'description': '인식매출 일별 피벗: ads, api, subscription, credit, partnership, contract, b2b_ax, gov_grant',
        'columns': {
            'dt': {'type': 'DATE', 'description': '날짜'},
            'ads': {'type': 'NUMERIC', 'description': 'ads 인식매출'},
            'api': {'type': 'NUMERIC', 'description': 'api 인식매출'},
            'subscription': {'type': 'NUMERIC', 'description': 'subscription 인식매출'},
            'credit': {'type': 'NUMERIC', 'description': 'credit 인식매출'},
            'partnership': {'type': 'NUMERIC', 'description': 'partnership 인식매출'},
            'contract': {'type': 'NUMERIC', 'description': 'contract 인식매출'},
            'b2b_ax': {'type': 'NUMERIC', 'description': 'b2b_ax 인식매출'},
            'gov_grant': {'type': 'NUMERIC', 'description': 'gov_grant 인식매출'},
        }
    },

    'like.int_revenue_daily_by_business_model_received': {
        'full_name': 'liner-219011.like.int_revenue_daily_by_business_model_received',
        'description': '수취매출 일별 피벗: 8가지 business model',
        'columns': {
            'dt': {'type': 'DATE', 'description': '날짜'},
            'ads': {'type': 'NUMERIC', 'description': 'ads 수취매출'},
            'api': {'type': 'NUMERIC', 'description': 'api 수취매출'},
            'subscription': {'type': 'NUMERIC', 'description': 'subscription 수취매출'},
            'credit': {'type': 'NUMERIC', 'description': 'credit 수취매출'},
        }
    },

    'cdc_service_db_new_liner.agent_credit_item': {
        'full_name': 'liner-219011.cdc_service_db_new_liner.agent_credit_item',
        'description': '크레딧 항목: 할당된 크레딧 지갑 기록 (스냅샷)',
        'columns': {
            'id': {'type': 'INTEGER', 'description': 'Primary key'},
            'user_id': {'type': 'INTEGER', 'description': '사용자 ID'},
            'source_type': {'type': 'STRING', 'description': "'purchase', 'subscription', 'grant'"},
            'total_amount': {'type': 'INTEGER', 'description': '할당된 총 크레딧'},
            'remaining_amount': {'type': 'INTEGER', 'description': '남은 크레딧'},
            'created_at': {'type': 'TIMESTAMP', 'description': '할당 시간'},
            'expires_at': {'type': 'TIMESTAMP', 'description': '만료 시간'},
        },
        'note': '현재 상태 스냅샷만 제공 (트랜잭션 로그 아님)'
    },

    'like.met_individual_subscription_arr_ltm_daily': {
        'full_name': 'liner-219011.like.met_individual_subscription_arr_ltm_daily',
        'description': '개인 구독 ARR (LTM - Last 12 Months)',
        'columns': {
            'dt': {'type': 'DATE', 'description': '날짜'},
            'arr_amount': {'type': 'NUMERIC', 'description': 'ARR 금액'},
            'mrr_amount': {'type': 'NUMERIC', 'description': 'MRR 금액'},
            'active_subscription_count': {'type': 'INTEGER', 'description': '활성 구독 수'},
        },
        'usage': 'ARR/MRR 분석'
    },

    'like.met_team_subscription_arr_ltm_daily': {
        'full_name': 'liner-219011.like.met_team_subscription_arr_ltm_daily',
        'description': '팀 구독 ARR (LTM)',
        'usage': '팀 구독 ARR/MRR 분석'
    },

    'langfuse_data.observations': {
        'full_name': 'liner-219011.langfuse_data.observations',
        'description': 'Langfuse 개별 작업 기록 (people_search, API 호출 등)',
        'row_count_estimate': '~10M+',
        'date_range': '2026-04-20 ~ present (metadata)',
        'columns': {
            'id': {'type': 'STRING', 'description': 'Observation ID'},
            'trace_id': {'type': 'STRING', 'description': 'Trace ID (fct_langfuse_traces 조인용)'},
            'name': {'type': 'STRING', 'description': "'handle_people_search' 등"},
            'start_time': {'type': 'TIMESTAMP', 'description': '시작 시간 (UTC)'},
            'metadata': {
                'type': 'JSON',
                'description': '메타데이터',
                'important_keys': {
                    'exa_call_count': 'INT64 — exa API 총 호출 수',
                    'exa_r1_count': 'INT64 — r1 iteration 호출 수',
                    'exa_r2_count': 'INT64 — r2 iteration 호출 수',
                    'raw_card_count': 'INT64 — 필터 전 카드 수',
                    'final_card_count': 'INT64 — 반환된 카드 수',
                }
            },
        },
        'critical_pattern': {
            'description': 'People Search 데이터 추출',
            'method': "WHERE name = 'handle_people_search' AND DATE(start_time) >= '2026-04-20'"
        }
    },

    'langfuse_data.traces': {
        'full_name': 'liner-219011.langfuse_data.traces',
        'description': 'Langfuse 전체 트레이스 (쿼리 전체 플로우)',
        'columns': {
            'id': {'type': 'STRING', 'description': 'Trace ID'},
            'name': {'type': 'STRING', 'description': '트레이스 이름'},
            'start_time': {'type': 'TIMESTAMP', 'description': '시작 시간'},
            'total_cost': {'type': 'NUMERIC', 'description': '총 비용 (USD)'},
            'input_cost': {'type': 'NUMERIC', 'description': 'Input 비용'},
            'output_cost': {'type': 'NUMERIC', 'description': 'Output 비용'},
        },
        'usage': '쿼리당 AI 비용 분석 (trace_id JOIN으로 credit_usage와 연결)'
    },

    'paddle.transaction': {
        'full_name': 'liner-219011.paddle.transaction',
        'description': 'Paddle 결제 플랫폼 트랜잭션 (MOR)',
        'date_range': '2026-03-31 ~ present',
        'columns': {
            'id': {'type': 'STRING', 'description': 'Transaction ID'},
            'status': {'type': 'STRING', 'description': "'completed', 'pending'"},
            'origin': {'type': 'STRING', 'description': "'subscription_update' for plan change, 'subscription' for normal"},
            'total_earnings': {'type': 'INTEGER', 'description': 'VAT-excluded 금액 (센트 단위 USD, 원 단위 KRW)'},
            'total_fee': {'type': 'INTEGER', 'description': 'Paddle 수수료'},
            'currency': {'type': 'STRING', 'description': "'KRW', 'USD'"},
            'created_at': {'type': 'TIMESTAMP', 'description': '생성 시간'},
        },
        'critical_pattern': {
            'description': 'Paddle 금액 처리',
            'method': 'USD: /100.0 (센트→달러), KRW: 그대로 사용 (원 단위)'
        }
    },

    'paddle.adjustment': {
        'full_name': 'liner-219011.paddle.adjustment',
        'description': 'Paddle 환불/차지백 조정 (transaction 외 모든 post-payment 조정)',
        'columns': {
            'id': {'type': 'STRING', 'description': 'Adjustment ID'},
            'action': {'type': 'STRING', 'description': "'refund', 'chargeback', 'credit', 'credit_reverse'"},
            'total_amount': {'type': 'INTEGER', 'description': '절대값 (양수) — int 모델에서 action 기반 부호 결정'},
            'currency': {'type': 'STRING', 'description': "'KRW', 'USD'"},
        },
        'note': '모든 금액이 양수로 저장됨 (action으로 부호 결정)'
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

### 4. agent_credit_usage_log (Credit 사용 로그)
- AI 서비스(Scholar, Write 등) 사용 시 credit 차감 기록
- 약 500K+ 행
- 주요 컬럼: user_id, delta_amount (음수 = 사용), used_at, service
- ★ 중요: delta_amount < 0 으로 필터링해야 사용량만 조회

## 자주 하는 작업

1. 섹터별 리텐션: EVENTS_296805에서 event_type='make_chat'인 첫 이벤트 찾고, 일주일 후 재활동 여부 확인
2. 구독 전환율: EVENTS_296805와 fct_moon_subscription 조인, 이벤트 후 7일 내 구독 여부
3. 이벤트 시퀀스: 사용자별로 event_type을 시간 순서대로 나열하기
4. 텍스트 분석: message_text에서 키워드로 sector 분류
"""
