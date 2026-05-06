"""
Liner의 비즈니스 도메인 지식

LLM이 쿼리 작성 시 참고할 수 있는 비즈니스 컨텍스트
"""

# 섹터 정의 (쿼리 텍스트 기반)
SECTORS = {
    'professional': {
        'keywords': [
            '이력서', 'resume', 'cv', '자소서', '자기소개서',
            '커버레터', 'cover letter', '포트폴리오', 'portfolio'
        ],
        'description': '채용/취업 관련 쿼리',
        'typical_queries': [
            '이력서 첨삭 부탁합니다',
            'CV 작성법',
            '자기소개서 어떻게 써?'
        ]
    },

    'educational': {
        'keywords': [
            'essay', '에세이', '논문', 'paper', '과제',
            'homework', '숙제', 'assignment', '리포트',
            'research', '연구', '대학원', '학위'
        ],
        'description': '교육/학업 관련 쿼리',
        'typical_queries': [
            '에세이 작성법',
            '논문 개요 어떻게?',
            '숙제 도와줄 수 있나?'
        ]
    },

    'content': {
        'keywords': [
            '블로그', 'blog', 'sns', '소셜미디어', '글쓰기',
            '포스트', 'post', '콘텐츠', 'content', '인스타그램',
            '유튜브', 'youtube'
        ],
        'description': '콘텐츠 작성/글쓰기',
        'typical_queries': [
            '블로그 글쓰기 팁',
            'SNS 포스트 작성',
            '인스타그램 캡션'
        ]
    },

    'business': {
        'keywords': [
            '사업계획서', 'business plan', '기획서',
            '전략', 'strategy', 'proposal', '계약서',
            '보고서', 'report'
        ],
        'description': '비즈니스 문서 작성',
        'typical_queries': [
            '사업계획서 작성',
            '제안서 어떻게?',
            '마케팅 전략'
        ]
    }
}

# 이벤트 타입의 의미
EVENT_TYPE_MEANINGS = {
    'make_chat': {
        'description': '사용자가 쿼리/질문을 작성',
        'importance': 'CRITICAL - 가장 중요한 engagement 신호',
        'interpretation': '실제 사용자의 니즈 표현'
    },

    'view_contents_pricing_page': {
        'description': '가격 페이지 조회',
        'importance': 'HIGH - 구독 고려',
        'interpretation': '서비스 가치를 평가 중'
    },

    'view_subscription_wall': {
        'description': '구독 벽 조회',
        'importance': 'VERY HIGH - 구독 거의 확정',
        'interpretation': '구독 결정 일보 직전'
    },

    'subscription_request_result_exposed': {
        'description': '구독 결과 노출',
        'importance': 'HIGH - 구독 완료',
        'interpretation': '실제 구독 액션 후'
    },

    'click_component_settings_page': {
        'description': '설정 페이지 클릭',
        'importance': 'MEDIUM - 설정 변경 의도',
        'interpretation': '기능 탐색'
    },

    'view_defensive_confirm_cancellation_modal': {
        'description': '취소 모달 조회',
        'importance': 'CRITICAL - 이탈 신호',
        'interpretation': '구독 취소를 고려 중'
    }
}

# 핵심 지표 정의
KEY_METRICS = {
    'retention': {
        'definition': 'D+N 리텐션 (N일 후에도 활동이 있는가)',
        'calculation': 'D+7 = 첫 이벤트 후 7-13일 사이에 재활동',
        'why_important': '사용자 충성도의 지표, 장기 생존 가능성',
        'typical_targets': [7, 14, 30],
        'good_rate': '40% 이상'
    },

    'conversion_rate': {
        'definition': '구독 전환율 (첫 쿼리 후 구독하는가)',
        'calculation': '구독자 수 / 첫 쿼리 작성자 수',
        'why_important': '서비스의 수익성을 결정',
        'typical_range': '10-20%',
        'good_rate': '15% 이상'
    },

    'event_frequency': {
        'definition': '평균 이벤트 수 (사용자당)',
        'calculation': '전체 이벤트 / 총 사용자 수',
        'why_important': '활동도 지표, 높을수록 좋음',
        'typical_range': '3-8개/사용자'
    },

    'dau': {
        'definition': 'Daily Active Users (일일 활성 사용자)',
        'calculation': '날짜별 고유 사용자 수',
        'why_important': '일일 건강도 지표'
    },

    'journey_depth': {
        'definition': '사용자 여정의 깊이 (이벤트 다양성)',
        'calculation': '사용자당 고유 event_type 수',
        'why_important': '서비스 전반 사용 수준',
        'interpretation': '높을수록 깊이 있게 사용'
    }
}

# 사용자 세그먼트 정의
USER_SEGMENTS = {
    'subscriber': {
        'definition': '현재 활성 구독자',
        'criteria': 'end_date IS NULL',
        'typical_churn_rate': '5-10% per month'
    },

    'churned': {
        'definition': '구독을 취소한 사용자',
        'criteria': 'end_date IS NOT NULL',
        'analysis_goal': '이탈 원인 파악'
    },

    'heavy_user': {
        'definition': '활동도 상위 10%',
        'criteria': 'event_count >= 90th percentile',
        'typical_retention': '60-70%'
    },

    'casual_user': {
        'definition': '가벼운 사용자',
        'criteria': 'event_count <= 25th percentile',
        'typical_retention': '10-20%'
    },

    'trial': {
        'definition': '트라이얼 사용자 (아직 구독 안 함)',
        'criteria': 'no subscription record',
        'conversion_goal': '얼마나 구독하는가'
    }
}

# 분석 프레임워크
ANALYSIS_FRAMEWORKS = {
    'retention_driver': {
        'question': '어떤 섹터의 리텐션이 높을까?',
        'steps': [
            '1. 첫 make_chat 이벤트 사용자 추출',
            '2. 쿼리 텍스트로 섹터 분류',
            '3. D+7 리텐션 계산',
            '4. 섹터별 비교'
        ],
        'output': '섹터별 리텐션율 테이블'
    },

    'conversion_driver': {
        'question': '어떤 섹터가 가장 구독 전환율이 높을까?',
        'steps': [
            '1. make_chat 사용자 (분모)',
            '2. 구독 사용자 (분자)',
            '3. 쿼리 텍스트로 섹터 분류',
            '4. 섹터별 전환율 계산'
        ],
        'output': '섹터별 전환율 비교'
    },

    'event_combo': {
        'question': '어떤 이벤트 조합이 구독 전환율이 높을까?',
        'steps': [
            '1. 사용자별 이벤트 시퀀스 추출',
            '2. 상위 이벤트 조합만 필터링',
            '3. 각 조합별 구독율 계산',
            '4. 전환율 높은 조합 찾기'
        ],
        'output': '이벤트 경로별 전환율'
    },

    'journey_mapping': {
        'question': '사용자들의 패턴 및 저니맵은 어떨지?',
        'steps': [
            '1. 사용자별 이벤트 시퀀스 추출',
            '2. 상위 N개 패턴 집계',
            '3. 각 패턴의 비율 계산',
            '4. 시각화'
        ],
        'output': '사용자 여정 맵'
    },

    'segment_analysis': {
        'question': '구독자/헤비유저의 주요 패턴은?',
        'steps': [
            '1. 사용자를 세그먼트로 분류',
            '2. 세그먼트별 이벤트 집계',
            '3. 주요 행동 패턴 추출',
            '4. 세그먼트 간 비교'
        ],
        'output': '세그먼트별 활동 프로필'
    }
}

# 암묵지 (경험 기반 판단)
IMPLICIT_KNOWLEDGE = {
    'make_chat_importance': {
        'rule': 'make_chat은 가장 중요한 신호. 다른 모든 이벤트는 그 다음 고려',
        'why': '실제 사용자의 니즈를 표현하는 유일한 신호'
    },

    'time_range_critical': {
        'rule': '시간 범위를 엄밀하게. 1주일과 1달은 다른 결과',
        'why': 'trend가 변할 수 있음. 계절성/이벤트 영향'
    },

    'sample_size_threshold': {
        'rule': '샘플 크기 100 이상 권장 (통계적 유의성)',
        'why': 'n < 100이면 잡음이 신호를 압도'
    },

    'sector_hybrid': {
        'rule': '섹터는 단일이 아닐 수 있음. 한 사용자가 여러 섹터 사용',
        'why': '첫 쿼리 섹터만 분석하거나, 세션별로 분리 필요'
    },

    'causality_warning': {
        'rule': '상관관계는 인과관계가 아님. 구독자의 높은 활동도 ≠ 활동도가 구독 유발',
        'why': 'reverse causality: 구독자라서 더 활동할 수도 있음'
    }
}

# 데이터 품질 체크
DATA_QUALITY_CHECKS = {
    'null_handling': {
        'issue': 'user_id가 NULL일 수 있음',
        'solution': 'user_id와 amplitude_id 함께 사용'
    },

    'time_zone': {
        'issue': 'event_time이 UTC (한국 시간 +9시간)',
        'solution': 'UTC 기준으로 쿼리 작성'
    },

    'duplicate_events': {
        'issue': '중복 이벤트가 있을 수 있음',
        'solution': 'DISTINCT 사용'
    },

    'sample_size_bias': {
        'issue': '작은 샘플은 noise가 큼',
        'solution': '최소 100개 샘플 확보 후 분석'
    }
}
