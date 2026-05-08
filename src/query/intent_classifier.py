"""
질문 의도 분류기 (게이트키퍼)

2단계 분류:
1. 휴리스틱 빠른 분류 - 명백한 케이스 즉시 처리 (무료, 빠름)
2. LLM 분류 - 모호한 케이스만 LLM 호출 (비용 절감)

분류 종류:
- DATA_QUESTION: SQL로 답할 수 있음
- META_QUESTION: 시스템/스키마 설명
- OUT_OF_SCOPE: 무관한 질문
- AMBIGUOUS: 명확화 필요
"""

import json
import re
from enum import Enum
from typing import Tuple

from openai import OpenAI

from src.config import LLMConfig
from src.logging_config import ContextualLogger
from src.types import Result

logger = ContextualLogger(__name__)


class QueryIntent(Enum):
    """질문 의도 분류"""
    DATA_QUESTION = "DATA_QUESTION"
    META_QUESTION = "META_QUESTION"
    OUT_OF_SCOPE = "OUT_OF_SCOPE"
    AMBIGUOUS = "AMBIGUOUS"


class IntentClassifier:
    """SQL 생성 전 게이트키퍼 (휴리스틱 + LLM 2단계)"""

    # 명백한 OUT_OF_SCOPE 패턴
    OUT_OF_SCOPE_PATTERNS = [
        r'^(안녕|hi|hello|hey|반가워)',
        r'^(저는|나는|내가)\s+(누구|뭐)',
        r'^(너는|당신은|claude는?)\s+(누구|뭐)',
        r'(날씨|기온|온도)',
        r'^(고마워|감사|thanks|thank you)',
        r'^(잘가|bye|goodbye)',
        r'(점심|저녁|아침)\s+(뭐|메뉴|추천)',
    ]

    # 명백한 META_QUESTION 패턴
    META_PATTERNS = [
        r'(어떤|무슨|어느)\s+(테이블|데이터|컬럼|필드)',
        r'(스키마|schema|구조)',
        r'(이\s+(시스템|도구|봇|서비스)|너는?\s+뭐|뭘\s+할\s+수)',
        r'(사용법|어떻게\s+써|how\s+to\s+use|도움말|help)',
    ]

    # 데이터 키워드 (있으면 DATA_QUESTION 가능성 ↑)
    DATA_KEYWORDS = [
        '사용자', 'user', 'dau', 'mau', 'wau',
        '구독', 'subscribe', 'subscription', '결제', 'payment',
        '리텐션', 'retention', '재방문', '이탈', 'churn',
        '이벤트', 'event', 'make_chat', 'chat',
        '쿼리', '섹터', 'sector', '카테고리', 'category',
        '전환', 'conversion', '전환율',
        '지난', '최근', '이번', '오늘', '어제', 'day',
        '활성', '비활성', 'active', 'inactive',
        'dau', 'mau', '분석', 'analysis',
    ]

    def __init__(self, config: LLMConfig | None = None) -> None:
        """
        초기화

        Args:
            config: LLMConfig 인스턴스 (기본값: 환경 변수)
        """
        self.config = config or LLMConfig()
        self.client = self._init_llm()

    def _init_llm(self) -> OpenAI:
        """OpenAI 호환 클라이언트 초기화"""
        kwargs = {"api_key": self.config.api_key}
        if self.config.api_base:
            kwargs["base_url"] = self.config.api_base
        return OpenAI(**kwargs)

    def _quick_classify(self, user_query: str) -> QueryIntent | None:
        """
        휴리스틱 빠른 분류

        Returns:
            확실한 분류면 QueryIntent, 모호하면 None (LLM으로 넘김)
        """
        q = user_query.lower().strip()

        # 너무 짧으면 모호
        if len(q) < 5:
            return None

        # 명백한 out-of-scope
        for pattern in self.OUT_OF_SCOPE_PATTERNS:
            if re.search(pattern, q):
                logger.info(f"휴리스틱 분류: OUT_OF_SCOPE ({pattern})")
                return QueryIntent.OUT_OF_SCOPE

        # 명백한 meta
        for pattern in self.META_PATTERNS:
            if re.search(pattern, q):
                logger.info(f"휴리스틱 분류: META_QUESTION ({pattern})")
                return QueryIntent.META_QUESTION

        # 데이터 키워드가 하나도 없으면 모호
        has_data_keyword = any(kw in q for kw in self.DATA_KEYWORDS)
        if not has_data_keyword:
            logger.info("휴리스틱: 데이터 키워드 없음 → LLM 분류로 넘김")
            return None

        # 데이터 키워드 있으면 DATA_QUESTION 가능성 높음
        logger.info(f"휴리스틱 분류: DATA_QUESTION (데이터 키워드 매칭)")
        return QueryIntent.DATA_QUESTION

    def classify(self, user_query: str) -> Tuple[QueryIntent, str]:
        """
        질문 분류 (2단계: 휴리스틱 → LLM)

        Args:
            user_query: 사용자의 자연어 질문

        Returns:
            (intent, message) — message는 사용자에게 보여줄 친절한 안내
        """
        # 단계 1: 휴리스틱 빠른 분류
        quick_intent = self._quick_classify(user_query)
        if quick_intent is not None:
            return quick_intent, ""

        # 단계 2: 모호하면 LLM 분류
        return self._llm_classify(user_query)

    def _llm_classify(self, user_query: str) -> Tuple[QueryIntent, str]:
        """LLM 기반 분류"""
        prompt = f"""당신은 BigQuery 분석 시스템의 게이트키퍼입니다.
사용자 질문이 SQL 데이터 분석으로 답할 수 있는지 판단하세요.

이 시스템이 답할 수 있는 데이터:
- Liner 사용자의 행동 이벤트 (make_chat, view_pricing 등)
- 사용자의 쿼리 텍스트 (이력서, 에세이 등)
- 구독 정보 (활성 구독자, 결제 등)
- 메시지 카테고리 분류

분류 기준:
- DATA_QUESTION: 위 데이터로 답 가능 (예: "지난주 DAU", "구독 전환율")
- META_QUESTION: 시스템·스키마에 대한 질문 (예: "어떤 테이블이 있어?", "이 시스템 뭐야?")
- OUT_OF_SCOPE: 무관함 (예: "안녕", "저는 누구야?", "날씨 어때?")
- AMBIGUOUS: 정보 부족 (예: "그거 보여줘", "전환율?")

사용자 질문: "{user_query}"

JSON으로만 응답:
{{"intent": "DATA_QUESTION|META_QUESTION|OUT_OF_SCOPE|AMBIGUOUS",
  "reason": "한 문장 이유",
  "user_message": "사용자에게 보여줄 친절한 안내 (DATA_QUESTION이면 빈 문자열)"}}
"""

        try:
            logger.info(f"LLM 분류 중 (휴리스틱 실패)...")
            response = self.client.chat.completions.create(
                model=self.config.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=300,
            )

            text = response.choices[0].message.content.strip()
            json_match = re.search(r'\{.*\}', text, re.DOTALL)
            if not json_match:
                logger.warning("LLM 분류 응답 파싱 실패, DATA_QUESTION으로 통과")
                return QueryIntent.DATA_QUESTION, ""

            result = json.loads(json_match.group())
            intent_str = result.get("intent", "DATA_QUESTION")
            message = result.get("user_message", "")
            reason = result.get("reason", "")

            try:
                intent = QueryIntent[intent_str]
                logger.info(f"LLM 분류 완료: {intent.value} ({reason})")
            except KeyError:
                logger.warning(f"알 수 없는 intent: {intent_str}, DATA_QUESTION으로 통과")
                intent = QueryIntent.DATA_QUESTION

            return intent, message

        except Exception as e:
            logger.error(f"LLM 분류 실패: {e}, DATA_QUESTION으로 통과")
            return QueryIntent.DATA_QUESTION, ""
