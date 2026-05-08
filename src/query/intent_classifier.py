"""
질문 의도 분류기 (게이트키퍼)

SQL 생성 전에 사용자 질문을 분류합니다.
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
    """SQL 생성 전 게이트키퍼"""

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

    def classify(self, user_query: str) -> Tuple[QueryIntent, str]:
        """
        질문 분류

        Args:
            user_query: 사용자의 자연어 질문

        Returns:
            (intent, message) — message는 사용자에게 보여줄 친절한 안내
        """
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
            logger.info(f"의도 분류 중...")
            response = self.client.chat.completions.create(
                model=self.config.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=300,
            )

            text = response.choices[0].message.content.strip()
            json_match = re.search(r'\{.*\}', text, re.DOTALL)
            if not json_match:
                logger.warning("분류 응답 파싱 실패, DATA_QUESTION으로 통과")
                return QueryIntent.DATA_QUESTION, ""

            result = json.loads(json_match.group())
            intent_str = result.get("intent", "DATA_QUESTION")
            message = result.get("user_message", "")
            reason = result.get("reason", "")

            try:
                intent = QueryIntent[intent_str]
                logger.info(f"분류 완료: {intent.value} ({reason})")
            except KeyError:
                logger.warning(f"알 수 없는 intent: {intent_str}, DATA_QUESTION으로 통과")
                intent = QueryIntent.DATA_QUESTION

            return intent, message

        except Exception as e:
            logger.error(f"분류 실패: {e}, DATA_QUESTION으로 통과")
            return QueryIntent.DATA_QUESTION, ""
