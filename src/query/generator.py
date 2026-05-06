"""
LLM 기반 SQL 생성기

context_builder의 프롬프트를 사용하여 LLM으로 SQL을 생성합니다.
재시도 로직과 응답 파싱을 포함한 프로덕션 레디 구현입니다.
"""

import os
import re
import time
from typing import Any

from src.config import LLMConfig
from src.exceptions import SQLGenerationError
from src.logging_config import ContextualLogger
from src.query.context_builder import ContextBuilder
from src.types import Result, SQL, ValidationResult

logger = ContextualLogger(__name__)


class SQLGenerator:
    """LLM 기반 SQL 생성기

    자연어 쿼리로부터 BigQuery SQL을 생성합니다.
    재시도 로직과 검증을 포함합니다.

    Example:
        gen = SQLGenerator()
        result = gen.generate("섹터별 리텐션")
        if result.is_success():
            sql = result.data
    """

    # SQL 추출 패턴 (순서대로 시도)
    _SQL_PATTERNS = [
        re.compile(r"```(?:sql)?\s*\n(.*?)\n```", re.DOTALL | re.IGNORECASE),
        re.compile(r"```(.*?)```", re.DOTALL),
    ]

    def __init__(self, config: LLMConfig | None = None) -> None:
        """
        초기화

        Args:
            config: LLMConfig 인스턴스 (기본값: 환경 변수)
        """
        self.config = config or LLMConfig()
        self.context_builder = ContextBuilder()
        self._llm_module: Any = None
        self._init_llm()

    def _init_llm(self) -> None:
        """LLM 라이브러리 초기화

        Raises:
            ImportError: litellm이 설치되지 않은 경우
        """
        try:
            import litellm

            self._llm_module = litellm
            logger.info("LiteLLM 초기화 완료")
        except ImportError as e:
            error_msg = "litellm이 설치되지 않았습니다. 설치: rye add litellm"
            logger.error(error_msg)
            raise ImportError(error_msg) from e

    def generate(
        self, user_query: str, max_retries: int = 3
    ) -> Result[SQL]:
        """
        자연어 쿼리로부터 SQL 생성

        Args:
            user_query: 사용자의 자연어 쿼리
            max_retries: 최대 재시도 횟수 (exponential backoff 사용)

        Returns:
            생성된 SQL 또는 에러
        """
        # LLM 모듈 확인
        if not self._llm_module:
            error_msg = (
                "LLM 초기화 실패. litellm 라이브러리를 설치하세요: rye add litellm"
            )
            logger.error(error_msg)
            return Result.failure(error_msg)

        logger.set_context(user_query=user_query[:100], max_retries=max_retries)

        prompt = self.context_builder.build_prompt(user_query)
        system_prompt = self.context_builder.get_system_prompt()

        for attempt in range(max_retries):
            try:
                # Exponential backoff
                if attempt > 0:
                    wait_time = 2 ** (attempt - 1)
                    logger.warning(
                        f"재시도 대기 중 ({attempt}/{max_retries})",
                        duration_ms=wait_time * 1000,
                    )
                    time.sleep(wait_time)

                # LLM 호출
                response = self._llm_module.completion(
                    model=self.config.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": prompt},
                    ],
                    temperature=self.config.temperature,
                    max_tokens=self.config.max_tokens,
                    api_key=self.config.api_key,
                )

                sql_text = response.choices[0].message.content
                sql = self._extract_sql(sql_text)

                if not sql:
                    logger.warning(
                        f"SQL 추출 실패 (시도 {attempt + 1}/{max_retries})"
                    )
                    continue

                logger.info(f"SQL 생성 완료 (시도 {attempt + 1})")
                return Result.success(sql)

            except Exception as e:
                logger.error(
                    f"LLM 호출 실패: {str(e)}", duration_ms=(attempt + 1) * 1000
                )
                if attempt == max_retries - 1:
                    return Result.failure(f"LLM 생성 실패: {str(e)}")

        return Result.failure("최대 재시도 횟수 초과")

    def _extract_sql(self, text: str) -> SQL | None:
        """
        LLM 응답에서 SQL 추출

        마크다운 코드블록 또는 평문 SQL을 추출합니다.

        Args:
            text: LLM 응답 텍스트

        Returns:
            추출된 SQL 또는 None
        """
        if not text:
            return None

        text = text.strip()

        # 1. 마크다운 코드블록
        for pattern in self._SQL_PATTERNS:
            matches = pattern.findall(text)
            if matches:
                return matches[0].strip()

        # 2. SELECT로 시작하는 평문
        if text.upper().startswith("SELECT"):
            return text

        # 3. SELECT가 포함된 경우 그 부분부터
        select_idx = text.upper().find("SELECT")
        if select_idx != -1:
            return text[select_idx:].strip()

        return None

    def generate_with_validation(
        self,
        user_query: str,
        validator,
        max_generation_attempts: int = 3,
        max_validation_attempts: int = 2,
    ) -> Result[SQL]:
        """
        검증과 함께 SQL 생성

        SQL을 생성하고 검증합니다. 검증 실패 시 피드백과 함께 재생성합니다.

        Args:
            user_query: 사용자 쿼리
            validator: SQLValidator 인스턴스
            max_generation_attempts: 생성 재시도 횟수
            max_validation_attempts: 검증 실패 시 재시도 횟수

        Returns:
            검증된 SQL 또는 에러
        """
        logger.set_context(user_query=user_query[:100])

        for validation_attempt in range(max_validation_attempts):
            # SQL 생성
            gen_result = self.generate(user_query, max_retries=max_generation_attempts)

            if not gen_result.is_success():
                return gen_result

            sql = gen_result.data

            # SQL 검증
            try:
                validation_result = validator.validate(sql)

                if validation_result.valid:
                    logger.info("SQL 검증 통과")
                    return Result.success(sql)

                # 검증 실패 시 피드백으로 재생성
                if validation_attempt < max_validation_attempts - 1:
                    errors_text = "; ".join(validation_result.errors)
                    feedback = f"이전 SQL 오류: {errors_text}"
                    logger.warning(f"검증 실패 (시도 {validation_attempt + 1})")

                    # 피드백과 함께 재시도
                    feedback_query = f"{user_query}\n\n주의: {feedback}"
                    user_query = feedback_query
                    continue

                # 최종 재시도도 실패
                error_text = "; ".join(validation_result.errors)
                return Result.failure(f"검증 실패: {error_text}")

            except Exception as e:
                logger.exception(f"검증 중 오류: {str(e)}")
                return Result.failure(f"검증 중 오류: {str(e)}")

        return Result.failure("최대 재시도 횟수 초과")
