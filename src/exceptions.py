"""
커스텀 예외 정의

NLQ Agent의 모든 예외는 이 모듈에서 정의되며,
일관된 에러 응답 구조를 제공합니다.
"""

from typing import Any, Optional


class NLQAgentException(Exception):
    """NLQ Agent의 기본 예외 클래스"""

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """
        초기화

        Args:
            message: 에러 메시지
            error_code: 에러 코드 (예: 'SQL_GENERATION_FAILED')
            details: 추가 컨텍스트 정보
        """
        self.message = message
        self.error_code = error_code or self.__class__.__name__
        self.details = details or {}
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        """에러를 사전으로 변환"""
        return {
            "error_code": self.error_code,
            "message": self.message,
            "details": self.details,
        }

    def __str__(self) -> str:
        return f"[{self.error_code}] {self.message}"


class ConfigurationError(NLQAgentException):
    """설정 오류"""

    def __init__(self, message: str, details: dict[str, Any] | None = None) -> None:
        super().__init__(message, "CONFIGURATION_ERROR", details)


class SQLGenerationError(NLQAgentException):
    """SQL 생성 실패"""

    def __init__(
        self,
        message: str,
        user_query: str | None = None,
        llm_error: str | None = None,
    ) -> None:
        details = {}
        if user_query:
            details["user_query"] = user_query
        if llm_error:
            details["llm_error"] = llm_error
        super().__init__(message, "SQL_GENERATION_FAILED", details)


class SQLValidationError(NLQAgentException):
    """SQL 검증 실패"""

    def __init__(
        self,
        message: str,
        sql: str | None = None,
        errors: list[str] | None = None,
        warnings: list[str] | None = None,
    ) -> None:
        details = {}
        if sql:
            details["sql"] = sql[:200]  # 처음 200자만
        if errors:
            details["errors"] = errors
        if warnings:
            details["warnings"] = warnings
        super().__init__(message, "SQL_VALIDATION_FAILED", details)


class BigQueryExecutionError(NLQAgentException):
    """BigQuery 실행 실패"""

    def __init__(
        self,
        message: str,
        sql: str | None = None,
        execution_time_ms: float | None = None,
    ) -> None:
        details = {}
        if sql:
            details["sql"] = sql[:200]
        if execution_time_ms is not None:
            details["execution_time_ms"] = execution_time_ms
        super().__init__(message, "BIGQUERY_EXECUTION_FAILED", details)


class DataProcessingError(NLQAgentException):
    """데이터 처리 실패"""

    def __init__(
        self,
        message: str,
        data_shape: tuple[int, int] | None = None,
        processing_step: str | None = None,
    ) -> None:
        details = {}
        if data_shape:
            details["data_shape"] = data_shape
        if processing_step:
            details["processing_step"] = processing_step
        super().__init__(message, "DATA_PROCESSING_FAILED", details)


class ContextBuildingError(NLQAgentException):
    """LLM 컨텍스트 구성 실패"""

    def __init__(self, message: str, component: str | None = None) -> None:
        details = {}
        if component:
            details["component"] = component
        super().__init__(message, "CONTEXT_BUILDING_FAILED", details)
