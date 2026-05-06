"""
타입 정의

전체 프로젝트에서 사용되는 타입 별칭, 프로토콜, 제네릭 타입을 정의합니다.
"""

from typing import Any, Generic, Protocol, TypeAlias, TypeVar

import pandas as pd

# 타입 별칭
SQL: TypeAlias = str
DataRow: TypeAlias = dict[str, Any]
Stats: TypeAlias = dict[str, dict[str, float]]

# 제네릭 타입 변수
T = TypeVar("T")
E = TypeVar("E", bound=Exception)


# 결과 타입
class Result(Generic[T]):
    """작업 결과를 나타내는 제네릭 클래스

    성공 또는 실패 상태와 함께 데이터 또는 에러를 포함합니다.

    Example:
        result: Result[pd.DataFrame] = Result.success(df)
        if result.is_success():
            df = result.data
        else:
            error = result.error
    """

    def __init__(self, success: bool, data: T | None = None, error: str | None = None):
        self.success = success
        self.data = data
        self.error = error

    @classmethod
    def success(cls, data: T) -> "Result[T]":
        """성공 결과 생성"""
        return cls(success=True, data=data)

    @classmethod
    def failure(cls, error: str) -> "Result[T]":
        """실패 결과 생성"""
        return cls(success=False, error=error)

    def is_success(self) -> bool:
        """성공 여부"""
        return self.success

    def is_failure(self) -> bool:
        """실패 여부"""
        return not self.success

    def to_dict(self) -> dict[str, Any]:
        """사전으로 변환"""
        return {
            "success": self.success,
            "data": self.data,
            "error": self.error,
        }


# 프로토콜 (인터페이스)
class SQLValidator(Protocol):
    """SQL 검증기 인터페이스"""

    def validate(self, sql: SQL) -> dict[str, Any]:
        """
        SQL 검증

        Args:
            sql: 검증할 SQL

        Returns:
            검증 결과 (valid, errors, warnings 포함)
        """
        ...


class ContextBuilder(Protocol):
    """컨텍스트 빌더 인터페이스"""

    def build_prompt(self, user_query: str) -> str:
        """
        사용자 쿼리로부터 LLM 프롬프트 생성

        Args:
            user_query: 사용자의 자연어 쿼리

        Returns:
            LLM에 전달할 프롬프트
        """
        ...


class SQLGenerator(Protocol):
    """SQL 생성기 인터페이스"""

    def generate(self, user_query: str) -> Result[SQL]:
        """
        사용자 쿼리로부터 SQL 생성

        Args:
            user_query: 사용자의 자연어 쿼리

        Returns:
            생성된 SQL 또는 에러
        """
        ...


class BigQueryClient(Protocol):
    """BigQuery 클라이언트 인터페이스"""

    def execute_query(self, sql: SQL, max_results: int = 10000) -> Result[pd.DataFrame]:
        """
        BigQuery에서 SQL 실행

        Args:
            sql: 실행할 SQL
            max_results: 최대 결과 행 수

        Returns:
            쿼리 결과 또는 에러
        """
        ...


class DataProcessor(Protocol):
    """데이터 처리기 인터페이스"""

    def process(self, df: pd.DataFrame) -> dict[str, Any]:
        """
        데이터 처리 및 통계 생성

        Args:
            df: 처리할 데이터프레임

        Returns:
            정리된 데이터와 통계를 포함하는 사전
        """
        ...


# 분석 결과 타입
class AnalysisResult:
    """분석 결과

    전체 분석 파이프라인의 최종 결과를 표현합니다.

    Attributes:
        query: 사용자의 원본 쿼리
        sql: 생성된 SQL
        data: 분석 결과 데이터
        stats: 기본 통계
        explanation: 결과 해석
        success: 분석 성공 여부
        error: 오류 메시지 (실패 시)
    """

    def __init__(
        self,
        query: str,
        sql: str,
        data: pd.DataFrame,
        stats: Stats,
        explanation: str,
        success: bool = True,
        error: str | None = None,
        **kwargs: Any,
    ):
        self.query = query
        self.sql = sql
        self.data = data
        self.stats = stats
        self.explanation = explanation
        self.success = success
        self.error = error
        self.extra = kwargs

    def to_dict(self) -> dict[str, Any]:
        """사전으로 변환"""
        return {
            "query": self.query,
            "sql": self.sql,
            "data": self.data,
            "stats": self.stats,
            "explanation": self.explanation,
            "success": self.success,
            "error": self.error,
            **self.extra,
        }

    def to_json_serializable(self) -> dict[str, Any]:
        """JSON 직렬화 가능한 형식으로 변환"""
        return {
            "query": self.query,
            "sql": self.sql,
            "data_shape": self.data.shape if isinstance(self.data, pd.DataFrame) else None,
            "data_head": self.data.head().to_dict("records") if isinstance(self.data, pd.DataFrame) else None,
            "stats": self.stats,
            "explanation": self.explanation,
            "success": self.success,
            "error": self.error,
        }


# 검증 결과 타입
class ValidationResult:
    """검증 결과

    SQL 또는 기타 검증의 결과를 표현합니다.

    Attributes:
        valid: 검증 성공 여부
        errors: 오류 목록
        warnings: 경고 목록
        suggestions: 제안 목록
    """

    def __init__(
        self,
        valid: bool,
        errors: list[str] | None = None,
        warnings: list[str] | None = None,
        suggestions: list[str] | None = None,
    ):
        self.valid = valid
        self.errors = errors or []
        self.warnings = warnings or []
        self.suggestions = suggestions or []

    def to_dict(self) -> dict[str, Any]:
        """사전으로 변환"""
        return {
            "valid": self.valid,
            "errors": self.errors,
            "warnings": self.warnings,
            "suggestions": self.suggestions,
        }

    def has_errors(self) -> bool:
        """오류 존재 여부"""
        return len(self.errors) > 0

    def has_warnings(self) -> bool:
        """경고 존재 여부"""
        return len(self.warnings) > 0
