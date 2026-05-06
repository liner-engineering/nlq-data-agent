"""
설정 관리

Pydantic 기반 설정으로 타입 검증 및 환경 변수 로드를 자동화합니다.
"""

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings

from src.exceptions import ConfigurationError


class LLMConfig(BaseModel):
    """LLM 설정"""

    provider: str = Field(default="litellm", description="LLM 프로바이더")
    model: str = Field(default="gemini-2.5-flash-lite-ai-studio", description="모델명")
    temperature: float = Field(
        default=0.2,
        ge=0.0,
        le=2.0,
        description="생성 온도 (낮을수록 일관성 높음)",
    )
    max_tokens: int = Field(
        default=2000,
        ge=100,
        le=4000,
        description="최대 생성 토큰 수",
    )
    api_key_env: str = Field(default="LITELLM_API_KEY", description="API 키 환경 변수명")
    base_url_env: str = Field(default="LITELLM_BASE_URL", description="API Base URL 환경 변수명")

    @property
    def api_key(self) -> str:
        """API 키 반환"""
        key = os.getenv(self.api_key_env)
        if not key:
            raise ConfigurationError(
                f"API 키를 찾을 수 없습니다. 환경 변수 '{self.api_key_env}'을 설정하세요."
            )
        return key

    @property
    def api_base(self) -> str | None:
        """API Base URL 반환 (선택사항)"""
        return os.getenv(self.base_url_env)


class BigQueryConfig(BaseModel):
    """BigQuery 설정"""

    project: str = Field(default="liner-219011", description="GCP 프로젝트 ID")
    location: str = Field(default="US", description="BigQuery 위치")
    timeout_seconds: int = Field(
        default=300,
        ge=10,
        le=3600,
        description="쿼리 타임아웃 (초)",
    )
    max_results: int = Field(
        default=10000,
        ge=1,
        le=100000,
        description="최대 결과 행 수",
    )
    maximum_bytes_billed: int = Field(
        default=10 * 1024 * 1024 * 1024,
        ge=1,
        description="최대 청구 바이트",
    )


class AnalysisConfig(BaseModel):
    """분석 설정"""

    min_sample_size: int = Field(
        default=10,
        ge=1,
        description="최소 샘플 크기",
    )
    recommended_sample_size: int = Field(
        default=100,
        ge=1,
        description="권장 샘플 크기",
    )
    statistical_significance_threshold: float = Field(
        default=0.05,
        ge=0.0,
        le=1.0,
        description="통계 유의성 임계값",
    )


class LoggingConfig(BaseModel):
    """로깅 설정"""

    level: str = Field(default="INFO", description="로그 레벨")
    format: str = Field(
        default="json",
        description="로그 포맷 (json 또는 text)",
    )
    file_path: str | None = Field(default=None, description="로그 파일 경로")

    @field_validator("level")
    @classmethod
    def validate_level(cls, v: str) -> str:
        """로그 레벨 검증"""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid_levels:
            raise ValueError(f"유효하지 않은 로그 레벨: {v}")
        return v.upper()


class Config(BaseModel):
    """통합 설정"""

    llm: LLMConfig = Field(default_factory=LLMConfig)
    bigquery: BigQueryConfig = Field(default_factory=BigQueryConfig)
    analysis: AnalysisConfig = Field(default_factory=AnalysisConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Config":
        """YAML 파일에서 설정 로드

        Args:
            path: YAML 파일 경로

        Returns:
            Config 인스턴스

        Raises:
            ConfigurationError: 파일을 찾을 수 없거나 검증 실패
        """
        try:
            path = Path(path)
            if not path.exists():
                raise ConfigurationError(f"설정 파일을 찾을 수 없습니다: {path}")

            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}

            return cls(**data)
        except Exception as e:
            raise ConfigurationError(f"설정 로드 실패: {str(e)}")

    @classmethod
    def from_env(cls) -> "Config":
        """환경 변수에서 설정 로드

        환경 변수 우선순위:
        - NLQ_CONFIG: YAML 파일 경로
        - 개별 변수: NLQ_LLM_TEMPERATURE 등
        """
        config_file = os.getenv("NLQ_CONFIG")
        if config_file:
            return cls.from_yaml(config_file)

        # 기본 설정으로 시작
        return cls()

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Config":
        """사전에서 설정 로드"""
        return cls(**data)

    def to_dict(self) -> dict[str, Any]:
        """설정을 사전으로 변환"""
        return self.model_dump()


def load_config(config_path: str | Path | None = None) -> Config:
    """설정 로드 (우선순위 순서)

    1. 전달된 경로
    2. 환경 변수 (NLQ_CONFIG)
    3. 기본 경로 (config/default.yaml)
    4. 기본값

    Args:
        config_path: 설정 파일 경로 (선택사항)

    Returns:
        로드된 Config 인스턴스
    """
    # 1. 전달된 경로
    if config_path:
        try:
            return Config.from_yaml(config_path)
        except ConfigurationError:
            raise

    # 2. 환경 변수
    env_config = os.getenv("NLQ_CONFIG")
    if env_config:
        try:
            return Config.from_yaml(env_config)
        except ConfigurationError:
            pass  # 다음 단계로

    # 3. 기본 경로
    default_path = Path(__file__).parent.parent / "config" / "default.yaml"
    if default_path.exists():
        try:
            return Config.from_yaml(default_path)
        except ConfigurationError:
            pass

    # 4. 기본값
    return Config()
