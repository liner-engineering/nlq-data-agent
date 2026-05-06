"""
설정 모듈 테스트
"""

import pytest

from src.config import AnalysisConfig, BigQueryConfig, Config, LLMConfig
from src.exceptions import ConfigurationError


def test_llm_config_defaults():
    """LLMConfig 기본값"""
    config = LLMConfig()

    assert config.provider == "litellm"
    assert config.model == "gemini-2.5-flash-lite"
    assert config.temperature == 0.2
    assert config.max_tokens == 2000


def test_llm_config_validation():
    """LLMConfig 검증"""
    with pytest.raises(ValueError):
        # temperature가 범위를 벗어남
        LLMConfig(temperature=3.0)

    with pytest.raises(ValueError):
        # max_tokens가 범위를 벗어남
        LLMConfig(max_tokens=10)


def test_bigquery_config_defaults():
    """BigQueryConfig 기본값"""
    config = BigQueryConfig()

    assert config.project == "liner-219011"
    assert config.location == "US"
    assert config.timeout_seconds == 300


def test_analysis_config_defaults():
    """AnalysisConfig 기본값"""
    config = AnalysisConfig()

    assert config.min_sample_size == 10
    assert config.recommended_sample_size == 100
    assert config.statistical_significance_threshold == 0.05


def test_config_creation():
    """Config 통합 생성"""
    config = Config()

    assert isinstance(config.llm, LLMConfig)
    assert isinstance(config.bigquery, BigQueryConfig)
    assert isinstance(config.analysis, AnalysisConfig)


def test_config_to_dict():
    """Config를 사전으로 변환"""
    config = Config()
    data = config.to_dict()

    assert "llm" in data
    assert "bigquery" in data
    assert "analysis" in data
    assert data["llm"]["model"] == "gemini-2.5-flash-lite"
