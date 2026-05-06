"""
NLQAgent - 자연어 데이터 분석 에이전트

메인 에이전트 클래스 (P0: 기본 틀)
P1에서 LLM 연결 및 실행 기능 추가
"""

import asyncio
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class NLQAgent:
    """자연어 → SQL → 데이터 분석 에이전트"""

    def __init__(self, config_path: str = 'config/default.yaml'):
        """
        초기화

        Args:
            config_path: 설정 파일 경로
        """
        self.config_path = config_path
        logger.info(f"NLQAgent initialized with config: {config_path}")

    async def analyze(self, user_query: str) -> Dict:
        """
        자연어 쿼리 분석

        흐름:
        1. 쿼리 해석 (LLM)
        2. SQL 생성 (LLM)
        3. 검증 (dry-run)
        4. 실행 (BigQuery)
        5. 결과 반환

        Args:
            user_query: 사용자의 자연어 쿼리

        Returns:
            {
                'query': str,           # 원본 쿼리
                'sql': str,             # 생성된 SQL
                'data': DataFrame,      # 분석 결과
                'stats': Dict,          # 기본 통계
                'explanation': str      # 쿼리 설명
            }
        """
        logger.info(f"Analyzing query: {user_query}")

        return {
            'query': user_query,
            'status': 'P0_FRAMEWORK_ONLY',
            'message': 'P1에서 LLM 연결 후 실행 가능',
            'next_steps': [
                '1. context_builder로 LLM 프롬프트 구성',
                '2. LLM으로 SQL 생성',
                '3. SQL 검증',
                '4. BigQuery 실행'
            ]
        }

    def _interpret_query(self, query: str) -> Dict:
        """자연어 쿼리 해석 (P1에서 구현)"""
        pass

    def _generate_sql(self, interpretation: Dict) -> str:
        """SQL 생성 (P1에서 구현)"""
        pass

    def _validate_sql(self, sql: str) -> bool:
        """SQL 검증 (P1에서 구현)"""
        pass

    def _execute_query(self, sql: str) -> Dict:
        """BigQuery 실행 (P1에서 구현)"""
        pass


# P0 테스트용 헬퍼
def test_context_loading():
    """BigQuery 컨텍스트 로드 테스트"""
    try:
        from src.bigquery_context import (
            BIGQUERY_SCHEMA,
            SAMPLE_EVENTS,
            SUCCESSFUL_QUERIES,
            ANTIPATTERNS,
            SECTORS
        )

        print("✓ schema_full.py loaded")
        print(f"  - Tables: {list(BIGQUERY_SCHEMA.keys())}")

        print("✓ sample_data.py loaded")
        print(f"  - Sample events: {len(SAMPLE_EVENTS)}")

        print("✓ successful_queries.py loaded")
        print(f"  - Query examples: {len(SUCCESSFUL_QUERIES)}")

        print("✓ antipatterns.py loaded")
        print(f"  - Patterns: {len(ANTIPATTERNS)}")

        print("✓ domain_knowledge.py loaded")
        print(f"  - Sectors: {list(SECTORS.keys())}")

        return True
    except Exception as e:
        logger.error(f"Context loading failed: {e}")
        return False


if __name__ == "__main__":
    print("NLQAgent P0 Framework Test\n")

    if test_context_loading():
        print("\n✓ All BigQuery contexts loaded successfully")
        print("\nP0 Status: Complete ✓")
        print("Next: P1 - LLM 통합 및 SQL 생성")
    else:
        print("\n✗ Context loading failed")
