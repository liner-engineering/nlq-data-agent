"""
NLQ Agent - 자연어 데이터 분석 에이전트

사용자의 자연어 쿼리를 해석하여 BigQuery SQL을 생성하고 실행합니다.
전체 분석 파이프라인을 조율합니다.
"""

from typing import Any

from src.config import Config, load_config
from src.executor.bigquery_client import BigQueryExecutor
from src.executor.data_processor import DataProcessor
from src.exceptions import NLQAgentException, SQLGenerationError
from src.logging_config import ContextualLogger, PerformanceLogger
from src.query.context_builder import ContextBuilder
from src.query.generator import SQLGenerator
from src.query.validator import SQLValidator
from src.types import AnalysisResult, SQL

logger = ContextualLogger(__name__)
perf = PerformanceLogger(logger)


class NLQAgent:
    """자연어 데이터 분석 에이전트

    사용자의 자연어 쿼리를 BigQuery SQL로 변환하고 분석합니다.

    파이프라인:
    1. SQL 생성 (LLM)
    2. SQL 검증
    3. BigQuery 실행
    4. 결과 처리 및 통계

    Example:
        agent = NLQAgent()
        result = agent.analyze("섹터별 D+7 리텐션")
        if result.success:
            print(result.data.head())
    """

    def __init__(self, config: Config | None = None) -> None:
        """
        초기화

        Args:
            config: Config 인스턴스 (기본값: 환경에서 로드)
        """
        self.config = config or load_config()
        logger.info("NLQAgent 초기화")

        # 모듈 초기화
        self.validator = SQLValidator()
        self.context_builder = ContextBuilder()
        self.generator = SQLGenerator(self.config.llm)
        self.bq_executor = BigQueryExecutor(self.config.bigquery)
        self.data_processor = DataProcessor(self.config.analysis)

    def analyze(self, user_query: str) -> AnalysisResult:
        """
        자연어 쿼리 분석 및 실행

        Args:
            user_query: 사용자의 자연어 쿼리

        Returns:
            AnalysisResult: 분석 결과

        Raises:
            NLQAgentException: 분석 중 에러
        """
        logger.set_context(user_query=user_query[:100])

        try:
            # 1. SQL 생성
            logger.info("Step 1: SQL 생성 중")
            with perf.timer("generate_sql"):
                sql_result = self.generator.generate_with_validation(
                    user_query, self.validator
                )

            if not sql_result.is_success():
                raise SQLGenerationError(
                    sql_result.error or "SQL 생성 실패", user_query=user_query
                )

            sql = sql_result.data
            logger.info(f"SQL 생성 완료: {len(sql)} chars")

            # 2. BigQuery 실행
            logger.info("Step 2: BigQuery 실행 중")
            with perf.timer("execute_query"):
                exec_result = self.bq_executor.execute(
                    sql, max_results=self.config.bigquery.max_results
                )

            if not exec_result.is_success():
                raise NLQAgentException(
                    exec_result.error or "BigQuery 실행 실패"
                )

            df = exec_result.data
            logger.info(f"쿼리 완료: {len(df)} rows")

            # 3. 데이터 처리
            logger.info("Step 3: 데이터 처리 중")
            with perf.timer("process_data"):
                proc_result = self.data_processor.process(df)

            if not proc_result.is_success():
                raise NLQAgentException(
                    proc_result.error or "데이터 처리 실패"
                )

            proc_data = proc_result.data

            logger.info("분석 완료")

            # 결과 조합
            return AnalysisResult(
                query=user_query,
                sql=sql,
                data=proc_data["df_cleaned"],
                stats=proc_data["stats"],
                explanation=proc_data["explanation"],
                success=True,
                data_quality=proc_data["data_quality"],
                sample_warning=proc_data["sample_warning"],
            )

        except NLQAgentException as e:
            logger.error(f"분석 실패: {e}")
            return AnalysisResult(
                query=user_query,
                sql="",
                data=None,
                stats={},
                explanation="",
                success=False,
                error=str(e),
            )

        except Exception as e:
            logger.exception(f"예상치 못한 오류: {str(e)}")
            return AnalysisResult(
                query=user_query,
                sql="",
                data=None,
                stats={},
                explanation="",
                success=False,
                error=f"예상치 못한 오류: {str(e)}",
            )


# CLI 진입점
def main() -> None:
    """CLI 인터페이스"""
    import sys

    if len(sys.argv) < 2:
        print("사용법: python -m src.agent '<쿼리>'")
        sys.exit(1)

    query = sys.argv[1]
    agent = NLQAgent()
    result = agent.analyze(query)

    if result.success:
        print(f"SQL:\n{result.sql}\n")
        print(f"결과 ({len(result.data)} rows):")
        print(result.data.head())
        print(f"\n{result.explanation}")
    else:
        print(f"오류: {result.error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
