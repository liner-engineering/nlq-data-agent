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
from src.logging_config import ContextualLogger, PerformanceLogger, setup_logging
from src.query.context_builder import ContextBuilder
from src.query.explanation_generator import ExplanationFormatter, ExplanationGenerator
from src.query.generator import SQLGenerator
from src.query.intent_classifier import IntentClassifier, QueryIntent
from src.query.sql_analyzer import SQLAnalyzer
from src.query.validator import SQLValidator
from src.types import AnalysisResult, SQL

# 비용 임계값 (바이트)
COST_THRESHOLDS = {
    "auto_execute": 10 * (1024 ** 3),  # 10GB
    "warning": 100 * (1024 ** 3),  # 100GB
    "alert": 1024 ** 4,  # 1TB
}

logger = ContextualLogger(__name__)
perf = PerformanceLogger(logger)


class NLQAgent:
    """자연어 데이터 분석 에이전트

    사용자의 자연어 쿼리를 BigQuery SQL로 변환하고 분석합니다.

    파이프라인:
    1. SQL 생성 (LLM)
    2. 비용 추정 (dry_run)
    3. SQL 검증
    4. BigQuery 실행
    5. 결과 처리 및 통계

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

        # 로깅 초기화 (config에 따라 파일 로깅 활성화)
        setup_logging(self.config.logging)
        logger.info("NLQAgent 초기화")
        logger.info(f"로그 파일: {self.config.logging.file_path or '(콘솔만)'}")

        # 모듈 초기화
        self.intent_classifier = IntentClassifier(self.config.llm)
        self.validator = SQLValidator()
        self.context_builder = ContextBuilder()
        self.generator = SQLGenerator(self.config.llm)
        self.bq_executor = BigQueryExecutor(self.config.bigquery)
        self.data_processor = DataProcessor(self.config.analysis)
        self.sql_analyzer = SQLAnalyzer()
        self.explanation_gen = ExplanationGenerator(
            self.generator.client, self.config.llm.model
        )

    def _estimate_cost(self, bytes_billed: int) -> tuple[str, str]:
        """
        비용 추정 및 상태 결정

        Args:
            bytes_billed: BigQuery에서 청구할 바이트

        Returns:
            (status, message) 튜플
            status: "ok", "warning", "alert", "blocked"
            message: 사용자에게 보여줄 메시지
        """
        gb_billed = bytes_billed / (1024 ** 3)

        if bytes_billed < COST_THRESHOLDS["auto_execute"]:
            return "ok", f"예상 비용: {gb_billed:.2f} GB (자동 실행)"

        elif bytes_billed < COST_THRESHOLDS["warning"]:
            return (
                "warning",
                f"예상 비용: {gb_billed:.2f} GB\n약 ${gb_billed * 6.5 / 1000:.2f} 소요 예상\n진행하시겠습니까?",
            )

        elif bytes_billed < COST_THRESHOLDS["alert"]:
            return (
                "alert",
                f"경고: {gb_billed:.2f} GB 스캔 예상\n약 ${gb_billed * 6.5 / 1000:.2f} 소요\n"
                "쿼리를 최적화하는 것을 권장합니다.\n진행하시겠습니까?",
            )

        else:
            return (
                "blocked",
                f"차단됨: {gb_billed:.2f} GB 스캔 예상 (1TB 초과)\n"
                "쿼리가 너무 비용이 많이 들 것으로 예상됩니다.\n"
                "더 구체적인 기간 범위를 지정하거나, "
                "필터를 추가해주세요.",
            )

    def analyze(self, user_query: str, skip_cost_check: bool = False) -> AnalysisResult:
        """
        자연어 쿼리 분석 및 실행

        Args:
            user_query: 사용자의 자연어 쿼리
            skip_cost_check: 비용 체크 건너뛸 여부 (사용자 승인 후)

        Returns:
            AnalysisResult: 분석 결과

        Raises:
            NLQAgentException: 분석 중 에러
        """
        logger.set_context(user_query=user_query[:100])

        try:
            # 0단계: 의도 분류 (게이트키퍼)
            logger.info("Step 0: 질문 의도 분류 (게이트키퍼)")
            intent, message = self.intent_classifier.classify(user_query)

            if intent == QueryIntent.DATA_QUESTION:
                return self._analyze_data_question(user_query, skip_cost_check)

            elif intent == QueryIntent.META_QUESTION:
                explanation = self._answer_meta_question()
                logger.info(f"메타 질문 응답: {explanation[:100]}")
                return AnalysisResult(
                    query=user_query, sql="", data=None, stats={},
                    explanation=explanation,
                    success=True, error=None,
                    data_quality={}, sample_warning="",
                )

            elif intent == QueryIntent.OUT_OF_SCOPE:
                explanation = message or (
                    "이 시스템은 Liner의 사용자 행동·구독·쿼리 데이터에 대한 "
                    "분석 질문에 답합니다.\n\n"
                    "예시:\n"
                    "- 지난 30일 일별 DAU 추이\n"
                    "- 섹터별 구독 전환율\n"
                    "- 이력서 관련 사용자의 D+7 리텐션"
                )
                logger.info(f"범위 밖 질문: {user_query[:50]}")
                return AnalysisResult(
                    query=user_query, sql="", data=None, stats={},
                    explanation=explanation,
                    success=True, error=None,
                    data_quality={}, sample_warning="",
                )

            elif intent == QueryIntent.AMBIGUOUS:
                explanation = message or (
                    "질문을 좀 더 구체적으로 말씀해주세요.\n\n"
                    "예: '전환율'보다는 '지난 30일 make_chat 사용자의 구독 전환율'"
                )
                logger.info(f"모호한 질문: {user_query[:50]}")
                return AnalysisResult(
                    query=user_query, sql="", data=None, stats={},
                    explanation=explanation,
                    success=True, error=None,
                    data_quality={}, sample_warning="",
                )

        except Exception as e:
            logger.exception(f"의도 분류 중 예상치 못한 오류: {str(e)}")
            # 분류 실패 시 보수적으로 데이터 분석 진행
            return self._analyze_data_question(user_query, skip_cost_check)

    def _analyze_data_question(self, user_query: str, skip_cost_check: bool = False) -> AnalysisResult:
        """
        데이터 분석 질문 처리

        Args:
            user_query: 사용자의 자연어 쿼리
            skip_cost_check: 비용 체크 건너뛸 여부

        Returns:
            AnalysisResult: 분석 결과
        """
        try:
            # 1. SQL 생성 + 검증 + 비용 확인 (self-correction 루프)
            logger.info("Step 1: SQL 생성 및 검증 중 (self-correction 루프)")
            with perf.timer("generate_sql"):
                sql_result = self.generator.generate_with_validation(
                    user_query,
                    self.validator,
                    bq_executor=self.bq_executor,  # 비용 검증 통합
                    max_retries=3
                )

            if not sql_result.is_success():
                raise SQLGenerationError(
                    sql_result.error or "SQL 생성 실패", user_query=user_query
                )

            sql = sql_result.data
            logger.info(f"SQL 생성 완료: {len(sql)} chars")

            # 1.5 SQL 설명 생성 (LLM + 템플릿)
            logger.info("SQL 설명 생성 중")
            sql_explanation = ""
            try:
                with perf.timer("generate_sql_explanation"):
                    structure = self.sql_analyzer.analyze(sql)
                    explanation_dict = self.explanation_gen.generate(
                        user_query, sql, structure
                    )
                    sql_explanation = ExplanationFormatter.format(explanation_dict)
                    logger.info("SQL 설명 생성 완료")
            except Exception as e:
                logger.warning(f"SQL 설명 생성 실패: {e}")
                sql_explanation = ""

            # 2. 비용 정보 조회 (이미 검증했지만 정보만 다시 수집)
            logger.info("Step 2: 최종 비용 확인")
            with perf.timer("cost_estimation"):
                cost_result = self.bq_executor.dry_run(sql)

            if not cost_result.is_success():
                logger.warning(f"비용 추정 실패: {cost_result.error}")
                cost_estimate = {}
                cost_status, cost_message = "unknown", "비용 추정 실패"
            else:
                cost_estimate = cost_result.data
                # dry-run에서는 bytes_billed가 0일 수 있으므로 bytes_processed 사용
                bytes_billed = cost_estimate.get("bytes_billed", 0)
                if bytes_billed == 0:
                    bytes_billed = cost_estimate.get("bytes_processed", 0)
                cost_status, cost_message = self._estimate_cost(bytes_billed)
                logger.info(
                    f"비용 추정: {bytes_billed / (1024**3):.2f}GB, "
                    f"status={cost_status}"
                )

            # 3. BigQuery 실행
            logger.info("Step 3: BigQuery 실행 중")
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

            # 4. 데이터 처리
            logger.info("Step 4: 데이터 처리 중")
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
                cost_estimate=cost_estimate,
                cost_status=cost_status,
                cost_message=cost_message,
                sql_explanation=sql_explanation,
            )

        except NLQAgentException as e:
            logger.error(f"데이터 분석 실패: {e}")
            return AnalysisResult(
                query=user_query,
                sql="",
                data=None,
                stats={},
                explanation="",
                success=False,
                error=str(e),
                data_quality={},
                sample_warning="",
            )

        except Exception as e:
            logger.exception(f"데이터 분석 중 예상치 못한 오류: {str(e)}")
            return AnalysisResult(
                query=user_query,
                sql="",
                data=None,
                stats={},
                explanation="",
                success=False,
                error=f"예상치 못한 오류: {str(e)}",
                data_quality={},
                sample_warning="",
            )

    def _answer_meta_question(self) -> str:
        """시스템에 대한 메타 질문에 답하기"""
        return (
            "저는 Liner의 BigQuery 데이터 분석 어시스턴트입니다.\n\n"
            "활용 가능한 데이터:\n"
            "- EVENTS_296805: Amplitude 이벤트 (make_chat, view_pricing 등)\n"
            "- fct_moon_subscription: 구독 정보\n"
            "- fct_question_answer_binding_message: 메시지 카테고리\n\n"
            "분석 질문을 자연어로 입력하시면 SQL을 생성·실행해드립니다."
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
