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

            # 2. 비용 정보 조회 스킵 (BigQuery 비용은 GCP에서 관리)
            cost_estimate = {}
            cost_status, cost_message = "", ""

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

            # 3.5 0행 결과 의심 로직
            if len(df) == 0:
                logger.warning("0행 결과 감지 — 가능한 원인 분석 중")
                suspicions = self._check_zero_rows_suspicion(sql, user_query)
                suspicion_msg = self._format_suspicions(suspicions)

                return AnalysisResult(
                    query=user_query,
                    sql=sql,
                    data=None,
                    stats={},
                    explanation=f"데이터가 없습니다.\n\n{suspicion_msg}",
                    success=False,
                    data_quality={},
                    sample_warning="데이터 없음",
                    cost_estimate={},
                    cost_status="",
                    cost_message="",
                    sql_explanation=sql_explanation,
                )

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

    def _check_zero_rows_suspicion(self, sql: str, user_query: str) -> list[str]:
        """
        0행 결과의 가능한 원인 분석

        Returns:
            의심 원인 리스트
        """
        import re
        from datetime import datetime

        suspicions = []

        # 1. 과거 연도 검사
        years_in_sql = re.findall(r"'(\d{4})-", sql)
        current_year = datetime.now().year
        for year in years_in_sql:
            year_int = int(year)
            if year_int < current_year - 1:
                suspicions.append(
                    f"SQL에 {year}년이 사용되었습니다. "
                    f"사용자 의도가 올해({current_year})였을 가능성 높음"
                )

        # 2. 타입 캐스팅 의심 (JOIN에서)
        if "CAST" in sql.upper() and "INT64" in sql.upper() and "JOIN" in sql.upper():
            suspicions.append(
                "JOIN 컬럼에 타입 캐스팅이 있습니다. "
                "EVENTS_296805.user_id(STRING) vs fct_moon_subscription.user_id(INT64) "
                "타입 불일치로 0건 반환될 수 있습니다"
            )

        # 3. CASE 문으로 구분하려는 데 필요 컬럼 없음
        if "CASE" in sql.upper() and "WHEN" in sql.upper():
            # GROUP BY에서 CASE 사용하는데, CTE에서 필요 컬럼 없음
            if "GROUP BY" in sql.upper():
                suspicions.append(
                    "CASE 문으로 그룹 구분하려고 하는데, "
                    "필요한 컬럼(예: plan_id, product_category)이 CTE SELECT에 없을 수 있습니다"
                )

        # 4. 시간 범위 부정확 (BETWEEN이 범위를 넘어감)
        if "DATE_TRUNC" in sql.upper() and "BETWEEN" in sql.upper():
            suspicions.append(
                "DATE_TRUNC가 있는 시간 범위는 의도와 다를 수 있습니다. "
                "명시적 날짜(예: '2026-04-01' AND '2026-04-30')를 확인하세요"
            )

        # 5. 활성 구독자 필터가 분석 기간과 불일치
        if "subscription_start_at" in sql.lower() and "subscription_ended_at" in sql.lower():
            if "CURRENT_DATE()" in sql:
                suspicions.append(
                    "활성 구독자(현재 기준)로만 필터링하면, "
                    "과거 기간의 구독 데이터를 놓칠 수 있습니다. "
                    "분석 기간에 활성이었던 사용자를 써야 합니다"
                )

        # 6. 사용자 쿼리에서 "4월"인데 SQL에 2024년
        if ("4월" in user_query or "april" in user_query.lower()) and "2024" in sql:
            suspicions.append(
                "사용자 질문: '4월', SQL: 2024년 4월. "
                "현재는 2026년이므로 2026년 4월 데이터를 확인하세요"
            )

        if not suspicions:
            suspicions.append(
                "특정 원인을 찾지 못했습니다. "
                "다음을 확인하세요:\n"
                "1. 시간 범위가 맞는가\n"
                "2. 필터 조건이 너무 까다로운가\n"
                "3. 테이블이 실제로 존재하는가"
            )

        return suspicions

    def _format_suspicions(self, suspicions: list[str]) -> str:
        """
        의심 사항을 포맷팅하여 사용자에게 친화적인 메시지 생성

        Args:
            suspicions: 의심 원인 리스트

        Returns:
            포맷팅된 메시지
        """
        if not suspicions:
            return "원인을 특정하지 못했습니다. SQL을 다시 확인해주세요."

        lines = ["다음을 의심해보세요:\n"]
        for i, susp in enumerate(suspicions, 1):
            lines.append(f"{i}. {susp}")

        lines.append(
            "\n혹은 SQL을 다시 작성해달라고 요청하세요. "
            "정정된 SQL을 제공할 수 있습니다."
        )

        return "\n".join(lines)

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
