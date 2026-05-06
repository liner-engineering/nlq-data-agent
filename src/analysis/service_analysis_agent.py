"""
Service Analysis Agent

BigQuery 데이터를 활용한 범용 서비스 분석 에이전트
자동 분석 + 통계 검정 + 인사이트 생성
"""

from dataclasses import dataclass
from typing import Any

import pandas as pd

from src.agent import NLQAgent
from src.analysis.statistical_tests import StatisticalTester, TestResult
from src.analysis.templates import find_template
from src.executor.bigquery_client import BigQueryExecutor
from src.logging_config import ContextualLogger

logger = ContextualLogger(__name__)


@dataclass
class AnalysisResult:
    """분석 결과"""
    question: str
    analysis_type: str
    data: pd.DataFrame
    statistics: dict[str, Any]
    test_results: list[TestResult]
    insights: list[str]
    recommendations: list[str]
    confidence: float  # 0.0 ~ 1.0

    def to_dict(self) -> dict[str, Any]:
        """결과를 딕셔너리로 변환"""
        return {
            'question': self.question,
            'analysis_type': self.analysis_type,
            'data_shape': self.data.shape if isinstance(self.data, pd.DataFrame) else None,
            'statistics': self.statistics,
            'test_results': [t.to_dict() for t in self.test_results],
            'insights': self.insights,
            'recommendations': self.recommendations,
            'confidence': round(self.confidence, 2),
        }


class ServiceAnalysisAgent:
    """범용 서비스 분석 에이전트

    BigQuery 데이터를 활용하여 자동으로 분석을 수행합니다.
    - 자연어 질문 → 분석 유형 자동 분류
    - 기본 분석 템플릿 활용
    - 통계 검정으로 유의성 확인
    - So What 원칙에 따른 인사이트 생성

    Example:
        agent = ServiceAnalysisAgent()
        result = agent.analyze_question("전환율이 어떻게 되나요?")
        print(result.insights)
    """

    def __init__(self) -> None:
        """초기화"""
        self.nlq_agent = NLQAgent()
        self.bq_executor = BigQueryExecutor(self.nlq_agent.config.bigquery)
        self.tester = StatisticalTester()
        logger.info("ServiceAnalysisAgent 초기화 완료")

    def analyze_question(self, question: str) -> AnalysisResult:
        """
        자연어 질문을 분석

        Args:
            question: 사용자 질문

        Returns:
            AnalysisResult

        Raises:
            Exception: 분석 실패
        """
        logger.set_context(question=question[:100])

        try:
            # 1. 분석 템플릿 자동 선택
            template = find_template(question)
            analysis_type = template.name if template else "자유 분석"

            # 템플릿 선택 사유 기록
            if template:
                matched_keywords = [kw for kw in template.keywords if kw in question.lower()]
                logger.info(f"분석 유형: {analysis_type} (매칭 키워드: {', '.join(matched_keywords)})")
            else:
                logger.info(f"분석 유형: {analysis_type} (템플릿 미매칭, LLM 자유 분석)")

            # 2. SQL 생성 및 실행
            if template:
                # 템플릿 기반 분석
                sql = template.sql_generator(question)
                result = self.bq_executor.execute(sql)

                if not result.is_success():
                    return self._create_error_result(question, analysis_type, result.error)

                data = result.data
            else:
                # 자유 쿼리 (NLQAgent 사용)
                nlq_result = self.nlq_agent.analyze(question)
                if not nlq_result.success:
                    return self._create_error_result(question, analysis_type, nlq_result.error)

                data = nlq_result.data

            logger.info(f"데이터 수집 완료: {len(data)} rows, {len(data.columns)} cols")

            # 3. 기본 통계 계산
            statistics = self._calculate_statistics(data)

            # 4. 통계 검정 (해당하는 경우)
            test_results = self._perform_tests(data, template)

            # 5. 인사이트 생성
            insights = self._generate_insights(data, statistics, test_results, analysis_type)

            # 6. 추천사항 생성
            recommendations = self._generate_recommendations(insights, analysis_type)

            # 7. 신뢰도 계산
            confidence = self._calculate_confidence(len(data), test_results)

            return AnalysisResult(
                question=question,
                analysis_type=analysis_type,
                data=data,
                statistics=statistics,
                test_results=test_results,
                insights=insights,
                recommendations=recommendations,
                confidence=confidence,
            )

        except Exception as e:
            logger.exception(f"분석 실패: {str(e)}")
            raise

    def _calculate_statistics(self, data: pd.DataFrame) -> dict[str, Any]:
        """기본 통계 계산"""
        stats = {
            'total_rows': len(data),
            'total_columns': len(data.columns),
            'null_counts': data.isnull().sum().to_dict(),
            'completeness_pct': round(
                100.0 * (1 - data.isnull().sum().sum() / (len(data) * len(data.columns))), 2
            ),
        }

        # 수치 컬럼 통계
        numeric_cols = data.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            for col in numeric_cols:
                col_data = data[col].dropna()
                if len(col_data) > 0:
                    stats[f'{col}_mean'] = round(col_data.mean(), 4)
                    stats[f'{col}_median'] = round(col_data.median(), 4)
                    stats[f'{col}_std'] = round(col_data.std(), 4)
                    stats[f'{col}_min'] = round(col_data.min(), 4)
                    stats[f'{col}_max'] = round(col_data.max(), 4)

        return stats

    def _perform_tests(self, data: pd.DataFrame, template) -> list[TestResult]:
        """통계 검정 수행"""
        results = []

        try:
            # 템플릿별 검정 전략
            if template and 'retention' in template.name.lower():
                # 리텐션 분석: 세그먼트별 카이제곱 검정
                if 'retention_rate_pct' in data.columns and 'sector' in data.columns:
                    # 고정 기준(50%)과 비교
                    retention_rates = data['retention_rate_pct'].dropna()
                    if len(retention_rates) > 0:
                        above_50pct = (retention_rates >= 50).sum()
                        total = len(retention_rates)

                        if total >= 5:
                            test_result = self.tester.chi_square_test(
                                {'above_50pct': [above_50pct], 'below_50pct': [total - above_50pct]},
                                category_name="리텐션율"
                            )
                            results.append(test_result)

            elif template and 'churn' in template.name.lower():
                # 이탈 분석: 상태별 카이제곱 검정
                if 'status' in data.columns and 'user_count' in data.columns:
                    contingency = data.set_index('status')['user_count'].to_dict()
                    if len(contingency) >= 2:
                        test_result = self.tester.chi_square_test(
                            contingency, category_name="이탈 상태"
                        )
                        results.append(test_result)

        except Exception as e:
            logger.warning(f"통계 검정 중 오류: {str(e)}")

        return results

    def _generate_insights(
        self, data: pd.DataFrame, statistics: dict, test_results: list, analysis_type: str
    ) -> list[str]:
        """인사이트 생성 (So What 원칙)"""
        insights = []

        # 샘플 크기 확인
        total_rows = statistics.get('total_rows', 0)
        if total_rows < 10:
            return ["샘플 크기가 너무 작아 신뢰성 있는 분석이 불가능합니다."]

        if total_rows < 100:
            insights.append("샘플 크기가 작아 통계적 신뢰성이 낮습니다.")

        # 통계 검정 결과
        for test in test_results:
            if test.significant:
                insights.append(f"[유의미] {test.interpretation}")
            else:
                insights.append(f"[무의미] {test.interpretation}")

        # 수치 데이터 기반 인사이트
        numeric_cols = data.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            if '_pct' in col or 'rate' in col.lower():
                mean_val = statistics.get(f'{col}_mean', 0)
                max_val = statistics.get(f'{col}_max', 0)

                if mean_val > 0:
                    if 'retention' in col.lower() and mean_val < 30:
                        insights.append(f"리텐션율이 평균 {mean_val:.1f}%로 낮습니다. 개선이 필요합니다.")
                    elif 'conversion' in col.lower() and mean_val < 5:
                        insights.append(f"전환율이 평균 {mean_val:.2f}%로 매우 낮습니다.")
                    elif max_val > mean_val * 1.5:
                        insights.append(f"{col}의 편차가 큽니다. 세그먼트별 전략이 필요합니다.")

        # 일반적인 관찰
        completeness = statistics.get('completeness_pct', 0)
        if completeness < 80:
            insights.append(f"데이터 품질: {completeness:.1f}% (null 값 많음)")

        return insights if insights else ["데이터가 기본적인 요건을 충족합니다."]

    def _generate_recommendations(self, insights: list[str], analysis_type: str) -> list[str]:
        """추천사항 생성"""
        recommendations = []

        # 분석 유형별 추천
        if 'retention' in analysis_type.lower():
            recommendations.append("리텐션율이 낮은 세그먼트의 사용자 행동을 분석하세요.")
            recommendations.append("초기 온보딩 개선을 검토하세요.")

        elif 'conversion' in analysis_type.lower():
            recommendations.append("전환 경로의 병목을 식별하세요.")
            recommendations.append("A/B 테스트로 개선 사항을 검증하세요.")

        elif 'churn' in analysis_type.lower():
            recommendations.append("이탈 사용자의 공통점을 파악하세요.")
            recommendations.append("재참여 캠페인을 구성하세요.")

        elif 'revenue' in analysis_type.lower():
            recommendations.append("고가 사용자와 저가 사용자의 행동 차이를 분석하세요.")
            recommendations.append("가격 정책 최적화를 검토하세요.")

        else:
            recommendations.append("추가 분석을 통해 원인을 파악하세요.")
            recommendations.append("데이터 기반 액션 플랜을 수립하세요.")

        return recommendations

    def _calculate_confidence(self, sample_size: int, test_results: list) -> float:
        """신뢰도 계산 (0.0 ~ 1.0)"""
        confidence = 0.5

        if sample_size >= 1000:
            confidence += 0.3
        elif sample_size >= 100:
            confidence += 0.2
        elif sample_size >= 30:
            confidence += 0.1

        significant_tests = sum(1 for t in test_results if t.significant)
        if significant_tests > 0:
            confidence += 0.2

        return min(confidence, 1.0)

    def _create_error_result(self, question: str, analysis_type: str, error: str) -> AnalysisResult:
        """에러 결과 생성"""
        return AnalysisResult(
            question=question,
            analysis_type=analysis_type,
            data=pd.DataFrame(),
            statistics={},
            test_results=[],
            insights=[f"분석 실패: {error}"],
            recommendations=["쿼리를 다시 확인하거나 다른 분석을 시도하세요."],
            confidence=0.0,
        )
