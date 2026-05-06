"""
통계 검정 유틸리티

카이제곱 검정, t-검정, Fisher 검정 등을 수행합니다.
"""

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from src.logging_config import ContextualLogger

logger = ContextualLogger(__name__)


@dataclass
class TestResult:
    """통계 검정 결과"""
    test_name: str
    statistic: float
    p_value: float
    significant: bool  # p < 0.05 여부
    interpretation: str
    sample_sizes: dict[str, int] | None = None
    effect_size: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """결과를 딕셔너리로 변환"""
        return {
            'test': self.test_name,
            'statistic': round(self.statistic, 4),
            'p_value': round(self.p_value, 6),
            'significant': self.significant,
            'interpretation': self.interpretation,
            'sample_sizes': self.sample_sizes,
            'effect_size': round(self.effect_size, 4) if self.effect_size else None,
        }


class StatisticalTester:
    """통계 검정 수행기"""

    @staticmethod
    def chi_square_test(
        contingency_table: pd.DataFrame | dict, category_name: str = "카테고리"
    ) -> TestResult:
        """
        카이제곱 검정 (범주형 데이터)

        Args:
            contingency_table: 분할표 (행: 그룹, 열: 결과)
            category_name: 카테고리 이름

        Returns:
            TestResult
        """
        if isinstance(contingency_table, dict):
            contingency_table = pd.DataFrame(contingency_table)

        try:
            chi2, p_value, dof, expected = stats.chi2_contingency(contingency_table)

            significant = p_value < 0.05
            interpretation = (
                f"{category_name}별 차이가 통계적으로 유의미함 (p < 0.05)"
                if significant
                else f"{category_name}별 차이가 통계적으로 유의미하지 않음 (p ≥ 0.05)"
            )

            sample_sizes = {
                group: int(contingency_table.loc[group].sum())
                for group in contingency_table.index
            }

            return TestResult(
                test_name="카이제곱 검정 (Chi-Square)",
                statistic=chi2,
                p_value=p_value,
                significant=significant,
                interpretation=interpretation,
                sample_sizes=sample_sizes,
            )
        except Exception as e:
            logger.error(f"카이제곱 검정 실패: {str(e)}")
            raise

    @staticmethod
    def t_test(
        group1: pd.Series | list, group2: pd.Series | list, equal_var: bool = True
    ) -> TestResult:
        """
        독립표본 t-검정 (연속형 데이터)

        Args:
            group1: 첫 번째 그룹의 값
            group2: 두 번째 그룹의 값
            equal_var: 등분산 가정 여부

        Returns:
            TestResult
        """
        if isinstance(group1, pd.Series):
            group1 = group1.dropna().values
        if isinstance(group2, pd.Series):
            group2 = group2.dropna().values

        try:
            t_stat, p_value = stats.ttest_ind(group1, group2, equal_var=equal_var)

            significant = p_value < 0.05
            interpretation = (
                f"두 그룹의 평균 차이가 통계적으로 유의미함 (p < 0.05)"
                if significant
                else f"두 그룹의 평균 차이가 통계적으로 유의미하지 않음 (p ≥ 0.05)"
            )

            # Cohen's d (효과크기)
            pooled_std = np.sqrt(
                ((len(group1) - 1) * np.var(group1, ddof=1)
                 + (len(group2) - 1) * np.var(group2, ddof=1))
                / (len(group1) + len(group2) - 2)
            )
            cohens_d = (np.mean(group1) - np.mean(group2)) / pooled_std

            return TestResult(
                test_name="독립표본 t-검정 (t-test)",
                statistic=t_stat,
                p_value=p_value,
                significant=significant,
                interpretation=interpretation,
                sample_sizes={"그룹1": len(group1), "그룹2": len(group2)},
                effect_size=cohens_d,
            )
        except Exception as e:
            logger.error(f"t-검정 실패: {str(e)}")
            raise

    @staticmethod
    def fishers_exact_test(contingency_table: pd.DataFrame | dict) -> TestResult:
        """
        Fisher 검정 (2x2 분할표)

        Args:
            contingency_table: 2x2 분할표

        Returns:
            TestResult
        """
        if isinstance(contingency_table, dict):
            contingency_table = pd.DataFrame(contingency_table)

        if contingency_table.shape != (2, 2):
            raise ValueError("Fisher 검정은 2x2 분할표가 필요합니다")

        try:
            oddsratio, p_value = stats.fisher_exact(contingency_table)

            significant = p_value < 0.05
            interpretation = (
                f"두 그룹의 차이가 통계적으로 유의미함 (p < 0.05)"
                if significant
                else f"두 그룹의 차이가 통계적으로 유의미하지 않음 (p ≥ 0.05)"
            )

            sample_sizes = {
                group: int(contingency_table.loc[group].sum())
                for group in contingency_table.index
            }

            return TestResult(
                test_name="Fisher 정확성 검정 (Fisher's Exact)",
                statistic=oddsratio,
                p_value=p_value,
                significant=significant,
                interpretation=interpretation,
                sample_sizes=sample_sizes,
            )
        except Exception as e:
            logger.error(f"Fisher 검정 실패: {str(e)}")
            raise

    @staticmethod
    def mannwhitneyu_test(group1: pd.Series | list, group2: pd.Series | list) -> TestResult:
        """
        Mann-Whitney U 검정 (비모수 검정)

        Args:
            group1: 첫 번째 그룹의 값
            group2: 두 번째 그룹의 값

        Returns:
            TestResult
        """
        if isinstance(group1, pd.Series):
            group1 = group1.dropna().values
        if isinstance(group2, pd.Series):
            group2 = group2.dropna().values

        try:
            u_stat, p_value = stats.mannwhitneyu(group1, group2)

            significant = p_value < 0.05
            interpretation = (
                f"두 그룹의 분포가 통계적으로 유의미하게 다름 (p < 0.05)"
                if significant
                else f"두 그룹의 분포가 통계적으로 유의미하게 다르지 않음 (p ≥ 0.05)"
            )

            return TestResult(
                test_name="Mann-Whitney U 검정 (비모수)",
                statistic=u_stat,
                p_value=p_value,
                significant=significant,
                interpretation=interpretation,
                sample_sizes={"그룹1": len(group1), "그룹2": len(group2)},
            )
        except Exception as e:
            logger.error(f"Mann-Whitney U 검정 실패: {str(e)}")
            raise

    @staticmethod
    def proportion_ztest(count: int, nobs: int, value: float = 0.5) -> TestResult:
        """
        비율 z-검정 (proportion test)

        Args:
            count: 성공 횟수
            nobs: 전체 표본 수
            value: 귀무가설의 비율 (기본값: 0.5)

        Returns:
            TestResult
        """
        try:
            z_stat = (count / nobs - value) / np.sqrt(value * (1 - value) / nobs)
            p_value = 2 * stats.norm.sf(abs(z_stat))

            significant = p_value < 0.05
            interpretation = (
                f"비율이 {value:.0%}와 통계적으로 유의미하게 다름 (p < 0.05)"
                if significant
                else f"비율이 {value:.0%}와 통계적으로 유의미하게 다르지 않음 (p ≥ 0.05)"
            )

            return TestResult(
                test_name="비율 검정 (Proportion Test)",
                statistic=z_stat,
                p_value=p_value,
                significant=significant,
                interpretation=interpretation,
                sample_sizes={"전체": nobs, "성공": count},
            )
        except Exception as e:
            logger.error(f"비율 검정 실패: {str(e)}")
            raise
