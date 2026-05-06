"""
데이터 처리기

BigQuery 결과를 정리하고 통계를 계산합니다.
벡터화된 연산과 캐싱을 포함한 효율적인 구현입니다.
"""

from typing import Any

import numpy as np
import pandas as pd

from src.config import AnalysisConfig
from src.exceptions import DataProcessingError
from src.logging_config import ContextualLogger
from src.types import Result, Stats

logger = ContextualLogger(__name__)


class DataProcessor:
    """데이터 처리기

    BigQuery 결과 DataFrame을 정리하고 통계를 계산합니다.
    샘플 크기 검증과 데이터 품질 평가를 포함합니다.

    Example:
        processor = DataProcessor(config)
        result = processor.process(df)
        if result.is_success():
            stats = result.data['stats']
    """

    def __init__(self, config: AnalysisConfig | None = None) -> None:
        """
        초기화

        Args:
            config: AnalysisConfig 인스턴스
        """
        self.config = config or AnalysisConfig()

    def process(self, df: pd.DataFrame) -> Result[dict[str, Any]]:
        """
        DataFrame 처리 및 통계 생성

        Args:
            df: BigQuery 결과 DataFrame

        Returns:
            처리 결과 사전 또는 에러

        Raises:
            DataProcessingError: 데이터 처리 중 오류
        """
        if not isinstance(df, pd.DataFrame):
            return Result.failure("입력이 DataFrame이 아닙니다")

        if df.empty:
            return Result.failure("데이터가 없습니다")

        try:
            num_rows = len(df)
            num_cols = len(df.columns)

            logger.set_context(
                data_shape=(num_rows, num_cols),
                processing_step="데이터 정리",
            )

            # 1. 데이터 정리
            df_cleaned = self._clean_data(df)

            # 2. 통계 계산
            stats = self._calculate_statistics(df_cleaned)

            # 3. 데이터 품질 평가
            data_quality = self._assess_quality(df)

            # 4. 샘플 크기 경고
            sample_warning = self._generate_sample_warning(num_rows)

            # 5. 설명 생성
            explanation = self._generate_explanation(
                df_cleaned, stats, sample_warning
            )

            logger.info(f"데이터 처리 완료: {num_rows}행, {len(stats)}개 통계")

            return Result.success(
                {
                    "df_cleaned": df_cleaned,
                    "stats": stats,
                    "sample_warning": sample_warning,
                    "data_quality": data_quality,
                    "explanation": explanation,
                }
            )

        except Exception as e:
            logger.exception(f"데이터 처리 실패: {str(e)}")
            return Result.failure(f"데이터 처리 실패: {str(e)}")

    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        데이터 정리

        수치 컬럼을 반올림하고 정리합니다.

        Args:
            df: 원본 DataFrame

        Returns:
            정리된 DataFrame
        """
        df_clean = df.copy()

        # 부동소수점 반올림 (표시용)
        numeric_cols = df_clean.select_dtypes(include=[np.number]).columns

        for col in numeric_cols:
            # 벡터화된 연산으로 성능 개선
            if df_clean[col].dtype in [np.float32, np.float64]:
                df_clean[col] = df_clean[col].round(3)

        return df_clean

    def _calculate_statistics(self, df: pd.DataFrame) -> Stats:
        """
        수치 컬럼의 통계 계산

        벡터화된 numpy 연산으로 효율성을 높입니다.

        Args:
            df: DataFrame

        Returns:
            컬럼별 통계 사전
        """
        stats: Stats = {}
        numeric_cols = df.select_dtypes(include=[np.number]).columns

        for col in numeric_cols:
            col_data = df[col].dropna()

            if len(col_data) == 0:
                continue

            try:
                # numpy를 사용한 벡터화 연산
                col_array = col_data.values
                stats[col] = {
                    "count": int(len(col_data)),
                    "mean": float(np.round(np.mean(col_array), 3)),
                    "median": float(np.round(np.median(col_array), 3)),
                    "std": float(np.round(np.std(col_array), 3)),
                    "min": float(np.round(np.min(col_array), 3)),
                    "max": float(np.round(np.max(col_array), 3)),
                    "q25": float(np.round(np.percentile(col_array, 25), 3)),
                    "q75": float(np.round(np.percentile(col_array, 75), 3)),
                }
            except Exception as e:
                logger.warning(f"통계 계산 실패 ({col}): {str(e)}")

        return stats

    def _assess_quality(self, df: pd.DataFrame) -> dict[str, Any]:
        """
        데이터 품질 평가

        Args:
            df: DataFrame

        Returns:
            품질 메트릭 사전
        """
        num_rows = len(df)
        num_cols = len(df.columns)

        # NULL 개수 (벡터화)
        null_counts = df.isnull().sum()
        null_info = {col: int(count) for col, count in null_counts.items() if count > 0}

        # 완성도 (% 비율)
        total_cells = num_rows * num_cols
        null_cells = null_counts.sum()
        completeness = 100 * (1 - null_cells / total_cells) if total_cells > 0 else 100

        return {
            "total_rows": num_rows,
            "total_columns": num_cols,
            "null_counts": null_info,
            "completeness_pct": round(completeness, 1),
        }

    def _generate_sample_warning(self, num_rows: int) -> str:
        """
        샘플 크기 경고 생성

        Args:
            num_rows: 데이터 행 수

        Returns:
            경고 메시지
        """
        if num_rows < self.config.min_sample_size:
            return (
                f"⚠️ 샘플 크기 부족 ({num_rows}개). "
                f"결과가 의미 없을 수 있습니다."
            )
        elif num_rows < self.config.recommended_sample_size:
            return (
                f"⚠️ 샘플 크기 작음 ({num_rows}개). "
                f"통계적 신뢰성이 낮을 수 있습니다."
            )
        else:
            return f"✓ 충분한 샘플 크기 ({num_rows}개)"

    def _generate_explanation(
        self, df: pd.DataFrame, stats: Stats, sample_warning: str
    ) -> str:
        """
        결과 설명 생성

        Args:
            df: DataFrame
            stats: 통계 사전
            sample_warning: 샘플 크기 경고

        Returns:
            설명 텍스트
        """
        parts = [sample_warning]

        # 핵심 통계
        if stats:
            parts.append("\n핵심 지표:")
            for col, col_stats in list(stats.items())[:3]:  # 처음 3개
                mean = col_stats["mean"]
                count = col_stats["count"]
                parts.append(f"  - {col}: 평균 {mean} (n={count})")

        # 데이터셋 정보
        if len(df.columns) > 0:
            parts.append(f"\n결과: {len(df)}행 × {len(df.columns)}열")
            cols_str = ", ".join(df.columns[:5])
            parts.append(f"컬럼: {cols_str}")
            if len(df.columns) > 5:
                parts.append(f"및 {len(df.columns) - 5}개 더...")

        return "\n".join(parts)

    def format_for_display(self, df: pd.DataFrame, max_rows: int = 10) -> str:
        """
        결과를 표시용으로 포맷팅

        Args:
            df: DataFrame
            max_rows: 표시할 최대 행 수

        Returns:
            포맷된 문자열
        """
        if df.empty:
            return "결과 없음"

        return df.head(max_rows).to_string()
