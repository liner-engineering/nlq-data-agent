"""
SQL 결과 비교기 (Execution Accuracy의 핵심)

두 SQL을 실행한 결과 DataFrame이 "의미상 동일"한지 판단합니다.
Spider/BIRD 벤치마크의 execution accuracy 패턴을 따릅니다.

핵심 원칙:
- 컬럼 이름은 다를 수 있다 (별칭 차이는 허용)
- 컬럼 순서는 다를 수 있다 (SELECT 순서 차이는 허용)
- 행 순서는 ORDER BY가 있을 때만 중요 (없으면 정렬 후 비교)
- 부동소수점은 epsilon 허용
- NULL 처리 일관됨
"""

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd


@dataclass
class ComparisonResult:
    """비교 결과"""

    match: bool
    reason: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def __bool__(self) -> bool:
        return self.match


class ResultComparator:
    """두 DataFrame 결과를 의미적으로 비교

    Example:
        cmp = ResultComparator()
        result = cmp.compare(gold_df, pred_df, order_sensitive=False)
        if result.match:
            print("결과 일치")
        else:
            print(f"불일치: {result.reason}")
    """

    def __init__(
        self,
        float_tolerance: float = 1e-4,
        ignore_column_names: bool = True,
    ) -> None:
        """
        Args:
            float_tolerance: 부동소수점 허용 오차 (기본 1e-4)
            ignore_column_names: True면 컬럼 이름 차이 무시 (위치/타입 기반 비교)
        """
        self.float_tolerance = float_tolerance
        self.ignore_column_names = ignore_column_names

    def compare(
        self,
        gold: pd.DataFrame,
        pred: pd.DataFrame,
        order_sensitive: bool = False,
    ) -> ComparisonResult:
        """
        두 DataFrame 비교

        Args:
            gold: 정답 결과
            pred: 예측 결과
            order_sensitive: True면 행 순서까지 일치해야 함 (ORDER BY 있는 쿼리)

        Returns:
            ComparisonResult
        """
        # 1. 형태 검사
        if gold.shape != pred.shape:
            return ComparisonResult(
                match=False,
                reason=f"shape 불일치: gold={gold.shape}, pred={pred.shape}",
                details={"gold_shape": gold.shape, "pred_shape": pred.shape},
            )

        # 빈 결과끼리는 일치
        if gold.empty and pred.empty:
            return ComparisonResult(match=True, reason="empty match")

        # 2. 정규화: 컬럼명 무시 옵션
        if self.ignore_column_names:
            gold_norm, pred_norm = self._align_by_position(gold, pred)
        else:
            # 컬럼 이름 매칭 시도
            common_cols = sorted(set(gold.columns) & set(pred.columns))
            if len(common_cols) != len(gold.columns):
                missing = set(gold.columns) - set(pred.columns)
                return ComparisonResult(
                    match=False,
                    reason=f"컬럼 누락: {missing}",
                    details={"missing_columns": list(missing)},
                )
            gold_norm = gold[common_cols].copy()
            pred_norm = pred[common_cols].copy()

        # 3. 정렬 (order-insensitive 모드)
        if not order_sensitive:
            gold_norm = self._stable_sort(gold_norm)
            pred_norm = self._stable_sort(pred_norm)

        # 4. 셀 단위 비교
        return self._cell_compare(gold_norm, pred_norm)

    def _align_by_position(
        self, gold: pd.DataFrame, pred: pd.DataFrame
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        컬럼 이름 무시하고 위치 + 타입 기반으로 정렬.

        Spider 방식: 컬럼 셋(set)이 같다고 가정하고, 각 컬럼을 정렬해서 비교.
        실제로는 더 robust하게 'frozenset of values per column' 으로 매칭.
        """
        # 단순화: 컬럼 위치는 유지하되, 컬럼 이름을 인덱스로 통일
        gold_norm = gold.copy()
        pred_norm = pred.copy()
        gold_norm.columns = [f"c{i}" for i in range(len(gold.columns))]
        pred_norm.columns = [f"c{i}" for i in range(len(pred.columns))]

        # 각 컬럼을 값 기준으로 정렬해서 위치 차이를 흡수
        # (SELECT a, b vs SELECT b, a 차이를 흡수)
        gold_sorted_cols = self._sort_columns_by_signature(gold_norm)
        pred_sorted_cols = self._sort_columns_by_signature(pred_norm)

        return gold_sorted_cols, pred_sorted_cols

    def _sort_columns_by_signature(self, df: pd.DataFrame) -> pd.DataFrame:
        """각 컬럼의 시그니처(정렬된 값들의 해시)로 컬럼 순서 정규화"""
        if df.empty:
            return df

        signatures = []
        for col in df.columns:
            try:
                sorted_vals = tuple(
                    sorted(
                        df[col].fillna("__NULL__").astype(str).tolist()
                    )
                )
                signatures.append((sorted_vals, col))
            except Exception:
                signatures.append(((str(df[col].dtype),), col))

        # 시그니처로 정렬
        signatures.sort(key=lambda x: x[0])
        new_order = [col for _, col in signatures]
        return df[new_order]

    def _stable_sort(self, df: pd.DataFrame) -> pd.DataFrame:
        """모든 컬럼 기준으로 안정 정렬"""
        if df.empty:
            return df
        try:
            # 모든 컬럼을 string으로 변환해서 정렬 (mixed type 안전)
            sort_keys = [df[col].astype(str) for col in df.columns]
            order = pd.DataFrame({f"k{i}": k for i, k in enumerate(sort_keys)}).sort_values(
                by=[f"k{i}" for i in range(len(sort_keys))],
                kind="stable",
            ).index
            return df.loc[order].reset_index(drop=True)
        except Exception:
            return df.reset_index(drop=True)

    def _cell_compare(
        self, gold: pd.DataFrame, pred: pd.DataFrame
    ) -> ComparisonResult:
        """셀 단위 비교 (numeric은 epsilon, 그 외는 string equality)"""
        mismatches = []

        for col_idx, (gcol, pcol) in enumerate(zip(gold.columns, pred.columns)):
            g_series = gold[gcol].reset_index(drop=True)
            p_series = pred[pcol].reset_index(drop=True)

            # 숫자형끼리 비교
            if pd.api.types.is_numeric_dtype(g_series) and pd.api.types.is_numeric_dtype(
                p_series
            ):
                # NaN 위치 일치 확인
                g_nan = g_series.isna()
                p_nan = p_series.isna()
                if not (g_nan == p_nan).all():
                    mismatches.append(
                        f"컬럼 {col_idx}: NULL 위치 불일치"
                    )
                    continue

                # 비-NaN 값 비교
                g_valid = g_series[~g_nan].astype(float).values
                p_valid = p_series[~p_nan].astype(float).values

                if not np.allclose(
                    g_valid, p_valid, rtol=self.float_tolerance, atol=self.float_tolerance
                ):
                    diff_count = int(
                        np.sum(
                            np.abs(g_valid - p_valid)
                            > self.float_tolerance * (np.abs(g_valid) + 1)
                        )
                    )
                    mismatches.append(
                        f"컬럼 {col_idx}: {diff_count}개 셀 수치 불일치"
                    )
            else:
                # 그 외: string으로 normalize 후 비교
                g_str = g_series.fillna("__NULL__").astype(str)
                p_str = p_series.fillna("__NULL__").astype(str)
                if not (g_str.values == p_str.values).all():
                    diff_count = int((g_str.values != p_str.values).sum())
                    mismatches.append(
                        f"컬럼 {col_idx}: {diff_count}개 셀 문자열 불일치"
                    )

        if mismatches:
            return ComparisonResult(
                match=False,
                reason="; ".join(mismatches),
                details={"mismatches": mismatches},
            )

        return ComparisonResult(match=True, reason="all cells match")
