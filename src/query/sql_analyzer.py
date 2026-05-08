"""
SQL 구조 분석기

sqlglot을 사용하여 SQL을 파싱하고 구조적 메타데이터를 추출합니다.
LLM 호출 없이 빠르고 결정론적인 분석을 수행합니다.
"""

import re
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp

from src.logging_config import ContextualLogger

logger = ContextualLogger(__name__)


@dataclass
class SQLStructure:
    """SQL의 구조적 분석 결과"""
    tables_used: list[str] = field(default_factory=list)
    time_range: dict | None = None      # {"amount": 30, "unit": "DAY"}
    filters: list[dict] = field(default_factory=list)  # [{"column": "event_type", "op": "=", "value": "make_chat"}]
    aggregations: list[str] = field(default_factory=list)  # ["COUNT(DISTINCT user_id)"]
    group_by_cols: list[str] = field(default_factory=list)
    having_conditions: list[str] = field(default_factory=list)
    order_by_cols: list[str] = field(default_factory=list)
    limit: int | None = None
    has_join: bool = False
    has_subquery: bool = False
    has_cte: bool = False


class SQLAnalyzer:
    """SQL을 파싱해서 구조 정보 추출 (LLM 호출 없음, 결정론적)"""

    def analyze(self, sql: str) -> SQLStructure:
        """
        SQL을 분석하여 구조 정보 추출

        Args:
            sql: BigQuery SQL

        Returns:
            SQLStructure: 분석된 구조 정보
        """
        try:
            parsed = sqlglot.parse_one(sql, dialect="bigquery")
        except Exception as e:
            logger.warning(f"SQL 파싱 실패: {e}")
            return SQLStructure()

        structure = SQLStructure()

        try:
            # 1. 사용된 테이블
            for table in parsed.find_all(exp.Table):
                name = table.name
                if name and name not in structure.tables_used:
                    structure.tables_used.append(name)

            # 2. CTE 사용 여부
            structure.has_cte = bool(parsed.find(exp.CTE))

            # 3. JOIN 여부
            structure.has_join = bool(parsed.find(exp.Join))

            # 4. 서브쿼리 여부
            structure.has_subquery = bool(parsed.find(exp.Subquery))

            # 5. 시간 범위 추출 (DATE_SUB(CURRENT_DATE(), INTERVAL N DAY))
            structure.time_range = self._extract_time_range(parsed)

            # 6. WHERE 조건
            where = parsed.find(exp.Where)
            if where:
                for condition in where.find_all(exp.EQ):
                    left = condition.left
                    right = condition.right
                    if isinstance(left, exp.Column):
                        col_name = left.name
                        value = None
                        if isinstance(right, exp.Literal):
                            value = right.this
                        elif isinstance(right, exp.Identifier):
                            value = right.name
                        if col_name and value is not None:
                            structure.filters.append({
                                "column": col_name,
                                "op": "=",
                                "value": str(value),
                            })

            # 7. 집계 함수
            for agg in parsed.find_all(exp.Count, exp.Sum, exp.Avg, exp.Max, exp.Min):
                agg_sql = agg.sql(dialect="bigquery")
                if agg_sql not in structure.aggregations:
                    structure.aggregations.append(agg_sql)

            # 8. GROUP BY
            group = parsed.find(exp.Group)
            if group:
                for col in group.expressions:
                    col_sql = col.sql(dialect="bigquery")
                    if col_sql not in structure.group_by_cols:
                        structure.group_by_cols.append(col_sql)

            # 9. ORDER BY
            order = parsed.find(exp.Order)
            if order:
                for col in order.expressions:
                    col_sql = col.sql(dialect="bigquery")
                    if col_sql not in structure.order_by_cols:
                        structure.order_by_cols.append(col_sql)

            # 10. LIMIT
            limit = parsed.find(exp.Limit)
            if limit and isinstance(limit.expression, exp.Literal):
                try:
                    structure.limit = int(limit.expression.this)
                except (ValueError, TypeError):
                    pass

        except Exception as e:
            logger.warning(f"SQL 구조 추출 중 에러: {e}")

        return structure

    def _extract_time_range(self, parsed) -> dict | None:
        """DATE_SUB(CURRENT_DATE(), INTERVAL N DAY) 패턴에서 시간 범위 추출"""
        try:
            # SQL 문자열에서 정규식으로 INTERVAL 패턴 찾기
            sql_text = parsed.sql(dialect="bigquery")
            match = re.search(
                r'INTERVAL\s+(\d+)\s+(DAY|WEEK|MONTH|YEAR)',
                sql_text,
                re.IGNORECASE
            )
            if match:
                return {
                    "amount": int(match.group(1)),
                    "unit": match.group(2).upper(),
                }
        except Exception:
            pass
        return None
