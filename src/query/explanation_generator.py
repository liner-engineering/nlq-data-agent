"""
SQL 설명 생성기

SQL 구조 정보를 입력받아 사용자 친화적인 설명을 생성합니다.
- 단계 1: 구조 정보로 자명한 부분 채우기 (템플릿)
- 단계 2: LLM으로 의도와 caveats 추론
- 단계 3: 마크다운으로 포매팅
"""

import json
import re
from typing import Any

from openai import OpenAI

from src.bigquery_context import BIGQUERY_SCHEMA
from src.logging_config import ContextualLogger
from src.query.sql_analyzer import SQLStructure

logger = ContextualLogger(__name__)


class ExplanationGenerator:
    """SQL 설명 생성 (LLM 의존도 최소화)"""

    def __init__(self, llm_client: OpenAI | None = None, model: str = ""):
        self.client = llm_client
        self.model = model
        self.schema = BIGQUERY_SCHEMA

    def generate(self, user_query: str, sql: str, structure: SQLStructure) -> dict[str, Any]:
        """
        SQL 설명 생성

        Args:
            user_query: 사용자 질문
            sql: 생성된 SQL
            structure: SQL 구조 정보

        Returns:
            {
                "intent": "분석 의도",
                "tables": [...],
                "filters": [...],
                "calculation": "계산 방식",
                "time_range": "시간 범위",
                "caveats": ["주의사항 1", ...],
            }
        """
        # 단계 1: 결정론적 부분 (LLM 없이)
        deterministic = self._build_deterministic_parts(structure)

        # 단계 2: LLM으로 의도와 caveats 추론 (선택사항)
        llm_part = {}
        if self.client and self.model:
            llm_part = self._llm_explain_intent_and_caveats(user_query, sql, structure)

        # 단계 3: 병합
        result = {**deterministic, **llm_part}

        logger.info(f"설명 생성 완료: intent={result.get('intent', '')[:50]}...")
        return result

    def _build_deterministic_parts(self, s: SQLStructure) -> dict[str, Any]:
        """LLM 없이 SQL 구조에서 직접 부분 생성"""
        result: dict[str, Any] = {}

        # 테이블 설명
        tables = []
        for tbl in s.tables_used:
            table_info = self._find_table_info(tbl)
            tables.append({
                "name": tbl,
                "description": table_info.get("description", ""),
            })
        result["tables"] = tables

        # 필터 설명
        filters = []
        for f in s.filters:
            col_info = self._find_column_info(f["column"])
            human_readable = f"{col_info.get('description', f['column'])}: {f['value']}"
            filters.append({
                "raw": f"{f['column']} = '{f['value']}'",
                "human_readable": human_readable,
            })
        result["filters"] = filters

        # 시간 범위
        if s.time_range:
            unit_kr = {"DAY": "일", "WEEK": "주", "MONTH": "개월", "YEAR": "년"}
            unit = unit_kr.get(s.time_range["unit"], s.time_range["unit"])
            result["time_range"] = f"최근 {s.time_range['amount']}{unit}"
        else:
            result["time_range"] = None

        # 계산 방식
        calc_parts = []
        if s.group_by_cols:
            group_str = ", ".join(s.group_by_cols)
            calc_parts.append(f"{group_str}로 그룹화")
        if s.aggregations:
            agg_str = ", ".join(s.aggregations)
            calc_parts.append(f"집계: {agg_str}")
        result["calculation"] = " · ".join(calc_parts) if calc_parts else "단순 조회"

        return result

    def _llm_explain_intent_and_caveats(
        self, user_query: str, sql: str, s: SQLStructure
    ) -> dict[str, Any]:
        """LLM으로 의도와 주의사항만 추론 (좁은 범위, 빠른 응답)"""

        prompt = f"""사용자가 데이터 분석 질문을 했고, 시스템이 SQL을 생성했습니다.
이 SQL의 분석 의도를 한 문장으로 표현하고, 사용자가 알아야 할 주의사항을 1-3개 정도 알려주세요.

사용자 질문: "{user_query}"

생성된 SQL:
```sql
{sql}
```

JSON으로만 응답하세요:
{{
  "intent": "한 문장의 분석 의도 (사용자에게 보여줄 친절한 표현)",
  "caveats": ["주의사항 1", "주의사항 2"]
}}

주의사항 가이드:
- "같은 사용자가 여러 번 활동해도 1명으로 카운트됩니다" (DISTINCT 사용 시)
- "이 수치는 누적값입니다 (일별 추이가 아닙니다)" (GROUP BY 없을 시)
- "현재 활성 구독자만 포함합니다" (WHERE subscription_ended_at IS NULL)
- "쿼리 텍스트에 '키워드' 포함된 케이스만입니다" (LIKE 필터 사용 시)
- "비로그인 사용자는 제외됩니다" (user_id IS NOT NULL)

중요: caveats는 SQL의 한계나 사용자가 쉽게 오해할 수 있는 지점을 짚어주세요.
"""

        try:
            logger.info("LLM으로 의도/caveats 추론 중...")
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=400,
            )

            text = response.choices[0].message.content.strip()
            json_match = re.search(r'\{.*\}', text, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
                logger.info(f"LLM 완료: intent={result.get('intent', '')[:50]}...")
                return result
        except Exception as e:
            logger.warning(f"LLM 설명 생성 실패: {e}")

        return {"intent": "", "caveats": []}

    def _find_table_info(self, table_name: str) -> dict[str, Any]:
        """스키마에서 테이블 정보 찾기"""
        for key, info in self.schema.items():
            if table_name.lower() in key.lower():
                return info
        return {}

    def _find_column_info(self, column: str) -> dict[str, Any]:
        """스키마에서 컬럼 정보 찾기"""
        for table_info in self.schema.values():
            cols = table_info.get("columns", {})
            if column in cols:
                return cols[column]
        return {}


class ExplanationFormatter:
    """메타데이터를 사용자 친화적 마크다운으로 포매팅"""

    @staticmethod
    def format(explanation: dict[str, Any]) -> str:
        """
        설명 딕셔너리를 마크다운으로 포매팅

        Args:
            explanation: ExplanationGenerator.generate() 결과

        Returns:
            포매팅된 마크다운 문자열
        """
        parts = []

        # 의도
        if explanation.get("intent"):
            parts.append(f"### 📊 분석 의도\n\n{explanation['intent']}\n")

        # 사용한 데이터
        tables = explanation.get("tables", [])
        if tables:
            parts.append("### 📋 사용한 데이터\n")
            for t in tables:
                desc = f" — {t['description']}" if t.get('description') else ""
                parts.append(f"- **{t['name']}**{desc}")
            parts.append("")

        # 적용된 조건
        filters = explanation.get("filters", [])
        time_range = explanation.get("time_range")
        if filters or time_range:
            parts.append("### 🔍 적용된 조건\n")
            if time_range:
                parts.append(f"- **시간 범위**: {time_range}")
            for f in filters:
                parts.append(f"- {f['human_readable']}")
            parts.append("")

        # 계산 방식
        calc = explanation.get("calculation")
        if calc:
            parts.append(f"### 📐 계산 방식\n\n{calc}\n")

        # 주의사항
        caveats = explanation.get("caveats", [])
        if caveats:
            parts.append("### 💡 참고\n")
            for c in caveats:
                parts.append(f"- {c}")
            parts.append("")

        return "\n".join(parts)
