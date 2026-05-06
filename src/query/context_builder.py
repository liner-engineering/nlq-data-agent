"""
LLM 프롬프트 컨텍스트 빌더

BigQuery 스키마, 성공 사례, 금지 패턴을 LLM 프롬프트로 조합하여
LLM에 최적화된 프롬프트를 생성합니다.
"""

from typing import Any

from src.bigquery_context import (
    ANTIPATTERNS,
    BIGQUERY_SCHEMA,
    SAMPLE_DATA,
    SECTORS,
    SUCCESSFUL_QUERIES,
)
from src.exceptions import ContextBuildingError


class ContextBuilder:
    """LLM 프롬프트 컨텍스트 생성기

    BigQuery 스키마, 성공 사례, 안티패턴, 비즈니스 규칙을
    구조화된 프롬프트로 조합합니다.

    Example:
        builder = ContextBuilder()
        prompt = builder.build_prompt("섹터별 D+7 리텐션이 뭐야?")
    """

    SYSTEM_PROMPT = """당신은 liner의 BigQuery 데이터 분석 전문가입니다.

사용자의 자연어 쿼리를 정확한 BigQuery SQL로 변환하는 것이 목표입니다.

## 핵심 규칙

1. **항상 DISTINCT 사용**: 특히 조인 후 중복 제거
2. **GROUP BY 필수**: 집계 함수 사용 시 반드시 포함
3. **event_properties는 JSON**: JSON_EXTRACT_SCALAR()로 추출
4. **테이블명 전체 경로**: liner-219011.analysis.EVENTS_296805
5. **날짜 형식**: YYYY-MM-DD (따옴표 포함)
6. **샘플 크기**: HAVING 절로 최소 10명 이상 확인

## 응답 형식

SQL 코드블록만 반환. 설명 없음.

```sql
SELECT ...
```
"""

    def __init__(self) -> None:
        """초기화"""
        self.schema = BIGQUERY_SCHEMA
        self.success_queries = SUCCESSFUL_QUERIES
        self.antipatterns = ANTIPATTERNS
        self.sample_data = SAMPLE_DATA
        self.sectors = SECTORS

    def build_prompt(self, user_query: str) -> str:
        """
        사용자 쿼리에 대한 완전한 LLM 프롬프트 생성

        Args:
            user_query: 사용자의 자연어 쿼리 (예: "섹터별 D+7 리텐션")

        Returns:
            LLM에 전달할 완전한 프롬프트

        Raises:
            ContextBuildingError: 프롬프트 구성 실패
        """
        try:
            parts = [
                self.SYSTEM_PROMPT,
                "\n" + "=" * 80 + "\n",
                self._build_schema_section(),
                "\n" + "=" * 80 + "\n",
                self._build_success_examples_section(),
                "\n" + "=" * 80 + "\n",
                self._build_antipatterns_section(),
                "\n" + "=" * 80 + "\n",
                self._build_business_rules_section(),
                "\n" + "=" * 80 + "\n\n",
                f"## 사용자 쿼리\n\n{user_query}\n\n",
                "위 규칙과 예시를 참고하여 BigQuery SQL을 작성하세요.",
            ]

            return "".join(parts)

        except Exception as e:
            raise ContextBuildingError(f"프롬프트 생성 실패: {str(e)}") from e

    def _build_schema_section(self) -> str:
        """
        테이블 스키마 섹션 생성

        Returns:
            마크다운 형식의 스키마 정보
        """
        parts = ["## BigQuery 테이블 정의\n"]

        for table_key, table_info in self.schema.items():
            parts.append(f"\n### {table_key}\n")
            parts.append(f"**설명**: {table_info['description']}\n")

            if "row_count_estimate" in table_info:
                parts.append(f"**행 수**: {table_info['row_count_estimate']}\n")

            parts.append("\n**컬럼**:\n\n")

            for col_name, col_info in table_info["columns"].items():
                col_type = col_info["type"]
                nullable = "nullable" if col_info.get("nullable", True) else "NOT NULL"
                desc = col_info.get("description", "")

                parts.append(f"- `{col_name}` ({col_type}, {nullable}): {desc}\n")

                if "examples" in col_info:
                    examples = ", ".join(str(e) for e in col_info["examples"][:3])
                    parts.append(f"  예: {examples}\n")

        return "".join(parts)

    def _build_success_examples_section(self) -> str:
        """
        성공한 쿼리 예시 섹션

        Returns:
            마크다운 형식의 쿼리 예시
        """
        parts = ["## 성공한 쿼리 예시\n"]

        for i, (key, info) in enumerate(list(self.success_queries.items())[:3], 1):
            parts.append(f"\n### 예시 {i}: {info['description']}\n")
            parts.append(f"**사용 사례**: {info['use_case']}\n\n")
            parts.append(f"```sql\n{info['sql'].strip()}\n```\n")

        return "".join(parts)

    def _build_antipatterns_section(self) -> str:
        """
        금지 패턴 섹션

        Returns:
            마크다운 형식의 안티패턴 정보
        """
        parts = ["## 반드시 피할 패턴\n"]

        for i, pattern in enumerate(self.antipatterns[:5], 1):
            parts.append(f"\n### {i}. {pattern['problem']}\n")
            parts.append(f"**잘못됨**: `{pattern['pattern']}`\n")
            parts.append(f"**해결**: {pattern['fix']}\n")

        return "".join(parts)

    def _build_business_rules_section(self) -> str:
        """
        비즈니스 규칙 섹션

        Returns:
            마크다운 형식의 비즈니스 규칙
        """
        parts = ["## 비즈니스 규칙\n"]

        # 리텐션 정의
        parts.append("\n### 리텐션 정의\n")
        parts.append("- **D+7**: 첫 이벤트 후 7~13일 사이 재활동\n")
        parts.append("- 같은 날짜 제외 (다른 날짜에 재활동)\n")

        # 섹터
        parts.append("\n### 섹터 분류\n")
        for sector, info in self.sectors.items():
            keywords = ", ".join(info["keywords"][:3])
            parts.append(f"- **{sector}**: {info['description']} (예: {keywords})\n")

        # 시간대
        parts.append("\n### 시간대\n")
        parts.append("- event_time: UTC 기준\n")
        parts.append("- 한국 시간: UTC + 9시간\n")

        # 샘플 크기
        parts.append("\n### 샘플 크기\n")
        parts.append("- **100+**: 통계적 신뢰성 높음\n")
        parts.append("- **10-100**: 주의 필요\n")
        parts.append("- **<10**: 결과 신뢰 불가\n")
        parts.append("- `HAVING COUNT(DISTINCT user_id) >= 10`으로 제한\n")

        return "".join(parts)

    def get_system_prompt(self) -> str:
        """시스템 프롬프트만 반환"""
        return self.SYSTEM_PROMPT

    def get_schema_only(self) -> str:
        """스키마 정보만 반환"""
        return self._build_schema_section()

    def get_examples_only(self) -> str:
        """성공 사례만 반환"""
        return self._build_success_examples_section()
