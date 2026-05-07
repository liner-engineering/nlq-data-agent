"""
LLM 프롬프트 컨텍스트 빌더

BigQuery 스키마, 성공 사례, 금지 패턴을 LLM 프롬프트로 조합하여
LLM에 최적화된 프롬프트를 생성합니다.
"""

from typing import Any

from src.bigquery_context import (
    ANTIPATTERNS,
    BIGQUERY_SCHEMA,
    SAMPLE_EVENTS,
    SECTORS,
    SUCCESSFUL_QUERIES,
)
from src.exceptions import ContextBuildingError
from src.query.example_selector import get_selector


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

## ⚠️ CRITICAL: 시간 범위 필수

**모든 EVENTS_296805 쿼리는 반드시 시간 범위를 포함해야 합니다.**

### 시간 범위 해석 규칙

- **"지난 30일"**: `WHERE DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND DATE(event_time) < CURRENT_DATE()`
  (오늘 제외, 완성된 30일)

- **"이번 주"**: `WHERE DATE(event_time) >= DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)) AND DATE(event_time) <= CURRENT_DATE()`
  (월요일부터 오늘까지)

- **"어제"**: `WHERE DATE(event_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)`

- **"지난달"**: `WHERE DATE(event_time) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) AND DATE(event_time) < DATE_TRUNC(CURRENT_DATE(), MONTH)`
  (전월 1일~말일, 이번달 1일 제외)

❌ 틀린 예: `DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)` (7일 전, "이번 주"가 아님!)

절대 시간 필터 없이 풀스캔하지 말 것 (비용, 성능, 정확성 문제)

## ⚠️ CRITICAL: 제품(Service) 필터링

**사용자가 "Write", "Scholar", "AI Search" 등 제품을 언급하면:**

반드시 `liner_product` 필드를 사용하세요. 다른 필드명은 없습니다!

```sql
WHERE JSON_EXTRACT_SCALAR(event_properties, '$.liner_product') = 'write'        -- Write
   OR JSON_EXTRACT_SCALAR(event_properties, '$.liner_product') = 'researcher'   -- Scholar
   OR JSON_EXTRACT_SCALAR(event_properties, '$.liner_product') = 'ai_search'    -- AI Search
```

❌ 틀린 예: `'$.service'`, `'$.product'` - 이 필드들은 존재하지 않음!
✓ 올바른 예: `'$.liner_product'` - 반드시 이 필드만 사용

## ⚠️ CRITICAL: 사용자 세그먼트 분류 방법

**중요**: Liner의 사용자 분류는 "쿼리 내용"으로 한다!

- make_chat 이벤트의 query 텍스트를 분석한다
- 예: "이력서", "취업", "면접" 키워드 → "취업 관심 사용자"
- 예: "영문", "수료증" 키워드 → "교육 관심 사용자"
- 예: "컨설팅", "법률" 키워드 → "비즈니스 사용자"

## 핵심 SQL 패턴

1. **make_chat 이벤트에서 쿼리 추출**:
   ```sql
   JSON_EXTRACT_SCALAR(event_properties, '$.query') as query_text
   ```

2. **키워드로 사용자 필터링**:
   ```sql
   WHERE event_type = 'make_chat'
     AND LOWER(JSON_EXTRACT_SCALAR(event_properties, '$.query')) LIKE '%키워드%'
   ```

3. **구독 데이터 쿼리 패턴**:
   ```sql
   -- 활성 구독자 (현재)
   WHERE status = 'active' AND subscription_ended_at IS NULL

   -- 특정 기간 신규 구독자
   WHERE DATE(subscription_start_at) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH)
     AND DATE(subscription_start_at) < DATE_TRUNC(CURRENT_DATE(), MONTH)
   ```

4. **사용자 마스터 테이블 조인** (선택):
   ```sql
   FROM `liner-219011.analysis.EVENTS_296805` e
   JOIN `liner-219011.like.dim_user` u ON e.user_id = u.user_id
   ```

## 규칙

1. **DISTINCT 사용**: 조인 후 중복 제거
2. **GROUP BY 필수**: 집계 함수 사용 시
3. **날짜 형식**: YYYY-MM-DD (따옴표 포함)
4. **테이블 전체 경로**: liner-219011.analysis.EVENTS_296805
5. **구독 테이블 필터링**:
   - 활성 구독자: `WHERE status = 'active' AND subscription_ended_at IS NULL` (두 조건 모두 필수)
   - 날짜 필터: TIMESTAMP 필드는 DATE() 변환 후 비교 (`DATE(subscription_start_at) >= ...`)
   - 절대 불필요한 테이블을 JOIN하지 말 것 (구독 테이블만으로 충분)

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
        self.sample_events = SAMPLE_EVENTS
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
            self._user_query = user_query
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
        테이블 스키마 섹션 (CREATE TABLE 형식 + 역할 태깅)

        각 컬럼의 역할(ENTITY, DIMENSION, TIME, SEMI_STRUCTURED, ATTRIBUTE)과
        각 테이블의 금지 용도(not_for)를 명시하여 LLM의 오류를 사전에 방지.

        Returns:
            SQL 주석 형식의 스키마 정보
        """
        parts = ["/* Given the following BigQuery schema: */\n\n"]

        for table_key, table_info in self.schema.items():
            full_name = table_info.get("full_name", table_key)
            parts.append(f"CREATE TABLE `{full_name}` (\n")

            col_lines = []
            for col_name, col_info in table_info["columns"].items():
                col_type = col_info["type"]
                nullable = "" if col_info.get("nullable", True) else " NOT NULL"

                comment_parts = []
                if "role" in col_info:
                    comment_parts.append(f"[{col_info['role']}]")
                comment_parts.append(col_info.get("description", ""))

                if "examples" in col_info and col_info["examples"]:
                    examples = [str(e) for e in col_info["examples"][:3] if e is not None]
                    if examples:
                        comment_parts.append(f"예: {', '.join(examples)}")

                comment = " | ".join(comment_parts)
                col_lines.append(f"  {col_name} {col_type}{nullable},  -- {comment}")

            if col_lines:
                col_lines[-1] = col_lines[-1].rstrip(",")

            parts.append("\n".join(col_lines))
            parts.append(f"\n);\n-- {table_info['description']}\n")

            # 금지사항 추가 — LLM이 잘못된 테이블 선택 안 하도록
            if "not_for" in table_info:
                parts.append("-- 주의: 다음 용도로는 이 테이블 사용 금지:\n")
                for item in table_info["not_for"]:
                    parts.append(f"--   - {item}\n")
            parts.append("\n")

        return "".join(parts)

    def _build_success_examples_section(self) -> str:
        """
        성공한 쿼리 예시 섹션 (의미론적 유사도 기반 동적 선택 - DAIL-SQL 패턴)

        Returns:
            마크다운 형식의 쿼리 예시
        """
        parts = ["## 성공한 쿼리 예시\n"]

        # 동적 예시 선택 (사용자 쿼리와 의미론적 유사도 기반)
        user_query = self._user_query if hasattr(self, '_user_query') else ""
        if not user_query:
            # fallback: 처음 3개 예시
            selected = list(self.success_queries.values())[:3]
        else:
            try:
                selector = get_selector()
                selected = selector.select_examples(user_query, top_k=3)
            except Exception:
                # 임베딩 오류 시 fallback
                selected = list(self.success_queries.values())[:3]

        # 마크다운 생성
        for i, info in enumerate(selected, 1):
            similarity = f" (유사도: {info.get('similarity_score', 0):.2f})" \
                if "similarity_score" in info else ""
            parts.append(f"\n### 예시 {i}: {info['description']}{similarity}\n")
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
