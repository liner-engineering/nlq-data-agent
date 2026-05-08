"""
SQL 검증 모듈

BigQuery SQL의 문법, 테이블명, 필드명, 안티패턴을 검증합니다.
캐싱 및 성능 최적화를 포함한 프로덕션 레디 구현입니다.
"""

import re
from functools import lru_cache
from typing import Any

from src.bigquery_context import ANTIPATTERNS, BIGQUERY_SCHEMA
from src.bigquery_context.glossary import GLOSSARY
from src.exceptions import SQLValidationError
from src.types import ValidationResult


class SQLValidator:
    """BigQuery SQL 검증기

    SQL의 다양한 측면을 검증합니다:
    - 기본 SQL 문법 (SELECT 시작)
    - 테이블명 존재 여부
    - 필드명 유효성
    - 안티패턴 감지
    - 날짜 형식
    - JSON 추출 패턴
    - GROUP BY 절 검증

    Example:
        validator = SQLValidator()
        result = validator.validate(sql)
        if result.valid:
            print("SQL은 유효합니다")
        else:
            print(f"오류: {result.errors}")
    """

    # 정규식 캐싱 (성능 최적화)
    _CTE_PATTERN = re.compile(r"(?:WITH|,)\s+(\w+)\s+AS\s*\(", re.IGNORECASE)  # CTE 이름 추출 (여러 개 지원)
    _TABLE_PATTERN = re.compile(r"(?:FROM|JOIN)\s+`?([a-zA-Z0-9_.-]+)`?", re.IGNORECASE)
    _DATE_PATTERN = re.compile(r"[\"'](\d{4}-\d{2}-\d{2})[\"']")
    _BETWEEN_PATTERN = re.compile(
        r'BETWEEN\s+["\'](\d{4}-\d{2}-\d{2})["\']\s+AND\s+["\'](\d{4}-\d{2}-\d{2})["\']'
    )
    _AGGREGATE_PATTERN = re.compile(r"(COUNT|SUM|AVG|MIN|MAX)\s*\(", re.IGNORECASE)
    _USER_SPECIFIC_PATTERN = re.compile(
        r"user_id\s*=\s*(?:\d+|[\"'][^\"']+[\"'])",
        re.IGNORECASE,
    )

    def __init__(self) -> None:
        """초기화"""
        self.table_names = list(BIGQUERY_SCHEMA.keys())
        self.full_table_names = [t["full_name"] for t in BIGQUERY_SCHEMA.values()]
        self.antipatterns = ANTIPATTERNS

    def validate(self, sql: str, user_query: str | None = None) -> ValidationResult:
        """
        SQL 검증

        Args:
            sql: 검증할 BigQuery SQL
            user_query: 사용자 쿼리 (glossary lint용, 선택)

        Returns:
            ValidationResult: 검증 결과

        Raises:
            SQLValidationError: 심각한 에러 발생 시
        """
        errors: list[str] = []
        warnings: list[str] = []
        suggestions: list[str] = []

        # 기본 검증
        if not sql or not sql.strip():
            raise SQLValidationError("SQL이 비어있습니다", sql=sql)

        sql_upper = sql.upper()

        # 1. SELECT 또는 WITH 시작 확인
        if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
            errors.append("SQL은 SELECT 또는 WITH로 시작해야 합니다")

        # 2. 테이블명 검증
        table_errors = self._validate_table_names(sql)
        errors.extend(table_errors)

        # 3. 필드명 검증
        field_warnings = self._validate_field_names(sql)
        warnings.extend(field_warnings)

        # 4. 안티패턴 감지
        pattern_errors, pattern_warnings = self._detect_antipatterns(sql)
        errors.extend(pattern_errors)
        warnings.extend(pattern_warnings)

        # 5. 날짜 형식 검증
        date_errors = self._validate_date_format(sql)
        errors.extend(date_errors)

        # 6. JSON 패턴 검증
        json_warnings = self._validate_json_pattern(sql)
        warnings.extend(json_warnings)

        # 7. GROUP BY 검증
        groupby_warnings = self._validate_group_by(sql)
        warnings.extend(groupby_warnings)

        # 8. 시간 범위 검증
        time_range_warnings = self._validate_time_range(sql)
        warnings.extend(time_range_warnings)

        # 9. Glossary 기반 lint (사용자 쿼리가 있을 때만)
        if user_query:
            glossary_errors = self._lint_glossary_violations(sql, user_query)
            errors.extend(glossary_errors)

            # 9.5 컬럼-값 매핑 검증 (forbidden_in_columns)
            value_col_errors = self._validate_value_column_mapping(sql, user_query)
            errors.extend(value_col_errors)

        # 10. SQL 의미성 검증 (무의미한 쿼리 패턴)
        meaningfulness_errors = self._validate_meaningfulness(sql)
        errors.extend(meaningfulness_errors)

        # 결과 반환
        is_valid = len(errors) == 0

        return ValidationResult(
            valid=is_valid,
            errors=list(set(errors)),  # 중복 제거
            warnings=list(set(warnings)),
            suggestions=suggestions,
        )

    def _validate_table_names(self, sql: str) -> list[str]:
        """
        테이블명 검증 (CTE 제외)

        Returns:
            오류 목록
        """
        errors: list[str] = []

        # CTE 이름 추출 (WITH 절에서)
        cte_names = set(self._CTE_PATTERN.findall(sql))

        for table in self._TABLE_PATTERN.findall(sql):
            # CTE 이름이면 스킵 (실제 테이블이 아님)
            if table in cte_names:
                continue

            # 프로젝트명 포함 여부 확인
            if "." in table:
                short_table = ".".join(table.split(".")[-2:])
            else:
                short_table = table

            # 테이블 존재 확인
            valid = any(
                t.endswith(short_table) or t == table
                for t in self.table_names + self.full_table_names
            )

            if not valid:
                errors.append(
                    f"테이블 '{table}'을 찾을 수 없습니다. "
                    f"사용 가능: {', '.join(self.table_names)}"
                )

        return errors

    def _validate_field_names(self, sql: str) -> list[str]:
        """
        필드명 기본 검증

        Returns:
            경고 목록
        """
        warnings: list[str] = []
        sql_lower = sql.lower()

        # EVENTS_296805 사용 시 필수 필드 확인
        if "events_296805" in sql_lower:
            # 집계 함수 사용 시 user_id 또는 amplitude_id 필요
            if ("GROUP BY" in sql.upper() or "COUNT" in sql.upper()) and (
                "user_id" not in sql_lower and "amplitude_id" not in sql_lower
            ):
                warnings.append(
                    "user_id 또는 amplitude_id 필드가 없습니다. "
                    "사용자 식별이 필요한 경우 추가하세요"
                )

            # 필터링 시 event_type 또는 event_properties 권장
            if ("WHERE" in sql.upper() or "HAVING" in sql.upper()) and (
                "event_type" not in sql_lower and "event_properties" not in sql_lower
            ):
                warnings.append(
                    "event_type 또는 event_properties를 이용한 필터링을 권장합니다"
                )

        return warnings

    def _detect_antipatterns(self, sql: str) -> tuple[list[str], list[str]]:
        """
        안티패턴 감지

        Returns:
            (에러 목록, 경고 목록)
        """
        errors: list[str] = []
        warnings: list[str] = []

        sql_upper = sql.upper()
        sql_lower = sql.lower()

        # 1. 특정 사용자만 조회하는 패턴 (WHERE 절에서만 감지, ON 절 제외)
        where_match = re.search(r"WHERE\s+(.*?)(?:GROUP BY|ORDER BY|LIMIT|;|$)", sql, re.IGNORECASE | re.DOTALL)
        if where_match and "GROUP BY" not in sql_upper:
            where_clause = where_match.group(1)
            if self._USER_SPECIFIC_PATTERN.search(where_clause):
                warnings.append(
                    "특정 사용자 1명만 조회하는 것 같습니다. "
                    "GROUP BY를 사용해서 집계하세요"
                )

        # 2. NOW() 함수 사용
        if "NOW()" in sql_upper and "DATE_DIFF" in sql_upper:
            errors.append(
                "DATE_DIFF(event_time, NOW())를 사용하면 시간이 지나면서 부정확해집니다. "
                "특정 날짜 기준으로 계산하세요"
            )

        # 3. event_type과 LIKE 조합
        if "event_type" in sql_lower and "LIKE" in sql_upper:
            warnings.append("event_type은 정확한 문자열이므로 LIKE 대신 IN()을 사용하세요")

        # 4. 같은 날짜 범위 필터링
        for start_date, end_date in self._BETWEEN_PATTERN.findall(sql):
            if start_date == end_date:
                errors.append(
                    f"같은 날짜({start_date})로 필터링하면 1일 데이터만 조회됩니다. "
                    "최소 30일 이상의 기간을 설정하세요"
                )

        return errors, warnings

    def _validate_date_format(self, sql: str) -> list[str]:
        """
        날짜 형식 검증 (YYYY-MM-DD)

        Returns:
            오류 목록
        """
        errors: list[str] = []

        for date_str in self._DATE_PATTERN.findall(sql):
            try:
                year, month, day = map(int, date_str.split("-"))
                if not (1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31):
                    errors.append(f"유효하지 않은 날짜: {date_str}")
            except (ValueError, IndexError):
                errors.append(f"날짜 형식 오류: {date_str}")

        return errors

    def _validate_json_pattern(self, sql: str) -> list[str]:
        """
        JSON 추출 패턴 검증

        Returns:
            경고 목록
        """
        warnings: list[str] = []

        if "event_properties" in sql.lower() and "JSON_EXTRACT" not in sql.upper():
            warnings.append(
                "event_properties는 JSON 필드입니다. "
                "JSON_EXTRACT_SCALAR()를 사용하여 값을 추출하세요"
            )

        return warnings

    def _validate_group_by(self, sql: str) -> list[str]:
        """
        GROUP BY 절 검증

        Returns:
            경고 목록
        """
        warnings: list[str] = []

        # 집계 함수 사용 시 GROUP BY 필요
        if self._AGGREGATE_PATTERN.search(sql) and "GROUP BY" not in sql.upper():
            # COUNT(*)는 전체 집계이므로 GROUP BY 불필요
            if "COUNT(*)" not in sql.upper():
                warnings.append("집계 함수를 사용하면 GROUP BY절을 추가하는 것을 권장합니다")

        return warnings

    def _validate_time_range(self, sql: str) -> list[str]:
        """
        시간 범위 검증 (EVENTS_296805 사용 시)

        Returns:
            경고 목록
        """
        warnings: list[str] = []
        sql_lower = sql.lower()

        if "events_296805" in sql_lower:
            # 시간 범위 필터 확인
            has_date_filter = (
                "date(event_time)" in sql_lower
                or "event_time >=" in sql_lower
                or "event_time <=" in sql_lower
                or "event_time between" in sql_lower
                or "date_diff" in sql_lower
                or "date_sub" in sql_lower
                or "date_add" in sql_lower
            )

            if not has_date_filter:
                warnings.append(
                    "EVENTS_296805 쿼리에 시간 범위 필터가 없습니다. "
                    "비용과 성능을 위해 WHERE DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 추가를 권장합니다"
                )

        return warnings

    def _lint_glossary_violations(self, sql: str, user_query: str) -> list[str]:
        """
        Glossary 기반 lint: 질문의 도메인 용어가 SQL에서 올바르게 처리되었는지 확인

        Args:
            sql: 검증할 SQL
            user_query: 사용자 쿼리

        Returns:
            위반 항목 (에러)
        """
        errors: list[str] = []
        sql_lower = sql.lower()
        user_query_lower = user_query.lower()

        # 각 glossary 항목을 확인
        for term, info in GLOSSARY.items():
            # 용어가 질문에 포함되어 있는가?
            term_found = False

            # 주 용어
            if term in user_query_lower:
                term_found = True

            # 동의어
            if not term_found and 'alternative_terms' in info:
                for alt in info['alternative_terms']:
                    if alt.lower() in user_query_lower:
                        term_found = True
                        break

            if not term_found:
                continue

            # 용어가 질문에 있으면, anti-pattern 확인
            if 'anti_patterns' in info:
                for anti_pattern in info['anti_patterns']:
                    # anti_pattern 문자열을 정규식으로 변환 (간단한 서브스트링 검색)
                    # 예: "LIKE '%credit%'" → "like '%credit%'"
                    pattern_to_check = anti_pattern.replace("❌ ", "").lower()

                    # 패턴 검사 (대소문자 무시)
                    if "like" in pattern_to_check and "%" in pattern_to_check:
                        # LIKE '%X%' 패턴 검사
                        like_match = re.search(r"like\s+['\"]%([^'\"]+)%['\"]", sql_lower)
                        if like_match:
                            keyword = like_match.group(1)
                            if keyword in user_query_lower:
                                errors.append(
                                    f"[Glossary Violation] '{term}' 질문에서 LIKE '%{keyword}%' 사용 금지. "
                                    f"올바른 소스: {info.get('primary_source', 'schema 확인')}"
                                )

                    # 필드명 체크 (예: start_date는 subscription_start_at이어야 함)
                    elif "field" not in pattern_to_check:
                        # 직접 문자열 검사
                        if pattern_to_check in sql_lower:
                            errors.append(
                                f"[Glossary Violation] '{term}' 관련 쿼리에서 금지된 패턴 감지: {anti_pattern}"
                            )

        return errors

    def _validate_meaningfulness(self, sql: str) -> list[str]:
        """
        무의미한 SQL 패턴 검출

        다음 패턴을 감지:
        - 리터럴 상수만 SELECT하는 경우
        - 집계 없이 LIMIT 1 사용
        """
        errors: list[str] = []
        sql_upper = sql.upper()

        # SELECT 절 추출
        select_match = re.search(r"SELECT\s+(.+?)\s+FROM", sql_upper, re.DOTALL)
        if select_match:
            select_clause = select_match.group(1).strip()

            # 패턴 1: 리터럴 상수만 SELECT하는 경우 ('Unknown' AS col)
            # 예: SELECT 'Unknown' AS user_segment FROM ...
            if re.match(r"^\s*['\"]([^'\"]*)['\"]\s+AS\s+\w+\s*$", select_clause, re.IGNORECASE):
                errors.append(
                    "리터럴 상수만 SELECT하고 있습니다. "
                    "실제 데이터 컬럼이나 집계(COUNT, SUM 등)가 필요합니다."
                )

            # 패턴 2: 집계 없는 LIMIT 1 (ORDER BY는 가능)
            # LIMIT 1 + 집계 함수 없음 + ORDER BY 없음 = 의심
            if "LIMIT 1" in sql_upper or "LIMIT\n1" in sql_upper:
                has_aggregate = any(
                    agg in sql_upper
                    for agg in ["COUNT(", "SUM(", "AVG(", "MAX(", "MIN(", "GROUP BY"]
                )
                has_order_by = "ORDER BY" in sql_upper

                if not has_aggregate and not has_order_by:
                    errors.append(
                        "집계나 정렬 없이 LIMIT 1을 사용하고 있습니다. "
                        "분석 의도가 불명확합니다. "
                        "GROUP BY, 정렬, 또는 집계 함수를 추가하세요."
                    )

        return errors

    def _validate_value_column_mapping(self, sql: str, user_query: str) -> list[str]:
        """
        Glossary의 forbidden_in_columns 기반 검증.

        어떤 컬럼에 어떤 값이 와선 안 되는지를 검증합니다.
        예: pro/max는 plan_id에서만 유효하고, liner_product에 와선 안 됨.

        Args:
            sql: 검증할 SQL
            user_query: 사용자 쿼리

        Returns:
            위반 항목 (에러)
        """
        errors: list[str] = []
        user_query_lower = user_query.lower()

        for term, info in GLOSSARY.items():
            # 용어가 질문에 포함되어 있는가?
            term_found = False

            # 주 용어
            if term in user_query_lower:
                term_found = True

            # 동의어
            if not term_found and 'alternative_terms' in info:
                for alt in info.get('alternative_terms', []):
                    if alt.lower() in user_query_lower:
                        term_found = True
                        break

            if not term_found:
                continue

            # 금지된 컬럼-값 매핑 확인
            forbidden_list = info.get('forbidden_in_columns', [])
            if not forbidden_list:
                continue

            for forbidden in forbidden_list:
                wrong_col = forbidden.get('wrong_column', '')
                wrong_vals = forbidden.get('wrong_values', [])
                reason = forbidden.get('reason', '')

                if not wrong_col or not wrong_vals:
                    continue

                # 각 잘못된 값에 대해 패턴 검사
                for val in wrong_vals:
                    # 패턴들을 정의 (LIKE, =, IN, JSON_EXTRACT_SCALAR 형태 모두 포함)
                    patterns = [
                        # 패턴 1: column = 'val'
                        rf"\b{re.escape(wrong_col)}\s*=\s*['\"]?{re.escape(val)}['\"]?",
                        # 패턴 2: column IN (...'val'...)
                        rf"\b{re.escape(wrong_col)}\s*IN\s*\([^)]*['\"]?{re.escape(val)}['\"]?[^)]*\)",
                        # 패턴 3: column LIKE '%val%'
                        rf"\b{re.escape(wrong_col)}\s*LIKE\s*['\"]%?{re.escape(val)}%?['\"]",
                        # 패턴 4: JSON_EXTRACT_SCALAR(..., '$.column') = 'val'
                        rf"JSON_EXTRACT_SCALAR\s*\([^,]+,\s*['\"]?\$\.{re.escape(wrong_col)}['\"]?\s*\)\s*=\s*['\"]?{re.escape(val)}['\"]?",
                        # 패턴 5: JSON_EXTRACT_SCALAR(..., '$.column') IN (...)
                        rf"JSON_EXTRACT_SCALAR\s*\([^,]+,\s*['\"]?\$\.{re.escape(wrong_col)}['\"]?\s*\)\s*IN\s*\([^)]*['\"]?{re.escape(val)}['\"]?[^)]*\)",
                    ]

                    for pattern in patterns:
                        if re.search(pattern, sql, re.IGNORECASE):
                            error_msg = (
                                f"[Glossary Violation] '{term}' 질문이지만 "
                                f"'{wrong_col}' 컬럼에서 '{val}'을(를) 찾고 있습니다. "
                                f"이유: {reason}. "
                                f"올바른 위치: {info.get('primary_source', 'schema 확인')}"
                            )
                            errors.append(error_msg)
                            break  # 한 패턴 매칭되면 다음 값으로

        return errors
