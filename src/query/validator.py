"""
SQL 검증 모듈

BigQuery SQL의 문법, 테이블명, 필드명, 안티패턴을 검증합니다.
캐싱 및 성능 최적화를 포함한 프로덕션 레디 구현입니다.
"""

import re
from functools import lru_cache
from typing import Any

from src.bigquery_context import ANTIPATTERNS, BIGQUERY_SCHEMA
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

    def validate(self, sql: str) -> ValidationResult:
        """
        SQL 검증

        Args:
            sql: 검증할 BigQuery SQL

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
