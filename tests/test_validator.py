"""
SQLValidator 테스트
"""

import pytest

from src.query.validator import SQLValidator


@pytest.fixture
def validator():
    """SQLValidator 인스턴스"""
    return SQLValidator()


def test_validate_valid_sql(validator):
    """유효한 SQL 검증"""
    sql = "SELECT user_id, COUNT(*) FROM `liner-219011.analysis.EVENTS_296805` GROUP BY user_id LIMIT 10"
    result = validator.validate(sql)

    assert result.valid is True
    assert len(result.errors) == 0


def test_validate_empty_sql(validator):
    """빈 SQL 검증"""
    from src.exceptions import SQLValidationError

    with pytest.raises(SQLValidationError):
        validator.validate("")


def test_validate_no_select(validator):
    """SELECT 없는 SQL 검증"""
    sql = "DELETE FROM table WHERE id = 1"
    result = validator.validate(sql)

    assert result.valid is False
    assert len(result.errors) > 0


def test_detect_antipattern_now(validator):
    """NOW() 사용 안티패턴 감지"""
    sql = "SELECT * FROM EVENTS_296805 WHERE DATE_DIFF(event_time, NOW(), DAY) < 7"
    result = validator.validate(sql)

    assert result.valid is False
    assert len(result.errors) > 0


def test_detect_antipattern_same_date(validator):
    """같은 날짜 필터링 안티패턴"""
    sql = "SELECT * FROM EVENTS_296805 WHERE DATE(event_time) BETWEEN '2026-03-01' AND '2026-03-01'"
    result = validator.validate(sql)

    assert result.valid is False
    assert len(result.errors) > 0


def test_validate_date_format(validator):
    """날짜 형식 검증"""
    sql = "SELECT * FROM EVENTS_296805 WHERE DATE(event_time) BETWEEN '2026-99-99' AND '2026-04-30'"
    result = validator.validate(sql)

    assert result.valid is False
    assert len(result.errors) > 0
