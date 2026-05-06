"""
로깅 설정

구조화된 로깅으로 디버깅 및 모니터링을 지원합니다.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Any, Optional

from src.config import LoggingConfig


class JSONFormatter(logging.Formatter):
    """JSON 형식의 로거"""

    def format(self, record: logging.LogRecord) -> str:
        """로그 레코드를 JSON으로 변환"""
        log_data = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # 추가 속성 (extra로 전달된 정보)
        if hasattr(record, "context"):
            log_data["context"] = record.context
        if hasattr(record, "user_query"):
            log_data["user_query"] = record.user_query
        if hasattr(record, "sql"):
            log_data["sql"] = record.sql[:100]  # 처음 100자만
        if hasattr(record, "duration_ms"):
            log_data["duration_ms"] = record.duration_ms

        # 에러 정보
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    """텍스트 형식의 로거"""

    def format(self, record: logging.LogRecord) -> str:
        """로그 레코드를 텍스트로 변환"""
        timestamp = self.formatTime(record)
        message = record.getMessage()

        # 추가 컨텍스트 정보
        extra = ""
        if hasattr(record, "context"):
            extra = f" | {record.context}"
        if hasattr(record, "duration_ms"):
            extra += f" | {record.duration_ms}ms"

        base = f"{timestamp} | {record.levelname:8} | {record.name:20} | {message}{extra}"

        if record.exc_info:
            base += f"\n{self.formatException(record.exc_info)}"

        return base


def setup_logging(config: LoggingConfig) -> logging.Logger:
    """로깅 설정

    Args:
        config: LoggingConfig 인스턴스

    Returns:
        설정된 로거
    """
    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(config.level)

    # 기존 핸들러 제거
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # 포맷터 선택
    formatter_class = JSONFormatter if config.format == "json" else TextFormatter
    formatter = formatter_class()

    # 콘솔 핸들러
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(config.level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # 파일 핸들러 (선택사항)
    if config.file_path:
        file_path = Path(config.file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(file_path)
        file_handler.setLevel(config.level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # 라이브러리 로거 레벨 조정 (너무 verbose하지 않도록)
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)

    return root_logger


class ContextualLogger:
    """컨텍스트 기반 로거

    사용 예:
        logger = ContextualLogger(__name__)
        with logger.context(user_query="..."):
            logger.info("처리 시작")
    """

    def __init__(self, name: str) -> None:
        """초기화

        Args:
            name: 로거명 (__name__ 권장)
        """
        self._logger = logging.getLogger(name)
        self._context: dict[str, Any] = {}

    def set_context(self, **kwargs: Any) -> None:
        """컨텍스트 설정"""
        self._context.update(kwargs)

    def clear_context(self) -> None:
        """컨텍스트 초기화"""
        self._context.clear()

    def _get_extra(self) -> dict[str, Any]:
        """로그 메시지에 추가될 정보"""
        return self._context.copy()

    def debug(self, message: str, **kwargs: Any) -> None:
        """DEBUG 레벨 로그"""
        extra = {**self._get_extra(), **kwargs}
        self._logger.debug(message, extra=extra)

    def info(self, message: str, **kwargs: Any) -> None:
        """INFO 레벨 로그"""
        extra = {**self._get_extra(), **kwargs}
        self._logger.info(message, extra=extra)

    def warning(self, message: str, **kwargs: Any) -> None:
        """WARNING 레벨 로그"""
        extra = {**self._get_extra(), **kwargs}
        self._logger.warning(message, extra=extra)

    def error(self, message: str, **kwargs: Any) -> None:
        """ERROR 레벨 로그"""
        extra = {**self._get_extra(), **kwargs}
        self._logger.error(message, extra=extra)

    def critical(self, message: str, **kwargs: Any) -> None:
        """CRITICAL 레벨 로그"""
        extra = {**self._get_extra(), **kwargs}
        self._logger.critical(message, extra=extra)

    def exception(self, message: str, **kwargs: Any) -> None:
        """예외 정보와 함께 ERROR 로그"""
        extra = {**self._get_extra(), **kwargs}
        self._logger.exception(message, extra=extra)


class PerformanceLogger:
    """성능 로깅

    사용 예:
        perf = PerformanceLogger(__name__)
        with perf.timer("bq_query"):
            # 쿼리 실행
            pass  # 자동으로 실행 시간 로깅
    """

    def __init__(self, logger: logging.Logger | ContextualLogger) -> None:
        """초기화

        Args:
            logger: 로거 인스턴스
        """
        self._logger = logger

    def timer(self, operation: str) -> "TimerContext":
        """타이머 컨텍스트

        Args:
            operation: 작업명

        Returns:
            컨텍스트 매니저
        """
        return TimerContext(self._logger, operation)


class TimerContext:
    """타이머 컨텍스트 매니저"""

    def __init__(self, logger: logging.Logger | ContextualLogger, operation: str) -> None:
        self._logger = logger
        self._operation = operation
        self._start_time: Optional[float] = None

    def __enter__(self) -> "TimerContext":
        import time

        self._start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        import time

        duration_ms = (time.time() - self._start_time) * 1000

        if exc_type is None:
            if isinstance(self._logger, ContextualLogger):
                self._logger.info(
                    f"작업 완료: {self._operation}",
                    duration_ms=duration_ms,
                )
            else:
                self._logger.info(
                    f"작업 완료: {self._operation} ({duration_ms:.2f}ms)"
                )
        else:
            if isinstance(self._logger, ContextualLogger):
                self._logger.error(
                    f"작업 실패: {self._operation}",
                    duration_ms=duration_ms,
                )
            else:
                self._logger.error(
                    f"작업 실패: {self._operation} ({duration_ms:.2f}ms)"
                )
