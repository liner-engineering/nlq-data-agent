"""
BigQuery 클라이언트

SQL을 BigQuery에서 실행하고 결과를 DataFrame으로 반환합니다.
싱글톤 패턴과 재시도 로직을 포함합니다.
"""

import time
from typing import Any

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

from src.config import BigQueryConfig
from src.exceptions import BigQueryExecutionError
from src.logging_config import ContextualLogger, PerformanceLogger
from src.types import Result

logger = ContextualLogger(__name__)
perf_logger = PerformanceLogger(logger)

# 싱글톤 클라이언트 (설정별)
_bq_clients: dict[str, bigquery.Client] = {}


def get_bq_client(config: BigQueryConfig) -> bigquery.Client:
    """
    BigQuery 클라이언트 싱글톤 조회 또는 생성

    설정별(project + location)으로 별도의 클라이언트를 유지합니다.

    Args:
        config: BigQueryConfig 인스턴스

    Returns:
        초기화된 BigQuery 클라이언트

    Raises:
        BigQueryExecutionError: 클라이언트 초기화 실패
    """
    cache_key = f"{config.project}:{config.location}"

    if cache_key not in _bq_clients:
        try:
            client = bigquery.Client(project=config.project, location=config.location)
            _bq_clients[cache_key] = client
            logger.info(f"BigQuery client initialized: {cache_key}")
        except Exception as e:
            raise BigQueryExecutionError(
                f"BigQuery 클라이언트 초기화 실패: {str(e)}"
            ) from e

    return _bq_clients[cache_key]


class BigQueryExecutor:
    """BigQuery SQL 실행기

    BigQuery에서 SQL을 실행하고 결과를 처리합니다.
    연결 풀을 관리하고 재시도 로직을 포함합니다.

    Example:
        executor = BigQueryExecutor(config)
        result = await executor.execute(sql)
        if result.is_success():
            df = result.data
    """

    def __init__(self, config: BigQueryConfig) -> None:
        """
        초기화

        Args:
            config: BigQueryConfig 인스턴스
        """
        self.config = config
        self.client = get_bq_client(config)

    def execute(self, sql: str, max_results: int | None = None) -> Result[pd.DataFrame]:
        """
        SQL 쿼리 실행

        Args:
            sql: BigQuery SQL
            max_results: 최대 결과 행 수 (기본값: config.max_results)

        Returns:
            쿼리 결과 DataFrame 또는 에러
        """
        if not sql or not sql.strip():
            return Result.failure("SQL이 비어있습니다")

        max_results = max_results or self.config.max_results
        logger.set_context(sql=sql[:100])

        try:
            with perf_logger.timer("bq_execute"):
                # 쿼리 설정
                job_config = bigquery.QueryJobConfig(
                    use_legacy_sql=False,
                    maximum_bytes_billed=self.config.maximum_bytes_billed,
                    priority=bigquery.QueryPriority.INTERACTIVE,
                )

                logger.info(
                    f"쿼리 실행 중 (timeout: {self.config.timeout_seconds}s)"
                )

                # 쿼리 실행
                query_job = self.client.query(
                    sql, job_config=job_config, timeout=self.config.timeout_seconds
                )

                # 결과 수집
                results = query_job.result(
                    timeout=self.config.timeout_seconds, max_results=max_results
                )
                df = results.to_dataframe()

                logger.info(f"쿼리 완료: {len(df)} rows, {len(df.columns)} columns")

                return Result.success(df)

        except GoogleCloudError as e:
            error_msg = f"BigQuery 오류: {str(e)}"
            logger.error(error_msg)
            return Result.failure(error_msg)

        except TimeoutError as e:
            error_msg = f"쿼리 타임아웃 ({self.config.timeout_seconds}s)"
            logger.error(error_msg)
            return Result.failure(error_msg)

        except Exception as e:
            error_msg = f"쿼리 실행 실패: {str(e)}"
            logger.exception(error_msg)
            return Result.failure(error_msg)

    def test_connection(self) -> bool:
        """
        연결 테스트

        Returns:
            연결 성공 여부
        """
        try:
            self.client.query("SELECT 1 as test").result(timeout=10)
            logger.info("BigQuery 연결 테스트 통과")
            return True
        except Exception as e:
            logger.error(f"BigQuery 연결 테스트 실패: {str(e)}")
            return False

    def dry_run(self, sql: str) -> Result[dict[str, int]]:
        """
        SQL의 bytes_billed 비용 추정 (dry_run)

        Args:
            sql: BigQuery SQL

        Returns:
            {'bytes_processed': int, 'bytes_billed': int} 또는 에러
        """
        if not sql or not sql.strip():
            return Result.failure("SQL이 비어있습니다")

        logger.set_context(sql=sql[:100])

        try:
            with perf_logger.timer("bq_dry_run"):
                job_config = bigquery.QueryJobConfig(
                    use_legacy_sql=False,
                    dry_run=True,
                    use_query_cache=False,  # 캐시 비활성화: 실제 스캔량 측정
                    priority=bigquery.QueryPriority.INTERACTIVE,
                )

                query_job = self.client.query(sql, job_config=job_config, timeout=10)

                bytes_processed = query_job.total_bytes_processed or 0
                bytes_billed = query_job.total_bytes_billed or 0

                logger.info(
                    f"Dry run: {bytes_processed:,} bytes processed, "
                    f"{bytes_billed:,} bytes billed"
                )

                return Result.success({
                    "bytes_processed": bytes_processed,
                    "bytes_billed": bytes_billed,
                })

        except Exception as e:
            error_msg = f"Dry run 실패: {str(e)}"
            logger.error(error_msg)
            return Result.failure(error_msg)

    def get_table_schema(self, table_id: str) -> dict[str, str] | None:
        """
        테이블 스키마 조회

        Args:
            table_id: 테이블 ID (project.dataset.table)

        Returns:
            {컬럼명: 타입} 사전 또는 None
        """
        try:
            table = self.client.get_table(table_id)
            return {field.name: str(field.field_type) for field in table.schema}
        except Exception as e:
            logger.error(f"테이블 스키마 조회 실패 ({table_id}): {str(e)}")
            return None
