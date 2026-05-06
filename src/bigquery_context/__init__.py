"""BigQuery 컨텍스트 모듈"""

from .schema_full import BIGQUERY_SCHEMA, COMMON_JOINS
from .sample_data import SAMPLE_EVENTS, SAMPLE_SUBSCRIPTIONS
from .successful_queries import SUCCESSFUL_QUERIES
from .antipatterns import ANTIPATTERNS
from .domain_knowledge import SECTORS, EVENT_TYPE_MEANINGS, KEY_METRICS, USER_SEGMENTS

__all__ = [
    'BIGQUERY_SCHEMA',
    'COMMON_JOINS',
    'SAMPLE_EVENTS',
    'SAMPLE_SUBSCRIPTIONS',
    'SUCCESSFUL_QUERIES',
    'ANTIPATTERNS',
    'SECTORS',
    'EVENT_TYPE_MEANINGS',
    'KEY_METRICS',
    'USER_SEGMENTS',
]
