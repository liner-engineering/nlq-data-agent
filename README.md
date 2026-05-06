# NLQ Data Agent — 자연어 데이터 분석 에이전트

자연어 쿼리를 BigQuery SQL로 변환하고 자동 실행하는 LLM 기반 에이전트입니다.

## 특징

- **LLM 기반 SQL 생성**: Claude/Gemini 등 다양한 LLM 지원 (litellm)
- **BigQuery 컨텍스트 기반**: 스키마, 샘플 데이터, 성공 패턴, 안티패턴을 LLM에 제공
- **자동 검증**: 생성된 SQL의 문법, 테이블명, 필드명 검증
- **성능 최적화**: 싱글톤 클라이언트, 재시도 로직, 캐싱
- **시니어 코드 품질**: 타입 힌팅, Protocol 기반 설계, 구조화된 로깅
- **프로덕션 레디**: Pydantic 설정, 커스텀 예외, 의존성 주입

## 빠른 시작

### 설치 (rye 사용)

```bash
# Python 3.12 설정
rye sync

# 또는 pip 사용
pip install -e .
```

### 설정

```bash
# 환경 변수 설정
export LITELLM_API_KEY="your-api-key"
export NLQ_CONFIG="config/default.yaml"  # 선택사항
```

### 사용

```python
from src import NLQAgent

agent = NLQAgent()
result = agent.analyze("섹터별 D+7 리텐션이 뭐야?")

if result.success:
    print(f"SQL: {result.sql}")
    print(f"데이터: {len(result.data)} rows")
    print(result.explanation)
else:
    print(f"오류: {result.error}")
```

## 아키텍처

```
src/
├── agent.py                    # 메인 에이전트
├── config.py                   # Pydantic 기반 설정
├── exceptions.py               # 커스텀 예외
├── logging_config.py           # 구조화된 로깅
├── types.py                    # 타입 정의 & Protocol
│
├── bigquery_context/
│   ├── schema_full.py          # 테이블 스키마
│   ├── successful_queries.py   # 성공 사례
│   ├── antipatterns.py         # 금지 패턴
│   ├── domain_knowledge.py     # 비즈니스 규칙
│   └── sample_data.py          # 샘플 데이터
│
├── query/
│   ├── validator.py            # SQL 검증 (캐싱)
│   ├── context_builder.py      # LLM 프롬프트
│   └── generator.py            # SQL 생성 (재시도 로직)
│
└── executor/
    ├── bigquery_client.py      # BigQuery 실행 (싱글톤)
    └── data_processor.py       # 통계 계산 (벡터화)
```

## 의존성

### 필수
- `google-cloud-bigquery` - BigQuery 클라이언트
- `pandas`, `numpy` - 데이터 처리
- `pydantic` - 설정 검증
- `litellm` - LLM 추상화
- `pyyaml` - YAML 설정

### 개발
- `pytest` - 테스트 프레임워크
- `black`, `ruff`, `mypy` - 코드 품질

## 설정

### pyproject.toml (권장)
rye를 사용하여 의존성 관리:

```bash
rye add <package>
rye sync
```

### config/default.yaml
```yaml
llm:
  provider: litellm
  model: gemini-2.5-flash-lite
  temperature: 0.2

bigquery:
  project: liner-219011
  timeout_seconds: 300

analysis:
  min_sample_size: 10
  recommended_sample_size: 100
```

## 테스트

```bash
# 테스트 실행
pytest tests/

# 특정 테스트
pytest tests/test_validator.py

# 커버리지
pytest --cov=src tests/
```

## 코드 품질

```bash
# 타입 검사
mypy src/

# 린팅
ruff check src/

# 포맷팅
black src/
```

## 주요 클래스

### NLQAgent
자연어 쿼리를 분석하는 메인 에이전트.

```python
agent = NLQAgent(config)
result = agent.analyze("질문")
```

### SQLValidator
SQL 검증기. 테이블명, 필드명, 안티패턴 감지.

```python
validator = SQLValidator()
result = validator.validate(sql)
```

### SQLGenerator
LLM 기반 SQL 생성기. 재시도 및 검증 로직 포함.

```python
generator = SQLGenerator(config)
result = generator.generate_with_validation(query, validator)
```

### BigQueryExecutor
BigQuery SQL 실행기. 싱글톤 클라이언트 관리.

```python
executor = BigQueryExecutor(config)
result = executor.execute(sql)
```

### DataProcessor
결과 처리 및 통계 계산.

```python
processor = DataProcessor(config)
result = processor.process(df)
```

## 에러 처리

모든 예외는 `NLQAgentException`을 상속합니다:

```python
from src.exceptions import (
    SQLValidationError,
    SQLGenerationError,
    BigQueryExecutionError,
    DataProcessingError,
)
```

## 로깅

구조화된 로깅 지원:

```python
from src.logging_config import ContextualLogger

logger = ContextualLogger(__name__)
logger.set_context(user_query="...")
logger.info("메시지", duration_ms=100)
```

## 라이선스

MIT
