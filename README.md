# NLQ Data Agent — 자연어 데이터 분석 에이전트

자연어 쿼리 → LLM 해석 → BigQuery SQL 생성 → 데이터 분석

## 특징

- **LLM 기반 자연어 해석**: 사용자의 자연어 쿼리를 이해하고 SQL로 변환
- **BigQuery 컨텍스트 기반**: 스키마, 샘플 데이터, 성공한 쿼리, 안티패턴을 LLM에 제공
- **자동 검증**: 생성된 SQL의 문법과 테이블 존재 여부 검증
- **DataFrame 반환**: 결과를 pandas DataFrame + 기본 통계로 반환
- **확장 가능**: 다양한 분석 패턴 지원

## 빠른 시작

```bash
# 설치
pip install -r requirements.txt

# 환경 설정
cp config/.env.example .env
# .env에 LITELLM_API_KEY, AMPLITUDE_TOKEN 입력

# 분석 실행
python examples/01_sector_retention.py
```

## 구조

```
src/
├── bigquery_context/          # ★ BigQuery 컨텍스트 (최우선)
│   ├── schema_full.py         # 테이블 스키마
│   ├── sample_data.py         # 샘플 데이터
│   ├── successful_queries.py  # 성공한 쿼리
│   ├── antipatterns.py        # 금지 패턴
│   └── domain_knowledge.py    # 비즈니스 지식
│
├── query/                     # SQL 생성 및 검증
│   ├── context_builder.py     # LLM 프롬프트
│   ├── generator.py           # SQL 생성
│   └── validator.py           # 검증
│
├── executor/                  # BigQuery 실행
│   ├── bigquery_client.py
│   └── data_processor.py
│
└── agent.py                   # 메인 에이전트
```

## 사용 예시

```python
from src.agent import NLQAgent

agent = NLQAgent()

# 쿼리 실행
result = await agent.analyze("어떤 섹터의 리텐션이 높을까?")

# 결과
print(result['sql'])        # 생성된 SQL
print(result['data'])       # DataFrame
print(result['stats'])      # 기본 통계
print(result['explanation']) # 쿼리 설명
```

## 개발 로드맵

- **P0 (Week 1)**: BigQuery 컨텍스트 구축 ★ 가장 중요
- **P1 (Week 2)**: LLM 기반 SQL 생성
- **P2 (Week 3)**: 시각화 및 테스트

## 문서

- [BigQuery 스키마](docs/BIGQUERY_SCHEMA.md)
- [쿼리 예시](docs/SAMPLE_QUERIES.md)
- [금지 패턴](docs/ANTIPATTERNS.md)
- [도메인 지식](docs/DOMAIN_KNOWLEDGE.md)
