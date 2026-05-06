# BigQuery Service Analysis Agent - 설계 문서

## 개요

BigQuery 데이터를 활용하여 서비스 전반의 자연어 질문에 답하는 범용 분석 에이전트입니다.

**목표**:
- 자연어로 질문 → 자동 분석 또는 SQL 생성
- 데이터 중심 접근으로 실행 가능한 인사이트 제공
- 통계적 유의성으로 신뢰성 확보

---

## 아키텍처

### 전체 흐름

```
사용자 입력
    ↓
┌─────────────────────────────────────────┐
│      Streamlit 대시보드 (app.py)         │
├─────────────────────────────────────────┤
│                                         │
├─ 탭1: NLQ 모드                         │
│  └→ NLQAgent                           │
│     └→ SQL 생성 → BigQuery 실행        │
│                                         │
├─ 탭2: 분석 모드                        │
│  └→ ServiceAnalysisAgent               │
│     ├→ 템플릿 자동 분류                │
│     ├→ SQL 실행                        │
│     ├→ 통계 계산                       │
│     ├→ 유의성 검정                     │
│     └→ 인사이트 생성                   │
│                                         │
└─────────────────────────────────────────┘
```

### 모듈 구조

```
src/
├── agent.py
│   └── NLQAgent: LLM 기반 SQL 생성 (기존)
│
├── analysis/
│   ├── __init__.py
│   ├── service_analysis_agent.py
│   │   └── ServiceAnalysisAgent: 범용 분석 엔진
│   ├── templates.py
│   │   ├── AnalysisTemplate: 분석 템플릿
│   │   ├── 6가지 기본 템플릿 (전환, 리텐션 등)
│   │   └── find_template(): 자동 분류
│   └── statistical_tests.py
│       └── StatisticalTester: 통계 검정
│
└── app.py
    └── Streamlit 대시보드 (NLQ + 분석 탭)
```

---

## 핵심 설계 원칙

### 1. 데이터 중심 접근

**원칙**: 가설 검증이 아닌 데이터 탐색에서 시작

- 사용자의 질문에서 분석 유형을 자동 분류
- 해당 분석 템플릿으로 기본 쿼리 생성
- 데이터를 먼저 보고 패턴 발견

**예시**:
```
Q: "이탈 사용자 분석해"
→ 이탈 템플릿 자동 선택
→ 이탈/위험/활성 상태별로 집계
→ 데이터를 보고 인사이트 도출
```

### 2. 통계적 검증

**원칙**: 모든 비교에 유의성 검정 포함

- p < 0.05인 결과만 "유의미"로 표시
- 샘플 크기가 작으면 경고
- 효과 크기(Cohen's d 등) 표시

**지원 검정**:
- 카이제곱 검정: 범주형 데이터 비교
- t-검정: 연속형 데이터 비교
- Fisher 정확성: 소표본
- Mann-Whitney U: 비모수
- 비율 검정: 비율 비교

### 3. So What 원칙

**원칙**: "그래서 어쩌라고?"에 대한 답이 없으면 제시하지 않음

- 샘플 크기 < 10 → "신뢰성 부족" 반환
- 유의미한 차이가 없으면 "차이 없음" 명시
- 각 인사이트마다 추천사항 포함

**예시**:
```
잘못된 인사이트: "A가 B보다 10% 높습니다"
올바른 인사이트: "A가 B보다 10% 높으며 통계적으로 유의미합니다 (p<0.01). 
                   → A의 전환 경로를 벤치마킹하세요."
```

### 4. 신뢰도 계산

```python
confidence = 0.5  # 기본값

# 샘플 크기별
if sample_size >= 1000:
    confidence += 0.3
elif sample_size >= 100:
    confidence += 0.2
elif sample_size >= 30:
    confidence += 0.1

# 검정 결과
if significant_tests > 0:
    confidence += 0.2

# 최종: 0.0 ~ 1.0
```

---

## 주요 컴포넌트

### ServiceAnalysisAgent

```python
agent = ServiceAnalysisAgent()

# 분석 수행
result = agent.analyze_question("전환율이 어떻게 되나요?")

# 결과 구조
AnalysisResult(
    question: str,              # 원본 질문
    analysis_type: str,         # "전환율 분석" 등
    data: DataFrame,            # 쿼리 결과
    statistics: dict,           # 기본 통계
    test_results: List[TestResult],  # 유의성 검정
    insights: List[str],        # 해석된 인사이트
    recommendations: List[str], # 실행 제안
    confidence: float           # 0.0~1.0
)
```

**주요 메서드**:
- `analyze_question()`: 자연어 질문 분석
- `_calculate_statistics()`: 기본 통계 계산
- `_perform_tests()`: 통계 검정 수행
- `_generate_insights()`: So What 원칙 적용
- `_generate_recommendations()`: 실행 제안

### 분석 템플릿 (6가지)

#### 1. 전환율 분석 (CONVERSION_TEMPLATE)
```
키워드: 전환율, conversion, 전환, 구매율, 가입율, 구독율

쿼리: 일별 전환율 추이
결과:
  - total_users: 일일 사용자 수
  - converted_users: 전환자 수
  - conversion_rate_pct: 전환율
```

#### 2. 리텐션 분석 (RETENTION_TEMPLATE)
```
키워드: 리텐션, retention, 유지, 재활동, D+7

쿼리: 섹터별 D+7 리텐션
결과:
  - first_time_users: 신규 사용자
  - day7_retained: 재활동자 (D+7~D+13)
  - retention_rate_pct: 리텐션율
```

#### 3. 이탈 분석 (CHURN_TEMPLATE)
```
키워드: 이탈, churn, 이탈율, 취소, unsubscribe

쿼리: 사용자를 이탈/위험/활성으로 분류
결과:
  - churned: 30+ 일 비활성
  - at_risk: 14+ 일 비활성
  - active: 활성
```

#### 4. 매출 분석 (REVENUE_TEMPLATE)
```
키워드: 매출, revenue, 결제, 수익, ARPU

쿼리: 월별 결제 통계
결과:
  - total_revenue: 총 매출
  - paying_users: 결제 사용자 수
  - avg_transaction: 평균 거래액
```

#### 5. 사용자 세그먼트 (USER_SEGMENT_TEMPLATE)
```
키워드: 세그먼트, segment, 분류, 그룹, 유형

쿼리: Power/Active/Regular/Casual User
결과:
  - user_segment: 사용자 군
  - user_count: 인원
  - avg_events: 평균 이벤트 수
```

#### 6. 기능 사용 분석 (FEATURE_USAGE_TEMPLATE)
```
키워드: 기능, feature, 사용, usage, 인기, 이벤트

쿼리: 상위 20개 이벤트 타입
결과:
  - event_type: 기능명
  - event_count: 사용 횟수
  - adoption_rate_pct: 채택률
```

### StatisticalTester

```python
tester = StatisticalTester()

# 카이제곱 검정 (범주형)
result = tester.chi_square_test(contingency_table, "카테고리")

# t-검정 (연속형)
result = tester.t_test(group1, group2)

# Fisher 정확성 (2x2)
result = tester.fishers_exact_test(table_2x2)

# Mann-Whitney U (비모수)
result = tester.mannwhitneyu_test(group1, group2)

# 비율 검정
result = tester.proportion_ztest(count, total, base_rate=0.5)

# 결과 구조
TestResult(
    test_name: str,         # 검정 이름
    statistic: float,       # 검정통계량
    p_value: float,         # p-value
    significant: bool,      # p < 0.05 여부
    interpretation: str,    # 해석 문장
    sample_sizes: dict,     # 표본 크기
    effect_size: float      # 효과 크기 (선택)
)
```

---

## 데이터 흐름 예시

### 사례 1: 자동 분석 (리텐션)

```
입력: "D+7 리텐션 분석해줘"
  ↓
[ServiceAnalysisAgent]
  ├─ find_template() → RETENTION_TEMPLATE 선택
  ├─ SQL 생성: 섹터별 신규/재활동 사용자 집계
  ├─ BigQuery 실행 → DataFrame
  │  sector | first_time_users | day7_retained | retention_rate_pct
  │ --------|-----------------|---------------|--------------------
  │ prof   | 150             | 75            | 50.0
  │ edu    | 200             | 120           | 60.0
  │ ...
  ├─ 통계 계산:
  │  - 평균 리텐션율: 52.3%
  │  - 최고: 65.0% (최저: 35.0%)
  ├─ 카이제곱 검정 (리텐션 >= 50% vs < 50%)
  │  - χ² = 8.34, p = 0.015 → 유의미
  ├─ 인사이트 생성:
  │  ✓ 리텐션율이 섹터별로 통계적으로 다릅니다
  │  ✓ 교육 섹터(60%)가 전문(50%)보다 높습니다
  │  ✓ 신규 사용자 150명 이상 확보되어 통계 신뢰성 높음
  └─ 추천사항:
    → 교육 섹터의 온보딩 프로세스를 벤치마킹하세요
    → 전문가 섹터의 초기 경험을 개선하세요
    → 전체 리텐션이 50% 이하인 점을 개선하세요

출력: AnalysisResult 객체
```

### 사례 2: 자유 쿼리 (NLQ)

```
입력: "최근 30일 구매율 추이를 월별로 보여줘"
  ↓
[NLQAgent]
  ├─ LLM이 SQL 자동 생성
  ├─ SQL 검증 (테이블, 컬럼, 문법)
  ├─ BigQuery 실행 → DataFrame
  ├─ 결과 정처리 (포맷, 통계)
  └─ 설명 생성
    → "구매율이 날짜에 따라 변함"

출력: AnalysisResult 객체
```

---

## Streamlit 대시보드 UI

### 탭 1: NLQ 쿼리

```
┌─────────────────────────────────┐
│ 🔍 NLQ 쿼리                     │
├─────────────────────────────────┤
│ LLM이 자동으로 SQL을 생성합니다  │
│                                 │
│ [텍스트 영역: 자연어 쿼리]      │
│ "2026년 4월 professional..."   │
│                                 │
│ [🚀 실행]                      │
├─────────────────────────────────┤
│                                 │
│ ✅ 생성된 SQL                   │
│ [코드 블록: SELECT...]         │
│                                 │
│ 📊 쿼리 결과                    │
│ [데이터 테이블]                │
│                                 │
│ 📈 통계 정보                    │
│ [행 수: 15 | 컬럼: 4]          │
│                                 │
│ 💭 분석 설명                    │
│ [결과 해석]                    │
│                                 │
└─────────────────────────────────┘
```

### 탭 2: 자동 분석

```
┌─────────────────────────────────┐
│ 📊 자동 분석                     │
├─────────────────────────────────┤
│ 키워드를 인식하여 자동 분석     │
│                                 │
│ [텍스트 영역: 분석 질문]        │
│ "전환율이 어떻게 되나요?"      │
│                                 │
│ [🚀 분석]                      │
├─────────────────────────────────┤
│                                 │
│ 📊 전환율 분석                  │
│ [행: 30 | 컬럼: 3 | 신뢰도: 72%] │
│                                 │
│ 📈 데이터                       │
│ [테이블: date, total, converted] │
│                                 │
│ 💡 주요 인사이트                │
│ ✓ 전환율이 날짜별로 다릅니다   │
│ ✓ 지난주(8%) vs 이번주(12%)   │
│ ✓ 통계적으로 유의미한 변화     │
│                                 │
│ 🎯 추천사항                    │
│ → 이번주 전환 증가 원인 파악   │
│ → 성공한 변수를 스케일업      │
│                                 │
│ 📊 통계 검정                   │
│ [카이제곱 검정: χ²=6.2, p=0.03] │
│                                 │
│ 📋 상세 통계                    │
│ [평균, 표준편차, 분포 등]     │
│                                 │
└─────────────────────────────────┘
```

---

## 설정 및 환경 변수

### 필수 환경 변수

```bash
# LLM API 키 (LiteLLM 지원 모델)
export LITELLM_API_KEY="your-key"

# BigQuery (자동 인증)
# application_default_credentials 사용
```

### Streamlit 실행

```bash
# 의존성 설치
rye sync

# 대시보드 실행
rye run streamlit run app.py

# 접속
http://localhost:8501
```

---

## 향후 개선 사항 (P2)

### 분석 기능 확장
- [ ] 시계열 분석 (trend, seasonality)
- [ ] 코호트 분석 (cohort analysis)
- [ ] 상관관계 분석 (correlation)
- [ ] A/B 테스트 검정 (A/B test)

### UI/UX 개선
- [ ] 차트 시각화 (Plotly)
- [ ] 쿼리 히스토리 저장
- [ ] CSV/PDF 내보내기
- [ ] 맞춤 템플릿 생성 UI

### 성능 최적화
- [ ] 결과 캐싱
- [ ] 쿼리 최적화
- [ ] 병렬 실행

### 배포
- [ ] FastAPI + Slack Bot
- [ ] 팀 공유 기능
- [ ] 권한 관리

---

## 사용 가이드

### 예시 1: 전환율 분석

```
Q: "어제 전환율이 어떻게 되었나요?"
→ 자동으로 CONVERSION_TEMPLATE 적용
→ 일별 전환율 계산
→ "어제(12%) vs 지난주 평균(10.5%)" 비교
→ p=0.15 (유의미하지 않음) 표시
→ 추천: "더 많은 데이터를 모아 트렌드 확인하세요"
```

### 예시 2: 이탈 분석

```
Q: "이탈 사용자의 특징은?"
→ 자동으로 CHURN_TEMPLATE 적용
→ 이탈/위험/활성 사용자 분류
→ "이탈자 n=120, 활성 n=850" 
→ χ² 검정: p<0.001 (매우 유의미)
→ 추천: "이탈 원인 분석 후 재참여 캠프 설계하세요"
```

### 예시 3: 자유 쿼리

```
Q: "professional 섹터에서 chat 이벤트 후 24시간 내 재활동률은?"
→ NLQAgent가 SQL 자동 생성
→ BigQuery에서 직접 계산
→ 결과: "45.3%"
→ 설명: "약 절반의 사용자가 재활동합니다"
```

---

## 주의사항

1. **샘플 크기**: n < 10이면 "신뢰성 부족" 경고
2. **통계 검정**: p < 0.05만 "유의미"로 표시
3. **데이터 품질**: 완성도 < 80%이면 경고
4. **시간대**: event_time은 UTC (한국 시간 -9시간)
5. **JSON 필드**: event_properties는 JSON_EXTRACT_SCALAR() 필요

---

## 기술 스택

- **Backend**: Python 3.12
- **LLM**: LiteLLM (Gemini, Claude 등 지원)
- **BigQuery**: google-cloud-bigquery
- **통계**: scipy.stats, numpy, pandas
- **Frontend**: Streamlit
- **의존성 관리**: Rye

---

## 참고 자료

- [Streamlit 문서](https://docs.streamlit.io/)
- [scipy.stats 검정](https://docs.scipy.org/doc/scipy/reference/stats.html)
- [BigQuery SQL 가이드](https://cloud.google.com/bigquery/docs/reference/standard-sql)
- [LiteLLM 모델 목록](https://docs.litellm.ai/docs/providers)
