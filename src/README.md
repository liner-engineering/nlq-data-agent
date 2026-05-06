# Execution-based Eval 사용 가이드

## 무엇이 바뀌었나

기존 `evaluator.py`는 **syntactic eval** 이었습니다.

```python
# 기존: SQL 문자열에 키워드 들어있는지만 확인
if "DATE_DIFF" in sql_lower and "INTERVAL" in sql_lower:
    pass = True
```

이 방식의 문제:
- 윈도우가 7일이어야 하는데 30일이어도 통과
- JOIN 조건이 틀려도 통과
- 집계 단위가 틀려도 통과

새 evaluator는 **execution accuracy** 를 측정합니다 (Spider/BIRD 벤치마크 방식).

```python
# 새 방식: gold SQL과 예측 SQL을 둘 다 실행해서 결과 비교
gold_df  = bq.execute(case.gold_sql)
pred_df  = bq.execute(agent.analyze(case.question).sql)
match    = comparator.compare(gold_df, pred_df)
```

## 파일 구성

```
src/eval/
├── result_comparator.py     # 두 DataFrame 의미적 비교 (★ 핵심)
├── execution_eval_set.py    # ExecutionEvalCase + gold SQL 정의
└── execution_evaluator.py   # pass@k 측정 evaluator
```

## 통합 방법

### 1단계: 파일 배치
3개 파일을 `src/eval/` 디렉토리에 추가하세요. 기존 `eval_set.py`, `evaluator.py`는 당분간 그대로 두고 새 evaluator와 병행 운영합니다.

### 2단계: gold SQL 작성 (★ 가장 중요)

`execution_eval_set.py`의 `EXECUTION_EVAL_CASES` 리스트에 케이스를 추가합니다. 각 케이스는:

```python
ExecutionEvalCase(
    id="volume_001",
    question="지난 30일간 일별 DAU 추이",
    gold_sql="""
SELECT
  DATE(event_time) AS date,
  COUNT(DISTINCT user_id) AS dau
FROM `liner-219011.analysis.EVENTS_296805`
WHERE DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  AND DATE(event_time) < CURRENT_DATE()
GROUP BY date
ORDER BY date
    """.strip(),
    category="timeseries",
    order_sensitive=True,    # ORDER BY가 의미있으면 True
    verified=False,           # 본인이 BigQuery에서 결과 확인 후 True로
)
```

**gold SQL 작성 원칙**:
- 정답이 모호하면 명시적으로 정의(노트에 적기): "지난달 = 전월 1일~말일", "DAU = distinct user_id"
- 빈 결과가 나오는 SQL은 false-pass의 원인. `expect_empty_ok=True`로 표시하거나 시간 범위 조정
- BigQuery에서 직접 실행해서 결과가 도메인 의도와 맞는지 확인 후 `verified=True`

### 3단계: 베이스라인 측정 (템플릿 폐지 *전*)

```bash
# 검증된 케이스만 3회씩
python -m src.eval.execution_evaluator --save baseline.json

# 또는 검증 안 된 케이스도 포함
python -m src.eval.execution_evaluator --all --n 3 --save baseline.json
```

출력:
```
📊 pass@1       : 47.0%   (첫 시도 정확도)
📊 pass@k       : 73.0%   (3회 중 1회 이상 성공)
📊 평균 통과율  : 53.0%   (안정성 지표)
📊 평균 SQL변종 : 2.30    (낮을수록 재현성↑)

카테고리별:
  [retention      ] n= 3  pass@1=33%  pass@k=67%  variants=2.7
  [sector         ] n= 5  pass@1=60%  pass@k=80%  variants=2.0
  ...
```

### 4단계: 템플릿 폐지

이전에 합의한 옵션 B 적용. 이때 `baseline.json`을 보관하세요.

### 5단계: 재측정 + 비교

```bash
python -m src.eval.execution_evaluator --save after_template_removal.json
```

`baseline.json` vs `after_template_removal.json` 비교 → **진짜 정확도 변화 확인**.

## 해석 방법

| 지표 | 의미 | 좋은 신호 |
|------|------|----------|
| pass@1 | 첫 시도가 맞을 확률 | ↑ |
| pass@k | k번 중 한번이라도 맞을 확률 | ↑ |
| pass@k − pass@1 | 비결정성 폭 | ↓ |
| avg_pass_rate | 케이스당 평균 통과율 | ↑ |
| avg_sql_variants | 평균 SQL 변종 수 | 1에 가까울수록 ↑ |

**조심해야 할 패턴**:
- pass@1 = 80%, pass@k = 80% → 결정적. 좋음
- pass@1 = 40%, pass@k = 90% → 매우 비결정적. 재현성 문제
- pass@1 = pass@k = 90%, variants = 5 → SQL은 다양해도 결과가 같음. 괜찮음

## 비용 통제

BigQuery 비용이 발생합니다. 각 eval 실행:
- gold SQL: 케이스당 1회 (캐시됨)
- pred SQL: 케이스당 N회 (n_attempts)

15 케이스 × 3회 = **gold 15회 + pred 45회 = 60회 쿼리**

권장:
- `BigQueryConfig.maximum_bytes_billed`로 쿼리당 비용 상한 설정
- gold SQL은 모두 시간 범위 필터 포함 (이미 `validator`가 강제)
- 처음에는 작은 케이스셋으로 시작 (5~10개)

## 한계 및 알려진 위험

1. **gold SQL이 틀리면 모든 평가가 무의미**합니다. 작성자(=프로젝트 오너)의 도메인 검증이 절대 필수입니다.
2. **시간 의존성**: `CURRENT_DATE()`를 쓰는 gold SQL은 실행일에 따라 결과가 다릅니다. 같은 날 baseline과 after를 측정하거나, 고정 날짜로 작성하세요.
3. **non-deterministic SQL**: BigQuery의 `RAND()`, 정의되지 않은 ORDER 등은 비교 자체가 불안정. gold SQL에서 피하세요.
4. **빈 결과 false-pass**: gold도 비고 pred도 비면 "일치"로 판정. `expect_empty_ok` 플래그로 명시적 처리.

## 다음 단계

이 eval로 측정 가능해진 후:
1. Temperature를 0으로 내려서 비결정성 제거 → variants가 줄어드는지 확인
2. Self-reflection의 silent failure 제거 → pass@1이 떨어지면 안 됨
3. 템플릿을 SUCCESSFUL_QUERIES로 이주 → category별 pass rate가 유지되는지 확인

각 변경마다 같은 eval로 측정하면 진짜 개선인지 노이즈인지 판별 가능합니다.
