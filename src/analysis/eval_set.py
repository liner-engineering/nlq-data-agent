"""
평가 세트 - 템플릿 기반 synthetic eval set

30개의 (질문, 예상 SQL 구조) 페어로 구성
- 6개 템플릿 × 5개 변형 = 30개
- 각 변형은 SQL 구조는 같되 자연어 표현만 다름

평가 방식:
- expected_tables: 사용할 테이블들 (Schema Linking)
- expected_sql_features: 포함해야 하는 SQL 구문 (정확도)
- forbidden_patterns: 포함하면 안 되는 패턴 (정확도)
"""

EVAL_SET = [
    # ============================================
    # 1. 쿼리 볼륨 분석 (5개)
    # ============================================
    {
        "id": "volume_001",
        "question": "지난 30일 일일 쿼리 추이를 보고 싶어요",
        "category": "query_volume",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": [
            "DATE(event_time)",
            "COUNT(*) as query_count",
            "COUNT(DISTINCT user_id)",
            "GROUP BY",
            "DATE_SUB",
            "INTERVAL 30 DAY"
        ],
        "forbidden_patterns": ["sector", "retention", "subscription"],
    },
    {
        "id": "volume_002",
        "question": "DAU 추이",
        "category": "query_volume",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": [
            "DATE(event_time)",
            "COUNT(DISTINCT user_id)",
            "GROUP BY",
            "ORDER BY"
        ],
        "forbidden_patterns": ["sector", "retention"],
    },
    {
        "id": "volume_003",
        "question": "일별 쿼리 볼륨은 어떻게 되나요?",
        "category": "query_volume",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["DATE", "COUNT(*)"],
        "forbidden_patterns": ["sector"],
    },
    {
        "id": "volume_004",
        "question": "최근 한 달 쿼리 수 변화",
        "category": "query_volume",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["DATE_SUB(CURRENT_DATE()"],
        "forbidden_patterns": ["retention", "subscription"],
    },
    {
        "id": "volume_005",
        "question": "시간대별 사용자 수 추이",
        "category": "query_volume",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["COUNT(DISTINCT user_id)", "GROUP BY"],
        "forbidden_patterns": ["sector"],
    },

    # ============================================
    # 2. 섹터별 분포 (5개)
    # ============================================
    {
        "id": "sector_001",
        "question": "사용자들이 가장 관심 있는 섹터는?",
        "category": "sector_distribution",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": [
            "CASE WHEN",
            "LIKE",
            "섹터",  # sector 컬럼
            "COUNT(DISTINCT user_id)",
            "GROUP BY"
        ],
        "forbidden_patterns": ["retention", "subscription"],
    },
    {
        "id": "sector_002",
        "question": "섹터별 사용자 분포",
        "category": "sector_distribution",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["CASE WHEN", "GROUP BY sector"],
        "forbidden_patterns": ["date_add", "interval"],
    },
    {
        "id": "sector_003",
        "question": "어떤 주제의 쿼리가 가장 많은가?",
        "category": "sector_distribution",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["JSON_EXTRACT_SCALAR", "query"],
        "forbidden_patterns": ["retention"],
    },
    {
        "id": "sector_004",
        "question": "카테고리별 활성 사용자 현황",
        "category": "sector_distribution",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["COUNT(DISTINCT", "GROUP BY"],
        "forbidden_patterns": ["subscription"],
    },
    {
        "id": "sector_005",
        "question": "관심사별 사용자 수",
        "category": "sector_distribution",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["sector", "user_count"],
        "forbidden_patterns": ["retention_rate"],
    },

    # ============================================
    # 3. 섹터별 리텐션 (5개)
    # ============================================
    {
        "id": "retention_001",
        "question": "섹터별 D+7 리텐션은?",
        "category": "sector_retention",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": [
            "first_events",
            "retention_check",
            "DATE_ADD",
            "INTERVAL 7 DAY",
            "retention_rate_pct",
            "sector"
        ],
        "forbidden_patterns": ["subscription", "dim_user"],
    },
    {
        "id": "retention_002",
        "question": "섹터별 재방문율은 몇 퍼센트?",
        "category": "sector_retention",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["ROUND(100.0", "retention_rate"],
        "forbidden_patterns": ["subscription"],
    },
    {
        "id": "retention_003",
        "question": "D+7 지속률을 섹터별로 분석해줘",
        "category": "sector_retention",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["DATE_ADD", "INTERVAL"],
        "forbidden_patterns": ["subscription"],
    },
    {
        "id": "retention_004",
        "question": "어떤 섹터 사용자가 더 오래 머무나?",
        "category": "sector_retention",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["retention_rate_pct"],
        "forbidden_patterns": ["subscription"],
    },
    {
        "id": "retention_005",
        "question": "섹터별 활동 지속성 비교",
        "category": "sector_retention",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["ORDER BY retention_rate_pct"],
        "forbidden_patterns": ["subscription"],
    },

    # ============================================
    # 4. 파워 사용자 (5개)
    # ============================================
    {
        "id": "power_001",
        "question": "파워 사용자는 몇 명인가?",
        "category": "power_user",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": [
            "CASE WHEN query_count",
            "user_tier",
            "COUNT(DISTINCT user_id)",
            "GROUP BY user_tier"
        ],
        "forbidden_patterns": ["sector", "retention", "subscription"],
    },
    {
        "id": "power_002",
        "question": "활동 빈도별 사용자 분류",
        "category": "power_user",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["CASE WHEN", "50+", "20-49", "5-19"],
        "forbidden_patterns": ["sector"],
    },
    {
        "id": "power_003",
        "question": "활발한 사용자는 누구인가?",
        "category": "power_user",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["query_count", "AVG(query_count)"],
        "forbidden_patterns": ["subscription"],
    },
    {
        "id": "power_004",
        "question": "자주 쓰는 사용자 분석",
        "category": "power_user",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["COUNT(*) as query_count"],
        "forbidden_patterns": ["sector"],
    },
    {
        "id": "power_005",
        "question": "사용자별 쿼리 빈도 분포",
        "category": "power_user",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["GROUP BY user_id"],
        "forbidden_patterns": ["retention"],
    },

    # ============================================
    # 5. 이탈 분석 (5개)
    # ============================================
    {
        "id": "churn_001",
        "question": "이탈 사용자는 몇 명?",
        "category": "churn_analysis",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": [
            "MAX(DATE(event_time))",
            "DATE_DIFF",
            "Churned",
            "At Risk",
            "Active"
        ],
        "forbidden_patterns": ["sector", "subscription"],
    },
    {
        "id": "churn_002",
        "question": "비활성 사용자 현황",
        "category": "churn_analysis",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["last_active_date", "status"],
        "forbidden_patterns": ["sector"],
    },
    {
        "id": "churn_003",
        "question": "최근 14일 이상 활동 없는 사용자는?",
        "category": "churn_analysis",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["DATE_DIFF", "INTERVAL"],
        "forbidden_patterns": ["sector"],
    },
    {
        "id": "churn_004",
        "question": "휴면 사용자 분석",
        "category": "churn_analysis",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["CASE WHEN", "30+ days"],
        "forbidden_patterns": ["subscription"],
    },
    {
        "id": "churn_005",
        "question": "떠난 사용자 비율은?",
        "category": "churn_analysis",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["COUNT(DISTINCT user_id)"],
        "forbidden_patterns": ["sector"],
    },

    # ============================================
    # 6. 쿼리 유형 분석 (5개)
    # ============================================
    {
        "id": "query_type_001",
        "question": "쿼리 길이별 분포",
        "category": "query_type_analysis",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": [
            "LENGTH(",
            "query_length",
            "Short",
            "Medium",
            "Long"
        ],
        "forbidden_patterns": ["sector", "retention"],
    },
    {
        "id": "query_type_002",
        "question": "짧은 쿼리와 긴 쿼리의 비율?",
        "category": "query_type_analysis",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["LENGTH(JSON_EXTRACT_SCALAR"],
        "forbidden_patterns": ["sector"],
    },
    {
        "id": "query_type_003",
        "question": "평균 쿼리 길이는?",
        "category": "query_type_analysis",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["AVG(query_length)"],
        "forbidden_patterns": ["sector"],
    },
    {
        "id": "query_type_004",
        "question": "복잡도별 쿼리 분석",
        "category": "query_type_analysis",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["CASE WHEN"],
        "forbidden_patterns": ["sector"],
    },
    {
        "id": "query_type_005",
        "question": "사용자가 주로 짧은 쿼리를 쓰나요?",
        "category": "query_type_analysis",
        "expected_tables": ["EVENTS_296805"],
        "expected_sql_features": ["query_length_category"],
        "forbidden_patterns": ["sector"],
    },
]


def evaluate(agent, eval_set=EVAL_SET, verbose=False) -> dict:
    """
    Agent를 평가 세트에 대해 테스트

    Args:
        agent: NLQAgent 인스턴스
        eval_set: 평가 세트 (기본값: EVAL_SET)
        verbose: 상세 출력 여부

    Returns:
        {"pass": 통과 개수, "total": 총 개수, "rate": 통과율, "details": []}
    """
    import re

    passed = 0
    total = len(eval_set)
    details = []

    for case in eval_set:
        question = case["question"]
        try:
            result = agent.analyze(question)
            sql = result.sql.lower()

            # 1. 예상 테이블 확인
            tables_found = all(
                table.lower() in sql
                for table in case["expected_tables"]
            )

            # 2. 예상 SQL 구문 확인
            features_found = all(
                feature.lower() in sql
                for feature in case["expected_sql_features"]
            )

            # 3. 금지 패턴 확인
            forbidden_found = any(
                pattern.lower() in sql
                for pattern in case.get("forbidden_patterns", [])
            )

            # 판정
            is_pass = tables_found and features_found and not forbidden_found

            if is_pass:
                passed += 1
                status = "PASS"
            else:
                status = "FAIL"

            if verbose:
                print(f"\n{status} | {case['id']}: {question}")
                if not tables_found:
                    print(f"  ✗ 테이블 미매칭")
                if not features_found:
                    print(f"  ✗ SQL 구문 미매칭")
                if forbidden_found:
                    print(f"  ✗ 금지 패턴 감지")

            details.append({
                "id": case["id"],
                "status": status,
                "question": question,
                "tables_ok": tables_found,
                "features_ok": features_found,
                "no_forbidden": not forbidden_found,
            })

        except Exception as e:
            if verbose:
                print(f"\nERROR | {case['id']}: {question}")
                print(f"  예외: {str(e)[:100]}")
            details.append({
                "id": case["id"],
                "status": "ERROR",
                "question": question,
                "error": str(e)[:100],
            })

    return {
        "passed": passed,
        "total": total,
        "rate": round(100.0 * passed / total, 1),
        "details": details,
    }


if __name__ == "__main__":
    # 테스트 실행 예시
    print("=" * 70)
    print("평가 세트 정보")
    print("=" * 70)
    print(f"총 평가 케이스: {len(EVAL_SET)}개")

    categories = {}
    for case in EVAL_SET:
        cat = case["category"]
        categories[cat] = categories.get(cat, 0) + 1

    print("\n카테고리별 분포:")
    for cat, count in sorted(categories.items()):
        print(f"  - {cat}: {count}개")

    print("\n첫 3개 케이스:")
    for case in EVAL_SET[:3]:
        print(f"\n  ID: {case['id']}")
        print(f"  Q: {case['question']}")
        print(f"  테이블: {case['expected_tables']}")
        print(f"  SQL 구문 수: {len(case['expected_sql_features'])}")
