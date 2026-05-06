"""
평가 실행기 — 한 줄로 점수 확인
"""

from src.agent import NLQAgent
from src.eval.eval_set import EVAL_CASES, EvalCase
from src.logging_config import ContextualLogger

logger = ContextualLogger(__name__)


def evaluate_case(agent: NLQAgent, case: EvalCase, dry_run: bool = True) -> dict:
    """단일 케이스 평가. dry_run이면 SQL 생성만 하고 실행 안 함."""
    try:
        if dry_run:
            # SQL 생성만
            sql_result = agent.generator.generate_with_validation(
                case.question, agent.validator
            )
            if not sql_result.is_success():
                return {
                    "case": case.question,
                    "passed": False,
                    "reason": "생성 실패",
                    "sql": "",
                    "error": sql_result.error,
                }
            sql = sql_result.data
        else:
            # 실제 실행
            result = agent.analyze(case.question)
            if not result.success:
                return {
                    "case": case.question,
                    "passed": False,
                    "reason": "실행 실패",
                    "sql": result.sql,
                    "error": result.error,
                }
            sql = result.sql

        sql_lower = sql.lower()

        # 테이블 체크
        for table in case.expected_tables:
            if table.lower() not in sql_lower:
                return {
                    "case": case.question,
                    "passed": False,
                    "reason": f"테이블 누락: {table}",
                    "sql": sql,
                }

        # 필수 포함
        for token in case.must_contain:
            if token.lower() not in sql_lower:
                return {
                    "case": case.question,
                    "passed": False,
                    "reason": f"패턴 누락: {token}",
                    "sql": sql,
                }

        # 금지 패턴
        for token in case.must_not_contain:
            if token.lower() in sql_lower:
                return {
                    "case": case.question,
                    "passed": False,
                    "reason": f"금지 패턴 포함: {token}",
                    "sql": sql,
                }

        return {"case": case.question, "passed": True, "sql": sql}

    except Exception as e:
        logger.warning(f"케이스 평가 중 예외: {str(e)}")
        return {
            "case": case.question,
            "passed": False,
            "reason": f"예외: {str(e)[:50]}",
            "sql": "",
        }


def run_eval(agent: NLQAgent | None = None, dry_run: bool = True) -> dict:
    """전체 평가. 카테고리별 정확도 출력."""
    agent = agent or NLQAgent()
    results = []

    print(f"\n{'='*70}")
    print(f"NL2SQL 평가셋 실행 (dry_run={dry_run})")
    print(f"{'='*70}\n")

    for i, case in enumerate(EVAL_CASES, 1):
        result = evaluate_case(agent, case, dry_run)
        result["category"] = case.category
        results.append(result)

        status = "✓" if result["passed"] else "✗"
        print(f"{status} [{i:2d}/{len(EVAL_CASES)}] {case.category:12s} | {case.question[:40]}")
        if not result["passed"]:
            print(f"         └─ {result['reason']}")

    # 카테고리별 집계
    by_category = {}
    for r in results:
        cat = r["category"]
        by_category.setdefault(cat, {"pass": 0, "fail": 0, "fails": []})
        if r["passed"]:
            by_category[cat]["pass"] += 1
        else:
            by_category[cat]["fail"] += 1
            by_category[cat]["fails"].append(r)

    total_pass = sum(1 for r in results if r["passed"])
    total = len(results)

    print(f"\n{'='*70}")
    print(f"전체: {total_pass}/{total} ({100*total_pass/total:.1f}%)")
    print(f"{'='*70}")

    for cat, stats in sorted(by_category.items()):
        n = stats["pass"] + stats["fail"]
        pct = 100 * stats["pass"] / n if n > 0 else 0
        print(f"\n[{cat:15s}] {stats['pass']}/{n} ({pct:.0f}%)")
        for fail in stats["fails"]:
            print(f"  ✗ {fail['case'][:50]}")
            print(f"     └─ {fail['reason']}")

    return {
        "total": total,
        "passed": total_pass,
        "results": results,
        "by_category": by_category,
        "score": total_pass / total if total > 0 else 0.0,
    }


if __name__ == "__main__":
    import os

    api_key = os.getenv("LITELLM_API_KEY")
    if not api_key:
        print("⚠️  LITELLM_API_KEY가 설정되지 않았습니다")
    else:
        run_eval(dry_run=True)
