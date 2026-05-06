"""
Execution-based Evaluator

각 eval case에 대해:
  1. agent로 SQL 생성 (N회 반복 - 재현성 측정)
  2. 생성된 SQL과 gold SQL 둘 다 BigQuery에서 실행
  3. 결과 DataFrame을 ResultComparator로 비교
  4. pass@1, pass@k, 분산 등 통계 산출

기존 evaluator.py와 다른 점:
- syntactic check가 아니라 execution result 비교
- N회 반복으로 비결정성 측정
- 카테고리별 분산 보고

⚠️ 주의:
- BigQuery 비용이 발생합니다 (gold SQL + pred SQL × N회)
- 비용 통제: maximum_bytes_billed 설정, 시간 범위 필터 강제
- dry_run으로 먼저 비용 추정 후 실행 권장
"""

from __future__ import annotations

import json
import statistics
import time
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

# 프로젝트 모듈 import
# (실제 실행 시 PYTHONPATH에 src 추가 필요)
try:
    from src.agent import NLQAgent
    from src.executor.bigquery_client import BigQueryExecutor
    from src.logging_config import ContextualLogger

    logger = ContextualLogger(__name__)
except ImportError:
    # 개발 환경에서는 stub
    import logging

    logger = logging.getLogger(__name__)
    NLQAgent = Any  # type: ignore
    BigQueryExecutor = Any  # type: ignore

from execution_eval_set import ExecutionEvalCase, get_verified_cases, get_all_cases
from result_comparator import ComparisonResult, ResultComparator


@dataclass
class CaseRun:
    """단일 케이스의 단일 시도 결과"""

    case_id: str
    attempt: int
    pred_sql: str = ""
    gen_success: bool = False
    gen_error: str = ""
    exec_success: bool = False
    exec_error: str = ""
    match: bool = False
    match_reason: str = ""
    duration_ms: float = 0.0


@dataclass
class CaseSummary:
    """단일 케이스의 N회 시도 요약"""

    case_id: str
    question: str
    category: str
    n_attempts: int
    n_gen_success: int
    n_exec_success: int
    n_match: int
    pass_rate: float  # n_match / n_attempts
    sql_variants: int  # 고유 SQL 개수 (재현성 지표)
    avg_duration_ms: float
    runs: list[CaseRun] = field(default_factory=list)
    error_summary: str = ""


@dataclass
class EvalReport:
    """전체 평가 리포트"""

    n_cases: int
    n_attempts_per_case: int
    total_runs: int
    overall_pass_at_1: float  # 첫 시도 성공률
    overall_pass_at_k: float  # k회 중 한번이라도 성공한 비율
    avg_pass_rate: float  # 케이스별 평균 통과율
    avg_sql_variants: float  # 케이스별 평균 SQL 변종 수 (낮을수록 재현성 ↑)
    by_category: dict[str, dict[str, float]] = field(default_factory=dict)
    case_summaries: list[CaseSummary] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "n_cases": self.n_cases,
            "n_attempts_per_case": self.n_attempts_per_case,
            "total_runs": self.total_runs,
            "overall_pass_at_1": round(self.overall_pass_at_1, 3),
            "overall_pass_at_k": round(self.overall_pass_at_k, 3),
            "avg_pass_rate": round(self.avg_pass_rate, 3),
            "avg_sql_variants": round(self.avg_sql_variants, 2),
            "by_category": self.by_category,
        }


class ExecutionEvaluator:
    """Execution-based evaluator with pass@k measurement"""

    def __init__(
        self,
        agent: Any,
        bq_executor: Any,
        comparator: ResultComparator | None = None,
    ) -> None:
        """
        Args:
            agent: NLQAgent 인스턴스
            bq_executor: BigQueryExecutor 인스턴스
            comparator: ResultComparator 인스턴스 (기본값 사용 가능)
        """
        self.agent = agent
        self.bq_executor = bq_executor
        self.comparator = comparator or ResultComparator()

        # gold SQL 결과는 캐시 (같은 SQL은 한 번만 실행)
        self._gold_cache: dict[str, pd.DataFrame] = {}

    def evaluate(
        self,
        cases: list[ExecutionEvalCase],
        n_attempts: int = 3,
        verbose: bool = True,
    ) -> EvalReport:
        """
        평가 실행

        Args:
            cases: 평가할 케이스들
            n_attempts: 케이스당 시도 횟수 (재현성 측정)
            verbose: 진행 출력 여부

        Returns:
            EvalReport
        """
        if verbose:
            print(f"\n{'='*70}")
            print(f"Execution-based Evaluation")
            print(f"  cases: {len(cases)}")
            print(f"  attempts per case: {n_attempts}")
            print(f"  total runs: {len(cases) * n_attempts}")
            print(f"{'='*70}\n")

        case_summaries: list[CaseSummary] = []

        for i, case in enumerate(cases, 1):
            if verbose:
                print(f"\n[{i}/{len(cases)}] {case.id} | {case.category}")
                print(f"  Q: {case.question}")
                if not case.verified:
                    print(f"  ⚠️ gold SQL not yet verified")

            summary = self._evaluate_case(case, n_attempts, verbose)
            case_summaries.append(summary)

            if verbose:
                print(
                    f"  → pass: {summary.n_match}/{n_attempts} "
                    f"(rate={summary.pass_rate:.2f}, variants={summary.sql_variants})"
                )

        report = self._build_report(case_summaries, n_attempts)

        if verbose:
            self._print_report(report)

        return report

    def _evaluate_case(
        self, case: ExecutionEvalCase, n_attempts: int, verbose: bool
    ) -> CaseSummary:
        """단일 케이스를 N번 시도"""
        # 1. gold SQL 결과 (캐시 활용)
        gold_df = self._get_gold_result(case)

        # 2. N회 시도
        runs: list[CaseRun] = []
        seen_sqls: set[str] = set()

        for attempt in range(1, n_attempts + 1):
            run = self._run_single_attempt(case, attempt, gold_df)
            runs.append(run)
            if run.pred_sql:
                # SQL 정규화 (whitespace) 후 변종 카운트
                seen_sqls.add(self._normalize_sql(run.pred_sql))

        # 3. 요약
        n_gen = sum(1 for r in runs if r.gen_success)
        n_exec = sum(1 for r in runs if r.exec_success)
        n_match = sum(1 for r in runs if r.match)
        avg_duration = (
            statistics.mean([r.duration_ms for r in runs]) if runs else 0.0
        )

        # 에러 요약
        errors = [r.gen_error or r.exec_error or r.match_reason for r in runs if not r.match]
        error_summary = "; ".join(set(e[:100] for e in errors if e))[:200]

        return CaseSummary(
            case_id=case.id,
            question=case.question,
            category=case.category,
            n_attempts=n_attempts,
            n_gen_success=n_gen,
            n_exec_success=n_exec,
            n_match=n_match,
            pass_rate=n_match / n_attempts if n_attempts > 0 else 0.0,
            sql_variants=len(seen_sqls),
            avg_duration_ms=avg_duration,
            runs=runs,
            error_summary=error_summary,
        )

    def _run_single_attempt(
        self, case: ExecutionEvalCase, attempt: int, gold_df: pd.DataFrame | None
    ) -> CaseRun:
        """한 번의 SQL 생성 + 실행 + 비교"""
        run = CaseRun(case_id=case.id, attempt=attempt)
        t0 = time.time()

        # (1) SQL 생성
        try:
            result = self.agent.analyze(case.question)
            if not result.success:
                run.gen_error = result.error or "unknown gen error"
                run.duration_ms = (time.time() - t0) * 1000
                return run
            run.pred_sql = result.sql
            run.gen_success = True
            pred_df = result.data
        except Exception as e:
            run.gen_error = f"exception: {str(e)[:200]}"
            run.duration_ms = (time.time() - t0) * 1000
            return run

        # agent.analyze가 이미 실행까지 한 경우 데이터가 있음
        if pred_df is None:
            # SQL만 생성된 경우 직접 실행 (안전망)
            try:
                exec_result = self.bq_executor.execute(run.pred_sql)
                if not exec_result.is_success():
                    run.exec_error = exec_result.error or "exec failed"
                    run.duration_ms = (time.time() - t0) * 1000
                    return run
                pred_df = exec_result.data
            except Exception as e:
                run.exec_error = f"exec exception: {str(e)[:200]}"
                run.duration_ms = (time.time() - t0) * 1000
                return run

        run.exec_success = True

        # (2) gold가 없으면 비교 불가
        if gold_df is None:
            run.match_reason = "gold SQL 실행 실패"
            run.duration_ms = (time.time() - t0) * 1000
            return run

        # (3) 결과 비교
        cmp = self.comparator.compare(
            gold_df, pred_df, order_sensitive=case.order_sensitive
        )
        run.match = cmp.match
        run.match_reason = cmp.reason
        run.duration_ms = (time.time() - t0) * 1000
        return run

    def _get_gold_result(self, case: ExecutionEvalCase) -> pd.DataFrame | None:
        """gold SQL 실행 결과 (캐시)"""
        cache_key = case.gold_sql
        if cache_key in self._gold_cache:
            return self._gold_cache[cache_key]

        try:
            result = self.bq_executor.execute(case.gold_sql)
            if not result.is_success():
                logger.warning(
                    f"gold SQL 실행 실패 [{case.id}]: {result.error}"
                )
                self._gold_cache[cache_key] = None
                return None

            df = result.data
            if df.empty and not case.expect_empty_ok:
                logger.warning(
                    f"gold SQL 결과가 비어 있음 [{case.id}] - "
                    f"false-pass 위험. expect_empty_ok=True로 표시하거나 "
                    f"시간 범위를 확인하세요."
                )

            self._gold_cache[cache_key] = df
            return df
        except Exception as e:
            logger.error(f"gold SQL 예외 [{case.id}]: {e}")
            self._gold_cache[cache_key] = None
            return None

    def _normalize_sql(self, sql: str) -> str:
        """SQL 정규화 (whitespace, case)"""
        return " ".join(sql.lower().split())

    def _build_report(
        self, summaries: list[CaseSummary], n_attempts: int
    ) -> EvalReport:
        """전체 리포트 산출"""
        n_cases = len(summaries)
        total_runs = n_cases * n_attempts

        if n_cases == 0:
            return EvalReport(
                n_cases=0, n_attempts_per_case=n_attempts, total_runs=0,
                overall_pass_at_1=0.0, overall_pass_at_k=0.0,
                avg_pass_rate=0.0, avg_sql_variants=0.0,
            )

        # pass@1: 각 케이스의 첫 시도가 성공한 비율
        pass_at_1 = sum(
            1 for s in summaries if s.runs and s.runs[0].match
        ) / n_cases

        # pass@k: k회 중 한번이라도 성공한 비율
        pass_at_k = sum(1 for s in summaries if s.n_match > 0) / n_cases

        avg_pass_rate = statistics.mean(s.pass_rate for s in summaries)
        avg_variants = statistics.mean(s.sql_variants for s in summaries)

        # 카테고리별 집계
        by_cat: dict[str, list[CaseSummary]] = {}
        for s in summaries:
            by_cat.setdefault(s.category, []).append(s)

        cat_stats = {}
        for cat, items in by_cat.items():
            cat_stats[cat] = {
                "n_cases": len(items),
                "pass_at_1": round(
                    sum(1 for s in items if s.runs and s.runs[0].match) / len(items), 3
                ),
                "pass_at_k": round(
                    sum(1 for s in items if s.n_match > 0) / len(items), 3
                ),
                "avg_pass_rate": round(
                    statistics.mean(s.pass_rate for s in items), 3
                ),
                "avg_variants": round(
                    statistics.mean(s.sql_variants for s in items), 2
                ),
            }

        return EvalReport(
            n_cases=n_cases,
            n_attempts_per_case=n_attempts,
            total_runs=total_runs,
            overall_pass_at_1=pass_at_1,
            overall_pass_at_k=pass_at_k,
            avg_pass_rate=avg_pass_rate,
            avg_sql_variants=avg_variants,
            by_category=cat_stats,
            case_summaries=summaries,
        )

    def _print_report(self, report: EvalReport) -> None:
        """리포트 출력"""
        print(f"\n{'='*70}")
        print(f"평가 리포트")
        print(f"{'='*70}")
        print(f"케이스: {report.n_cases}")
        print(f"시도/케이스: {report.n_attempts_per_case}")
        print(f"총 실행: {report.total_runs}")
        print(f"")
        print(f"📊 pass@1       : {report.overall_pass_at_1:.1%}  (첫 시도 정확도)")
        print(f"📊 pass@k       : {report.overall_pass_at_k:.1%}  (k회 중 1회 이상 성공)")
        print(f"📊 평균 통과율  : {report.avg_pass_rate:.1%}      (안정성 지표)")
        print(f"📊 평균 SQL변종 : {report.avg_sql_variants:.2f}     (낮을수록 재현성↑)")
        print(f"")
        print(f"카테고리별:")
        for cat, stats in sorted(report.by_category.items()):
            print(
                f"  [{cat:15s}] n={stats['n_cases']:2d}  "
                f"pass@1={stats['pass_at_1']:.0%}  "
                f"pass@k={stats['pass_at_k']:.0%}  "
                f"variants={stats['avg_variants']:.1f}"
            )

        # 실패 케이스
        failed = [s for s in report.case_summaries if s.n_match == 0]
        if failed:
            print(f"\n전부 실패한 케이스 ({len(failed)}개):")
            for s in failed:
                print(f"  ✗ [{s.case_id}] {s.question[:50]}")
                print(f"     └ {s.error_summary[:120]}")

        # 비결정적 케이스 (일부만 성공)
        flaky = [s for s in report.case_summaries if 0 < s.n_match < s.n_attempts]
        if flaky:
            print(f"\n비결정적 케이스 ({len(flaky)}개) — 재현성 문제:")
            for s in flaky:
                print(
                    f"  ~ [{s.case_id}] pass {s.n_match}/{s.n_attempts}  "
                    f"variants={s.sql_variants}"
                )


def save_report(report: EvalReport, path: str) -> None:
    """리포트를 JSON으로 저장 (시점 비교용)"""
    data = report.to_dict()
    data["case_details"] = [
        {
            "case_id": s.case_id,
            "category": s.category,
            "n_match": s.n_match,
            "n_attempts": s.n_attempts,
            "pass_rate": round(s.pass_rate, 3),
            "sql_variants": s.sql_variants,
            "error_summary": s.error_summary[:200],
        }
        for s in report.case_summaries
    ]
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"리포트 저장: {path}")


# CLI 진입점
def main() -> None:
    """
    실행 예시:
        python execution_evaluator.py              # 검증된 케이스만, 3회씩
        python execution_evaluator.py --all        # 모든 케이스
        python execution_evaluator.py --n 5        # 5회씩 시도
    """
    import argparse
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="검증 안 된 케이스도 포함")
    parser.add_argument("--n", type=int, default=3, help="케이스당 시도 횟수")
    parser.add_argument("--save", type=str, help="리포트 저장 경로")
    args = parser.parse_args()

    cases = get_all_cases() if args.all else get_verified_cases()
    if not cases:
        print("⚠️ 검증된 케이스가 없습니다. --all로 미검증 케이스도 실행하거나, "
              "execution_eval_set.py에서 verified=True로 설정하세요.")
        sys.exit(1)

    # 의존성 import (런타임)
    from src.agent import NLQAgent
    from src.executor.bigquery_client import BigQueryExecutor

    agent = NLQAgent()
    evaluator = ExecutionEvaluator(
        agent=agent, bq_executor=agent.bq_executor
    )
    report = evaluator.evaluate(cases, n_attempts=args.n)

    if args.save:
        save_report(report, args.save)


if __name__ == "__main__":
    main()
