"""
LLM 기반 SQL 생성기

context_builder의 프롬프트를 사용하여 LLM으로 SQL을 생성합니다.
재시도 로직, self-reflection 검증을 포함한 프로덕션 레디 구현입니다.

Self-Reflection 패턴 (당근페이):
- SQL 생성 후 LLM이 자신의 SQL을 다시 검토
- 의미적 오류(잘못된 테이블 선택, 누락된 필터) 감지
- 자동 수정 또는 재생성 요청
"""

import json
import os
import re
import time
from typing import Any

from openai import OpenAI

from src.bigquery_context.glossary import GLOSSARY
from src.config import LLMConfig
from src.exceptions import SQLGenerationError
from src.logging_config import ContextualLogger
from src.query.context_builder import ContextBuilder
from src.types import Result, SQL, ValidationResult

logger = ContextualLogger(__name__)


class SQLGenerator:
    """LLM 기반 SQL 생성기

    자연어 쿼리로부터 BigQuery SQL을 생성합니다.
    재시도 로직과 검증을 포함합니다.

    Example:
        gen = SQLGenerator()
        result = gen.generate("섹터별 리텐션")
        if result.is_success():
            sql = result.data
    """

    # SQL 추출 패턴 (순서대로 시도)
    _SQL_PATTERNS = [
        re.compile(r"```(?:sql)?\s*\n(.*?)\n```", re.DOTALL | re.IGNORECASE),
        re.compile(r"```(.*?)```", re.DOTALL),
    ]

    def __init__(self, config: LLMConfig | None = None) -> None:
        """
        초기화

        Args:
            config: LLMConfig 인스턴스 (기본값: 환경 변수)
        """
        self.config = config or LLMConfig()
        self.context_builder = ContextBuilder()
        self.client = self._init_llm()

    def _init_llm(self) -> OpenAI:
        """OpenAI 호환 클라이언트 초기화

        Returns:
            OpenAI 클라이언트

        Raises:
            ImportError: openai 클라이언트 초기화 실패
        """
        try:
            kwargs = {
                "api_key": self.config.api_key,
            }

            if self.config.api_base:
                kwargs["base_url"] = self.config.api_base

            client = OpenAI(**kwargs)
            logger.info(
                f"OpenAI 클라이언트 초기화 완료 (base_url: {self.config.api_base or '기본값'})"
            )
            return client
        except Exception as e:
            error_msg = f"OpenAI 클라이언트 초기화 실패: {str(e)}"
            logger.error(error_msg)
            raise ImportError(error_msg) from e

    def generate(
        self, user_query: str, max_retries: int = 3, with_reflection: bool = True
    ) -> Result[SQL]:
        """
        자연어 쿼리로부터 SQL 생성 (선택적 self-reflection 포함)

        Args:
            user_query: 사용자의 자연어 쿼리
            max_retries: 최대 재시도 횟수 (exponential backoff 사용)
            with_reflection: Self-Reflection 단계 포함 여부 (기본값: True)

        Returns:
            생성된 SQL 또는 에러
        """
        logger.set_context(
            user_query=user_query[:100], max_retries=max_retries, reflection=with_reflection
        )

        # P3: ContextBuilder가 단일 진입점. 시스템+컨텍스트+질문을 모두 포함한 프롬프트 생성
        prompt = self.context_builder.build_prompt(user_query)

        for attempt in range(max_retries):
            try:
                # Exponential backoff
                if attempt > 0:
                    wait_time = 2 ** (attempt - 1)
                    logger.warning(
                        f"재시도 대기 중 ({attempt}/{max_retries})",
                        duration_ms=wait_time * 1000,
                    )
                    time.sleep(wait_time)

                # P3: 단순화된 LLM 호출. 프롬프트가 이미 완전함
                response = self.client.chat.completions.create(
                    model=self.config.model,
                    messages=[
                        {"role": "user", "content": prompt},
                    ],
                    temperature=self.config.temperature,
                    max_tokens=self.config.max_tokens,
                )

                sql_text = response.choices[0].message.content
                sql = self._extract_sql(sql_text)

                if not sql:
                    logger.warning(
                        f"SQL 추출 실패 (시도 {attempt + 1}/{max_retries})"
                    )
                    continue

                # Self-Reflection (선택사항)
                if with_reflection:
                    is_correct, result_sql = self._self_reflect(user_query, sql)
                    if not is_correct:
                        logger.warning(
                            f"Self-reflection 실패: {result_sql} (재시도 {attempt + 1}/{max_retries})"
                        )
                        continue
                    sql = result_sql

                logger.info(f"SQL 생성 완료 (시도 {attempt + 1})")
                return Result.success(sql)

            except Exception as e:
                logger.error(
                    f"LLM 호출 실패: {str(e)}", duration_ms=(attempt + 1) * 1000
                )
                if attempt == max_retries - 1:
                    return Result.failure(f"LLM 생성 실패: {str(e)}")

        return Result.failure("최대 재시도 횟수 초과")

    def _self_reflect(self, user_query: str, sql: str) -> tuple[bool, str]:
        """
        Self-Reflection: 생성된 SQL을 LLM이 다시 검토

        당근페이 패턴으로 의미적 오류(잘못된 테이블 선택, 누락된 필터)를 감지합니다.
        dry-run은 BigQuery 문법만 잡지만, 의미적 오류는 LLM만 잡을 수 있습니다.

        Args:
            user_query: 사용자 쿼리
            sql: 검토할 SQL

        Returns:
            (is_correct: bool, corrected_sql_or_reason: str)
            - is_correct=True이면 corrected_sql_or_reason은 최종 SQL (원본 또는 수정본)
            - is_correct=False이면 corrected_sql_or_reason은 오류 설명
        """
        reflection_prompt = f"""다음 SQL이 사용자의 질문에 정확히 답하는지 검토하세요.

사용자 질문: {user_query}

생성된 SQL:
```sql
{sql}
```

체크리스트:
1. 질문의 핵심 엔티티가 SQL에 반영되었는가?
   - 예: "구독자"가 질문이면 fct_moon_subscription 조인이 있는가?
   - 예: "메시지"가 질문이면 fct_question_answer_binding_message 사용?

2. 시간 범위가 적절한가?
   - 질문에 명시적 기간이 있으면 SQL에도 있는가?
   - 기간 없으면 최근 30일 필터가 있는가?

3. 잘못된 테이블을 사용하고 있는가?
   - 결제 분석에 EVENTS_296805 사용? (금지)
   - 구독 분석에 EVENTS_296805만 사용? (fct_moon_subscription 필요)

4. JOIN 조건이 정확한가?
   - 구독 조인 시 시간 범위 포함? (date(event_time) >= start_date AND ...)
   - user_id 타입 일치? (STRING vs INTEGER)

5. 질문의 수량/순서 표현이 SQL에 반영되었는가?
   - "가장 많이/적게" 표현 → ORDER BY DESC/ASC + LIMIT 1이 있는가?
   - "TOP N" 표현 → LIMIT N이 있는가?
   - "평균/합계" 표현 → AVG/SUM이 있는가?
   - "몇 명" → COUNT(DISTINCT user_id)가 있는가?

6. GROUP BY가 의미 있는가?
   - 1행 결과면 의도와 맞는지? (집계 쿼리 vs 상세 쿼리)

7. 질문에 "credit" 또는 "크레딧"이 있는가? → 데이터 소스 재확인!
   - ❌ EVENTS_296805에서 credit 필드 추출 시도? (필드 없음, 금지)
   - ✓ agent_credit_usage_log 사용? (유일한 정답 소스)
   - ✓ delta_amount < 0 필터? (음수만 = 사용량)
   - ✓ INNER JOIN으로 조인? (WHERE IN 금지, BigQuery 바이트 제한 위험)

8. BigQuery 최적화 체크:
   - ✓ EVENTS_296805 쿼리에 파티션 필터 있는가? (DATE(_PARTITIONTIME) 또는 DATE(event_time) 범위 지정)
   - ✓ 같은 테이블을 여러 번 읽는 CTE 구조는 없는가? (base CTE로 한 번에 통합)
   - ✓ 필터 순서가 효율적인가? (WHERE에서 조기 필터링, HAVING 최소화)
   - ✓ 불필요한 JOIN이나 서브쿼리는 없는가? (WHERE IN은 최후의 수단)

응답은 JSON만 반환하세요 (다른 텍스트 없음):
{{"correct": true/false, "issues": ["문제점 목록"], "corrected_sql": "수정된 SQL 또는 빈 문자열"}}
"""

        try:
            response = self.client.chat.completions.create(
                model=self.config.model,
                messages=[{"role": "user", "content": reflection_prompt}],
                temperature=0.1,  # 검토는 결정론적으로
                max_tokens=2000,
            )

            text = response.choices[0].message.content.strip()

            # JSON 추출
            json_match = re.search(r'\{.*\}', text, re.DOTALL)
            if not json_match:
                error_msg = "Self-reflection JSON 파싱 실패: 응답이 JSON 형식이 아님"
                logger.error(error_msg)
                raise ValueError(error_msg)

            result = json.loads(json_match.group())

            if result.get("correct", True):
                logger.info("Self-reflection: SQL 승인")
                return True, sql

            # 문제 발견 + 수정 SQL 있으면 사용
            corrected = result.get("corrected_sql", "").strip()
            if corrected:
                issues = result.get("issues", [])
                logger.info(f"Self-reflection 수정: {issues}")
                return True, corrected

            # 문제는 있는데 수정안 없으면 실패
            issues = result.get("issues", ["unknown"])
            reason = "; ".join(issues)
            logger.warning(f"Self-reflection 실패: {reason}")
            return False, reason

        except json.JSONDecodeError as e:
            error_msg = f"Self-reflection JSON 파싱 실패: {str(e)}"
            logger.error(error_msg)
            raise ValueError(error_msg) from e

        except Exception as e:
            error_msg = f"Self-reflection 중 오류: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

    def _extract_sql(self, text: str) -> SQL | None:
        """
        LLM 응답에서 SQL 추출

        마크다운 코드블록 또는 평문 SQL을 추출합니다.

        Args:
            text: LLM 응답 텍스트

        Returns:
            추출된 SQL 또는 None
        """
        if not text:
            return None

        text = text.strip()

        # 1. 마크다운 코드블록
        for pattern in self._SQL_PATTERNS:
            matches = pattern.findall(text)
            if matches:
                return matches[0].strip()

        # 2. SELECT로 시작하는 평문
        if text.upper().startswith("SELECT"):
            return text

        # 3. SELECT가 포함된 경우 그 부분부터
        select_idx = text.upper().find("SELECT")
        if select_idx != -1:
            return text[select_idx:].strip()

        return None

    def generate_with_self_correction(
        self,
        user_query: str,
        validator,
        bq_executor=None,
        max_attempts: int = 3,
    ) -> Result[SQL]:
        """
        Self-Correction 파이프라인: 생성 → Self-Reflection → 검증 → Dry-Run → 비용 검증

        5단계 검증으로 의미적, 문법적, 실행 가능성, 비용을 모두 검사합니다.

        단계:
        1. [생성] LLM으로 SQL 생성
        2. [Self-Reflection] LLM이 자신의 SQL을 의미적으로 검토
           - 잘못된 테이블 선택, 누락된 필터, JOIN 조건 오류 감지
        3. [정적 검증] regex 기반 문법 검증
        4. [Dry-Run] BigQuery로 실행 가능성 확인
        5. [비용 검증] 1TB 이상이면 파티션 필터 제안 후 재생성

        Args:
            user_query: 사용자 자연어 쿼리
            validator: SQLValidator 인스턴스
            bq_executor: BigQueryExecutor 인스턴스 (dry_run 메서드 필요, 선택)
            max_attempts: 최대 재시도 횟수

        Returns:
            검증 완료된 SQL 또는 에러
        """
        logger.set_context(user_query=user_query[:100], pipeline="self_correction")

        previous_sql = None
        previous_error = None

        for attempt in range(max_attempts):
            # [1] SQL 생성 (에러 피드백 포함)
            augmented_query = user_query
            if attempt > 0 and previous_error:
                augmented_query = (
                    f"{user_query}\n\n"
                    f"[이전 시도 실패]\n"
                    f"SQL: {previous_sql}\n\n"
                    f"문제: {previous_error}\n\n"
                    f"위 문제를 해결한 새로운 SQL을 생성하세요."
                )

            logger.info(f"[{attempt + 1}/{max_attempts}] SQL 생성 중...")
            gen_result = self.generate(augmented_query, max_retries=2, with_reflection=False)

            if not gen_result.is_success():
                logger.warning(f"SQL 생성 실패: {gen_result.error}")
                if attempt == max_attempts - 1:
                    return gen_result
                previous_error = f"생성 실패: {gen_result.error}"
                continue

            sql = gen_result.data

            # [2] Self-Reflection: LLM이 자신의 SQL을 의미적으로 검토
            logger.info(f"[{attempt + 1}/{max_attempts}] Self-Reflection 중...")
            try:
                is_correct, result = self._self_reflect(user_query, sql)
                if not is_correct:
                    previous_sql = sql
                    previous_error = f"Self-reflection: {result}"
                    logger.warning(previous_error)
                    if attempt < max_attempts - 1:
                        continue
                    return Result.failure(previous_error)

                sql = result  # 수정된 SQL일 수 있음
            except (ValueError, RuntimeError) as e:
                previous_sql = sql
                previous_error = f"Self-reflection 오류: {str(e)}"
                logger.error(previous_error)
                if attempt < max_attempts - 1:
                    continue
                return Result.failure(previous_error)

            # [3] 정적 검증: regex 기반 문법 검증 + Glossary 린트
            logger.info(f"[{attempt + 1}/{max_attempts}] 정적 검증 중...")
            try:
                validation_result = validator.validate(sql, user_query=user_query)
                if not validation_result.valid:
                    previous_sql = sql
                    error_text = "; ".join(validation_result.errors)
                    previous_error = f"검증 실패: {error_text}"
                    logger.warning(previous_error)
                    if attempt < max_attempts - 1:
                        continue
                    return Result.failure(previous_error)
            except Exception as e:
                logger.exception(f"검증 중 오류: {str(e)}")
                if attempt == max_attempts - 1:
                    return Result.failure(f"검증 오류: {str(e)}")
                previous_error = f"검증 오류: {str(e)}"
                continue

            # [4] Dry-Run: BigQuery 실행 가능성 + 비용 검증
            if bq_executor:
                logger.info(f"[{attempt + 1}/{max_attempts}] Dry-Run 중...")
                try:
                    dry_run_result = bq_executor.dry_run(sql)
                    if not dry_run_result.is_success():
                        previous_sql = sql
                        previous_error = f"Dry-Run 실패: {dry_run_result.error}"
                        logger.warning(previous_error)
                        if attempt < max_attempts - 1:
                            continue
                        return dry_run_result

                    # [5] 비용 검증: 1TB 초과 시 자동 수정 제안
                    cost_data = dry_run_result.data
                    bytes_billed = cost_data.get("bytes_billed", 0)

                    if bytes_billed > 1024 ** 4:  # 1TB
                        gb_billed = bytes_billed / (1024 ** 3)
                        previous_sql = sql
                        previous_error = (
                            f"쿼리가 {gb_billed:.0f}GB 스캔 예상 (1TB 초과). "
                            f"DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL ...) "
                            f"형식으로 파티션 필터를 추가하거나 기간을 좁혀주세요."
                        )
                        logger.warning(previous_error)
                        if attempt < max_attempts - 1:
                            continue
                        return Result.failure(previous_error)

                    logger.info(f"비용 검증 통과: {bytes_billed / (1024**3):.2f}GB")

                except Exception as e:
                    logger.exception(f"Dry-Run 중 오류: {str(e)}")
                    if attempt == max_attempts - 1:
                        return Result.failure(f"Dry-Run 오류: {str(e)}")
                    previous_error = f"Dry-Run 오류: {str(e)}"
                    continue

            # 모든 검증 통과
            logger.info(f"✓ SQL 생성 완료 (시도 {attempt + 1}/{max_attempts}, 모든 검증 통과)")
            return Result.success(sql)

        return Result.failure(
            f"최대 재시도 횟수({max_attempts}) 초과: {previous_error or '원인 미상'}"
        )

    def _build_correction_feedback(
        self,
        user_query: str,
        failed_sql: str,
        error: str,
    ) -> str:
        """
        검증 실패 시 LLM에게 줄 피드백.

        Glossary 위반이 있으면 정답(primary_source)까지 함께 전달하여
        LLM이 같은 실수를 반복하지 않도록 합니다.

        Args:
            user_query: 사용자의 원본 쿼리
            failed_sql: 검증에 실패한 SQL (있으면)
            error: 검증 에러 메시지

        Returns:
            LLM에게 전달할 피드백 텍스트
        """
        user_query_lower = user_query.lower()

        # 질문과 매칭되는 glossary 항목 수집
        relevant_glossary = []
        for term, info in GLOSSARY.items():
            all_terms = [term] + info.get("alternative_terms", [])
            if any(t.lower() in user_query_lower for t in all_terms):
                relevant_glossary.append({
                    "term": term,
                    "primary_source": info.get("primary_source", ""),
                    "anti_patterns": info.get("anti_patterns", []),
                })

        # 피드백 메시지 조립
        parts = [user_query, "", "[이전 시도 실패]", ""]

        # 이전 SQL (있으면)
        if failed_sql:
            parts.extend([
                "이전 SQL:",
                "```sql",
                failed_sql,
                "```",
                "",
            ])

        # 검증 에러
        parts.extend([f"문제: {error}", ""])

        # Glossary 정보 (있으면)
        if relevant_glossary:
            parts.append("**아래 도메인 용어 매핑을 정확히 따르세요:**")
            parts.append("")
            for g in relevant_glossary:
                primary_source = g["primary_source"]
                if isinstance(primary_source, list):
                    parts.append(f"- **{g['term']}**:")
                    for source in primary_source:
                        parts.append(f"  - {source}")
                else:
                    parts.append(f"- **{g['term']}**: {primary_source}")
                for ap in g["anti_patterns"][:2]:  # 토큰 폭발 방지
                    parts.append(f"  - 금지: {ap}")
            parts.append("")

        # 마무리
        parts.append(
            "위 매핑을 정확히 따라 새로운 SQL을 생성하세요. "
            "동일한 실수 반복 금지."
        )

        return "\n".join(parts)

    def generate_with_validation(
        self,
        user_query: str,
        validator,
        bq_executor=None,
        max_retries: int = 3,
    ) -> Result[SQL]:
        """
        Self-Correct 루프: 생성 → 정적 검증 → 비용 검증 → 피드백 재생성

        검증 실패 시 에러를 LLM에 피드백하여 자동으로 재생성합니다.
        self-reflection은 포함하지 않고 빠른 반복 위주.

        단계:
        1. LLM으로 SQL 생성
        2. 정적 검증 (테이블, 컬럼, 문법)
        3. Dry-Run 비용 검증 (1TB 초과 차단)
        4. 실패 시 에러 피드백하여 재생성 (최대 3회)

        Args:
            user_query: 사용자의 자연어 쿼리
            validator: SQLValidator 인스턴스
            bq_executor: BigQueryExecutor 인스턴스 (dry_run 메서드 필요, 선택)
            max_retries: 최대 재시도 횟수

        Returns:
            검증 완료된 SQL 또는 에러
        """
        logger.set_context(user_query=user_query[:100], pipeline="validation_loop")

        last_error = None
        last_sql = None

        for attempt in range(max_retries):
            # [1] SQL 생성 (첫 시도 또는 에러 피드백 포함)
            if attempt == 0:
                current_query = user_query
            else:
                current_query = self._build_correction_feedback(
                    user_query=user_query,
                    failed_sql=last_sql or "",
                    error=last_error or "",
                )

            logger.info(f"[시도 {attempt + 1}/{max_retries}] SQL 생성 중...")
            gen_result = self.generate(current_query, max_retries=2, with_reflection=False)

            if not gen_result.is_success():
                logger.warning(f"SQL 생성 실패: {gen_result.error}")
                if attempt == max_retries - 1:
                    return gen_result
                last_error = f"생성 실패: {gen_result.error}"
                continue

            sql = gen_result.data

            # [2] 정적 검증
            logger.info(f"[시도 {attempt + 1}/{max_retries}] 검증 중...")
            try:
                validation_result = validator.validate(sql, user_query=user_query)

                if not validation_result.valid:
                    last_sql = sql
                    last_error = "; ".join(validation_result.errors)
                    logger.warning(f"검증 실패: {last_error}")
                    if attempt < max_retries - 1:
                        continue
                    return Result.failure(f"검증 실패: {last_error}")

            except Exception as e:
                logger.exception(f"검증 중 오류: {str(e)}")
                if attempt == max_retries - 1:
                    return Result.failure(f"검증 오류: {str(e)}")
                last_error = f"검증 오류: {str(e)}"
                continue

            # [3] 비용 검증 (dry_run)
            if bq_executor:
                logger.info(f"[시도 {attempt + 1}/{max_retries}] 비용 검증 중...")
                try:
                    cost_result = bq_executor.dry_run(sql)

                    if cost_result.is_success():
                        bytes_billed = cost_result.data.get("bytes_billed", 0)

                        # 1TB 초과 시 재생성 요청
                        if bytes_billed > 1024 ** 4:
                            gb_billed = bytes_billed / (1024 ** 3)
                            last_sql = sql
                            last_error = (
                                f"쿼리가 {gb_billed:.0f}GB 스캔 예상 (1TB 초과). "
                                f"DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL ...) "
                                f"형식으로 파티션 필터를 추가하거나 조회 기간을 좁혀주세요. "
                                f"또는 더 구체적인 필터(예: liner_product='write')를 추가하세요."
                            )
                            logger.warning(last_error)
                            if attempt < max_retries - 1:
                                continue
                            return Result.failure(last_error)

                        logger.info(f"비용 검증 통과: {bytes_billed / (1024**3):.2f}GB")

                    else:
                        logger.warning(f"Dry-Run 실패: {cost_result.error}")
                        if attempt == max_retries - 1:
                            return cost_result
                        last_error = f"비용 검증 실패: {cost_result.error}"
                        continue

                except Exception as e:
                    logger.exception(f"비용 검증 중 오류: {str(e)}")
                    if attempt == max_retries - 1:
                        return Result.failure(f"비용 검증 오류: {str(e)}")
                    last_error = f"비용 검증 오류: {str(e)}"
                    continue

            # 모든 검증 통과
            logger.info(f"✓ SQL 생성 완료 (시도 {attempt + 1}/{max_retries})")
            return Result.success(sql)

        return Result.failure(
            f"최대 재시도 횟수({max_retries}) 초과. 마지막 에러: {last_error or '원인 미상'}"
        )
