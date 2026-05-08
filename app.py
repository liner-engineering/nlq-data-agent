"""
NLQ Data Agent Streamlit Dashboard

자연어 쿼리를 SQL로 변환하고 BigQuery에서 실행하는 웹 대시보드
"""

import os
import streamlit as st
import pandas as pd
from src.agent import NLQAgent
from src.analysis.service_analysis_agent import ServiceAnalysisAgent
from src.exceptions import NLQAgentException

# 페이지 설정
st.set_page_config(
    page_title="NLQ Data Agent",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS 스타일링 (선택사항)
st.markdown("""
    <style>
    .main { padding: 2rem; }
    .metric-container { margin: 1rem 0; }
    </style>
    """, unsafe_allow_html=True)


@st.cache_resource
def get_agent():
    """NLQAgent 싱글톤 (Streamlit 캐싱)"""
    try:
        agent = NLQAgent()
        return agent
    except Exception as e:
        st.error(f"에이전트 초기화 실패: {str(e)}")
        st.stop()


@st.cache_resource
def get_analysis_agent():
    """ServiceAnalysisAgent 싱글톤 (Streamlit 캐싱)"""
    try:
        agent = ServiceAnalysisAgent()
        return agent
    except Exception as e:
        st.error(f"분석 에이전트 초기화 실패: {str(e)}")
        st.stop()


def _init_session_state():
    """Session state 초기화 (LLM 비용 추적용)"""
    if "llm_call_history" not in st.session_state:
        st.session_state.llm_call_history = []
    if "total_llm_cost" not in st.session_state:
        st.session_state.total_llm_cost = 0.0
    if "total_input_tokens" not in st.session_state:
        st.session_state.total_input_tokens = 0
    if "total_output_tokens" not in st.session_state:
        st.session_state.total_output_tokens = 0


def _llm_model_price(model: str) -> dict[str, float]:
    """LLM 모델별 가격 (input, output per 1M tokens)"""
    prices = {
        "gemini-2.5-flash-lite": {"input": 0.075, "output": 0.30},
        "gemini-2.5-flash": {"input": 0.075, "output": 0.30},
        "gpt-4": {"input": 0.03, "output": 0.06},
        "gpt-3.5-turbo": {"input": 0.0005, "output": 0.0015},
    }
    return prices.get(model, {"input": 0.075, "output": 0.30})


def _add_llm_cost(model: str, input_tokens: int, output_tokens: int):
    """LLM 호출 비용 추가"""
    _init_session_state()
    prices = _llm_model_price(model)
    input_cost = (input_tokens / 1_000_000) * prices["input"]
    output_cost = (output_tokens / 1_000_000) * prices["output"]
    total_cost = input_cost + output_cost

    st.session_state.llm_call_history.append({
        "model": model.split("/")[-1],  # 간단한 모델명
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "cost": total_cost,
    })
    st.session_state.total_llm_cost += total_cost
    st.session_state.total_input_tokens += input_tokens
    st.session_state.total_output_tokens += output_tokens


def display_llm_cost_statistics():
    """좌측 사이드바에 LLM 누적 비용 통계 표시"""
    _init_session_state()

    st.sidebar.markdown("---")
    st.sidebar.subheader("💰 LLM 비용 통계")

    col1, col2 = st.sidebar.columns(2)
    with col1:
        st.metric("누적 비용", f"${st.session_state.total_llm_cost:.6f}", delta=None)
    with col2:
        st.metric("API 호출", len(st.session_state.llm_call_history), delta=None)

    col1, col2 = st.sidebar.columns(2)
    with col1:
        st.metric("입력 토큰", f"{st.session_state.total_input_tokens:,}", delta=None)
    with col2:
        st.metric("출력 토큰", f"{st.session_state.total_output_tokens:,}", delta=None)

    # 최근 LLM 호출 이력
    if st.session_state.llm_call_history:
        with st.sidebar.expander("📜 최근 API 호출"):
            for i, call in enumerate(reversed(st.session_state.llm_call_history[-5:]), 1):
                st.write(f"{i}. {call['model']}")
                st.caption(
                    f"  비용: ${call['cost']:.8f} | "
                    f"입력: {call['input_tokens']:,} | "
                    f"출력: {call['output_tokens']:,}"
                )


def display_cost_info(result):
    """비용 정보 표시 (BigQuery 비용은 GCP Console에서 확인)"""
    # BigQuery 비용 계산 제거 (GCP에서 관리)
    pass


def display_results(result):
    """결과 표시"""
    if result.success:
        # SQL 표시
        st.subheader("생성된 SQL")
        st.code(result.sql, language="sql")

        # 결과 DataFrame
        st.subheader("쿼리 결과")
        st.dataframe(result.data, use_container_width=True)

        # 통계 정보
        if result.stats:
            st.subheader("통계 정보")
            cols = st.columns(3)
            with cols[0]:
                st.metric("행 수", len(result.data))
            with cols[1]:
                st.metric("컬럼 수", len(result.data.columns))
            with cols[2]:
                if result.data_quality and 'completeness_pct' in result.data_quality:
                    st.metric("완성도", f"{result.data_quality['completeness_pct']}%")

            # 상세 통계
            with st.expander("상세 통계"):
                st.json(result.stats)

        # 샘플 크기 경고
        sample_warning = getattr(result, 'sample_warning', '')
        if sample_warning:
            st.warning(sample_warning)

        # 데이터 품질 정보
        data_quality = getattr(result, 'data_quality', {})
        if data_quality:
            st.subheader("데이터 품질")
            with st.expander("상세 정보"):
                st.json(data_quality)

        # 설명
        if result.explanation:
            st.subheader("분석 결과")
            st.info(result.explanation)

    else:
        st.error(f"쿼리 실행 실패: {result.error}")


def display_analysis_results(result):
    """분석 결과 표시 (이모지 제거)"""
    st.subheader(f"📊 {result.analysis_type}")

    # 주요 지표
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("행 수", len(result.data))
    with col2:
        st.metric("컬럼 수", len(result.data.columns))
    with col3:
        st.metric("신뢰도", f"{result.confidence:.0%}")
    with col4:
        st.metric("통계 검정", len(result.test_results))

    # 데이터 표시
    st.subheader("데이터")
    st.dataframe(result.data, use_container_width=True)

    # 인사이트
    st.subheader("주요 인사이트")
    for insight in result.insights:
        st.write(f"• {insight}")

    # 추천사항
    st.subheader("추천사항")
    for rec in result.recommendations:
        st.write(f"→ {rec}")

    # 통계 검정 결과
    if result.test_results:
        st.subheader("통계 검정")
        for test in result.test_results:
            with st.expander(f"{test.test_name} (p={test.p_value:.4f})"):
                st.json(test.to_dict())

    # 상세 통계
    if result.statistics:
        st.subheader("상세 통계")
        with st.expander("통계 정보"):
            st.json(result.statistics)


def main():
    # 초기화
    _init_session_state()

    # 헤더
    st.title("NLQ Data Agent")
    st.markdown("자연어로 데이터를 분석하세요. SQL을 자동으로 생성하고 실행합니다.")

    # 사이드바 설정 (선택사항)
    with st.sidebar:
        st.header("설정")

        with st.expander("모델 설정", expanded=False):
            model_name = st.text_input(
                "LLM 모델",
                value="gemini-2.5-flash-lite-ai-studio",
                help="LiteLLM을 통해 지원되는 모델명"
            )
            temperature = st.slider(
                "온도 (낮을수록 일관성 높음)",
                min_value=0.0,
                max_value=1.0,
                value=0.2,
                step=0.1
            )
            timeout = st.number_input(
                "타임아웃 (초)",
                min_value=10,
                max_value=600,
                value=300
            )

        st.divider()

        # 환경 변수 확인
        api_key_set = bool(os.getenv("LITELLM_API_KEY"))
        st.markdown(f"**API 키 설정**: {'설정됨' if api_key_set else '미설정'}")

        if not api_key_set:
            st.warning("LITELLM_API_KEY 환경 변수가 설정되어 있지 않습니다.")

        st.divider()
        st.markdown("### 도움말")
        st.markdown("""
        **NLQ 모드:**
        자유로운 자연어 쿼리로 맞춤형 SQL 생성

        **분석 모드:**
        자동 분석 템플릿 + 통계 검정

        **예시 질문:**
        - "2026년 4월 professional 섹터의 D+7 리텐션이 몇 퍼센트인가?"
        - "전환율이 어떻게 되나요?"
        - "이탈 사용자의 특징은?"
        """)

        # LLM 비용 통계
        display_llm_cost_statistics()

    # 탭 선택
    tab1, tab2 = st.tabs(["NLQ 쿼리", "자동 분석"])

    # 탭 1: NLQ 쿼리
    with tab1:
        st.subheader("자유 쿼리 입력")
        st.markdown("*LLM이 자동으로 SQL을 생성합니다*")

        user_query = st.text_area(
            "자연어 쿼리",
            placeholder="예: 섹터별 리텐션을 알고 싶어요 / 파워 사용자 분석 / 이탈 사용자는?",
            height=100,
            label_visibility="collapsed",
            key="nlq_query"
        )

        col1, col2 = st.columns([1, 4])
        with col1:
            generate_sql = st.button("SQL 생성", type="primary", key="nlq_generate")

        if generate_sql:
            query_stripped = user_query.strip()
            if not query_stripped:
                st.error("질문을 입력해주세요.")
            elif len(query_stripped) < 5:
                st.error("너무 짧은 질문입니다. 좀 더 자세히 설명해주세요.")
                st.info("예: '섹터별 리텐션 분석', '파워 사용자는 누가 있나요?'")
            else:
                try:
                    with st.spinner("SQL 생성 중 (검증 및 비용 확인 포함)..."):
                        agent = get_agent()
                        from src.query.generator import SQLGenerator
                        from src.query.validator import SQLValidator

                        validator = SQLValidator()
                        generator = SQLGenerator(agent.config.llm)

                        # Self-correction 루프: 생성 → 검증 → 비용 확인 (자동)
                        sql_result = generator.generate_with_validation(
                            query_stripped,
                            validator,
                            bq_executor=agent.bq_executor,  # 비용 검증 통합
                            max_retries=3
                        )

                        if sql_result.is_success():
                            sql = sql_result.data

                            # LLM 비용 추적 (추정 토큰)
                            estimated_input_tokens = len(query_stripped) // 4 + 500  # 프롬프트 추정
                            estimated_output_tokens = len(sql) // 4  # SQL 출력 추정
                            _add_llm_cost("gemini-2.5-flash-lite", estimated_input_tokens, estimated_output_tokens)

                            st.session_state.pending_sql = sql
                            st.session_state.pending_query = query_stripped
                            st.success("SQL 생성 및 검증 완료!")
                        else:
                            st.error(f"SQL 생성 실패: {sql_result.error}")
                except Exception as e:
                    import traceback
                    st.error(f"예상치 못한 오류: {str(e)}")
                    st.write(traceback.format_exc())

        # SQL이 생성된 경우 표시
        if "pending_sql" in st.session_state:
            st.subheader("생성된 SQL (검토 후 실행하세요)")
            st.code(st.session_state.pending_sql, language="sql")

            # 실행 버튼
            col1, col2, col3 = st.columns([1, 1, 3])
            with col1:
                confirm_execute = st.button("실행", type="primary", key="nlq_execute")

            if confirm_execute:
                try:
                    with st.spinner("쿼리 실행 중..."):
                        agent = get_agent()
                        from src.executor.bigquery_client import BigQueryExecutor
                        from src.executor.data_processor import DataProcessor

                        # BigQuery 실행
                        bq_executor = BigQueryExecutor(agent.config.bigquery)
                        exec_result = bq_executor.execute(
                            st.session_state.pending_sql,
                            max_results=agent.config.bigquery.max_results
                        )

                        if not exec_result.is_success():
                            st.error(f"쿼리 실행 실패: {exec_result.error}")
                        else:
                            df = exec_result.data

                            # 데이터 처리
                            data_processor = DataProcessor(agent.config.analysis)
                            proc_result = data_processor.process(df)

                            if proc_result.is_success():
                                proc_data = proc_result.data
                                from src.types import AnalysisResult

                                # 상태 저장 (display_results 호출 전에 session_state에서 삭제)
                                saved_sql = st.session_state.pending_sql
                                saved_query = st.session_state.pending_query
                                del st.session_state.pending_sql
                                del st.session_state.pending_query

                                analysis_result = AnalysisResult(
                                    query=saved_query,
                                    sql=saved_sql,
                                    data=proc_data["df_cleaned"],
                                    stats=proc_data["stats"],
                                    explanation=proc_data["explanation"],
                                    success=True,
                                    data_quality=proc_data["data_quality"],
                                    sample_warning=proc_data["sample_warning"],
                                    cost_estimate={},
                                    cost_status="",
                                    cost_message="",
                                )
                                display_results(analysis_result)

                                # 완료 후 나머지 상태 초기화
                                if "cost_estimate" in st.session_state:
                                    del st.session_state.cost_estimate
                                if "cost_status" in st.session_state:
                                    del st.session_state.cost_status
                                if "cost_message" in st.session_state:
                                    del st.session_state.cost_message
                            else:
                                st.error(f"데이터 처리 실패: {proc_result.error}")
                except Exception as e:
                    st.error(f"예상치 못한 오류: {str(e)}")

            with col2:
                cancel_sql = st.button("취소", key="nlq_cancel")
                if cancel_sql:
                    del st.session_state.pending_sql
                    del st.session_state.pending_query
                    if "cost_estimate" in st.session_state:
                        del st.session_state.cost_estimate
                        del st.session_state.cost_status
                        del st.session_state.cost_message
                    st.rerun()

    # 탭 2: 자동 분석
    with tab2:
        st.subheader("📊 자동 분석")
        st.markdown("*키워드를 인식하여 자동으로 분석을 수행합니다*")

        analysis_query = st.text_area(
            "분석 질문",
            placeholder="예: 전환율이 어떻게 되나요? / 이탈 사용자는? / 리텐션 분석",
            height=100,
            label_visibility="collapsed",
            key="analysis_query"
        )

        col1, col2 = st.columns([1, 4])
        with col1:
            run_analysis = st.button("🚀 분석", type="primary", key="analysis_run")

        if run_analysis:
            query_stripped = analysis_query.strip()
            if not query_stripped:
                st.error("질문을 입력해주세요.")
            elif len(query_stripped) < 5:
                st.error("너무 짧은 질문입니다. 좀 더 자세히 설명해주세요.")
                st.info("예: '리텐션 분석', '이탈 사용자 특징', '쿼리 볼륨 추세'")
            else:
                try:
                    with st.spinner("분석 중..."):
                        analysis_agent = get_analysis_agent()
                        result = analysis_agent.analyze_question(query_stripped)
                        display_analysis_results(result)
                except Exception as e:
                    st.error(f"분석 실패: {str(e)}")


if __name__ == "__main__":
    main()
