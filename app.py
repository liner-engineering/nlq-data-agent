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


def display_results(result):
    """결과 표시"""
    if result.success:
        # SQL 표시
        st.subheader("✅ 생성된 SQL")
        st.code(result.sql, language="sql")

        # 결과 DataFrame
        st.subheader("📊 쿼리 결과")
        st.dataframe(result.data, use_container_width=True)

        # 통계 정보
        if result.stats:
            st.subheader("📈 통계 정보")
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
        if result.sample_warning:
            st.warning(result.sample_warning)

        # 데이터 품질 정보
        if result.data_quality:
            st.subheader("📋 데이터 품질")
            with st.expander("상세 정보"):
                st.json(result.data_quality)

        # 설명
        if result.explanation:
            st.subheader("💭 분석 결과")
            st.info(result.explanation)

    else:
        st.error(f"쿼리 실행 실패: {result.error}")


def display_analysis_results(result):
    """분석 결과 표시"""
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
    st.subheader("📈 데이터")
    st.dataframe(result.data, use_container_width=True)

    # 인사이트
    st.subheader("💡 주요 인사이트")
    for insight in result.insights:
        st.write(f"• {insight}")

    # 추천사항
    st.subheader("🎯 추천사항")
    for rec in result.recommendations:
        st.write(f"→ {rec}")

    # 통계 검정 결과
    if result.test_results:
        st.subheader("📊 통계 검정")
        for test in result.test_results:
            with st.expander(f"{test.test_name} (p={test.p_value:.4f})"):
                st.json(test.to_dict())

    # 상세 통계
    if result.statistics:
        st.subheader("📋 상세 통계")
        with st.expander("통계 정보"):
            st.json(result.statistics)


def main():
    # 헤더
    st.title("🚀 NLQ Data Agent")
    st.markdown("자연어로 데이터를 분석하세요. SQL을 자동으로 생성하고 실행합니다.")

    # 사이드바 설정 (선택사항)
    with st.sidebar:
        st.header("⚙️ 설정")

        with st.expander("모델 설정", expanded=False):
            model_name = st.text_input(
                "LLM 모델",
                value="gemini-2.5-flash-lite",
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
        st.markdown(f"**API 키 설정**: {'✅ 설정됨' if api_key_set else '❌ 미설정'}")

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

    # 탭 선택
    tab1, tab2 = st.tabs(["🔍 NLQ 쿼리", "📊 자동 분석"])

    # 탭 1: NLQ 쿼리
    with tab1:
        st.subheader("📝 자유 쿼리 입력")
        st.markdown("*LLM이 자동으로 SQL을 생성합니다*")

        user_query = st.text_area(
            "자연어 쿼리",
            placeholder="예: 2026년 4월 professional 섹터의 D+7 리텐션이 몇 퍼센트인가?",
            height=100,
            label_visibility="collapsed",
            key="nlq_query"
        )

        col1, col2 = st.columns([1, 4])
        with col1:
            run_nlq = st.button("🚀 실행", type="primary", key="nlq_run")

        if run_nlq:
            if not user_query.strip():
                st.error("질문을 입력해주세요.")
            else:
                try:
                    with st.spinner("쿼리 분석 중..."):
                        agent = get_agent()
                        result = agent.analyze(user_query)
                        display_results(result)
                except NLQAgentException as e:
                    st.error(f"에이전트 오류: {e.to_dict()}")
                except Exception as e:
                    st.error(f"예상치 못한 오류: {str(e)}")

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
            if not analysis_query.strip():
                st.error("질문을 입력해주세요.")
            else:
                try:
                    with st.spinner("분석 중..."):
                        analysis_agent = get_analysis_agent()
                        result = analysis_agent.analyze_question(analysis_query)
                        display_analysis_results(result)
                except Exception as e:
                    st.error(f"분석 실패: {str(e)}")


if __name__ == "__main__":
    main()
