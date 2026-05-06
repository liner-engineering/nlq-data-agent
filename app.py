"""
NLQ Data Agent Streamlit Dashboard

자연어 쿼리를 SQL로 변환하고 BigQuery에서 실행하는 웹 대시보드
"""

import os
import streamlit as st
import pandas as pd
from src.agent import NLQAgent
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
        **사용 방법:**
        1. 자연어로 질문을 입력
        2. '🚀 실행' 버튼 클릭
        3. 생성된 SQL과 결과 확인

        **예시 질문:**
        - "2026년 4월 professional 섹터의 D+7 리텐션이 몇 퍼센트인가?"
        - "섹터별 평균 리텐션을 보여줘"
        - "가장 활발한 사용자 그룹은 어디인가?"
        """)

    # 메인 영역
    st.subheader("📝 질문을 입력하세요")

    user_query = st.text_area(
        "자연어 쿼리",
        placeholder="예: 2026년 4월 professional 섹터의 D+7 리텐션이 몇 퍼센트인가?",
        height=100,
        label_visibility="collapsed"
    )

    # 실행 버튼
    col1, col2 = st.columns([1, 4])
    with col1:
        run_button = st.button("🚀 실행", type="primary")

    # 결과 표시
    if run_button:
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


if __name__ == "__main__":
    main()
