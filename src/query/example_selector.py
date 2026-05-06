"""
동적 Few-shot 예시 선택 (DAIL-SQL 패턴)

사용자 쿼리와 의미론적으로 유사한 성공 사례를 자동으로 선택합니다.
- 1차: SentenceTransformer 임베딩 (가능 시)
- 2차 (폴백): 키워드 기반 유사도 (네트워크/메모리 제약 시)
"""

from typing import Any
import numpy as np

from src.bigquery_context.successful_queries import SUCCESSFUL_QUERIES
from src.logging_config import ContextualLogger

logger = ContextualLogger(__name__)


class DynamicExampleSelector:
    """사용자 쿼리 기반 동적 예시 선택기 (하이브리드: 임베딩 + 키워드 폴백)"""

    def __init__(self):
        """초기화 (임베딩 모델 선택적 로드)"""
        # SUCCESSFUL_QUERIES는 dict이므로 list로 변환
        self.examples = list(SUCCESSFUL_QUERIES.values()) if isinstance(SUCCESSFUL_QUERIES, dict) else SUCCESSFUL_QUERIES
        self.model = None
        self._example_embeddings = None
        self._use_embeddings = False

        # 임베딩 모델 시도 로드
        try:
            from sentence_transformers import SentenceTransformer
            self.model = SentenceTransformer("sentence-transformers/multilingual-MiniLM-L12-v2", local_files_only=False)
            self._compute_example_embeddings()
            self._use_embeddings = True
            logger.info(f"DynamicExampleSelector (임베딩 기반): {len(self.examples)}개 예시")
        except Exception as e:
            logger.warning(f"임베딩 로드 실패, 키워드 기반 폴백 사용: {str(e)}")
            self.model = None
            self._use_embeddings = False
            logger.info(f"DynamicExampleSelector (키워드 기반): {len(self.examples)}개 예시")

    def _compute_example_embeddings(self) -> None:
        """모든 예시의 임베딩 사전 계산"""
        if not self.model:
            return
        example_texts = [ex.get("description", "") + " " + ex.get("use_case", "") for ex in self.examples]
        self._example_embeddings = self.model.encode(example_texts, convert_to_tensor=True)

    def _similarity_embedding(self, query: str, top_k: int) -> list[tuple[int, float]]:
        """임베딩 기반 유사도 계산"""
        try:
            from sentence_transformers import util
            query_embedding = self.model.encode(query, convert_to_tensor=True)
            similarities = util.pytorch_cos_sim(query_embedding, self._example_embeddings)[0]
            top_k_indices = np.argsort(similarities.cpu().numpy())[::-1][:top_k]
            return [(int(idx), float(similarities[idx])) for idx in top_k_indices]
        except Exception as e:
            logger.warning(f"임베딩 유사도 계산 실패: {str(e)}")
            return []

    def _similarity_keyword(self, query: str, top_k: int) -> list[tuple[int, float]]:
        """키워드 기반 유사도 계산 (폴백)"""
        query_lower = query.lower()
        keywords = set(query_lower.split())

        scored = []
        for idx, ex in enumerate(self.examples):
            combined_text = (ex.get("description", "") + " " + ex.get("use_case", "")).lower()
            combined_words = set(combined_text.split())

            # 자카드 유사도
            if combined_words:
                intersection = len(keywords & combined_words)
                union = len(keywords | combined_words)
                score = intersection / union if union > 0 else 0.0
                if score > 0:
                    scored.append((idx, score))

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:top_k]

    def select_examples(self, query: str, top_k: int = 3) -> list[dict[str, Any]]:
        """
        사용자 쿼리와 유사한 예시 선택

        Args:
            query: 사용자 자연어 쿼리
            top_k: 반환할 예시 개수

        Returns:
            상위 K개 유사 예시 리스트
        """
        if not query:
            return list(self.examples)[:top_k]

        # 유사도 계산 (임베딩 또는 키워드)
        if self._use_embeddings and self._example_embeddings is not None:
            scored_indices = self._similarity_embedding(query, top_k)
            method = "embedding"
        else:
            scored_indices = self._similarity_keyword(query, top_k)
            method = "keyword"

        # 점수가 없으면 폴백
        if not scored_indices:
            logger.warning(f"유사도 계산 실패 ({method}), 상위 {top_k}개 반환")
            return list(self.examples)[:top_k]

        # 예시 반환 (유사도 점수 포함)
        selected = []
        for idx, score in scored_indices:
            if idx < len(self.examples):
                example = self.examples[idx].copy()
                example["similarity_score"] = score
                selected.append(example)

        logger.info(f"선택된 예시: {len(selected)}개 ({method} 기반, 상위 유사도: {scored_indices[0][1]:.3f})")
        return selected


# 싱글톤 인스턴스
_selector_instance = None


def get_selector() -> DynamicExampleSelector:
    """DynamicExampleSelector 싱글톤 획득"""
    global _selector_instance
    if _selector_instance is None:
        _selector_instance = DynamicExampleSelector()
    return _selector_instance
