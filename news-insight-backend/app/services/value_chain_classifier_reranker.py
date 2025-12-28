"""
Phase 2: BGE-M3 Re-ranking (밸류체인 분류)

의미 기반 후보 축소 (Top-3 → Top-2)
- 역할: 의미 기반 필터링 및 재랭킹
- 모델: BGE-M3 (8192 토큰, 장문 문맥 반영)
- 구현: DirectBGEM3Model (FlagEmbedding 완전 우회, Meta Tensor 문제 해결)
"""
import logging
import numpy as np
from typing import List, Dict, Optional
from functools import lru_cache

logger = logging.getLogger(__name__)

# DirectBGEM3Model 사용 (FlagEmbedding 완전 우회)
BGEM3_AVAILABLE = True
try:
    from app.services.bge_model_direct import get_direct_bge_model, DirectBGEM3Model
    logger.info("DirectBGEM3Model 사용 가능 (FlagEmbedding 우회, Meta Tensor 문제 해결)")
except ImportError as e:
    BGEM3_AVAILABLE = False
    logger.warning(f"DirectBGEM3Model import 실패: {e}. BGE-M3 reranking will not be available.")

from app.models.value_chain_reference import (
    get_value_chain_reference,
    get_all_value_chain_references,
    GENERAL_VALUE_CHAIN_REFERENCES
)
# 확장 밸류체인 정의(Distribution/Service/Finance 등) 활용을 위한 import
from app.services.value_chain_classifier_embedding import (
    VALUE_CHAIN_REFERENCES as EXT_VALUE_CHAIN_REFERENCES,
    _build_value_chain_reference_text as build_ext_vc_text,
)

# 모델 캐시
_bge_model = None
_value_chain_reference_embeddings_bge = {}


def get_bge_model(model_name: str = 'BAAI/bge-m3'):
    """
    BGE-M3 모델 로드 (캐싱)
    DirectBGEM3Model 사용 (FlagEmbedding 완전 우회, Meta Tensor 문제 해결)
    
    Args:
        model_name: HuggingFace 모델 이름
    
    Returns:
        DirectBGEM3Model 인스턴스
    """
    global _bge_model
    
    if not BGEM3_AVAILABLE:
        raise ImportError("DirectBGEM3Model is required for BGE-M3")
    
    if _bge_model is None:
        try:
            logger.info(f"Loading DirectBGEM3Model (bypassing FlagEmbedding): {model_name}")
            _bge_model = get_direct_bge_model(model_name=model_name, use_fp16=False)
            logger.info(f"✅ DirectBGEM3Model loaded successfully on {_bge_model.device}")
        except Exception as e:
            logger.error(f"Failed to load DirectBGEM3Model: {e}", exc_info=True)
            raise
    
    return _bge_model


def get_value_chain_reference_embeddings_bge(
    sector_code: str,
    model=None,
    force_reload: bool = True  # 확장 밸류체인 추가 반영을 위해 기본 재계산
) -> Dict[str, np.ndarray]:
    """
    섹터별 밸류체인 위치 Reference 텍스트 임베딩 사전 계산 (BGE-M3, 캐싱)
    
    Args:
        sector_code: 섹터 코드
        model: BGEM3FlagModel 인스턴스 (None이면 자동 로드)
        force_reload: 강제 재로드 여부
    
    Returns:
        {value_chain: embedding_vector} 딕셔너리
    """
    global _value_chain_reference_embeddings_bge
    
    cache_key = sector_code
    
    if force_reload and cache_key in _value_chain_reference_embeddings_bge:
        del _value_chain_reference_embeddings_bge[cache_key]
    
    if cache_key not in _value_chain_reference_embeddings_bge:
        if model is None:
            model = get_bge_model()
        
        logger.debug(f"Pre-computing BGE-M3 value chain reference embeddings for {sector_code}...")
        vc_refs = get_all_value_chain_references(sector_code)
        
        vc_types: List[str] = []
        ref_texts: List[str] = []
        
        # 1) 섹터별 특화 Reference (UP/MID/DOWN)
        for vc_type, ref_text in vc_refs.items():
            if ref_text:
                vc_types.append(vc_type)
                ref_texts.append(ref_text)
        
        # 2) 확장 밸류체인 Reference (DISTRIBUTION/SERVICE/FINANCE/INFRA/RND 등)
        for vc_type, ref in EXT_VALUE_CHAIN_REFERENCES.items():
            if vc_type in vc_types:
                continue
            try:
                ref_text = build_ext_vc_text(ref)
                vc_types.append(vc_type)
                ref_texts.append(ref_text)
            except Exception:
                # 만약 빌드에 실패해도 다른 타입들은 계속 처리
                continue
        
        # 3) 그래도 없으면 일반 Reference 사용 (안전장치)
        if not ref_texts:
            for vc_type, ref_text in GENERAL_VALUE_CHAIN_REFERENCES.items():
                vc_types.append(vc_type)
                ref_texts.append(ref_text)
        
        # 배치 임베딩 생성
        embeddings = model.encode(
            ref_texts,
            batch_size=len(ref_texts),
            max_length=8192
        )
        
        if isinstance(embeddings, dict):
            embeddings = embeddings.get('dense_vecs', embeddings)
        
        if isinstance(embeddings, list):
            embeddings = np.array(embeddings)
        
        _value_chain_reference_embeddings_bge[cache_key] = {
            vc_type: embedding
            for vc_type, embedding in zip(vc_types, embeddings)
        }
        
        logger.debug(f"Pre-computed {len(_value_chain_reference_embeddings_bge[cache_key])} BGE-M3 value chain reference embeddings for {sector_code}")
    
    return _value_chain_reference_embeddings_bge[cache_key]


def compute_cosine_similarity_bge(emb1: np.ndarray, emb2: np.ndarray) -> float:
    """
    코사인 유사도 계산 (BGE-M3용)
    
    Args:
        emb1: 첫 번째 임베딩 벡터
        emb2: 두 번째 임베딩 벡터
    
    Returns:
        코사인 유사도 (0.0 ~ 1.0)
    """
    # 1차원 벡터로 변환
    if emb1.ndim > 1:
        emb1 = emb1.flatten()
    if emb2.ndim > 1:
        emb2 = emb2.flatten()
    
    dot_product = np.dot(emb1, emb2)
    norm1 = np.linalg.norm(emb1)
    norm2 = np.linalg.norm(emb2)
    
    if norm1 == 0 or norm2 == 0:
        return 0.0
    
    return float(dot_product / (norm1 * norm2))


def rerank_value_chain_candidates(
    company_text: str,
    sector_code: str,
    candidates: List[Dict[str, any]],
    top_k: int = 2,
    model_name: Optional[str] = None
) -> List[Dict[str, any]]:
    """
    BGE-M3 기반 밸류체인 위치 후보 Re-ranking (Top-3 → Top-2)
    
    역할: 의미 기반 필터링 및 재랭킹
    
    Args:
        company_text: 회사 설명 텍스트 (전체 DART 문서 또는 biz_summary)
        sector_code: 섹터 코드
        candidates: 임베딩 모델에서 생성된 Top-3 후보
                   [{'value_chain': 'MIDSTREAM', 'similarity': 0.85, 'confidence': 'HIGH'}, ...]
        top_k: 최종 선택할 후보 개수 (기본값: 2)
        model_name: 모델 이름 (None이면 기본값 사용)
    
    Returns:
        Re-ranking된 Top-K 후보
        [{'value_chain': 'MIDSTREAM', 'similarity': 0.82, 'bge_score': 0.82, 'confidence': 'HIGH'}, ...]
    """
    if not candidates or not company_text or not company_text.strip():
        logger.warning("Empty candidates or company text provided")
        return candidates[:top_k] if candidates else []
    
    if not BGEM3_AVAILABLE:
        logger.warning("BGE-M3 not available. Skipping reranking.")
        return candidates[:top_k]
    
    try:
        # 모델 로드
        model = get_bge_model(model_name) if model_name else get_bge_model()
        
        # 회사 텍스트 임베딩 (장문 지원: max_length=8192)
        company_embedding = model.encode([company_text], batch_size=1, max_length=8192)
        if isinstance(company_embedding, dict):
            company_embedding = company_embedding.get('dense_vecs', company_embedding)
        
        if isinstance(company_embedding, list):
            company_embedding = company_embedding[0]
        
        company_embedding = np.array(company_embedding)
        
        # 섹터별 밸류체인 위치 Reference 임베딩 가져오기 (캐싱됨)
        vc_embeddings = get_value_chain_reference_embeddings_bge(sector_code, model)
        
        # 후보 밸류체인 위치들만 Re-ranking
        reranked_candidates = []
        for candidate in candidates:
            vc_type = candidate['value_chain']
            original_similarity = candidate.get('similarity', 0.0)
            
            if vc_type not in vc_embeddings:
                logger.warning(f"Value chain {vc_type} not found in reference embeddings for {sector_code}. Skipping.")
                continue
            
            ref_embedding = vc_embeddings[vc_type]
            bge_similarity = compute_cosine_similarity_bge(company_embedding, ref_embedding)
            
            # BGE-M3 점수와 원본 점수 평균 (가중 평균)
            combined_score = 0.7 * bge_similarity + 0.3 * original_similarity
            
            # Confidence 결정
            if combined_score > 0.75:
                confidence = 'HIGH'
            elif combined_score > 0.60:
                confidence = 'MEDIUM'
            else:
                confidence = 'LOW'
            
            reranked_candidates.append({
                'value_chain': vc_type,
                'similarity': float(combined_score),
                'bge_score': float(bge_similarity),
                'original_score': float(original_similarity),
                'confidence': confidence
            })
        
        # 점수 내림차순 정렬 후 Top-K 추출
        reranked_candidates.sort(key=lambda x: x['similarity'], reverse=True)
        top_reranked = reranked_candidates[:top_k]
        
        logger.debug(
            f"Re-ranked {len(top_reranked)} value chain candidates "
            f"(sector={sector_code}, top_k={top_k})"
        )
        
        if not top_reranked:
            # Re-ranking 결과 없으면 원본 후보 반환
            logger.warning("Re-ranking 결과 없음. 원본 후보 반환.")
            return candidates[:top_k]
        
        return top_reranked
        
    except Exception as e:
        logger.error(f"Error in BGE-M3 value chain reranking: {e}", exc_info=True)
        # 에러 발생 시 원본 후보 반환
        return candidates[:top_k]

