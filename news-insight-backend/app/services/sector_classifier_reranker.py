"""
Phase 2: BGE-M3 Re-ranking

의미 기반 후보 축소 (Top-5 → Top-2)
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

from app.models.sector_reference import get_sector_reference, get_all_sector_references, LEGACY_SECTOR_MAPPING

# 모델 캐시
_bge_model = None
_sector_reference_embeddings_bge = None


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
            import time
            load_start = time.time()
            logger.info(f"🔄 [BGE-M3] 모델 로딩 시작: {model_name}")
            # GPU 강제 사용
            import torch
            device = "cuda" if torch.cuda.is_available() else "cpu"
            _bge_model = get_direct_bge_model(model_name=model_name, device=device, use_fp16=(device == "cuda"))
            load_time = time.time() - load_start
            logger.info(f"✅ [BGE-M3] 모델 로딩 완료: {_bge_model.device} ({load_time:.2f}초)")
        except Exception as e:
            logger.error(f"❌ [BGE-M3] 모델 로딩 실패: {e}", exc_info=True)
            raise
    
    return _bge_model


def get_sector_reference_embeddings_bge(model=None) -> Dict[str, np.ndarray]:
    """
    섹터별 Reference 텍스트 임베딩 사전 계산 (BGE-M3, 캐싱)
    
    Args:
        model: BGEM3FlagModel 인스턴스 (None이면 자동 로드)
    
    Returns:
        {sector_code: embedding_vector} 딕셔너리
    """
    global _sector_reference_embeddings_bge
    
    if _sector_reference_embeddings_bge is None:
        if model is None:
            model = get_bge_model()
        
        logger.info("Pre-computing sector reference embeddings (BGE-M3)...")
        sector_refs = get_all_sector_references()
        
        sector_codes = []
        ref_texts = []
        for sector_code, ref_text in sector_refs.items():
            if ref_text:
                sector_codes.append(sector_code)
                ref_texts.append(ref_text)
        
        # BGE-M3 배치 임베딩 생성
        embeddings = model.encode(ref_texts, batch_size=32, max_length=8192)
        # dense embeddings만 사용 (BGE-M3는 dense, sparse, colbert 3가지 제공)
        if isinstance(embeddings, dict):
            embeddings = embeddings.get('dense_vecs', embeddings)
        
        _sector_reference_embeddings_bge = {
            sector_code: embedding
            for sector_code, embedding in zip(sector_codes, embeddings)
        }
        
        logger.info(f"Pre-computed {len(_sector_reference_embeddings_bge)} sector reference embeddings (BGE-M3)")
    
    return _sector_reference_embeddings_bge


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


def rerank_sector_candidates(
    company_text: str,
    candidates: List[Dict[str, float]],
    top_k: int = 2,
    model_name: Optional[str] = None
) -> List[Dict[str, float]]:
    """
    BGE-M3 기반 섹터 후보 Re-ranking (Top-5 → Top-2)
    
    역할: 의미 기반 필터링 및 재랭킹
    
    Args:
        company_text: 회사 설명 텍스트 (전체 DART 문서 또는 biz_summary)
        candidates: 임베딩 모델에서 생성된 Top-5 후보
                   [{'sector': 'SEC_SEMI', 'score': 0.85}, ...]
        top_k: 최종 선택할 후보 개수 (기본값: 2)
        model_name: 모델 이름 (None이면 기본값 사용)
    
    Returns:
        Re-ranking된 Top-K 후보
        [{'sector': 'SEC_SEMI', 'score': 0.82, 'bge_score': 0.82}, ...]
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
        
        # 섹터별 Reference 임베딩 가져오기 (캐싱됨)
        sector_embeddings = get_sector_reference_embeddings_bge(model)
        
        # 후보 섹터들만 Re-ranking
        reranked_candidates = []
        for candidate in candidates:
            sector_code = candidate['sector']
            original_score = candidate.get('score', 0.0)
            
            # 기존 섹터 코드 매핑 (하위 호환성)
            if sector_code not in sector_embeddings and sector_code in LEGACY_SECTOR_MAPPING:
                mapped_code = LEGACY_SECTOR_MAPPING[sector_code]
                logger.info(f"Sector {sector_code} → {mapped_code} 매핑")
                sector_code = mapped_code
            
            if sector_code not in sector_embeddings:
                logger.warning(f"Sector {sector_code} not found in reference embeddings. Skipping.")
                # 원본 후보에 포함하여 Fallback
                reranked_candidates.append({
                    'sector': candidate['sector'],
                    'score': original_score,
                    'bge_score': original_score,  # BGE 점수 없으면 원본 점수 사용
                    'combined_score': original_score
                })
                continue
            
            ref_embedding = np.array(sector_embeddings[sector_code])
            
            # BGE-M3 유사도 계산
            bge_score = compute_cosine_similarity_bge(company_embedding, ref_embedding)
            
            reranked_candidates.append({
                'sector': sector_code,
                'score': original_score,  # 원본 임베딩 모델 점수
                'bge_score': float(bge_score),  # BGE-M3 점수
                'combined_score': float((original_score * 0.5 + bge_score * 0.5))  # 가중 평균
            })
        
        # BGE-M3 점수 기준으로 정렬
        reranked_candidates.sort(key=lambda x: x['bge_score'], reverse=True)
        
        # Top-K 추출
        top_reranked = reranked_candidates[:top_k]
        
        logger.debug(f"Reranked {len(candidates)} candidates → {len(top_reranked)} final candidates (top_k={top_k})")
        
        # Re-ranking 결과가 없으면 원본 후보 반환 (Fallback)
        if not top_reranked:
            logger.warning("Re-ranking 결과 없음. 원본 후보 반환")
            return candidates[:top_k]
        
        return top_reranked
        
    except Exception as e:
        logger.error(f"Error in BGE-M3 reranking: {e}", exc_info=True)
        # 에러 발생 시 원본 후보 반환
        return candidates[:top_k]

