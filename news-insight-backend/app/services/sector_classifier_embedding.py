"""
Phase 1: 임베딩 모델 기반 후보 생성기 (Solar Embedding 전환)

Solar Embedding 모델을 사용하여 섹터 분류 후보 생성
- 역할: Top-5 후보 섹터 추출 (최종 결정 아님)
- 모델: solar-embedding-1-large-passage (Upstage)
- 벡터 DB 활용: 재임베딩 최소화
"""
import logging
import numpy as np
from typing import List, Dict, Tuple, Optional
from functools import lru_cache
import os
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

from app.models.sector_reference import SECTOR_REFERENCES, get_sector_reference, get_all_sector_references, LEGACY_SECTOR_MAPPING
from app.services.solar_embedding_model import (
    get_or_create_embedding,
    get_sector_reference_embeddings as get_solar_sector_reference_embeddings,
    encode_solar_embedding
)

# 호환성을 위한 변수
SENTENCE_TRANSFORMERS_AVAILABLE = True  # Solar Embedding 사용으로 항상 True

# 섹터 참조 임베딩 캐시
_sector_reference_embeddings = None


def get_embedding_model(model_name: str = None):
    """
    Solar Embedding 모델 (호환성을 위한 래퍼)
    
    Args:
        model_name: 모델 이름 (무시됨, Solar Embedding만 사용)
    
    Returns:
        None (Solar Embedding은 API 기반이므로 모델 객체 없음)
    """
    # 호환성을 위해 None 반환 (실제로는 사용 안 함)
    return None


def get_sector_reference_embeddings(model=None, db: Optional[Session] = None) -> Dict[str, np.ndarray]:
    """
    각 섹터의 참조 텍스트 임베딩을 미리 계산 (캐싱)
    
    Args:
        model: 임베딩 모델 (호환성을 위한 파라미터, 사용 안 함)
        db: DB 세션 (Solar Embedding은 DB 사용 안 함, 섹터 참조는 메모리 캐싱)
    
    Returns:
        섹터별 참조 임베딩 딕셔너리
    """
    global _sector_reference_embeddings
    
    if _sector_reference_embeddings is not None:
        return _sector_reference_embeddings
    
    # Solar Embedding의 섹터 참조 임베딩 생성
    _sector_reference_embeddings = get_solar_sector_reference_embeddings(db=db, model=None)
    
    logger.info(f"Pre-computed {len(_sector_reference_embeddings)} sector reference embeddings (Solar Embedding, 캐시됨)")
    
    return _sector_reference_embeddings


def _build_sector_reference_text(ref) -> str:
    """
    섹터 참조 정보를 텍스트로 변환
    
    Args:
        ref: 섹터 참조 정보 (문자열 또는 딕셔너리)
    
    Returns:
        텍스트 문자열
    """
    # 문자열인 경우 그대로 반환
    if isinstance(ref, str):
        return ref
    
    # 딕셔너리인 경우 처리
    if not isinstance(ref, dict):
        return str(ref) if ref else ""
    
    parts = []
    
    # 섹터 이름
    if 'name_ko' in ref:
        parts.append(ref['name_ko'])
    
    # 서브섹터 이름들
    if 'sub_sectors' in ref:
        for sub in ref['sub_sectors'].values():
            if isinstance(sub, dict) and 'name_ko' in sub:
                parts.append(sub['name_ko'])
            elif isinstance(sub, str):
                parts.append(sub)
    
    # 키워드
    if 'keywords' in ref and isinstance(ref['keywords'], list):
        parts.extend(ref['keywords'][:20])  # 상위 20개 키워드만
    
    return ' '.join(parts) if parts else str(ref)


def compute_cosine_similarity(emb1: np.ndarray, emb2: np.ndarray) -> float:
    """
    코사인 유사도 계산
    
    Args:
        emb1: 첫 번째 임베딩 벡터
        emb2: 두 번째 임베딩 벡터
    
    Returns:
        코사인 유사도 (0.0 ~ 1.0)
    """
    dot_product = np.dot(emb1, emb2)
    norm1 = np.linalg.norm(emb1)
    norm2 = np.linalg.norm(emb2)
    
    if norm1 == 0 or norm2 == 0:
        return 0.0
    
    return float(dot_product / (norm1 * norm2))


def generate_sector_candidates(
    company_text: str,
    top_k: int = 5,
    min_threshold: float = 0.3,
    model_name: Optional[str] = None,
    db: Optional[Session] = None,
    ticker: Optional[str] = None,
    force_regenerate: bool = False
) -> List[Dict[str, float]]:
    """
    Solar Embedding 기반 섹터 후보 생성 (Top-K)
    
    역할: 후보 생성기 (최종 결정 아님)
    
    Args:
        company_text: 회사 설명 텍스트 (biz_summary + products + keywords)
        top_k: 추출할 후보 개수 (기본값: 5)
        min_threshold: 최소 유사도 임계값 (기본값: 0.3)
        model_name: 모델 이름 (호환성을 위한 파라미터, 사용 안 함)
        db: DB 세션 (벡터 DB 조회/저장용)
        ticker: 종목코드 (벡터 DB 조회/저장용)
        force_regenerate: 강제 재생성 여부
    
    Returns:
        [
            {'sector': 'SEC_SEMI', 'score': 0.85},
            {'sector': 'SEC_BATTERY', 'score': 0.72},
            ...
        ]
    """
    if not company_text or len(company_text.strip()) < 10:
        logger.warning("Company text is too short for embedding-based classification")
        return []
    
    try:
        # 섹터 참조 임베딩 로드
        sector_embeddings = get_sector_reference_embeddings(model=None, db=db)
        
        if not sector_embeddings:
            logger.error("Sector reference embeddings not available")
            return []
        
        # 회사 텍스트 임베딩 계산 (벡터 DB 활용)
        if db and ticker:
            # 벡터 DB에서 조회 또는 생성
            company_embedding = get_or_create_embedding(
                db=db,
                ticker=ticker,
                text=company_text,
                force_regenerate=force_regenerate
            )
        else:
            # DB 없이 직접 생성 (테스트용)
            logger.warning("DB or ticker not provided, generating embedding without caching")
            company_embedding = encode_solar_embedding(company_text)
        
        if company_embedding is None:
            logger.warning(f"임베딩 생성 실패, Rule-based로 fallback: {ticker if ticker else 'unknown'}")
            return []  # ✅ Rule-based 분류로 자동 fallback
        
        # 각 섹터와의 유사도 계산
        similarities = []
        for sector_code, sector_embedding in sector_embeddings.items():
            similarity = compute_cosine_similarity(company_embedding, sector_embedding)
            if similarity >= min_threshold:
                similarities.append({
                    'sector': sector_code,
                    'score': similarity
                })
        
        # 유사도 기준 정렬 후 Top-K 반환
        similarities.sort(key=lambda x: x['score'], reverse=True)
        
        return similarities[:top_k]
        
    except Exception as e:
        logger.error(f"Solar Embedding-based sector candidate generation failed: {e}")
        import traceback
        traceback.print_exc()
        return []


def get_embedding_for_text(
    text: str,
    model=None,
    db: Optional[Session] = None,
    ticker: Optional[str] = None
) -> Optional[np.ndarray]:
    """
    텍스트의 임베딩 벡터 반환 (Solar Embedding)
    
    Args:
        text: 임베딩할 텍스트
        model: 임베딩 모델 (호환성을 위한 파라미터, 사용 안 함)
        db: DB 세션 (벡터 DB 조회/저장용)
        ticker: 종목코드 (벡터 DB 조회/저장용)
    
    Returns:
        임베딩 벡터 (numpy 배열) 또는 None
    """
    if not text or len(text.strip()) < 2:
        return None
    
    try:
        if db and ticker:
            # 벡터 DB에서 조회 또는 생성
            embedding = get_or_create_embedding(
                db=db,
                ticker=ticker,
                text=text,
                force_regenerate=False
            )
        else:
            # DB 없이 직접 생성
            embedding = encode_solar_embedding(text)
        
        return embedding
    
    except Exception as e:
        logger.error(f"Failed to generate Solar Embedding: {e}")
        return None


def batch_generate_embeddings(
    texts: List[str],
    batch_size: int = 10,  # Solar Embedding API 배치 크기
    model=None,
    db: Optional[Session] = None,
    tickers: Optional[List[str]] = None
) -> List[np.ndarray]:
    """
    배치 임베딩 생성 (Solar Embedding)

    Args:
        texts: 임베딩할 텍스트 리스트
        batch_size: 배치 크기 (Solar Embedding API 최적화)
        model: 임베딩 모델 (호환성을 위한 파라미터, 사용 안 함)
        db: DB 세션 (벡터 DB 조회/저장용)
        tickers: 종목코드 리스트 (벡터 DB 조회/저장용)
    
    Returns:
        임베딩 벡터 리스트
    """
    if not texts:
        return []
    
    try:
        # tickers가 제공된 경우 벡터 DB 활용
        if db and tickers and len(tickers) == len(texts):
            embeddings = []
            for text, ticker in zip(texts, tickers):
                embedding = get_or_create_embedding(
                    db=db,
                    ticker=ticker,
                    text=text,
                    force_regenerate=False
                )
                if embedding is not None:
                    embeddings.append(embedding)
                else:
                    logger.warning(f"Failed to generate embedding for ticker: {ticker}")
            return embeddings
        else:
            # DB 없이 직접 생성 (배치 API 호출)
            embeddings = encode_solar_embedding(texts, batch_size=batch_size)
            if isinstance(embeddings, np.ndarray) and len(embeddings.shape) == 1:
                # 단일 텍스트인 경우 리스트로 변환
                return [embeddings]
            return list(embeddings) if isinstance(embeddings, list) else [embeddings]
    
    except Exception as e:
        logger.error(f"Failed to generate batch Solar Embeddings: {e}")
        return []
