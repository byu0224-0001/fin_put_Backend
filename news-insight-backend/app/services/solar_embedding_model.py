"""
Solar Embedding 모델 - Upstage API 연동

solar-embedding-1-large-passage 모델 사용
- 4,000 토큰 지원
- 벡터 차원: 4096
- Passage Embedding 최적화
"""
import logging
import os
import hashlib
import time
import re
from typing import List, Optional, Dict, Any, Union
import numpy as np
import requests
from requests.exceptions import RequestException, HTTPError
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)

# 모델 설정
MODEL_NAME = "solar-embedding-1-large-passage"
MAX_TOKENS = 4000
EMBEDDING_DIM = 4096
API_BASE_URL = "https://api.upstage.ai/v1/embeddings"

# 전역 캐시 (메모리)
_embedding_cache = {}


def get_upstage_api_key() -> Optional[str]:
    """Upstage API 키 조회"""
    api_key = os.getenv('UPSTAGE_API_KEY')
    if not api_key:
        logger.warning("UPSTAGE_API_KEY 환경 변수가 설정되지 않았습니다.")
    return api_key


def normalize_text_for_hash(text: str) -> str:
    """
    텍스트 정규화 (hash 일관성 보장)
    
    동일 의미의 텍스트가 항상 동일한 hash를 가지도록 정규화
    - 공백/줄바꿈 차이 제거
    - 연속 공백 → 1칸
    """
    if not text:
        return ""
    
    # 1. strip
    text = text.strip()
    
    # 2. 탭/개행 문자를 공백으로 통일
    text = text.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
    
    # 3. 연속 공백 → 1칸
    text = re.sub(r'\s+', ' ', text)
    
    # 4. 앞뒤 공백 제거
    text = text.strip()
    
    return text


def calculate_text_hash(text: str) -> str:
    """
    정규화된 텍스트의 SHA256 해시 계산 (재임베딩 방지용)
    
    ⚠️ 중요: 정규화를 통해 동일 의미 텍스트가 항상 동일한 hash를 가짐
    """
    normalized = normalize_text_for_hash(text)
    return hashlib.sha256(normalized.encode('utf-8')).hexdigest()


def encode_solar_embedding(
    texts: Union[str, List[str]],
    batch_size: int = 10,
    max_retries: int = 5,
    retry_delay: float = 2.0
) -> Union[np.ndarray, List[np.ndarray]]:
    """
    Solar Embedding API를 사용하여 텍스트 임베딩 생성 (재시도 로직 포함)
    
    Args:
        texts: 단일 텍스트 또는 텍스트 리스트
        batch_size: 배치 크기 (API 호출 최적화)
        max_retries: 최대 재시도 횟수 (기본값: 5)
        retry_delay: 재시도 대기 시간(초) (기본값: 2.0)
    
    Returns:
        단일 텍스트인 경우: numpy 배열 (4096차원)
        리스트인 경우: numpy 배열 리스트
    """
    api_key = get_upstage_api_key()
    if not api_key:
        # .env 파일 경로 확인 메시지 추가
        env_path = os.path.join(os.path.dirname(__file__), '..', '..', '.env')
        env_path_abs = os.path.abspath(env_path)
        error_msg = (
            f"UPSTAGE_API_KEY가 설정되지 않았습니다.\n"
            f"확인 사항:\n"
            f"  1. .env 파일 경로: {env_path_abs}\n"
            f"  2. .env 파일에 'UPSTAGE_API_KEY=...' 형식으로 설정되어 있는지 확인\n"
            f"  3. 현재 환경 변수: {os.getenv('UPSTAGE_API_KEY', '없음')}"
        )
        raise ValueError(error_msg)
    
    is_single = isinstance(texts, str)
    if is_single:
        texts = [texts]
    
    all_embeddings = []
    
    # 배치 처리 (각 배치마다 재시도)
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(texts) + batch_size - 1) // batch_size
        
        # 배치별 재시도 로직
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    API_BASE_URL,
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": MODEL_NAME,
                        "input": batch
                    },
                    timeout=120  # VPN/네트워크 오류 대응을 위해 타임아웃 증가
                )
                response.raise_for_status()
                
                result = response.json()
                batch_embeddings = [item['embedding'] for item in result['data']]
                all_embeddings.extend(batch_embeddings)
                
                # 성공하면 다음 배치로
                logger.debug(f"[Solar] 배치 {batch_num}/{total_batches} 임베딩 생성 성공")
                break
                
            except requests.exceptions.Timeout as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (attempt + 1)
                    logger.warning(f"[Solar] 배치 {batch_num}/{total_batches} 타임아웃 (시도 {attempt+1}/{max_retries}), {wait_time:.1f}초 후 재시도...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"[Solar] 배치 {batch_num}/{total_batches} 타임아웃 - 최대 재시도 횟수 초과")
                    raise
                    
            except requests.exceptions.ConnectionError as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (attempt + 1)
                    logger.warning(f"[Solar] 배치 {batch_num}/{total_batches} 연결 오류 (시도 {attempt+1}/{max_retries}), {wait_time:.1f}초 후 재시도... (VPN/네트워크 확인 필요)")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"[Solar] 배치 {batch_num}/{total_batches} 연결 오류 - 최대 재시도 횟수 초과 (VPN/네트워크 확인 필요)")
                    raise
                    
            except requests.exceptions.RequestException as e:
                # HTTP 429 (Rate Limit) 또는 기타 오류
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (attempt + 1)
                    status_code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
                    logger.warning(f"[Solar] 배치 {batch_num}/{total_batches} API 오류 (HTTP {status_code}, 시도 {attempt+1}/{max_retries}), {wait_time:.1f}초 후 재시도...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"[Solar] 배치 {batch_num}/{total_batches} API 호출 실패 - 최대 재시도 횟수 초과: {e}")
                    raise
    
    # numpy 배열로 변환
    embeddings = [np.array(emb, dtype=np.float32) for emb in all_embeddings]
    
    if is_single:
        return embeddings[0]
    return embeddings


def get_embedding_from_db(
    db: Session,
    ticker: str,
    text_hash: str
) -> Optional[np.ndarray]:
    """
    벡터 DB에서 임베딩 조회
    
    Args:
        db: DB 세션
        ticker: 종목코드
        text_hash: 텍스트 해시
    
    Returns:
        임베딩 벡터 (numpy 배열) 또는 None
    """
    try:
        # pgvector 사용하여 조회
        query = text("""
            SELECT embedding_vector 
            FROM company_embeddings 
            WHERE ticker = :ticker AND text_hash = :text_hash
        """)
        result = db.execute(query, {"ticker": ticker, "text_hash": text_hash}).fetchone()
        
        if result:
            # pgvector의 vector 타입은 문자열로 반환되므로 파싱 필요
            embedding_str = result[0]
            # PostgreSQL의 vector 타입은 '[0.1,0.2,...]' 형식
            embedding = np.array(eval(embedding_str), dtype=np.float32)
            return embedding
        return None
    except Exception as e:
        logger.warning(f"벡터 DB 조회 실패 (ticker={ticker}): {e}")
        return None


def save_embedding_to_db(
    db: Session,
    ticker: str,
    text_hash: str,
    embedding: np.ndarray
) -> bool:
    """
    벡터 DB에 임베딩 저장 (단일)
    
    Args:
        db: DB 세션
        ticker: 종목코드
        text_hash: 텍스트 해시
        embedding: 임베딩 벡터 (numpy 배열)
    
    Returns:
        저장 성공 여부
    """
    try:
        # numpy 배열을 PostgreSQL vector 형식으로 변환
        # '[0.1,0.2,...]' 형식
        embedding_str = '[' + ','.join(map(str, embedding.tolist())) + ']'
        
        # UPSERT (INSERT ... ON CONFLICT UPDATE)
        # CAST 함수를 사용하여 타입 캐스팅
        query = text("""
            INSERT INTO company_embeddings (ticker, text_hash, embedding_vector, updated_at, last_status, last_error_type, last_attempted_at)
            VALUES (:ticker, :text_hash, CAST(:embedding_vector AS vector), NOW(), 'SUCCESS', NULL, NOW())
            ON CONFLICT (ticker) 
            DO UPDATE SET 
                text_hash = EXCLUDED.text_hash,
                embedding_vector = CAST(:embedding_vector AS vector),
                updated_at = NOW(),
                last_status = 'SUCCESS',
                last_error_type = NULL,
                last_attempted_at = NOW()
        """)
        db.execute(query, {
            "ticker": ticker,
            "text_hash": text_hash,
            "embedding_vector": embedding_str
        })
        db.commit()
        return True
    except Exception as e:
        logger.error(f"벡터 DB 저장 실패 (ticker={ticker}): {e}")
        db.rollback()
        return False


def save_embeddings_batch_to_db(
    db: Session,
    embeddings_data: List[Dict[str, Any]]
) -> Dict[str, bool]:
    """
    벡터 DB에 임베딩 배치 저장 (트랜잭션 개선)
    
    Args:
        db: DB 세션
        embeddings_data: [{'ticker': str, 'text_hash': str, 'embedding': np.ndarray}, ...] 리스트
    
    Returns:
        {ticker: success_bool} 딕셔너리
    """
    results = {}
    
    if not embeddings_data:
        return results
    
    try:
        # 배치 단위로 한 번에 저장
        values_list = []
        for item in embeddings_data:
            ticker = item['ticker']
            text_hash = item['text_hash']
            embedding = item['embedding']
            
            # numpy 배열을 PostgreSQL vector 형식으로 변환
            embedding_str = '[' + ','.join(map(str, embedding.tolist())) + ']'
            
            values_list.append({
                "ticker": ticker,
                "text_hash": text_hash,
                "embedding_vector": embedding_str
            })
        
        # 배치 INSERT (PostgreSQL의 VALUES 사용)
        # 단일 쿼리로 여러 행 삽입
        query_parts = []
        params = {}
        
        for idx, item in enumerate(values_list):
            ticker_key = f"ticker_{idx}"
            hash_key = f"hash_{idx}"
            vec_key = f"vec_{idx}"
            
            query_parts.append(f"(:{ticker_key}, :{hash_key}, CAST(:{vec_key} AS vector), NOW(), 'SUCCESS', NULL, NOW())")
            params[ticker_key] = item['ticker']
            params[hash_key] = item['text_hash']
            params[vec_key] = item['embedding_vector']
        
        query = text(f"""
            INSERT INTO company_embeddings (ticker, text_hash, embedding_vector, updated_at, last_status, last_error_type, last_attempted_at)
            VALUES {', '.join(query_parts)}
            ON CONFLICT (ticker) 
            DO UPDATE SET 
                text_hash = EXCLUDED.text_hash,
                embedding_vector = EXCLUDED.embedding_vector,
                updated_at = NOW(),
                last_status = 'SUCCESS',
                last_error_type = NULL,
                last_attempted_at = NOW()
        """)
        
        db.execute(query, params)
        db.commit()
        
        # 모든 항목 성공
        for item in embeddings_data:
            results[item['ticker']] = True
        
        return results
        
    except Exception as e:
        logger.error(f"배치 저장 실패: {e}")
        db.rollback()
        
        # 개별 저장으로 fallback
        logger.info(f"배치 저장 실패 -> 개별 저장으로 전환 ({len(embeddings_data)}개 항목)")
        for item in embeddings_data:
            ticker = item['ticker']
            success = save_embedding_to_db(db, ticker, item['text_hash'], item['embedding'])
            results[ticker] = success
        
        return results


def get_or_create_embedding(
    db: Session,
    ticker: str,
    text: str,
    force_regenerate: bool = False,
    max_retries: int = 3,
    retry_delay: float = 1.0
) -> Optional[np.ndarray]:
    """
    벡터 DB에서 임베딩 조회 또는 생성 (재시도 로직 포함)
    
    Args:
        db: DB 세션
        ticker: 종목코드
        text: 임베딩할 텍스트
        force_regenerate: 강제 재생성 여부
        max_retries: 최대 재시도 횟수 (기본값: 3)
        retry_delay: 재시도 대기 시간(초) (기본값: 1.0)
    
    Returns:
        임베딩 벡터 (numpy 배열) 또는 None (실패 시)
    """
    text_hash = calculate_text_hash(text)
    
    # 강제 재생성이 아니면 DB에서 조회 시도
    if not force_regenerate:
        embedding = get_embedding_from_db(db, ticker, text_hash)
        if embedding is not None:
            logger.debug(f"[Solar] 벡터 DB에서 임베딩 조회 성공: {ticker} (cache_hit: true)")
            # 상태 업데이트 (CACHE_HIT)
            try:
                db.execute(text("""
                    UPDATE company_embeddings
                    SET last_status = 'CACHE_HIT', last_attempted_at = NOW()
                    WHERE ticker = :ticker
                """), {"ticker": ticker})
                db.commit()
            except Exception:
                pass  # 상태 업데이트 실패해도 임베딩은 반환
            return embedding
    
    # DB에 없거나 강제 재생성인 경우 API 호출 (재시도 로직)
    logger.info(f"[Solar] 임베딩 생성 중: {ticker} (text_hash: {text_hash[:8]}..., api_call: true)")
    start_time = time.time()
    
    for attempt in range(max_retries):
        try:
            embedding = encode_solar_embedding(text)
            
            # 벡터 DB에 저장
            db_save_success = save_embedding_to_db(db, ticker, text_hash, embedding)
            elapsed = time.time() - start_time
            
            if db_save_success:
                logger.info(f"[Solar] 임베딩 생성 및 저장 완료: {ticker} ({elapsed:.2f}초, db_save_success: true)")
            else:
                logger.warning(f"[Solar] 임베딩 생성 완료 (저장 실패): {ticker} ({elapsed:.2f}초, db_save_success: false, db_save_fail_reason: DB 저장 오류)")
                # 저장 실패해도 임베딩은 반환 (다음 호출 시 재시도 가능)
            
            return embedding
            
        except HTTPError as e:
            # Rate Limit 오류 처리
            if e.response.status_code == 429:
                wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                if attempt < max_retries - 1:
                    logger.warning(
                        f"[Solar] Rate Limit 오류 ({ticker}), "
                        f"{wait_time:.1f}초 후 재시도 ({attempt+1}/{max_retries}, api_error: RATE_LIMIT)"
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"[Solar] Rate Limit 오류 최종 실패: {ticker} (api_error: RATE_LIMIT, db_save_success: false)")
                    # 상태 업데이트
                    try:
                        db.execute(text("""
                            UPDATE company_embeddings
                            SET last_status = 'API_ERROR', last_error_type = 'RATE_LIMIT', last_attempted_at = NOW()
                            WHERE ticker = :ticker
                        """), {"ticker": ticker})
                        db.commit()
                    except Exception:
                        pass
                    return None
            elif e.response.status_code in [401, 403]:
                logger.error(f"[Solar] 인증 오류 ({ticker}): {e} (api_error: AUTH_ERROR, db_save_success: false)")
                # 상태 업데이트
                try:
                    db.execute(text("""
                        UPDATE company_embeddings
                        SET last_status = 'API_ERROR', last_error_type = 'AUTH_ERROR', last_attempted_at = NOW()
                        WHERE ticker = :ticker
                    """), {"ticker": ticker})
                    db.commit()
                except Exception:
                    pass
                return None
            else:
                logger.error(f"[Solar] API HTTP 오류 ({ticker}): {e} (api_error: HTTP_ERROR, status_code: {e.response.status_code}, db_save_success: false)")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                # 상태 업데이트
                try:
                    db.execute(text("""
                        UPDATE company_embeddings
                        SET last_status = 'API_ERROR', last_error_type = 'HTTP_ERROR', last_attempted_at = NOW()
                        WHERE ticker = :ticker
                    """), {"ticker": ticker})
                    db.commit()
                except Exception:
                    pass
                return None
                
        except RequestException as e:
            # 네트워크 오류 처리
            if "timeout" in str(e).lower() or "timed out" in str(e).lower():
                logger.error(f"[Solar] 타임아웃 오류 ({ticker}): {e} (api_error: TIMEOUT, db_save_success: false)")
                error_type = 'TIMEOUT'
            else:
                logger.error(f"[Solar] 네트워크 오류 ({ticker}): {e} (api_error: NETWORK_ERROR, db_save_success: false)")
                error_type = 'NETWORK_ERROR'
            if attempt < max_retries - 1:
                wait_time = retry_delay * (attempt + 1)
                logger.info(f"[Solar] {wait_time:.1f}초 후 재시도 ({attempt+1}/{max_retries})")
                time.sleep(wait_time)
                continue
            # 상태 업데이트
            try:
                db.execute(text("""
                    UPDATE company_embeddings
                    SET last_status = 'API_ERROR', last_error_type = :error_type, last_attempted_at = NOW()
                    WHERE ticker = :ticker
                """), {"ticker": ticker, "error_type": error_type})
                db.commit()
            except Exception:
                pass
            return None
            
        except Exception as e:
            # 기타 오류
            logger.error(f"[Solar] 임베딩 생성 실패 ({ticker}): {e} (api_error: UNKNOWN, db_save_success: false)")
            if attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue
            # 상태 업데이트
            try:
                db.execute(text("""
                    UPDATE company_embeddings
                    SET last_status = 'API_ERROR', last_error_type = 'UNKNOWN', last_attempted_at = NOW()
                    WHERE ticker = :ticker
                """), {"ticker": ticker})
                db.commit()
            except Exception:
                pass
            return None
    
    # 모든 재시도 실패
    logger.error(f"[Solar] 임베딩 생성 최종 실패: {ticker} (재시도 {max_retries}회 모두 실패)")
    return None


def prepare_company_text_for_solar(
    company_detail,
    company_name: Optional[str] = None
) -> str:
    """
    회사 텍스트를 Solar Embedding용으로 준비
    
    Args:
        company_detail: CompanyDetail 객체
        company_name: 회사명
    
    Returns:
        준비된 텍스트 문자열
    """
    text_parts = []
    
    if company_name:
        text_parts.append(f"회사명: {company_name}")
    
    if company_detail.biz_summary:
        text_parts.append(f"사업 개요: {company_detail.biz_summary}")
    
    # products/keywords 정렬하여 hash 일관성 보장
    if company_detail.products:
        products_sorted = sorted([str(p) for p in company_detail.products])
        products_text = ', '.join(products_sorted)
        text_parts.append(f"주요 제품: {products_text}")
    
    if company_detail.keywords:
        keywords_sorted = sorted([str(k) for k in company_detail.keywords])
        keywords_text = ', '.join(keywords_sorted)
        text_parts.append(f"키워드: {keywords_text}")
    
    if company_detail.clients:
        clients_sorted = sorted([str(c) for c in company_detail.clients])
        clients_text = ', '.join(clients_sorted)
        text_parts.append(f"주요 고객사: {clients_text}")
    
    if company_detail.supply_chain:
        supply_items = []
        for item in company_detail.supply_chain:
            if isinstance(item, dict):
                item_str = f"{item.get('item', '')} (공급사: {item.get('supplier', '')})"
            else:
                item_str = str(item)
            supply_items.append(item_str)
        supply_items_sorted = sorted(supply_items)
        supply_text = ', '.join(supply_items_sorted)
        text_parts.append(f"공급망: {supply_text}")
    
    return '\n\n'.join(text_parts)


def get_sector_reference_embeddings(
    db: Session,
    model=None  # 호환성을 위한 파라미터 (사용 안 함)
) -> Dict[str, np.ndarray]:
    """
    각 섹터의 참조 텍스트 임베딩 생성 (캐싱)
    
    Args:
        db: DB 세션
        model: 호환성을 위한 파라미터 (사용 안 함)
    
    Returns:
        섹터별 참조 임베딩 딕셔너리
    """
    from app.models.sector_reference import get_all_sector_references
    
    global _embedding_cache
    
    cache_key = "sector_references"
    if cache_key in _embedding_cache:
        return _embedding_cache[cache_key]
    
    sector_refs = get_all_sector_references()
    embeddings = {}
    
    logger.info(f"[Solar] 섹터 참조 임베딩 생성 시작: {len(sector_refs)}개")
    
    for sector_code, ref in sector_refs.items():
        # 섹터 참조 텍스트 구성
        # ref가 문자열인 경우와 딕셔너리인 경우 모두 처리
        if isinstance(ref, str):
            ref_text = ref
        elif isinstance(ref, dict):
            ref_text = f"{ref.get('name_ko', '')} {ref.get('description', '')}"
            if 'keywords' in ref and isinstance(ref['keywords'], list):
                keywords_text = ', '.join(ref['keywords'][:20])
                ref_text += f" {keywords_text}"
        else:
            ref_text = str(ref)
        
        try:
            # Solar Embedding 생성
            embedding = encode_solar_embedding(ref_text)
            embeddings[sector_code] = embedding
        except Exception as e:
            logger.warning(f"[Solar] 섹터 참조 임베딩 생성 실패: {sector_code}, {e}")
    
    _embedding_cache[cache_key] = embeddings
    logger.info(f"[Solar] 섹터 참조 임베딩 생성 완료: {len(embeddings)}개")
    
    return embeddings

