#!/usr/bin/env python3
"""
밸류체인 분류 스크립트 (최종 버전)

모든 기업 임베딩 vs 5개 Anchor cosine similarity 계산
- value_chain = top1
- value_chain_detail = top2 (gap < 0.1일 때만)
- value_chain_confidence = top1 - top2
"""

import sys
import os
import logging
import ast
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

# UTF-8 encoding for Windows (prevents Cursor serialization issues)
if sys.platform == 'win32':
    import codecs
    import locale
    # Force UTF-8 encoding
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    os.environ['PYTHONUTF8'] = '1'
    # Minimize terminal output encoding issues
    try:
        locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
    except locale.Error:
        pass

# 로깅 설정 (먼저 설정)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# .env 파일 로드 (여러 경로 시도)
env_path = project_root / '.env'
parent_env = project_root.parent / '.env'
current_env = Path.cwd() / '.env'

upstage_key = None
env_loaded_path = None

# .env 파일 로드 (간소화된 로그)

# .env 파일 시도 (순차적으로)
env_paths = [env_path, parent_env, current_env]
for path in env_paths:
    if path.exists():
        load_dotenv(path, override=True)
        upstage_key = os.getenv('UPSTAGE_API_KEY')
        if upstage_key:
            env_loaded_path = path
            break

# 자동 탐지 시도
if not upstage_key:
    load_dotenv(override=True)
    upstage_key = os.getenv('UPSTAGE_API_KEY')

# 환경 변수 확인
if not upstage_key:
    logger.error("[ERROR] UPSTAGE_API_KEY not found. Check .env file.")
    sys.exit(1)

from app.db import get_db
from app.services.value_chain_classifier_embedding import (
    VALUE_CHAIN_ANCHORS,
    GOLDEN_SET,
    SECTOR_PRIOR,
    get_value_chain_reference_embeddings,
    get_centroid_anchors,
    compute_cosine_similarity,
    compute_softmax_confidence,
    apply_sector_prior,
    get_embedding_model,
    clear_anchor_cache
)
from app.models.investor_sector import InvestorSector

# 5단계 밸류체인 우선순위
PRIORITY_VC_CODES = ['UPSTREAM', 'MID_HARD', 'MID_SOFT', 'DOWN_BIZ', 'DOWN_SERVICE']

# ⭐ Phase 6: 하이브리드 스코어링 (Centroid + Text Anchor)
USE_HYBRID_SCORING = True  # True: 하이브리드, False: Centroid만
CENTROID_WEIGHT = 0.6  # Centroid 비중
TEXT_WEIGHT = 0.4  # Text Anchor 비중 (변별력)


def get_company_embeddings_from_db(db: Session, regenerate: bool = False) -> Dict[str, np.ndarray]:
    """
    company_embeddings 테이블에서 모든 기업 임베딩 조회
    
    Args:
        db: DB 세션
        regenerate: True면 Solar Embedding으로 재생성
    
    Returns:
        {ticker: embedding_vector} 딕셔너리
    """
    if regenerate:
        logger.info("Regenerating company embeddings with Solar Embedding...")
        return generate_company_embeddings_solar(db)
    
    logger.info("Loading embeddings from company_embeddings table...")
    
    result = db.execute(text("""
        SELECT ticker, embedding_vector
        FROM company_embeddings
        WHERE embedding_vector IS NOT NULL
    """))
    
    embeddings = {}
    count = 0
    dims = set()
    
    for row in result:
        ticker = row[0]
        embedding_vector = row[1]
        
        # pgvector의 vector 타입을 numpy array로 변환 (다양한 형태 처리)
        try:
            if hasattr(embedding_vector, 'tolist'):
                # pgvector 객체인 경우
                embedding_array = np.array(embedding_vector.tolist(), dtype=np.float32)
            elif isinstance(embedding_vector, (list, tuple)):
                # 이미 리스트/튜플인 경우
                embedding_array = np.array(embedding_vector, dtype=np.float32)
            elif isinstance(embedding_vector, str):
                # pgvector가 문자열로 반환되는 경우: '[0.1,0.2,...]' 형식
                vec_list = ast.literal_eval(embedding_vector)
                embedding_array = np.array(vec_list, dtype=np.float32)
            else:
                # 기타 경우: 문자열로 변환 후 파싱 시도
                embedding_str = str(embedding_vector)
                if embedding_str.startswith('[') and embedding_str.endswith(']'):
                    vec_list = ast.literal_eval(embedding_str)
                    embedding_array = np.array(vec_list, dtype=np.float32)
                else:
                    logger.debug(f"[{ticker}] Unknown vector format, skip")
                    continue
        except Exception as e:
            logger.debug(f"[{ticker}] Vector conversion failed, skip")
            continue
        
        dim = len(embedding_array)
        dims.add(dim)
        embeddings[ticker] = embedding_array
        count += 1
    
    # Dimension check
    if dims:
        logger.info(f"[OK] Loaded {count:,} company embeddings (dims: {dims})")
        if 4096 not in dims:
            logger.warning(f"[WARN] No 4096-dim embeddings found. Use --regenerate option")
    else:
        logger.warning(f"[WARN] No embeddings found")
    
    return embeddings


def generate_company_embeddings_solar(db: Session) -> Dict[str, np.ndarray]:
    """
    Solar Embedding으로 기업 임베딩 재생성 (재개 가능: 이미 DB에 있는 것은 스킵)
    
    Returns:
        {ticker: embedding_vector} 딕셔너리
    """
    from app.services.solar_embedding_model import (
        encode_solar_embedding,
        prepare_company_text_for_solar,
        get_or_create_embedding,
        save_embedding_to_db,
        save_embeddings_batch_to_db,
        calculate_text_hash
    )
    from app.models.company_detail import CompanyDetail
    
    logger.info("Loading company info...")
    
    # company_details
    companies = db.query(CompanyDetail).all()
    
    logger.info(f"Total companies: {len(companies):,}")
    
    # Check existing embeddings (for resume)
    logger.info("Checking existing embeddings...")
    existing_embeddings = {}
    existing_result = db.execute(text("""
        SELECT ticker, embedding_vector
        FROM company_embeddings
        WHERE embedding_vector IS NOT NULL
    """))
    
    conversion_success = 0
    conversion_failed = 0
    
    for row in existing_result:
        ticker = row[0]
        embedding_vector = row[1]
        
        # pgvector 타입 변환 (문자열, 객체, 리스트 등 다양한 형태 처리)
        # ⚠️ 중요: pgvector는 PostgreSQL에서 문자열로 반환됨 - 문자열 체크를 먼저!
        try:
            if isinstance(embedding_vector, str):
                # pgvector가 문자열로 반환되는 경우: '[0.1,0.2,...]' 형식 (가장 일반적)
                vec_list = ast.literal_eval(embedding_vector)
                existing_embeddings[ticker] = np.array(vec_list, dtype=np.float32)
                conversion_success += 1
            elif hasattr(embedding_vector, 'tolist'):
                # pgvector 객체인 경우 (드묾)
                existing_embeddings[ticker] = np.array(embedding_vector.tolist(), dtype=np.float32)
                conversion_success += 1
            elif isinstance(embedding_vector, (list, tuple)):
                # 이미 리스트/튜플인 경우 (드묾)
                existing_embeddings[ticker] = np.array(embedding_vector, dtype=np.float32)
                conversion_success += 1
            else:
                # 기타 경우: 문자열로 변환 후 파싱 시도
                embedding_str = str(embedding_vector)
                if embedding_str.startswith('[') and embedding_str.endswith(']'):
                    vec_list = ast.literal_eval(embedding_str)
                    existing_embeddings[ticker] = np.array(vec_list, dtype=np.float32)
                    conversion_success += 1
                else:
                    logger.debug(f"[{ticker}] Unknown format, skip")
                    conversion_failed += 1
                    continue
        except Exception as e:
            logger.debug(f"[{ticker}] Conversion failed, skip")
            conversion_failed += 1
            continue
    
    logger.info(f"Found {len(existing_embeddings):,} existing embeddings (will skip)")
    if conversion_failed > 0:
        logger.info(f"  Converted: {conversion_success:,}, Failed: {conversion_failed:,}")
    logger.info("Generating Solar Embeddings...")
    
    embeddings = {}
    batch = []
    batch_size = 5  # VPN 오류 방지를 위해 배치 크기 감소 (10 → 5)
    max_retries = 8  # VPN 오류 대응을 위해 재시도 횟수 증가 (5 → 8)
    retry_delay = 3.0  # VPN 오류 대응을 위해 기본 대기 시간
    batch_delay = 2.0  # 배치 사이 대기 시간 (VPN 오류 방지)
    failed_tickers = []  # 임베딩 생성 실패한 티커 추적
    failed_db_saves = []  # DB 저장 실패한 항목 추적 (복구 가능)
    batch_num = 0  # 배치 번호 초기화
    
    import time
    
    total_batches = (len(companies) + batch_size - 1) // batch_size
    
    def is_network_error(error_msg: str) -> bool:
        """네트워크 오류인지 확인"""
        error_lower = str(error_msg).lower()
        network_keywords = [
            'timeout', 'connection', 'network', 'vpn', 'refused', 
            'unreachable', 'resolve', 'dns', 'ssl', 'tls',
            'connection reset', 'connection aborted', 'broken pipe'
        ]
        return any(keyword in error_lower for keyword in network_keywords)
    
    def process_batch_with_retry(tickers, texts, batch_num, total_batches, max_attempts=5):
        """Batch processing with retry logic"""
        for attempt in range(max_attempts):
            try:
                batch_embeddings = encode_solar_embedding(list(texts), batch_size=len(texts))
                result = {}
                for ticker, embedding in zip(tickers, batch_embeddings):
                    result[ticker] = embedding
                return result, []
            except Exception as e:
                error_msg = str(e)
                is_network = is_network_error(error_msg)
                
                if attempt < max_attempts - 1:
                    if is_network:
                        wait_time = retry_delay * (2 ** attempt) + 5.0
                        logger.warning(f"[Batch {batch_num}/{total_batches}] Network error, retry {attempt+1}/{max_attempts} in {wait_time:.0f}s")
                    else:
                        wait_time = retry_delay * (attempt + 1) + 2.0
                        logger.warning(f"[Batch {batch_num}/{total_batches}] Failed, retry {attempt+1}/{max_attempts} in {wait_time:.0f}s")
                    
                    time.sleep(wait_time)
                else:
                    logger.error(f"[Batch {batch_num}/{total_batches}] Final failure after {max_attempts} attempts")
                    return None, list(tickers)
        
        return None, list(tickers)
    
    def process_single_with_retry(ticker, text, max_attempts=5):
        """Single item retry with VPN error handling"""
        for attempt in range(max_attempts):
            try:
                embedding = encode_solar_embedding(text, batch_size=1, max_retries=3)
                return embedding
            except Exception as e:
                error_msg = str(e)
                is_network = is_network_error(error_msg)
                
                if attempt < max_attempts - 1:
                    if is_network:
                        wait_time = retry_delay * (2 ** attempt) + 5.0
                        logger.debug(f"[{ticker}] Network error, retry {attempt+1}/{max_attempts}")
                    else:
                        wait_time = retry_delay * (attempt + 1) + 2.0
                        logger.debug(f"[{ticker}] Failed, retry {attempt+1}/{max_attempts}")
                    time.sleep(wait_time)
                else:
                    logger.warning(f"[{ticker}] Final failure after {max_attempts} attempts")
                    return None
        
        return None
    
    for i, company in enumerate(companies, 1):
        try:
            ticker = company.ticker
            
            # 이미 DB에 임베딩이 있으면 스킵 (재개 가능)
            if ticker in existing_embeddings:
                # 기존 임베딩을 현재 세션의 embeddings에 추가 (나중에 update로 한번에 추가되지만, 진행률 표시를 위해)
                embeddings[ticker] = existing_embeddings[ticker]
                if i % 1000 == 0:
                    logger.info(f"Progress: {i}/{len(companies)} ({i*100//len(companies)}%) - using cached")
                continue
            
            # 회사명 가져오기 (Stock 관계를 통해)
            company_name = None
            if company.stock:
                company_name = company.stock.stock_name
            if not company_name:
                company_name = company.ticker
            
            # 회사 텍스트 준비
            company_text = prepare_company_text_for_solar(company, company_name)
            
            if not company_text.strip():
                logger.debug(f"[{ticker}] No text, skip")
                continue
            
            batch.append((ticker, company_text))
            
            # 배치 크기 도달 시 임베딩 생성
            if len(batch) >= batch_size:
                batch_num += 1
                tickers, texts = zip(*batch)
                batch_dict = {t: txt for t, txt in zip(tickers, texts)}  # ticker -> text 매핑
                result, failed_in_batch = process_batch_with_retry(tickers, texts, batch_num, total_batches, max_attempts=max_retries)
                
                if result:
                    # 배치 성공 - DB에 배치 저장 (트랜잭션 개선)
                    batch_save_data = []
                    for ticker, embedding in result.items():
                        embeddings[ticker] = embedding
                        company_txt = batch_dict[ticker]
                        text_hash = calculate_text_hash(company_txt)
                        batch_save_data.append({
                            'ticker': ticker,
                            'text_hash': text_hash,
                            'embedding': embedding
                        })
                    
                    # 배치 단위로 저장 (성능 향상)
                    save_results = save_embeddings_batch_to_db(db, batch_save_data)
                    
                    # Track failed saves
                    failed_saves = [ticker for ticker, success in save_results.items() if not success]
                    if failed_saves:
                        logger.warning(f"Batch {batch_num}/{total_batches} DB save failed: {len(failed_saves)} items")
                        for ticker in failed_saves:
                            failed_db_saves.append({
                                'ticker': ticker,
                                'text': batch_dict[ticker],
                                'embedding': result[ticker]
                            })
                    else:
                        if i % 500 == 0 or batch_num % 10 == 0:
                            logger.info(f"Progress: {i}/{len(companies)} ({i*100//len(companies)}%) - batch {batch_num}/{total_batches}")
                    # VPN 오류 방지를 위해 배치 사이 대기 시간 추가
                    if batch_num < total_batches:  # 마지막 배치가 아니면
                        time.sleep(batch_delay)
                else:
                    # Batch failed - process individually
                    logger.warning(f"Batch {batch_num}/{total_batches} failed, processing {len(failed_in_batch)} individually")
                    for ticker, company_txt in zip(tickers, texts):
                        if ticker in failed_in_batch:
                            embedding = process_single_with_retry(ticker, company_txt, max_attempts=3)
                            if embedding is not None:
                                embeddings[ticker] = embedding
                                try:
                                    text_hash = calculate_text_hash(company_txt)
                                    save_embedding_to_db(db, ticker, text_hash, embedding)
                                except Exception as e:
                                    logger.debug(f"[{ticker}] DB save failed: {e}")
                            else:
                                failed_tickers.append(ticker)
                                logger.debug(f"[{ticker}] Individual processing also failed")
                
                batch = []
        
        except Exception as e:
            logger.error(f"[{company.ticker}] Processing error: {e}")
            failed_tickers.append(company.ticker)
            continue
    
    # 남은 배치 처리
    if batch:
        batch_num += 1
        tickers, texts = zip(*batch)
        batch_dict = {t: txt for t, txt in zip(tickers, texts)}  # ticker -> text 매핑
        result, failed_in_batch = process_batch_with_retry(tickers, texts, batch_num, total_batches, max_attempts=max_retries)
        
        if result:
            # 배치 성공 - DB에 배치 저장
            batch_save_data = []
            for ticker, embedding in result.items():
                embeddings[ticker] = embedding
                company_txt = batch_dict[ticker]
                text_hash = calculate_text_hash(company_txt)
                batch_save_data.append({
                    'ticker': ticker,
                    'text_hash': text_hash,
                    'embedding': embedding
                })
            
            save_results = save_embeddings_batch_to_db(db, batch_save_data)
            failed_saves = [ticker for ticker, success in save_results.items() if not success]
            if failed_saves:
                for ticker in failed_saves:
                    failed_db_saves.append({
                        'ticker': ticker,
                        'text': batch_dict[ticker],
                        'embedding': result[ticker]
                    })
        else:
            # Process individually
            logger.warning(f"Last batch {batch_num}/{total_batches} failed, processing {len(failed_in_batch)} individually")
            for ticker, company_txt in zip(tickers, texts):
                if ticker in failed_in_batch:
                    embedding = process_single_with_retry(ticker, company_txt, max_attempts=3)
                    if embedding is not None:
                        embeddings[ticker] = embedding
                        text_hash = calculate_text_hash(company_txt)
                        success = save_embedding_to_db(db, ticker, text_hash, embedding)
                        if not success:
                            failed_db_saves.append({
                                'ticker': ticker,
                                'text': company_txt,
                                'embedding': embedding
                            })
                    else:
                        failed_tickers.append(ticker)
    
    # 기존 임베딩도 결과에 포함
    embeddings.update(existing_embeddings)
    
    logger.info(f"[OK] Total embeddings ready: {len(embeddings):,}")
    logger.info(f"  New: {len(embeddings) - len(existing_embeddings):,}, Cached: {len(existing_embeddings):,}")
    
    # Retry failed DB saves
    if failed_db_saves:
        logger.info(f"Retrying {len(failed_db_saves)} failed DB saves...")
        retry_success = 0
        retry_failed = []
        
        for item in failed_db_saves:
            ticker = item['ticker']
            text_hash = calculate_text_hash(item['text'])
            success = save_embedding_to_db(db, ticker, text_hash, item['embedding'])
            if success:
                retry_success += 1
            else:
                retry_failed.append(ticker)
        
        if retry_success > 0:
            logger.info(f"[Recovery] Saved {retry_success} items")
        if retry_failed:
            logger.warning(f"[Recovery] {len(retry_failed)} items still failed")
            
            # Save failed items for manual recovery
            import json
            from pathlib import Path
            recovery_file = Path(__file__).parent / 'failed_db_saves_recovery.json'
            recovery_data = [
                {
                    'ticker': item['ticker'],
                    'text_hash': calculate_text_hash(item['text']),
                    'text_preview': item['text'][:200] + '...' if len(item['text']) > 200 else item['text']
                }
                for item in failed_db_saves if item['ticker'] in retry_failed
            ]
            with open(recovery_file, 'w', encoding='utf-8') as f:
                json.dump(recovery_data, f, ensure_ascii=False, indent=2)
            logger.info(f"[Recovery] Saved to: {recovery_file}")
    
    if failed_tickers:
        logger.warning(f"{len(failed_tickers)} embedding generation failed (network/VPN error)")
        logger.info("Re-run script to retry failed items")
    
    return embeddings


# ⭐ Phase 6: Negative Rules 강화 (Safety Net)
NEGATIVE_RULES = {
    # 금융 섹터 → 제조/소프트웨어 아님
    'SEC_FINANCE': ['UPSTREAM', 'MID_HARD', 'MID_SOFT'],
    'SEC_BANK': ['UPSTREAM', 'MID_HARD', 'MID_SOFT'],
    'SEC_INSURANCE': ['UPSTREAM', 'MID_HARD', 'MID_SOFT'],
    'SEC_SECURITIES': ['UPSTREAM', 'MID_HARD', 'MID_SOFT'],
    # 건설 섹터 → 소프트웨어/플랫폼 아님
    'SEC_CONSTRUCT': ['MID_SOFT', 'DOWN_SERVICE'],
    # ⭐ 게임/엔터 섹터 → 원자재/제조 아님 (DOWN_SERVICE로 유도)
    'SEC_GAME': ['UPSTREAM', 'MID_HARD'],
    'SEC_ENT': ['UPSTREAM', 'MID_HARD'],
    'SEC_MEDIA': ['UPSTREAM', 'MID_HARD'],
}


def apply_negative_rules(
    similarities: List[Dict[str, Any]],
    sector_l1: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Negative Rule 적용: 섹터 기반으로 특정 밸류체인 점수를 0점으로 만듦
    
    Args:
        similarities: [{'value_chain': str, 'score': float}, ...] 리스트
        sector_l1: 섹터 L1 코드 (예: 'SEC_FINANCE')
    
    Returns:
        Negative Rule이 적용된 similarities 리스트
    """
    if not sector_l1 or sector_l1 not in NEGATIVE_RULES:
        return similarities
    
    excluded_vcs = NEGATIVE_RULES[sector_l1]
    
    # 제외된 밸류체인의 점수를 0으로 설정
    for sim in similarities:
        if sim['value_chain'] in excluded_vcs:
            sim['score'] = 0.0
            logger.debug(f"[Negative Rule] {sector_l1} → {sim['value_chain']} 점수 0점 처리")
    
    return similarities


def get_company_sector_l1(db: Session, ticker: str) -> Optional[str]:
    """
    기업의 섹터 L1 정보 조회
    
    Args:
        db: DB 세션
        ticker: 종목코드
    
    Returns:
        sector_l1 문자열 또는 None
    """
    try:
        result = db.execute(text("""
            SELECT sector_l1
            FROM investor_sector
            WHERE ticker = :ticker
                AND is_primary = true
            LIMIT 1
        """), {'ticker': ticker})
        
        row = result.fetchone()
        return row[0] if row and row[0] else None
    except Exception as e:
        logger.debug(f"[{ticker}] Sector query failed: {e}")
        return None


def is_holding_company(db: Session, ticker: str) -> bool:
    """
    지주사 여부 확인
    
    Args:
        db: DB 세션
        ticker: 종목코드
    
    Returns:
        지주사 여부 (True/False)
    """
    try:
        result = db.execute(text("""
            SELECT COUNT(*) > 0
            FROM investor_sector
            WHERE ticker = :ticker
                AND (sector_l1 = 'SEC_HOLDING' OR business_model_role = 'HOLDING')
            LIMIT 1
        """), {'ticker': ticker})
        
        row = result.fetchone()
        return row[0] if row and row[0] else False
    except Exception as e:
        logger.debug(f"[{ticker}] Holding company check failed: {e}")
        return False


def get_company_sector_info(db: Session, ticker: str) -> Tuple[Optional[str], Optional[str]]:
    """
    기업의 섹터 L1, L2 정보 조회
    
    Returns:
        (sector_l1, sector_l2) 튜플
    """
    try:
        result = db.execute(text("""
            SELECT sector_l1, sector_l2
            FROM investor_sector
            WHERE ticker = :ticker
                AND is_primary = true
            LIMIT 1
        """), {'ticker': ticker})
        
        row = result.fetchone()
        if row:
            return row[0], row[1]
        return None, None
    except Exception as e:
        logger.debug(f"[{ticker}] Sector query failed: {e}")
        return None, None


def compute_gap_confidence(top1_score: float, top2_score: float, max_gap: float = 0.15) -> float:
    """
    ⭐ Phase 6: Gap 기반 Confidence (0~1 정규화)
    
    Args:
        top1_score: 1등 유사도
        top2_score: 2등 유사도
        max_gap: 최대 gap으로 간주할 값 (이 이상이면 confidence = 1.0)
    
    Returns:
        0~1 사이의 confidence
    """
    gap = top1_score - top2_score
    confidence = min(1.0, gap / max_gap)
    return float(max(0.0, confidence))


def classify_value_chain_for_company(
    ticker: str,
    company_embedding: np.ndarray,
    centroid_anchors: Dict[str, np.ndarray],
    text_anchors: Optional[Dict[str, np.ndarray]] = None,
    db: Optional[Session] = None
) -> Dict[str, Any]:
    """
    단일 기업의 밸류체인 분류 (Phase 6: 하이브리드 스코어링 + Gap Confidence)
    
    Args:
        ticker: 종목코드
        company_embedding: 기업 임베딩 벡터
        centroid_anchors: Centroid 앵커 임베딩
        text_anchors: Text 앵커 임베딩 (하이브리드 모드용, Optional)
        db: DB 세션 (섹터 Prior/Negative Rule 적용 시 필요)
    
    Returns:
        {
            'value_chain': 'MID_SOFT',
            'value_chain_detail': 'MID_HARD',
            'value_chain_confidence': 0.75,  # Gap 기반 (0~1)
            'top1_score': 0.85,
            'top2_score': 0.70
        }
    """
    similarities = []
    
    # ⭐ Phase 6: 하이브리드 스코어 계산
    for vc_code in PRIORITY_VC_CODES:
        if vc_code not in centroid_anchors:
            continue
        
        # Centroid 유사도
        centroid_sim = compute_cosine_similarity(company_embedding, centroid_anchors[vc_code])
        
        # 하이브리드 모드: Text 유사도도 계산해서 가중 합산
        if USE_HYBRID_SCORING and text_anchors and vc_code in text_anchors:
            text_sim = compute_cosine_similarity(company_embedding, text_anchors[vc_code])
            # 하이브리드 스코어: Centroid(현실성) + Text(변별력)
            final_score = (centroid_sim * CENTROID_WEIGHT) + (text_sim * TEXT_WEIGHT)
        else:
            final_score = centroid_sim
        
        similarities.append({
            'value_chain': vc_code,
            'score': float(final_score)
        })
    
    # ⭐ 섹터 기반 Prior 적용 (가산점)
    sector_l1, sector_l2 = None, None
    if db:
        sector_l1, sector_l2 = get_company_sector_info(db, ticker)
        similarities = apply_sector_prior(similarities, sector_l1, sector_l2)
    
    # Negative Rule 적용 (섹터 기반 제외)
    if db and sector_l1:
        similarities = apply_negative_rules(similarities, sector_l1)
    
    # 정렬
    similarities.sort(key=lambda x: x['score'], reverse=True)
    
    if not similarities:
        return {
            'value_chain': None,
            'value_chain_detail': None,
            'value_chain_confidence': None,
            'top1_score': None,
            'top2_score': None
        }
    
    top1 = similarities[0]
    top1_score = float(top1['score'])
    top1_vc = top1['value_chain']
    
    # 지주사 여부 확인
    is_holding = False
    if db:
        is_holding = is_holding_company(db, ticker)
    
    # ⭐ Phase 6: Gap 기반 Confidence
    if len(similarities) >= 2:
        top2 = similarities[1]
        top2_score = float(top2['score'])
        top2_vc = top2['value_chain']
        
        # Gap 기반 confidence (0~1 정규화)
        confidence = compute_gap_confidence(top1_score, top2_score, max_gap=0.15)
        
        # ⭐ Phase 7: Confidence 기반 해석 (GPT 권고)
        # - confidence >= 0.25 → 단일 (확실)
        # - 0.15 <= confidence < 0.25 → 경계
        # - confidence < 0.15 → 복합
        if confidence >= 0.25:
            value_chain_detail = None  # 단일: 확실한 분류
        else:
            value_chain_detail = top2_vc  # 경계/복합: 2위 밸류체인 표시
            if confidence < 0.15:
                logger.debug(f"[{ticker}] Complex enterprise: conf={confidence:.3f}")
    else:
        top2_score = None
        confidence = 1.0
        value_chain_detail = None
    
    # 모든 score를 Python float으로 변환 (PostgreSQL 호환)
    return {
        'value_chain': top1_vc,
        'value_chain_detail': value_chain_detail,
        'value_chain_confidence': float(confidence) if confidence is not None else None,
        'top1_score': float(top1_score) if top1_score is not None else None,
        'top2_score': float(top2_score) if top2_score is not None else None
    }


def classify_all_companies(
    db: Session,
    batch_size: int = 100,
    regenerate_embeddings: bool = False
) -> Dict[str, int]:
    """
    모든 기업의 밸류체인 분류 및 DB 업데이트
    
    ⭐ Phase 6: 하이브리드 스코어링 (Centroid + Text Anchor)
    
    Args:
        db: DB 세션
        batch_size: 배치 크기
        regenerate_embeddings: 임베딩 재생성 여부
    
    Returns:
        통계 딕셔너리
    """
    # Start log
    mode = "HYBRID" if USE_HYBRID_SCORING else "CENTROID"
    embed_mode = "regenerate" if regenerate_embeddings else "cached"
    logger.info(f"Value chain classification started (mode={mode}, embed={embed_mode})")
    if USE_HYBRID_SCORING:
        logger.info(f"  Weights: Centroid={CENTROID_WEIGHT}, Text={TEXT_WEIGHT}")
    
    # 1. Load anchor embeddings
    logger.info("[Step 1] Loading anchor embeddings...")
    
    # 캐시 초기화 (재생성 모드)
    if regenerate_embeddings:
        clear_anchor_cache()
    
    # ⭐ Phase 6: Centroid 앵커 로드
    logger.info("[CENTROID] Loading Golden Set centroid anchors...")
    centroid_anchors = get_centroid_anchors(db, force_regenerate=regenerate_embeddings)
    
    if not centroid_anchors or len(centroid_anchors) < 5:
        logger.error("[ERROR] Centroid anchors failed!")
        return {'total': 0, 'success': 0, 'fail': 0}
    
    logger.info(f"[OK] {len(centroid_anchors)} centroid anchors ready")
    
    # ⭐ Phase 6: Text 앵커 로드 (하이브리드 모드)
    text_anchors = None
    if USE_HYBRID_SCORING:
        logger.info("[TEXT] Loading text-based anchors for discrimination...")
        text_anchors = get_value_chain_reference_embeddings(
            sector_code=None, 
            model=None,
            force_regenerate=regenerate_embeddings,
            use_centroid=False
        )
        logger.info(f"[OK] {len(text_anchors)} text anchors ready")
    
    # 2. Load company embeddings
    logger.info("[Step 2] Loading company embeddings...")
    company_embeddings = get_company_embeddings_from_db(db, regenerate=regenerate_embeddings)
    if not company_embeddings:
        logger.error("No company embeddings found. Generate embeddings first.")
        return {'total': 0, 'success': 0, 'fail': 0}
    
    logger.info(f"[OK] {len(company_embeddings):,} company embeddings ready")
    
    # 3. 분류 및 업데이트
    stats = {'total': len(company_embeddings), 'success': 0, 'fail': 0}
    batch = []
    
    for i, (ticker, company_embedding) in enumerate(company_embeddings.items(), 1):
        try:
            # ⭐ Phase 6: 하이브리드 스코어링
            result = classify_value_chain_for_company(
                ticker, 
                company_embedding, 
                centroid_anchors,
                text_anchors=text_anchors,  # 하이브리드 모드용
                db=db
            )
            
            # 배치에 추가
            batch.append((ticker, result))
            
            # DB update when batch size reached
            if len(batch) >= batch_size:
                update_batch(db, batch)
                stats['success'] += len(batch)
                batch = []
                if i % 500 == 0 or i == stats['total']:
                    logger.info(f"Progress: {i}/{stats['total']} ({i*100//stats['total']}%)")
        
        except Exception as e:
            logger.error(f"[{ticker}] Classification failed")
            stats['fail'] += 1
    
    # 마지막 배치 처리
    if batch:
        update_batch(db, batch)
        stats['success'] += len(batch)
    
    logger.info(f"Done: total={stats['total']}, success={stats['success']}, fail={stats['fail']}")
    
    return stats


def update_batch(db: Session, batch: List[tuple]):
    """Batch DB update"""
    try:
        for ticker, result in batch:
            sectors = db.query(InvestorSector).filter(
                InvestorSector.ticker == ticker,
                InvestorSector.is_primary == True
            ).all()
            
            for sector in sectors:
                sector.value_chain = result['value_chain']
                sector.value_chain_detail = result['value_chain_detail']
                sector.value_chain_confidence = result['value_chain_confidence']
        
        db.commit()
    
    except Exception as e:
        logger.error(f"Batch update failed: {e}")
        db.rollback()
        raise


def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Value chain classification script')
    parser.add_argument('--regenerate', action='store_true', 
                       help='Regenerate company embeddings with Solar Embedding')
    args = parser.parse_args()
    
    db = next(get_db())
    
    try:
        stats = classify_all_companies(db, regenerate_embeddings=args.regenerate)
        
        logger.info("=" * 60)
        logger.info(f"FINAL: total={stats['total']:,}, success={stats['success']:,}, fail={stats['fail']:,}")
        logger.info("=" * 60)
    
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == '__main__':
    main()

