"""
전체 기업 밸류체인 재분류 스크립트

섹터 재분류 완료 후 실행
- L2 정보 활용
- Driver Tags 정보 활용
- 개선된 밸류체인 분류 로직 적용
"""
import sys
import os
if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    os.environ['PYTHONUNBUFFERED'] = '1'

from pathlib import Path
project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
env_path = project_root / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)
    print(f"✅ .env 파일 로드 완료: {env_path}")
else:
    print(f"⚠️  .env 파일을 찾을 수 없습니다: {env_path}")

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.investor_sector import InvestorSector
from app.models.stock import Stock
from app.services.value_chain_classifier import classify_value_chain_hybrid
import logging
import argparse
import json
from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy.orm import Session

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_driver_tags_from_causal_structure(causal_structure: Dict[str, Any]) -> List[str]:
    """
    causal_structure에서 모든 Driver Tags 추출
    
    Returns:
        Driver Tags 리스트 (중복 제거)
    """
    driver_tags = []
    
    if not causal_structure:
        return driver_tags
    
    key_drivers = causal_structure.get('key_drivers', [])
    for driver in key_drivers:
        tags = driver.get('driver_tags', [])
        if isinstance(tags, list):
            driver_tags.extend(tags)
    
    return list(set(driver_tags))  # 중복 제거


def reclassify_value_chain_for_ticker(
    db: Session,
    ticker: str,
    company_detail: CompanyDetail,
    investor_sectors: List[InvestorSector]
) -> Dict[str, Any]:
    """
    특정 기업의 밸류체인 재분류
    
    Returns:
        재분류 결과 딕셔너리
    """
    results = []
    
    for inv_sector in investor_sectors:
        major_sector = inv_sector.major_sector or inv_sector.sector_l1
        sector_l2 = inv_sector.sector_l2 or inv_sector.sub_sector
        sector_l2_confidence = inv_sector.confidence_l2
        
        # causal_structure에서 Driver Tags 추출
        causal_structure = inv_sector.causal_structure
        driver_tags = extract_driver_tags_from_causal_structure(causal_structure) if causal_structure else []
        
        # 밸류체인 재분류 (개선된 로직 사용)
        try:
            value_chain_results = classify_value_chain_hybrid(
                company_detail=company_detail,
                sector=major_sector,
                company_name=None,  # 필요시 추가
                use_ensemble=True,
                use_gpt=True,
                sector_l2=sector_l2,
                driver_tags=driver_tags if driver_tags else None
            )
            
            if value_chain_results:
                primary_vc = value_chain_results[0]
                value_chain = primary_vc.get('value_chain')
                confidence = primary_vc.get('confidence', 'MEDIUM')
                weight = primary_vc.get('weight', 1.0)
                
                # investor_sector 업데이트
                inv_sector.value_chain = value_chain
                inv_sector.updated_at = datetime.utcnow()
                
                results.append({
                    'ticker': ticker,
                    'sector': major_sector,
                    'sector_l2': sector_l2,
                    'value_chain': value_chain,
                    'confidence': confidence,
                    'weight': weight,
                    'driver_tags_used': driver_tags,
                    'method': primary_vc.get('method', 'HYBRID')
                })
                
                logger.info(
                    f"[{ticker}] {major_sector}/{sector_l2} → {value_chain} "
                    f"(confidence={confidence}, driver_tags={len(driver_tags) if driver_tags else 0}개)"
                )
        
        except Exception as e:
            logger.error(f"[{ticker}] 밸류체인 재분류 실패 ({major_sector}): {e}", exc_info=True)
    
    return {
        'ticker': ticker,
        'results': results,
        'total_sectors': len(investor_sectors),
        'success_count': len(results)
    }


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='전체 기업 밸류체인 재분류')
    parser.add_argument('--limit', type=int, help='처리할 기업 수 제한 (테스트용)')
    parser.add_argument('--ticker', type=str, help='특정 티커만 처리')
    parser.add_argument('--skip-existing', action='store_true', help='이미 value_chain이 있는 기업 스킵')
    args = parser.parse_args()
    
    db = SessionLocal()
    
    try:
        # 섹터 재분류 완료 확인
        logger.info("=" * 60)
        logger.info("밸류체인 재분류 시작")
        logger.info("=" * 60)
        
        # 기업 목록 조회
        query = db.query(Stock.ticker).filter(Stock.is_active == True)
        if args.ticker:
            query = query.filter(Stock.ticker == args.ticker)
        
        tickers = [row[0] for row in query.all()]
        
        if args.limit:
            tickers = tickers[:args.limit]
            logger.info(f"⚠️ 테스트 모드: {args.limit}개 기업만 처리")
        
        logger.info(f"총 {len(tickers)}개 기업 밸류체인 재분류 시작")
        
        success_count = 0
        skip_count = 0
        error_count = 0
        
        for i, ticker in enumerate(tickers, 1):
            try:
                # CompanyDetail 조회
                company_detail = db.query(CompanyDetail).filter(
                    CompanyDetail.ticker == ticker
                ).first()
                
                if not company_detail:
                    logger.warning(f"[{ticker}] CompanyDetail 없음, 스킵")
                    skip_count += 1
                    continue
                
                # InvestorSector 조회
                investor_sectors = db.query(InvestorSector).filter(
                    InvestorSector.ticker == ticker
                ).all()
                
                if not investor_sectors:
                    logger.warning(f"[{ticker}] InvestorSector 없음, 스킵")
                    skip_count += 1
                    continue
                
                # 이미 value_chain이 있고 skip-existing 옵션이면 스킵
                if args.skip_existing:
                    has_vc = any(inv.value_chain for inv in investor_sectors)
                    if has_vc:
                        logger.debug(f"[{ticker}] 이미 value_chain 존재, 스킵")
                        skip_count += 1
                        continue
                
                # 밸류체인 재분류
                result = reclassify_value_chain_for_ticker(
                    db=db,
                    ticker=ticker,
                    company_detail=company_detail,
                    investor_sectors=investor_sectors
                )
                
                if result['success_count'] > 0:
                    success_count += 1
                    db.commit()
                else:
                    error_count += 1
                
                # 진행 상황 로깅
                if i % 10 == 0:
                    logger.info(f"진행 상황: {i}/{len(tickers)} ({i*100//len(tickers)}%)")
            
            except Exception as e:
                logger.error(f"[{ticker}] 처리 실패: {e}", exc_info=True)
                error_count += 1
                db.rollback()
        
        logger.info("=" * 60)
        logger.info(f"밸류체인 재분류 완료")
        logger.info(f"  성공: {success_count}개")
        logger.info(f"  스킵: {skip_count}개")
        logger.info(f"  실패: {error_count}개")
        logger.info("=" * 60)
    
    finally:
        db.close()


if __name__ == '__main__':
    main()

