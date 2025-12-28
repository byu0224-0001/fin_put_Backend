"""
SPAC 상태 모니터링 및 자동 전환 스크립트

SPAC이 POST_MERGER 상태로 전환되면 섹터 재분류를 트리거합니다.
"""
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from sqlalchemy.orm import Session
from app.models.stock import Stock
from app.models.company_detail import CompanyDetail
from app.services.spac_classifier import classify_spac

logger = logging.getLogger(__name__)

# 합병 완료 키워드
MERGER_COMPLETION_KEYWORDS = [
    '합병 완료',
    '인수 완료',
    'merger completed',
    '합병 효력 발생',
    '인수 효력 발생',
    '합병 절차 완료',
    '인수 절차 완료',
]


def check_spac_merger_completion(
    db: Session,
    ticker: str,
    company_detail: Optional[CompanyDetail] = None
) -> Optional[Dict[str, Any]]:
    """
    SPAC 합병 완료 여부 확인
    
    Args:
        db: DB 세션
        ticker: 종목코드
        company_detail: CompanyDetail 객체 (선택적)
    
    Returns:
        {
            'is_completed': bool,
            'merged_target': Optional[str],  # 합병 대상 기업명
            'merged_date': Optional[datetime],
            'evidence': List[str]
        }
    """
    if not company_detail:
        company_detail = db.query(CompanyDetail).filter(
            CompanyDetail.ticker == ticker
        ).first()
    
    if not company_detail or not company_detail.biz_summary:
        return None
    
    evidence = []
    text_lower = company_detail.biz_summary.lower()
    
    # 합병 완료 키워드 확인
    for keyword in MERGER_COMPLETION_KEYWORDS:
        if keyword.lower() in text_lower:
            evidence.append(f"합병 완료 키워드 발견: {keyword}")
            break
    
    if not evidence:
        return {
            'is_completed': False,
            'merged_target': None,
            'merged_date': None,
            'evidence': []
        }
    
    # 합병 대상 기업명 추출 (간단한 패턴 매칭)
    merged_target = None
    # 향후: 더 정교한 추출 로직 추가 가능
    
    return {
        'is_completed': True,
        'merged_target': merged_target,
        'merged_date': datetime.utcnow(),  # 실제로는 공시 날짜 사용
        'evidence': evidence
    }


def trigger_sector_reclassification(
    db: Session,
    ticker: str,
    force: bool = False
) -> bool:
    """
    SPAC → POST_MERGER 전환 시 섹터 재분류 트리거
    
    Args:
        db: DB 세션
        ticker: 종목코드
        force: 강제 재분류 (상태 확인 없이)
    
    Returns:
        재분류 성공 여부
    """
    stock = db.query(Stock).filter(Stock.ticker == ticker).first()
    if not stock:
        logger.warning(f"[{ticker}] Stock 데이터 없음")
        return False
    
    company_detail = db.query(CompanyDetail).filter(
        CompanyDetail.ticker == ticker
    ).first()
    
    if not company_detail:
        logger.warning(f"[{ticker}] CompanyDetail 데이터 없음")
        return False
    
    # SPAC 여부 확인
    spac_result = classify_spac(stock, company_detail)
    if not spac_result.get('is_spac'):
        logger.info(f"[{ticker}] SPAC이 아님, 재분류 불필요")
        return False
    
    current_status = spac_result.get('status')
    
    # POST_MERGER 상태 확인
    if not force and current_status != 'POST_MERGER':
        # 합병 완료 여부 재확인
        merger_check = check_spac_merger_completion(db, ticker, company_detail)
        if not merger_check or not merger_check.get('is_completed'):
            logger.info(f"[{ticker}] 아직 합병 완료되지 않음 (현재 상태: {current_status})")
            return False
    
    logger.info(f"[{ticker}] 🔄 SPAC → POST_MERGER 전환 감지, 섹터 재분류 트리거")
    
    # 섹터 분류 파이프라인 재실행
    try:
        from app.services.sector_classifier_ensemble_won import classify_sector_ensemble_won
        
        # POST_MERGER 상태인 경우 SPAC 필터링을 건너뛰고 일반 파이프라인 진행
        sector_results = classify_sector_ensemble_won(db, ticker, force_reclassify=True)
        
        if sector_results:
            logger.info(f"[{ticker}] ✅ 섹터 재분류 완료: {len(sector_results)}개 섹터")
            return True
        else:
            logger.warning(f"[{ticker}] ⚠️ 섹터 재분류 결과 없음")
            return False
            
    except Exception as e:
        logger.error(f"[{ticker}] ❌ 섹터 재분류 실패: {e}")
        return False


def batch_check_spac_mergers(
    db: Session,
    limit: int = 100
) -> Dict[str, Any]:
    """
    배치 작업: 모든 SPAC 기업의 합병 완료 여부 체크
    
    Args:
        db: DB 세션
        limit: 최대 체크할 기업 수
    
    Returns:
        {
            'checked': int,
            'completed': int,
            'reclassified': int,
            'errors': List[str]
        }
    """
    # SPAC 기업 찾기 (InvestorSector에서 company_type='SPAC'인 기업)
    # 또는 모든 기업을 체크하여 SPAC인지 확인
    
    results = {
        'checked': 0,
        'completed': 0,
        'reclassified': 0,
        'errors': []
    }
    
    # TODO: 실제 구현 시 SPAC 기업 목록 조회 로직 추가
    # 예: db.query(InvestorSector).filter(InvestorSector.company_type == 'SPAC').all()
    
    logger.info(f"배치 작업: SPAC 합병 완료 체크 시작 (최대 {limit}개)")
    
    # 임시: 모든 기업을 체크 (실제로는 SPAC만 필터링)
    stocks = db.query(Stock).limit(limit).all()
    
    for stock in stocks:
        try:
            results['checked'] += 1
            
            company_detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == stock.ticker
            ).first()
            
            if not company_detail:
                continue
            
            # SPAC 여부 확인
            spac_result = classify_spac(stock, company_detail)
            if not spac_result.get('is_spac'):
                continue
            
            # 합병 완료 여부 확인
            merger_check = check_spac_merger_completion(db, stock.ticker, company_detail)
            
            if merger_check and merger_check.get('is_completed'):
                results['completed'] += 1
                
                # 재분류 트리거
                if trigger_sector_reclassification(db, stock.ticker):
                    results['reclassified'] += 1
                    
        except Exception as e:
            error_msg = f"{stock.ticker}: {str(e)}"
            results['errors'].append(error_msg)
            logger.error(f"❌ {error_msg}")
    
    logger.info(f"배치 작업 완료: 체크={results['checked']}, 완료={results['completed']}, 재분류={results['reclassified']}")
    
    return results

