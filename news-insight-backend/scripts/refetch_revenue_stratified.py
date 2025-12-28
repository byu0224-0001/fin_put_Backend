# -*- coding: utf-8 -*-
"""
Stratified 샘플링 기반 매출 데이터 재수집 스크립트
금융지주/제조 대기업/사업형 지주/플랫폼 서비스 각 5개씩 샘플링
"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.stock import Stock
from app.models.investor_sector import InvestorSector
from app.models.company_detail import CompanyDetail
from sqlalchemy import and_, or_

def get_stratified_samples():
    """Stratified 샘플링으로 20개 기업 선택"""
    db = SessionLocal()
    
    try:
        samples = []
        
        # 1. 금융지주 5개
        financial_holdings = db.query(Stock).join(
            InvestorSector, Stock.ticker == InvestorSector.ticker
        ).filter(
            and_(
                InvestorSector.is_primary == True,
                or_(
                    Stock.stock_name.like('%금융지주%'),
                    Stock.stock_name.like('%금융지주회사%'),
                    Stock.stock_name.like('%KB금융%'),
                    Stock.stock_name.like('%신한지주%'),
                    Stock.stock_name.like('%하나금융지주%')
                )
            )
        ).limit(5).all()
        
        for stock in financial_holdings:
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == stock.ticker
            ).first()
            if detail and (not detail.revenue_by_segment or len(detail.revenue_by_segment) == 0):
                samples.append(('금융지주', stock.ticker, stock.stock_name))
        
        # 2. 제조 대기업 5개
        manufacturers = db.query(Stock).join(
            InvestorSector, Stock.ticker == InvestorSector.ticker
        ).filter(
            and_(
                InvestorSector.is_primary == True,
                InvestorSector.major_sector.in_(['SEC_SEMI', 'SEC_AUTO', 'SEC_MACH']),
                Stock.market_cap >= 10000000000000  # 10조 이상
            )
        ).limit(10).all()
        
        count = 0
        for stock in manufacturers:
            if count >= 5:
                break
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == stock.ticker
            ).first()
            if detail and (not detail.revenue_by_segment or len(detail.revenue_by_segment) == 0):
                samples.append(('제조 대기업', stock.ticker, stock.stock_name))
                count += 1
        
        # 3. 사업형 지주/중간지주 5개
        business_holdings = db.query(Stock).join(
            InvestorSector, Stock.ticker == InvestorSector.ticker
        ).filter(
            and_(
                InvestorSector.is_primary == True,
                or_(
                    Stock.stock_name.like('%홀딩스%'),
                    Stock.stock_name.like('%홀딩%')
                )
            )
        ).limit(10).all()
        
        count = 0
        for stock in business_holdings:
            if count >= 5:
                break
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == stock.ticker
            ).first()
            if detail and (not detail.revenue_by_segment or len(detail.revenue_by_segment) == 0):
                samples.append(('사업형 지주', stock.ticker, stock.stock_name))
                count += 1
        
        # 4. 플랫폼/서비스 5개
        platforms = db.query(Stock).join(
            InvestorSector, Stock.ticker == InvestorSector.ticker
        ).filter(
            and_(
                InvestorSector.is_primary == True,
                InvestorSector.major_sector.in_(['SEC_PLATFORM', 'SEC_SERVICE', 'SEC_IT'])
            )
        ).limit(10).all()
        
        count = 0
        for stock in platforms:
            if count >= 5:
                break
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == stock.ticker
            ).first()
            if detail and (not detail.revenue_by_segment or len(detail.revenue_by_segment) == 0):
                samples.append(('플랫폼/서비스', stock.ticker, stock.stock_name))
                count += 1
        
        return samples
        
    finally:
        db.close()

def check_override_companies_revenue():
    """Override 기업의 매출 데이터 적재 확인"""
    db = SessionLocal()
    
    try:
        print("\n" + "=" * 80)
        print("Override 기업 매출 데이터 적재 확인")
        print("=" * 80)
        
        # Override 기업 조회 (SK이노베이션 등)
        all_sectors = db.query(InvestorSector).filter(
            InvestorSector.is_primary == True
        ).all()
        
        override_companies = []
        for sector in all_sectors:
            boosting_log = sector.boosting_log or {}
            classification_meta = boosting_log.get('classification_meta', {})
            override_hit = classification_meta.get('override_hit', False)
            
            if override_hit:
                detail = db.query(CompanyDetail).filter(
                    CompanyDetail.ticker == sector.ticker
                ).first()
                
                has_revenue = bool(detail and detail.revenue_by_segment and 
                                 isinstance(detail.revenue_by_segment, dict) and 
                                 len(detail.revenue_by_segment) > 0)
                
                override_companies.append({
                    'ticker': sector.ticker,
                    'has_revenue': has_revenue,
                    'revenue_count': len(detail.revenue_by_segment) if detail and detail.revenue_by_segment else 0
                })
        
        print(f"\n[Override 기업] 총 {len(override_companies)}개")
        
        with_revenue = [c for c in override_companies if c['has_revenue']]
        without_revenue = [c for c in override_companies if not c['has_revenue']]
        
        print(f"  매출 데이터 있음: {len(with_revenue)}개")
        print(f"  매출 데이터 없음: {len(without_revenue)}개")
        
        if len(without_revenue) > 0:
            print(f"\n  [WARN] 매출 데이터 없는 Override 기업:")
            for company in without_revenue[:10]:
                print(f"    - {company['ticker']}")
            if len(without_revenue) > 10:
                print(f"    ... 외 {len(without_revenue) - 10}개")
            return False
        else:
            print(f"  [OK] 모든 Override 기업에 매출 데이터가 있습니다.")
            return True
            
    finally:
        db.close()

def main():
    samples = get_stratified_samples()
    
    print("=" * 80)
    print("Stratified 샘플링 결과 (20개)")
    print("=" * 80)
    
    categories = {}
    for category, ticker, name in samples:
        if category not in categories:
            categories[category] = []
        categories[category].append((ticker, name))
    
    for category, items in categories.items():
        print(f"\n[{category}] {len(items)}개")
        for ticker, name in items:
            print(f"  - {ticker}: {name}")
    
    print(f"\n총 {len(samples)}개 샘플")
    print("\n재수집 실행:")
    print(f"python scripts/refetch_all_missing_revenue.py --apply --tickers {','.join([t[1] for t in samples])}")
    
    # Override 기업 매출 데이터 확인
    check_override_companies_revenue()

if __name__ == '__main__':
    main()

