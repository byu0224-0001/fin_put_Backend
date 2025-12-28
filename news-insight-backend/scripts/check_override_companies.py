# -*- coding: utf-8 -*-
"""
Override 기업 확인 스크립트
SK이노베이션 등 Override 기업이 제대로 작동하는지 확인
"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.investor_sector import InvestorSector
from app.models.stock import Stock

def check_override_companies():
    """Override 기업 확인"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("Override 기업 확인")
        print("=" * 80)
        
        # Override 기업 목록 (하드코딩된 기업)
        override_tickers = ['096770']  # SK이노베이션
        
        override_companies = []
        
        for ticker in override_tickers:
            sector = db.query(InvestorSector).filter(
                InvestorSector.ticker == ticker,
                InvestorSector.is_primary == True
            ).first()
            
            stock = db.query(Stock).filter(Stock.ticker == ticker).first()
            
            if not sector:
                print(f"\n[{ticker}] InvestorSector 레코드 없음")
                continue
            
            boosting_log = sector.boosting_log or {}
            classification_meta = boosting_log.get('classification_meta', {})
            override_hit = classification_meta.get('override_hit', False)
            override_reason = classification_meta.get('override_reason', '')
            
            info = {
                'ticker': ticker,
                'name': stock.stock_name if stock else ticker,
                'major_sector': sector.major_sector,
                'confidence': sector.confidence,
                'override_hit': override_hit,
                'override_reason': override_reason,
                'primary_sector_source': classification_meta.get('primary_sector_source', 'N/A')
            }
            
            override_companies.append(info)
            
            print(f"\n[{ticker}] {stock.stock_name if stock else ticker}")
            print(f"  Major Sector: {sector.major_sector}")
            print(f"  Confidence: {sector.confidence}")
            print(f"  Override Hit: {override_hit}")
            print(f"  Override Reason: {override_reason}")
            print(f"  Primary Sector Source: {classification_meta.get('primary_sector_source', 'N/A')}")
            
            if override_hit:
                print(f"  [OK] Override 정상 작동")
            else:
                print(f"  [WARN] Override 미작동 - 재분류 필요")
        
        # 전체 Override 기업 조회
        all_sectors = db.query(InvestorSector).filter(
            InvestorSector.is_primary == True
        ).all()
        
        all_override_count = 0
        for sector in all_sectors:
            boosting_log = sector.boosting_log or {}
            classification_meta = boosting_log.get('classification_meta', {})
            override_hit = classification_meta.get('override_hit', False)
            
            if override_hit:
                all_override_count += 1
        
        print(f"\n[전체 Override 기업]")
        print(f"  총 {all_override_count}개")
        
        if all_override_count == 0:
            print(f"  [WARN] Override 기업이 0개입니다.")
            print(f"  → 재분류 실행 필요")
        else:
            print(f"  [OK] Override 기업 {all_override_count}개 확인")
        
        return {
            'override_companies': override_companies,
            'all_override_count': all_override_count
        }
        
    finally:
        db.close()

if __name__ == '__main__':
    check_override_companies()

