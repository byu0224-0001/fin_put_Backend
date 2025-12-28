# -*- coding: utf-8 -*-
"""
Override 기업 확인 스크립트 (함수 결과 기준)
DRY RUN에서도 Override 기업을 확인할 수 있도록 분류 함수를 직접 호출
"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.services.sector_classifier import classify_sector_rule_based

def check_override_from_classifier():
    """Override 기업 확인 (함수 결과 기준)"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("Override 기업 확인 (함수 결과 기준)")
        print("=" * 80)
        
        # Override 기업 목록 (하드코딩된 기업)
        override_tickers = ['096770']  # SK이노베이션
        
        override_companies = []
        
        for ticker in override_tickers:
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == ticker
            ).first()
            
            stock = db.query(Stock).filter(Stock.ticker == ticker).first()
            
            if not detail or not stock:
                print(f"\n[{ticker}] CompanyDetail 또는 Stock 레코드 없음")
                continue
            
            # 🆕 P0-A: 분류 함수를 직접 호출하여 Override 확인
            major, sub, vc, conf, boosting_log = classify_sector_rule_based(
                detail, stock.stock_name, ticker=ticker
            )
            
            classification_meta = boosting_log.get('classification_meta', {}) if boosting_log else {}
            override_hit = classification_meta.get('override_hit', False)
            override_reason = classification_meta.get('override_reason', '')
            override_obj = classification_meta.get('override', {})
            
            # override 객체에서도 확인
            if not override_hit and isinstance(override_obj, dict):
                override_hit = override_obj.get('hit', False)
                if not override_reason:
                    override_reason = override_obj.get('reason', '')
            
            info = {
                'ticker': ticker,
                'name': stock.stock_name,
                'major_sector': major,
                'confidence': conf,
                'override_hit': override_hit,
                'override_reason': override_reason,
                'primary_sector_source': classification_meta.get('primary_sector_source', 'N/A'),
                'classification_method': boosting_log.get('classification_method', 'N/A') if boosting_log else 'N/A'
            }
            
            override_companies.append(info)
            
            print(f"\n[{ticker}] {stock.stock_name}")
            print(f"  Major Sector: {major}")
            print(f"  Confidence: {conf}")
            print(f"  Override Hit: {override_hit}")
            print(f"  Override Reason: {override_reason}")
            print(f"  Primary Sector Source: {classification_meta.get('primary_sector_source', 'N/A')}")
            print(f"  Classification Method: {boosting_log.get('classification_method', 'N/A') if boosting_log else 'N/A'}")
            
            if override_hit:
                print(f"  [OK] Override 정상 작동")
            else:
                print(f"  [WARN] Override 미작동")
                print(f"  → MAJOR_COMPANY_SECTORS 또는 특별 처리 로직 확인 필요")
        
        # 전체 Override 기업 조회 (함수 결과 기준)
        print(f"\n[전체 Override 기업 확인]")
        print(f"  확인된 Override 기업: {len([c for c in override_companies if c['override_hit']])}개")
        
        if len([c for c in override_companies if c['override_hit']]) == 0:
            print(f"  [WARN] Override 기업이 0개입니다.")
            print(f"  → MAJOR_COMPANY_SECTORS 또는 특별 처리 로직 확인 필요")
        else:
            print(f"  [OK] Override 기업 확인됨")
        
        return {
            'override_companies': override_companies,
            'override_count': len([c for c in override_companies if c['override_hit']])
        }
        
    finally:
        db.close()

if __name__ == '__main__':
    check_override_from_classifier()

