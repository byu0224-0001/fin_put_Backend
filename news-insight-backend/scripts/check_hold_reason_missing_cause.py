# -*- coding: utf-8 -*-
"""
hold_reason_code 누락 원인 분류 스크립트
레거시 데이터인지 버그인지 판별
"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.investor_sector import InvestorSector
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.services.sector_classifier import classify_sector_rule_based
from sqlalchemy import or_

def check_missing_cause(tickers):
    """누락 원인 분류"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("hold_reason_code 누락 원인 분류")
        print("=" * 80)
        
        for ticker in tickers:
            print(f"\n[{ticker}]")
            
            # 현재 DB 상태 확인
            sector = db.query(InvestorSector).filter(
                InvestorSector.ticker == ticker
            ).first()
            
            if not sector:
                print(f"  [ERROR] InvestorSector 레코드 없음")
                continue
            
            boosting_log = sector.boosting_log or {}
            classification_meta = boosting_log.get('classification_meta', {})
            hold_reason_code = classification_meta.get('hold_reason_code')
            hold_reason = classification_meta.get('hold_reason')
            
            print(f"  현재 상태:")
            print(f"    - Confidence: {sector.confidence}")
            print(f"    - Major Sector: {sector.major_sector}")
            print(f"    - hold_reason_code: {hold_reason_code or 'N/A'}")
            print(f"    - hold_reason: {hold_reason or 'N/A'}")
            print(f"    - boosting_log 존재: {bool(boosting_log)}")
            print(f"    - classification_meta 존재: {bool(classification_meta)}")
            
            # 재분류 테스트
            stock = db.query(Stock).filter(Stock.ticker == ticker).first()
            detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == ticker).first()
            
            if not stock or not detail:
                print(f"  [ERROR] Stock 또는 CompanyDetail 데이터 없음")
                continue
            
            print(f"\n  재분류 테스트 실행 중...")
            major, sub, vc, conf, boosting_log_new = classify_sector_rule_based(
                detail, stock.stock_name, ticker=ticker
            )
            
            meta_new = boosting_log_new.get('classification_meta', {}) if boosting_log_new else {}
            hold_reason_code_new = meta_new.get('hold_reason_code')
            hold_reason_new = meta_new.get('hold_reason')
            
            print(f"  재분류 후:")
            print(f"    - Confidence: {conf}")
            print(f"    - Major Sector: {major}")
            print(f"    - hold_reason_code: {hold_reason_code_new or 'N/A'}")
            print(f"    - hold_reason: {hold_reason_new or 'N/A'}")
            
            # 원인 판별
            if conf and conf.startswith('HOLD:'):
                if hold_reason_code_new or hold_reason_new:
                    print(f"\n  [판별] 버그 아님 - 재분류 시 hold_reason_code 생성됨")
                    print(f"  -> 원인: 레거시 데이터 (재분류 필요)")
                else:
                    print(f"\n  [판별] 버그 가능성 - 재분류 시에도 hold_reason_code 없음")
                    print(f"  -> 원인: 특정 HOLD 경로에서 meta 저장 누락 (코드 버그)")
            else:
                print(f"\n  [판별] HOLD가 아님 - 재분류 시 confidence: {conf}")
                print(f"  -> 원인: 레거시 데이터 (이전에 HOLD였으나 현재는 아님)")
        
    finally:
        db.close()

if __name__ == '__main__':
    # 누락 레코드 2개
    missing_tickers = ['002170', '061040']
    check_missing_cause(missing_tickers)

