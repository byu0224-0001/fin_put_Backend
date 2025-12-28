# -*- coding: utf-8 -*-
"""
지주회사 탐지 로직 테스트

새로운 detect_holding_company 함수 검증
"""
import sys
from pathlib import Path
import json

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.services.krx_sector_filter import detect_holding_company, filter_sector_by_krx

# 토스증권 지주사 중 테스트 케이스
TEST_CASES = [
    # 회사명 패턴 있음
    "POSCO홀딩스",
    "롯데지주", 
    "영원무역홀딩스",
    
    # 패턴 없음 (사업지주)
    "SK",
    "CJ", 
    "GS",
    "LG",
    "한진칼",
    "두산",
    "효성",
    "코오롱",
    "대웅",
    
    # 비지주 (false positive 체크)
    "삼성전자",
    "현대차",
    "LG전자",
]


def test_holding_detection():
    """지주회사 탐지 테스트"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("[지주회사 탐지 로직 테스트]")
        print("=" * 80)
        
        results = []
        
        for company_name in TEST_CASES:
            # Stock에서 검색
            stock = db.query(Stock).filter(
                Stock.stock_name == company_name
            ).first()
            
            if not stock:
                stock = db.query(Stock).filter(
                    Stock.stock_name.contains(company_name)
                ).first()
            
            if not stock:
                print(f"\n[{company_name}] 미발견")
                continue
            
            ticker = stock.ticker
            name = stock.stock_name
            krx = stock.industry_raw
            
            # CompanyDetail
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == ticker
            ).first()
            
            keywords = detail.keywords if detail else []
            products = detail.products if detail else []
            
            # 1. filter_sector_by_krx 테스트
            sector, sub, conf = filter_sector_by_krx(krx, name, keywords)
            
            # 매출 비중 데이터 (있으면)
            revenue = detail.revenue_by_segment if detail else None
            
            # 2. detect_holding_company 테스트 (매출 비중 포함)
            # 🆕 R4: 시그니처 변경 반영 (company_detail 추가, 반환값 4개)
            is_holding, holding_conf, reason, holding_type = detect_holding_company(
                company_name=name,
                industry_raw=krx,
                keywords=keywords,
                products=products,
                revenue_by_segment=revenue,
                company_detail=detail  # 🆕 R4: company_detail 전달
            )
            
            result = {
                'name': name,
                'ticker': ticker,
                'krx': krx,
                'keywords': keywords[:3] if keywords else [],
                'products': products[:3] if products else [],
                'filter_sector': sector,
                'filter_conf': conf,
                'is_holding': is_holding,
                'holding_conf': holding_conf,
                'reason': reason
            }
            results.append(result)
            
            # 출력
            status = "✅ 지주회사" if is_holding else "❌ 비지주"
            print(f"\n[{name}] {status}")
            print(f"  KRX: {krx}")
            print(f"  키워드: {keywords[:3]}")
            print(f"  제품: {products[:3]}")
            print(f"  filter_sector_by_krx: {sector}, conf={conf:.2f}")
            print(f"  detect_holding: conf={holding_conf:.2f}, reason={reason}")
        
        # 요약
        print("\n" + "=" * 80)
        print("[요약]")
        print("=" * 80)
        
        detected = [r for r in results if r['is_holding']]
        not_detected = [r for r in results if not r['is_holding']]
        
        print(f"\n지주회사 탐지: {len(detected)}개")
        for r in detected:
            print(f"  ✅ {r['name']}: {r['reason']} (conf={r['holding_conf']:.2f})")
        
        print(f"\n비지주 판정: {len(not_detected)}개")
        for r in not_detected:
            print(f"  ❌ {r['name']}: KRX={r['krx'][:20] if r['krx'] else 'N/A'}")
        
        return results
        
    finally:
        db.close()


if __name__ == "__main__":
    test_holding_detection()

