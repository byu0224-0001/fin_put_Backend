# -*- coding: utf-8 -*-
"""
매출 비중 부스팅 로직 테스트
"""
import sys
sys.path.insert(0, '.')

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.services.sector_classifier import (
    classify_sector_rule_based, 
    calculate_revenue_sector_scores,
    calculate_dynamic_weights
)

def test_company(db, ticker: str, expected_sector: str = None):
    """특정 기업 분류 테스트"""
    detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == ticker).first()
    stock = db.query(Stock).filter(Stock.ticker == ticker).first()
    
    if not detail:
        print(f"[{ticker}] CompanyDetail 없음")
        return
    
    company_name = stock.stock_name if stock else ticker
    
    print(f"\n{'='*60}")
    print(f"[{ticker}] {company_name}")
    print(f"{'='*60}")
    
    # 매출 비중 출력
    if detail.revenue_by_segment:
        print(f"매출 비중:")
        for seg, pct in sorted(detail.revenue_by_segment.items(), key=lambda x: -x[1]):
            print(f"  - {seg}: {pct}%")
    else:
        print("매출 비중: 없음")
    
    # 매출 비중 점수 계산
    revenue_scores = calculate_revenue_sector_scores(detail.revenue_by_segment)
    if revenue_scores:
        print(f"\n매출비중 -> 섹터 점수:")
        for sector, score in sorted(revenue_scores.items(), key=lambda x: -x[1]):
            print(f"  - {sector}: {score:.3f}")
    
    # 동적 가중치
    w_r, w_k, w_p = calculate_dynamic_weights(detail.revenue_by_segment)
    print(f"\n가중치: revenue={w_r}, keyword={w_k}, product={w_p}")
    
    # 분류
    major, sub, vc, conf, _ = classify_sector_rule_based(detail, company_name)
    print(f"\n분류 결과:")
    print(f"  major_sector: {major}")
    print(f"  sub_sector: {sub}")
    print(f"  value_chain: {vc}")
    print(f"  confidence: {conf}")
    
    if expected_sector:
        status = "OK" if major == expected_sector else "WRONG"
        print(f"\n기대값: {expected_sector} -> [{status}]")


def main():
    db = SessionLocal()
    
    print("=" * 60)
    print("매출 비중 부스팅 로직 테스트")
    print("=" * 60)
    
    # 테스트 케이스
    test_cases = [
        ("028260", "SEC_CONST"),    # 삼성물산 (건설 44.3%)
        ("005380", "SEC_AUTO"),     # 현대차 (차량 78%)
        ("000270", "SEC_AUTO"),     # 기아
        ("005930", "SEC_SEMI"),     # 삼성전자
        ("000660", "SEC_SEMI"),     # SK하이닉스
        ("034730", "SEC_HOLDING"),  # SK (지주회사)
        ("003550", "SEC_HOLDING"),  # LG (지주회사)
    ]
    
    for ticker, expected in test_cases:
        try:
            test_company(db, ticker, expected)
        except Exception as e:
            print(f"[{ticker}] 오류: {e}")
    
    db.close()


if __name__ == '__main__':
    main()

