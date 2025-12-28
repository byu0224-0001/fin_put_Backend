# -*- coding: utf-8 -*-
"""
Sub-sector 분류 문제 분석 스크립트
"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.investor_sector import InvestorSector
from app.models.stock import Stock
from app.models.company_detail import CompanyDetail
from sqlalchemy import func

def main():
    db = SessionLocal()
    
    # 1. 현대자동차 확인
    print("=" * 80)
    print("[1. 현대자동차 확인]")
    print("=" * 80)
    
    results = db.query(InvestorSector, Stock).join(
        Stock, InvestorSector.ticker == Stock.ticker
    ).filter(
        Stock.stock_name.like('%현대%자동%')
    ).all()
    
    if results:
        for sector, stock in results:
            print(f"  {stock.stock_name} ({stock.ticker}): major={sector.major_sector}, sub={sector.sub_sector}, primary={sector.is_primary}")
    else:
        print("  InvestorSector에서 찾을 수 없음")
        # Stock 테이블에서 확인
        stocks = db.query(Stock).filter(Stock.stock_name.like('%현대%')).limit(20).all()
        print("  Stock 테이블에서 '현대' 검색:")
        for s in stocks:
            print(f"    {s.stock_name} ({s.ticker})")
    
    # 2. 삼성물산 확인 (왜 SEC_BIO로 분류됐는지)
    print()
    print("=" * 80)
    print("[2. 삼성물산 확인 - 왜 SEC_BIO로 분류?]")
    print("=" * 80)
    
    results = db.query(InvestorSector, Stock, CompanyDetail).outerjoin(
        Stock, InvestorSector.ticker == Stock.ticker
    ).outerjoin(
        CompanyDetail, InvestorSector.ticker == CompanyDetail.ticker
    ).filter(
        Stock.stock_name.contains('삼성물산')
    ).all()
    
    for sector, stock, detail in results:
        print(f"  {stock.stock_name} ({stock.ticker})")
        print(f"    major: {sector.major_sector}, sub: {sector.sub_sector}")
        print(f"    method: {sector.classification_method}")
        print(f"    KRX: {stock.industry_raw}")
        if detail:
            print(f"    Keywords: {detail.keywords}")
            products = detail.products[:150] + '...' if detail.products and len(detail.products) > 150 else detail.products
            print(f"    Products: {products}")
            biz = detail.biz_summary[:200] + '...' if detail.biz_summary and len(detail.biz_summary) > 200 else detail.biz_summary
            print(f"    Biz: {biz}")
    
    # 3. sub_sector가 이상한 케이스 - SEC_SEMI인데 DISTRIBUTION?
    print()
    print("=" * 80)
    print("[3. sub_sector 불일치 분석]")
    print("=" * 80)
    
    # 제조업 섹터인데 sub_sector가 DISTRIBUTION인 케이스
    manufacturing_sectors = ['SEC_SEMI', 'SEC_AUTO', 'SEC_BIO', 'SEC_ELECTRONICS', 'SEC_STEEL', 'SEC_CHEM']
    
    print("\n제조업 섹터인데 sub=DISTRIBUTION인 기업:")
    issues = db.query(InvestorSector, Stock).join(
        Stock, InvestorSector.ticker == Stock.ticker
    ).filter(
        InvestorSector.major_sector.in_(manufacturing_sectors),
        InvestorSector.sub_sector == 'DISTRIBUTION',
        InvestorSector.is_primary == True
    ).limit(20).all()
    
    for sector, stock in issues:
        print(f"  {stock.stock_name} ({stock.ticker}): {sector.major_sector} > DISTRIBUTION | KRX: {stock.industry_raw}")
    
    # 4. 전체 sub_sector 분포 확인
    print()
    print("=" * 80)
    print("[4. 섹터별 sub_sector 분포]")
    print("=" * 80)
    
    sub_dist = db.query(
        InvestorSector.major_sector,
        InvestorSector.sub_sector,
        func.count(InvestorSector.ticker)
    ).filter(
        InvestorSector.is_primary == True
    ).group_by(
        InvestorSector.major_sector,
        InvestorSector.sub_sector
    ).all()
    
    # 섹터별로 그룹화
    by_sector = {}
    for major, sub, count in sub_dist:
        if major not in by_sector:
            by_sector[major] = []
        by_sector[major].append((sub, count))
    
    for major in sorted(by_sector.keys()):
        subs = by_sector[major]
        total = sum(c for _, c in subs)
        print(f"\n{major} ({total}개):")
        for sub, count in sorted(subs, key=lambda x: -x[1]):
            pct = count / total * 100
            print(f"  {sub or 'NULL':25}: {count:4}개 ({pct:5.1f}%)")
    
    # 5. SEC_HOLDING의 오분류 패턴 심화 분석
    print()
    print("=" * 80)
    print("[5. SEC_HOLDING 분류 정확도 심화 분석]")
    print("=" * 80)
    
    holdings = db.query(InvestorSector, Stock, CompanyDetail).outerjoin(
        Stock, InvestorSector.ticker == Stock.ticker
    ).outerjoin(
        CompanyDetail, InvestorSector.ticker == CompanyDetail.ticker
    ).filter(
        InvestorSector.major_sector == 'SEC_HOLDING',
        InvestorSector.is_primary == True
    ).all()
    
    # 지주회사 키워드 체크
    holding_keywords = ['지주회사', '지주', '홀딩', '자회사 관리', '배당수익', '경영지원']
    
    confirmed_holding = []
    suspected_wrong = []
    
    for sector, stock, detail in holdings:
        name = stock.stock_name or ''
        keywords = detail.keywords if detail else []
        biz = detail.biz_summary if detail else ''
        
        # 지주회사 확인 점수
        score = 0
        reasons = []
        
        # 1. 이름에 지주/홀딩 있음 (+3)
        if any(p in name for p in ['지주', '홀딩', 'Holdings']):
            score += 3
            reasons.append('이름패턴')
        
        # 2. 키워드에 지주회사 관련 있음 (+2)
        if keywords:
            if any(kw in str(keywords) for kw in ['지주회사', '지주', '홀딩']):
                score += 2
                reasons.append('키워드')
        
        # 3. 사업설명에 지주회사 언급 (+2)
        if biz and any(kw in biz for kw in ['지주회사', '자회사를 지배', '지분 보유 및 관리']):
            score += 2
            reasons.append('사업설명')
        
        # 4. KRX가 "기타 금융업" (+1)
        if stock.industry_raw and '기타 금융업' in stock.industry_raw:
            score += 1
            reasons.append('KRX금융')
        
        if score >= 3:
            confirmed_holding.append((stock.stock_name, stock.ticker, score, reasons))
        else:
            suspected_wrong.append((stock.stock_name, stock.ticker, score, reasons, sector.sub_sector))
    
    print(f"\n확실한 지주회사: {len(confirmed_holding)}개")
    print(f"오분류 의심: {len(suspected_wrong)}개")
    
    print("\n[오분류 의심 기업 상세]:")
    for name, ticker, score, reasons, sub in suspected_wrong:
        detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == ticker).first()
        keywords = detail.keywords if detail else 'N/A'
        print(f"\n  {name} ({ticker}) - score: {score}, reasons: {reasons}")
        print(f"    sub: {sub}")
        print(f"    keywords: {keywords}")
    
    db.close()

if __name__ == '__main__':
    main()

