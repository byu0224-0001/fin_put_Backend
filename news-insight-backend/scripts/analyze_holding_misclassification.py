# -*- coding: utf-8 -*-
"""
SEC_HOLDING 오분류 분석 스크립트
- 지주회사로 잘못 분류된 사업회사 찾기
- 삼성전자, 현대차 등 대표 기업 확인
"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.investor_sector import InvestorSector
from app.models.stock import Stock
from app.models.company_detail import CompanyDetail
import json

def main():
    db = SessionLocal()
    
    # SEC_HOLDING으로 분류된 기업 조회
    holdings = db.query(InvestorSector, Stock).join(
        Stock, InvestorSector.ticker == Stock.ticker
    ).filter(
        InvestorSector.major_sector == 'SEC_HOLDING',
        InvestorSector.is_primary == True
    ).all()
    
    print(f"=== SEC_HOLDING 분류 기업 총 {len(holdings)}개 ===\n")
    
    # sub_sector별 분류
    by_sub = {}
    for sector, stock in holdings:
        sub = sector.sub_sector or 'UNKNOWN'
        if sub not in by_sub:
            by_sub[sub] = []
        by_sub[sub].append((sector, stock))
    
    print("[Sub-Sector 분포]")
    for sub, items in sorted(by_sub.items(), key=lambda x: -len(x[1])):
        print(f"  {sub}: {len(items)}개")
    
    print("\n" + "="*80)
    print("[전체 SEC_HOLDING 기업 목록]")
    print("="*80)
    
    for i, (sector, stock) in enumerate(holdings):
        name = stock.stock_name or ''
        ticker = stock.ticker or ''
        krx = (stock.industry_raw or 'N/A')[:30]
        sub = sector.sub_sector or 'N/A'
        method = sector.classification_method or 'N/A'
        print(f"{i+1:3}. {name:20} ({ticker}) | KRX: {krx:30} | Sub: {sub:20} | {method}")
    
    # 오분류 의심 기업 분석
    print("\n" + "="*80)
    print("[오분류 의심 기업 분석]")
    print("="*80)
    
    # 제조업 KRX 키워드
    manufacturing_krx = ['반도체', '자동차', '전자부품', '통신장비', '컴퓨터', '기계장비', 
                         '화학', '철강', '식품', '의약품', '건설', '조선', '항공']
    
    suspicious = []
    for sector, stock in holdings:
        name = stock.stock_name or ''
        krx = stock.industry_raw or ''
        
        # 지주회사가 아닌 것 같은 기업 필터
        has_holding_pattern = any(p in name for p in ['지주', '홀딩', 'Holdings'])
        has_manufacturing_krx = any(m in krx for m in manufacturing_krx)
        
        # 조건: 제조업 KRX인데 회사명에 지주/홀딩 없음
        if has_manufacturing_krx and not has_holding_pattern:
            # CompanyDetail 확인
            detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == stock.ticker).first()
            keywords = detail.keywords if detail else None
            biz_summary = (detail.biz_summary[:100] + '...') if detail and detail.biz_summary else 'N/A'
            
            suspicious.append({
                'name': name,
                'ticker': stock.ticker,
                'krx': krx,
                'sub_sector': sector.sub_sector,
                'method': sector.classification_method,
                'keywords': keywords,
                'biz_summary': biz_summary
            })
    
    print(f"\n제조업 KRX + 회사명에 지주/홀딩 없음: {len(suspicious)}개")
    for s in suspicious:
        print(f"\n  - {s['name']} ({s['ticker']})")
        print(f"    KRX: {s['krx']}")
        print(f"    Sub: {s['sub_sector']}, Method: {s['method']}")
        print(f"    Keywords: {s['keywords']}")
        print(f"    Biz: {s['biz_summary']}")
    
    # 대표 기업 확인 (삼성전자, 현대차 등)
    print("\n" + "="*80)
    print("[대표 기업 현재 분류 확인]")
    print("="*80)
    
    check_companies = [
        '삼성전자', 'SK하이닉스', 'LG전자', '현대자동차', '기아',
        'NAVER', '카카오', '셀트리온', '삼성바이오로직스',
        '삼성물산', 'SK', 'LG', 'POSCO홀딩스', '현대모비스'
    ]
    
    for company in check_companies:
        result = db.query(InvestorSector, Stock).join(
            Stock, InvestorSector.ticker == Stock.ticker
        ).filter(
            Stock.stock_name.contains(company),
            InvestorSector.is_primary == True
        ).first()
        
        if result:
            sector, stock = result
            status = "OK" if sector.major_sector != 'SEC_HOLDING' or '지주' in stock.stock_name or '홀딩' in stock.stock_name else "WRONG"
            if sector.major_sector == 'SEC_HOLDING' and status == "OK":
                status = "HOLDING_OK"
            print(f"  {stock.stock_name:20} | major: {sector.major_sector:15} | sub: {sector.sub_sector or 'N/A':20} | [{status}]")
        else:
            print(f"  {company:20} | NOT FOUND")
    
    # 모든 섹터별 분포 확인
    print("\n" + "="*80)
    print("[전체 섹터 분포]")
    print("="*80)
    
    sector_dist = db.query(
        InvestorSector.major_sector,
        db.query(InvestorSector).filter(InvestorSector.is_primary == True).subquery().c.major_sector
    ).filter(InvestorSector.is_primary == True).all()
    
    # 다시 쿼리
    from sqlalchemy import func
    sector_counts = db.query(
        InvestorSector.major_sector, 
        func.count(InvestorSector.ticker)
    ).filter(
        InvestorSector.is_primary == True
    ).group_by(InvestorSector.major_sector).all()
    
    for sector, count in sorted(sector_counts, key=lambda x: -x[1]):
        print(f"  {sector or 'NULL':20}: {count:5}개")
    
    db.close()
    
    # 결과 JSON 저장
    result = {
        'total_holding': len(holdings),
        'suspicious_count': len(suspicious),
        'suspicious_companies': suspicious
    }
    
    with open('reports/holding_misclassification_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print(f"\n결과 저장: reports/holding_misclassification_analysis.json")

if __name__ == '__main__':
    main()

