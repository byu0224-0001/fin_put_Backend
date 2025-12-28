# -*- coding: utf-8 -*-
"""
지주회사(홀딩스) 기업 키워드 분석

'지주', '홀딩스', 'Holdings' 등이 포함된 기업들의 공통 키워드 추출
"""
import sys
from pathlib import Path
import json
from collections import Counter

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from app.db import SessionLocal
from app.models.investor_sector import InvestorSector
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock


def analyze_holding_companies():
    """지주회사 키워드 분석"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("[지주회사(홀딩스) 키워드 분석]")
        print("=" * 80)
        
        # 1. '지주', '홀딩스', 'Holdings' 포함 기업 조회
        holding_patterns = ['지주', '홀딩스', '홀딩', 'Holdings', 'HOLDINGS']
        
        all_stocks = db.query(Stock).all()
        holding_stocks = []
        
        for stock in all_stocks:
            name = stock.stock_name or ''
            if any(pattern in name for pattern in holding_patterns):
                holding_stocks.append(stock)
        
        print(f"\n지주회사 패턴 기업: {len(holding_stocks)}개")
        
        # 2. 키워드 분석
        keyword_counter = Counter()
        product_counter = Counter()
        krx_counter = Counter()
        
        holding_companies = []
        
        for stock in holding_stocks:
            ticker = stock.ticker
            name = stock.stock_name
            industry_raw = stock.industry_raw or 'N/A'
            
            krx_counter[industry_raw] += 1
            
            # CompanyDetail 조회
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == ticker
            ).first()
            
            # InvestorSector 조회
            sector = db.query(InvestorSector).filter(
                InvestorSector.ticker == ticker,
                InvestorSector.is_primary == True
            ).first()
            
            company_info = {
                'ticker': ticker,
                'name': name,
                'industry_raw': industry_raw,
                'major_sector': sector.major_sector if sector else None,
                'keywords': [],
                'products': [],
                'biz_summary': ''
            }
            
            if detail:
                company_info['keywords'] = detail.keywords or []
                company_info['products'] = detail.products or []
                company_info['biz_summary'] = (detail.biz_summary or '')[:200]
                
                if detail.keywords:
                    for kw in detail.keywords:
                        if isinstance(kw, str) and len(kw) >= 2:
                            keyword_counter[kw] += 1
                
                if detail.products:
                    for prod in detail.products:
                        if isinstance(prod, str) and len(prod) >= 2:
                            product_counter[prod] += 1
            
            holding_companies.append(company_info)
        
        # 3. 결과 출력
        print(f"\n[KRX 업종 분포]")
        for industry, count in krx_counter.most_common(15):
            print(f"  {industry}: {count}개")
        
        print(f"\n[현재 섹터 분류 분포]")
        sector_counter = Counter(c['major_sector'] for c in holding_companies)
        for sector, count in sector_counter.most_common():
            print(f"  {sector}: {count}개")
        
        # 일반적인 단어 필터링
        common_words = {'제조', '생산', '판매', '개발', '서비스', '사업', '회사', 
                       '영업', '매출', '수익', '비용', '기타', '부문', '분야',
                       '국내', '해외', '제품', '상품', '기업', '업체', '시장',
                       '연구개발', '수출', '연구', '투자', '정보없음'}
        
        filtered_keywords = Counter({k: v for k, v in keyword_counter.items() 
                                    if k.lstrip('#') not in common_words})
        
        print(f"\n[키워드 Top 20]")
        for kw, count in filtered_keywords.most_common(20):
            coverage = count / len(holding_stocks) * 100 if holding_stocks else 0
            print(f"  {kw}: {count}회 ({coverage:.1f}%)")
        
        print(f"\n[제품/서비스 Top 15]")
        filtered_products = Counter({k: v for k, v in product_counter.items() 
                                    if k not in common_words and len(k) >= 3})
        for prod, count in filtered_products.most_common(15):
            print(f"  {prod}: {count}회")
        
        # 4. 지주회사 특성 키워드 추출
        print("\n" + "=" * 80)
        print("[지주회사 특성 키워드 추천]")
        print("=" * 80)
        
        # 5% 이상 커버리지 키워드
        min_coverage = max(3, int(len(holding_stocks) * 0.05))
        holding_keywords = [kw for kw, cnt in filtered_keywords.most_common(30) 
                          if cnt >= min_coverage]
        
        print(f"\n추천 키워드 (5%+ 커버리지):")
        for kw in holding_keywords[:10]:
            print(f"  - {kw}")
        
        # 결과 저장
        result = {
            'total_holding_companies': len(holding_stocks),
            'krx_distribution': dict(krx_counter.most_common(20)),
            'sector_distribution': dict(sector_counter),
            'top_keywords': filtered_keywords.most_common(30),
            'top_products': filtered_products.most_common(20),
            'recommended_keywords': holding_keywords[:10],
            'companies': holding_companies[:50]  # 상위 50개만
        }
        
        output_path = project_root / 'reports' / 'holding_company_analysis.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"\n결과 저장: {output_path}")
        
        return result
        
    finally:
        db.close()


if __name__ == "__main__":
    analyze_holding_companies()

