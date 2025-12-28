"""
섹터/밸류체인별 공통 키워드 분석

목적: DB에 저장된 기업 데이터를 분석하여 
      섹터/밸류체인별 Top 키워드 추출

결과: SECTOR_KEYWORDS_FROM_DB 딕셔너리 생성
"""
import sys
from pathlib import Path
from collections import Counter, defaultdict
from typing import Dict, List, Tuple
import json
import re

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.investor_sector import InvestorSector
from app.models.stock import Stock
from sqlalchemy import func


def extract_nouns(text: str) -> List[str]:
    """텍스트에서 명사 추출 (간단한 휴리스틱)"""
    if not text:
        return []
    
    # 한글 2글자 이상 단어 추출
    korean_words = re.findall(r'[가-힣]{2,}', text)
    
    # 영문 3글자 이상 단어 추출 (대문자 약어 포함)
    english_words = re.findall(r'[A-Za-z]{3,}', text)
    
    return korean_words + english_words


def analyze_sector_keywords():
    """섹터별 키워드 분석"""
    print("=" * 80)
    print("[섹터/밸류체인별 키워드 분석]")
    print("=" * 80)
    
    db = SessionLocal()
    
    try:
        # 1. 섹터별 기업 정보 수집
        sector_data = defaultdict(lambda: {
            'companies': [],
            'company_names': [],
            'keywords': Counter(),
            'products': Counter(),
            'biz_summary_words': Counter(),
            'value_chains': defaultdict(Counter)
        })
        
        # investor_sector + company_details 조인
        query = db.query(
            InvestorSector.major_sector,
            InvestorSector.sub_sector,
            InvestorSector.value_chain,
            InvestorSector.ticker,
            CompanyDetail.keywords,
            CompanyDetail.products,
            CompanyDetail.biz_summary,
            Stock.stock_name
        ).join(
            CompanyDetail, InvestorSector.ticker == CompanyDetail.ticker, isouter=True
        ).join(
            Stock, InvestorSector.ticker == Stock.ticker, isouter=True
        ).filter(
            InvestorSector.is_primary == True
        )
        
        total_companies = 0
        
        for row in query.all():
            sector = row.major_sector
            sub_sector = row.sub_sector
            value_chain = row.value_chain
            
            if not sector:
                continue
            
            total_companies += 1
            sector_data[sector]['companies'].append(row.ticker)
            if row.stock_name:
                sector_data[sector]['company_names'].append(row.stock_name)
            
            # 키워드 수집
            if row.keywords:
                for kw in row.keywords:
                    if isinstance(kw, str) and len(kw) >= 2:
                        sector_data[sector]['keywords'][kw] += 1
                        if value_chain:
                            sector_data[sector]['value_chains'][value_chain][kw] += 1
            
            # 제품 수집
            if row.products:
                for prod in row.products:
                    if isinstance(prod, str) and len(prod) >= 2:
                        sector_data[sector]['products'][prod] += 1
                        if value_chain:
                            sector_data[sector]['value_chains'][value_chain][prod] += 1
            
            # 사업 요약에서 명사 추출
            if row.biz_summary:
                nouns = extract_nouns(row.biz_summary)
                for noun in nouns:
                    if len(noun) >= 2:
                        sector_data[sector]['biz_summary_words'][noun] += 1
        
        print(f"\n총 분석 기업 수: {total_companies}개")
        print(f"총 섹터 수: {len(sector_data)}개\n")
        
        # 2. 섹터별 Top 키워드 출력
        result = {}
        
        for sector, data in sorted(sector_data.items()):
            company_count = len(data['companies'])
            
            if company_count < 2:
                continue
            
            print(f"\n{'='*60}")
            print(f"[{sector}] - {company_count}개 기업")
            print(f"{'='*60}")
            
            # 대표 기업명 출력
            if data['company_names']:
                sample_names = data['company_names'][:5]
                print(f"  대표 기업: {', '.join(sample_names)}")
            
            # 키워드 + 제품 합산
            combined = data['keywords'] + data['products']
            
            # 일반적인 단어 필터링
            common_words = {'제조', '생산', '판매', '개발', '서비스', '사업', '회사', 
                          '영업', '매출', '수익', '비용', '기타', '부문', '분야',
                          '국내', '해외', '제품', '상품', '기업', '업체', '시장'}
            
            filtered_combined = Counter({k: v for k, v in combined.items() 
                                        if k not in common_words})
            
            # 빈도 기준 Top 10
            top_keywords = filtered_combined.most_common(10)
            
            print(f"\n  Top 10 키워드 (빈도순):")
            for i, (kw, count) in enumerate(top_keywords, 1):
                coverage = count / company_count * 100
                print(f"    {i}. {kw}: {count}회 ({coverage:.1f}%)")
            
            # 커버리지 기준 필터링 (20% 이상 기업에서 언급)
            min_coverage = max(2, int(company_count * 0.2))
            filtered_keywords = [(kw, cnt) for kw, cnt in top_keywords if cnt >= min_coverage]
            
            if filtered_keywords:
                print(f"\n  필터링 후 (20%+ 커버리지, Top 3):")
                for kw, count in filtered_keywords[:3]:
                    coverage = count / company_count * 100
                    print(f"    - {kw}: {count}회 ({coverage:.1f}%)")
            
            # 밸류체인별 키워드
            if data['value_chains']:
                print(f"\n  밸류체인별 Top 키워드:")
                for vc in ['UPSTREAM', 'MIDSTREAM', 'DOWNSTREAM']:
                    if vc in data['value_chains'] and data['value_chains'][vc]:
                        vc_filtered = Counter({k: v for k, v in data['value_chains'][vc].items() 
                                              if k not in common_words})
                        vc_top = vc_filtered.most_common(3)
                        if vc_top:
                            keywords_str = ', '.join([f"{kw}({cnt})" for kw, cnt in vc_top])
                            print(f"    - {vc}: {keywords_str}")
            
            # 결과 저장
            result[sector] = {
                'company_count': company_count,
                'sample_companies': data['company_names'][:5],
                'top_keywords': [kw for kw, _ in filtered_keywords[:3]] if filtered_keywords else [kw for kw, _ in top_keywords[:3]],
                'all_keywords': [(kw, cnt) for kw, cnt in top_keywords],
                'value_chain_keywords': {
                    vc: [kw for kw, _ in Counter({k: v for k, v in data['value_chains'][vc].items() 
                                                  if k not in common_words}).most_common(3)]
                    for vc in ['UPSTREAM', 'MIDSTREAM', 'DOWNSTREAM']
                    if vc in data['value_chains'] and data['value_chains'][vc]
                }
            }
        
        # 3. 코드 생성용 출력
        print("\n" + "=" * 80)
        print("[코드 생성용 - SECTOR_KEYWORDS_FROM_DB]")
        print("=" * 80)
        
        print("\n# 자동 생성됨 - scripts/analyze_sector_keywords.py")
        print("SECTOR_KEYWORDS_FROM_DB = {")
        for sector, data in sorted(result.items()):
            if data['top_keywords']:
                keywords_str = ', '.join([f"'{kw}'" for kw in data['top_keywords']])
                companies_str = ', '.join(data['sample_companies'][:2]) if data['sample_companies'] else ''
                print(f"    '{sector}': [{keywords_str}],  # {data['company_count']}개 기업 ({companies_str})")
        print("}")
        
        # 밸류체인 키워드도 출력
        print("\n# 밸류체인별 키워드")
        print("VALUE_CHAIN_KEYWORDS_FROM_DB = {")
        for sector, data in sorted(result.items()):
            if data.get('value_chain_keywords'):
                print(f"    '{sector}': {{")
                for vc, keywords in data['value_chain_keywords'].items():
                    if keywords:
                        kw_str = ', '.join([f"'{kw}'" for kw in keywords])
                        print(f"        '{vc}': [{kw_str}],")
                print("    },")
        print("}")
        
        # 4. 결과 파일 저장
        output_path = project_root / 'reports' / 'sector_keywords_analysis.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"\n결과 저장: {output_path}")
        
        # 5. 품질 평가
        print("\n" + "=" * 80)
        print("[품질 평가]")
        print("=" * 80)
        
        good_sectors = []
        weak_sectors = []
        
        for sector, data in result.items():
            if len(data['top_keywords']) >= 2:
                good_sectors.append(sector)
            else:
                weak_sectors.append(sector)
        
        print(f"\n  ✅ 키워드 충분 (2개+): {len(good_sectors)}개 섹터")
        for s in good_sectors[:10]:
            kws = result[s]['top_keywords']
            print(f"     - {s}: {', '.join(kws)}")
        
        print(f"\n  ⚠️ 키워드 부족 (<2개): {len(weak_sectors)}개 섹터")
        for s in weak_sectors:
            print(f"     - {s}")
        
        return result
        
    finally:
        db.close()


if __name__ == "__main__":
    analyze_sector_keywords()

