# -*- coding: utf-8 -*-
"""
토스증권 화장품 섹터 vs DB 비교 및 키워드 분석
"""
import sys
from pathlib import Path
from collections import Counter

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from app.db import SessionLocal
from app.models.investor_sector import InvestorSector
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock

# 토스증권 화장품 섹터 77개 기업 목록 (이미지에서 추출)
TOSS_COSMETIC = [
    '에이피알', '아모레퍼시픽', 'LG생활건강', '파마리서치', '케어젠',
    '휴젤', '코스맥스', '달바글로벌', '한국콜마', '차바이오텍',
    '코스메카코리아', '펌텍코리아', '브이티', '현대바이오', '씨앤씨인터내셔널',
    '콜마비앤에이치', '애경산업', '글로벌텍스프리', '네오팜', '일신방직',
    '에이블씨엔씨', '잇츠한불', '아이패밀리에스씨', '클리오', '잉글우드랩',
    '마녀공장', '한국화장품제조', '지누스앤컴퍼니', '토니모리', '바이오비쥬',
    '차시헬스케어', '강스템바이오텍', '한국화장품', '아로마티카', '제닉',
    '대봉엘에스', '현대바이오랜드', '원익', '청담글로벌', '아이텍',
    '선진뷰티사이언스', '라파스', 'HLB글로벌', '페야', '내츄럴엔도텍',
    '코리아나', '에이에스텍', '씨티케이', '제로투세븐', '셀바이오휴먼텍',
    '엔에프씨', '에스엠씨지', '피엔케이피부임상연구센타', '삼양케이씨아이',
    '뷰티스킨', '오가닉티코스메틱', '인크레더블벅스', '대륙제관', '코디',
    '금비', '승일', '제이투케이바이오', '씨큐브', '메타랩스',
    '리더스코스메틱', '휴럼앤씨', '글로본', '에코글로우', '세화피앤씨',
    'CSA코스믹', '바른손', '디와이디', '아우딘퓨쳐스', '본느',
    '넥스트아이', '이노진', '메디앙스'
]


def analyze():
    db = SessionLocal()
    
    try:
        # 1. SEC_COSMETIC으로 분류된 기업 조회
        cosmetic_companies = db.query(
            InvestorSector.ticker,
            Stock.stock_name,
            InvestorSector.classification_method
        ).join(
            Stock, InvestorSector.ticker == Stock.ticker
        ).filter(
            InvestorSector.major_sector == 'SEC_COSMETIC',
            InvestorSector.is_primary == True
        ).all()
        
        db_names = {c[1] for c in cosmetic_companies}
        toss_set = set(TOSS_COSMETIC)
        
        print("=" * 60)
        print(f"[비교 결과]")
        print("=" * 60)
        print(f"토스증권 화장품 섹터: {len(toss_set)}개")
        print(f"DB SEC_COSMETIC: {len(db_names)}개")
        
        # 2. DB에는 있지만 토스증권에 없는 기업
        extra_in_db = db_names - toss_set
        print(f"\n=== DB에는 있지만 토스증권에 없는 기업 ({len(extra_in_db)}개) ===")
        for name in sorted(extra_in_db):
            print(f"  - {name}")
        
        # 3. 토스증권에는 있지만 DB에 없는 기업
        missing_in_db = toss_set - db_names
        print(f"\n=== 토스증권에는 있지만 DB에 없는 기업 ({len(missing_in_db)}개) ===")
        for name in sorted(missing_in_db):
            print(f"  - {name}")
        
        # 4. 토스증권 기업들의 키워드 분석
        print("\n" + "=" * 60)
        print("[토스증권 화장품 기업 키워드 분석]")
        print("=" * 60)
        
        keyword_counter = Counter()
        product_counter = Counter()
        
        for company_name in TOSS_COSMETIC:
            # 기업명으로 ticker 찾기
            stock = db.query(Stock).filter(Stock.stock_name == company_name).first()
            if not stock:
                continue
            
            # CompanyDetail에서 키워드/제품 가져오기
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == stock.ticker
            ).first()
            
            if detail:
                if detail.keywords:
                    for kw in detail.keywords:
                        if isinstance(kw, str) and len(kw) >= 2:
                            keyword_counter[kw] += 1
                
                if detail.products:
                    for prod in detail.products:
                        if isinstance(prod, str) and len(prod) >= 2:
                            product_counter[prod] += 1
        
        # 일반적인 단어 필터링
        common_words = {'제조', '생산', '판매', '개발', '서비스', '사업', '회사', 
                       '영업', '매출', '수익', '비용', '기타', '부문', '분야',
                       '국내', '해외', '제품', '상품', '기업', '업체', '시장',
                       '연구개발', '수출', '연구', '투자'}
        
        filtered_keywords = Counter({k: v for k, v in keyword_counter.items() 
                                    if k.lstrip('#') not in common_words})
        filtered_products = Counter({k: v for k, v in product_counter.items() 
                                    if k not in common_words})
        
        print("\n[키워드 Top 10]")
        for kw, count in filtered_keywords.most_common(10):
            coverage = count / len(TOSS_COSMETIC) * 100
            print(f"  {kw}: {count}회 ({coverage:.1f}%)")
        
        print("\n[제품 Top 10]")
        for prod, count in filtered_products.most_common(10):
            coverage = count / len(TOSS_COSMETIC) * 100
            print(f"  {prod}: {count}회 ({coverage:.1f}%)")
        
        # 5. 합산 Top 키워드
        combined = filtered_keywords + filtered_products
        print("\n" + "=" * 60)
        print("[합산 Top 키워드 (키워드 + 제품)]")
        print("=" * 60)
        for kw, count in combined.most_common(15):
            coverage = count / len(TOSS_COSMETIC) * 100
            print(f"  {kw}: {count}회 ({coverage:.1f}%)")
        
        # 6. Rule-based 키워드 추천
        print("\n" + "=" * 60)
        print("[Rule-based 키워드 추천]")
        print("=" * 60)
        
        # 커버리지 10% 이상인 키워드
        recommended = [(kw, count) for kw, count in combined.most_common(20) 
                      if count >= len(TOSS_COSMETIC) * 0.05]  # 5% 이상
        
        print("SECTOR_KEYWORDS_FROM_DB['SEC_COSMETIC'] = [")
        keywords_list = [kw.lstrip('#') for kw, _ in recommended[:8]]
        print(f"    {keywords_list}")
        print("]")
        
    finally:
        db.close()


if __name__ == "__main__":
    import json
    from pathlib import Path
    
    # 결과를 파일로도 저장
    analyze()
    
    # 추가로 JSON 결과 저장
    db = SessionLocal()
    
    keyword_counter = Counter()
    product_counter = Counter()
    
    for company_name in TOSS_COSMETIC:
        stock = db.query(Stock).filter(Stock.stock_name == company_name).first()
        if not stock:
            continue
        
        detail = db.query(CompanyDetail).filter(
            CompanyDetail.ticker == stock.ticker
        ).first()
        
        if detail:
            if detail.keywords:
                for kw in detail.keywords:
                    if isinstance(kw, str) and len(kw) >= 2:
                        keyword_counter[kw] += 1
            
            if detail.products:
                for prod in detail.products:
                    if isinstance(prod, str) and len(prod) >= 2:
                        product_counter[prod] += 1
    
    common_words = {'제조', '생산', '판매', '개발', '서비스', '사업', '회사', 
                   '영업', '매출', '수익', '비용', '기타', '부문', '분야',
                   '국내', '해외', '제품', '상품', '기업', '업체', '시장',
                   '연구개발', '수출', '연구', '투자'}
    
    filtered_keywords = Counter({k: v for k, v in keyword_counter.items() 
                                if k.lstrip('#') not in common_words})
    filtered_products = Counter({k: v for k, v in product_counter.items() 
                                if k not in common_words})
    combined = filtered_keywords + filtered_products
    
    result = {
        'top_keywords': combined.most_common(20),
        'keyword_only': filtered_keywords.most_common(15),
        'products_only': filtered_products.most_common(15)
    }
    
    output_path = Path(__file__).parent.parent / 'reports' / 'toss_cosmetic_keywords.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    
    print(f"\n결과 저장: {output_path}")
    
    db.close()

