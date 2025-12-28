# -*- coding: utf-8 -*-
"""
SEC_FASHION 섹터 신설 및 키워드 분석

토스증권 섬유/의류 섹터 95개 기업 기반
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

# 토스증권 섬유/의류 섹터 95개 기업 (이미지에서 추출)
# 카테고리: 의류브랜드, 의류제조, 섬유
TOSS_FASHION = {
    # 의류브랜드
    '에이피알': 'FASHION_BRAND',
    'F&F': 'FASHION_BRAND',
    '미스토홀딩스': 'FASHION_BRAND',
    '코오롱인더': 'FASHION_BRAND',
    '감성코퍼레이션': 'FASHION_BRAND',
    'LF': 'FASHION_BRAND',
    '한섬': 'FASHION_BRAND',
    '폰드그룹': 'FASHION_BRAND',
    'BYC': 'FASHION_BRAND',
    '에이유브랜즈': 'FASHION_BRAND',
    'LS네트웍스': 'FASHION_BRAND',
    '이월드': 'FASHION_BRAND',
    '좋은사람들': 'FASHION_BRAND',
    '제시믹스': 'FASHION_BRAND',
    '신영와코루': 'FASHION_BRAND',
    '아가방컴퍼니': 'FASHION_BRAND',
    '공구우먼': 'FASHION_BRAND',
    '플라리스AI': 'FASHION_BRAND',
    '더네이쳐홀딩스': 'FASHION_BRAND',
    '코데즈컴바인': 'FASHION_BRAND',
    '크리스에프앤씨': 'FASHION_BRAND',
    '인바이오젠': 'FASHION_BRAND',
    '블랙야크아이앤씨': 'FASHION_BRAND',
    '인디에프': 'FASHION_BRAND',
    '인크레더블벅스': 'FASHION_BRAND',
    '제이에스티나': 'FASHION_BRAND',
    '뉴키즈온': 'FASHION_BRAND',
    '형지엘리트': 'FASHION_BRAND',
    '메타랩스': 'FASHION_BRAND',
    '그리티': 'FASHION_BRAND',
    '씨싸이트': 'FASHION_BRAND',
    '디모아': 'FASHION_BRAND',
    '형지글로벌': 'FASHION_BRAND',
    '이스트아시아홀딩스': 'FASHION_BRAND',
    '비비안': 'FASHION_BRAND',
    '배럴': 'FASHION_BRAND',
    'TBH글로벌': 'FASHION_BRAND',
    '패션플랫폼': 'FASHION_BRAND',
    '예스티오': 'FASHION_BRAND',
    '토박스코리아': 'FASHION_BRAND',
    '지엔코': 'FASHION_BRAND',
    '대현': 'FASHION_BRAND',
    '노브랜드': 'FASHION_BRAND',
    
    # 의류제조 (OEM/ODM)
    '영원무역': 'FASHION_OEM',
    '한세실업': 'FASHION_OEM',
    '제이에스코퍼레이션': 'FASHION_OEM',
    '에코마케팅': 'FASHION_OEM',
    '화승엔터프라이즈': 'FASHION_OEM',
    '신원': 'FASHION_OEM',
    '화승인더': 'FASHION_OEM',
    '동인기연': 'FASHION_OEM',
    '제로투세븐': 'FASHION_OEM',
    '호전실업': 'FASHION_OEM',
    'SG세계물산': 'FASHION_OEM',
    '온타이드': 'FASHION_OEM',
    '형지I&C': 'FASHION_OEM',
    '한세엠케이': 'FASHION_OEM',
    '케이바이오': 'FASHION_OEM',
    '아즈텍WB': 'FASHION_OEM',
    '월비스': 'FASHION_OEM',
    '메디앙스': 'FASHION_OEM',
    '원풍물산': 'FASHION_OEM',
    '로젠': 'FASHION_OEM',
    'TP': 'FASHION_OEM',
    '힝성그룹': 'FASHION_OEM',
    
    # 섬유 (원료/소재)
    '효성티앤씨': 'TEXTILE',
    '태광산업': 'TEXTILE',
    '조광피혁': 'TEXTILE',
    'DI동일': 'TEXTILE',
    '일신방직': 'TEXTILE',
    '백산': 'TEXTILE',
    '경방': 'TEXTILE',
    '방림': 'TEXTILE',
    '동양': 'TEXTILE',
    '삼양통상': 'TEXTILE',
    '대한화섬': 'TEXTILE',
    '티케이케미칼': 'TEXTILE',
    '유니켐': 'TEXTILE',
    '레몬': 'TEXTILE',
    '휴비스': 'TEXTILE',
    '덕성': 'TEXTILE',
    '전방': 'TEXTILE',
    '성안머티리얼스': 'TEXTILE',
    '웰크론': 'TEXTILE',
    '디케이앤디': 'TEXTILE',
    '대한방직': 'TEXTILE',
    '대원화성': 'TEXTILE',
    '진도': 'TEXTILE',
    
    # 기타 (신세계는 SEC_RETAIL이 주 섹터이므로 제외)
    # '신세계': SEC_RETAIL (백화점/면세업이 주력)
    '신세계인터내셔날': 'FASHION_BRAND',  # 럭셔리 패션 브랜드 수입/유통
    'SAMG엔터': 'FASHION_BRAND',
    '진원생명과학': 'FASHION_BRAND',
    '오가닉티코스메틱': 'FASHION_OEM',
    '포니링크': 'FASHION_BRAND',
    '에어릿지': 'FASHION_BRAND',
}


def analyze_fashion_keywords():
    """토스증권 섬유/의류 기업들의 키워드 분석"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("[SEC_FASHION 섹터 신설 - 토스증권 95개 기업 분석]")
        print("=" * 80)
        
        keyword_counter = Counter()
        product_counter = Counter()
        sub_sector_counts = Counter()
        
        found_count = 0
        not_found = []
        
        for company_name, sub_sector in TOSS_FASHION.items():
            sub_sector_counts[sub_sector] += 1
            
            # DB에서 기업 조회
            stock = db.query(Stock).filter(Stock.stock_name == company_name).first()
            if not stock:
                not_found.append(company_name)
                continue
            
            found_count += 1
            
            # CompanyDetail 조회
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
            
            # 현재 분류 확인
            sector = db.query(InvestorSector).filter(
                InvestorSector.ticker == stock.ticker,
                InvestorSector.is_primary == True
            ).first()
            
            if sector and sector.major_sector != 'SEC_COSMETIC':
                # SEC_COSMETIC이 아닌 다른 섹터로 분류된 경우
                pass
        
        print(f"\n[기업 매칭 결과]")
        print(f"  - 매칭됨: {found_count}개")
        print(f"  - 미매칭: {len(not_found)}개")
        if not_found[:10]:
            print(f"  - 미매칭 기업(일부): {not_found[:10]}")
        
        print(f"\n[서브섹터 분포]")
        for sub, count in sub_sector_counts.most_common():
            print(f"  - {sub}: {count}개")
        
        # 일반적인 단어 필터링
        common_words = {'제조', '생산', '판매', '개발', '서비스', '사업', '회사', 
                       '영업', '매출', '수익', '비용', '기타', '부문', '분야',
                       '국내', '해외', '제품', '상품', '기업', '업체', '시장',
                       '연구개발', '수출', '연구', '투자'}
        
        filtered_keywords = Counter({k: v for k, v in keyword_counter.items() 
                                    if k.lstrip('#') not in common_words})
        
        print(f"\n[키워드 Top 20]")
        for kw, count in filtered_keywords.most_common(20):
            coverage = count / found_count * 100 if found_count > 0 else 0
            print(f"  {kw}: {count}회 ({coverage:.1f}%)")
        
        print(f"\n[제품 Top 15]")
        for prod, count in product_counter.most_common(15):
            print(f"  {prod}: {count}회")
        
        # SEC_FASHION 키워드 추천
        print("\n" + "=" * 80)
        print("[SEC_FASHION 섹터 정의 (L1/L2/L3)]")
        print("=" * 80)
        
        print("""
# L1: SEC_FASHION (섬유/의류)
# L2 (sub_sector):
#   - FASHION_BRAND: 의류 브랜드 (자체 브랜드 보유, 소매 중심)
#   - FASHION_OEM: 의류 제조 (OEM/ODM, 수출 중심)
#   - TEXTILE: 섬유 원료/소재 (원사, 직물, 방적)
#   - FASHION_RETAIL: 패션 유통 (백화점, 면세점 등)

# L3 Tags 예시:
#   - 스포츠웨어, 아웃도어, 캐주얼, 정장, 여성복, 남성복
#   - 기능성섬유, 친환경섬유, 폴리에스터, 나일론
#   - 니트, 우븐, 가죽
""")
        
        print("\n[SECTOR_KEYWORDS 정의]")
        print("""
'SEC_FASHION': {
    'keywords': [
        # 의류
        '의류', '패션', '섬유', '의복', '봉제', '어패럴',
        '니트', '우븐', '원단', '직물', '원사', '방적',
        # 서브카테고리
        '스포츠웨어', '아웃도어', '캐주얼', '정장', '란제리', '내의',
        '여성복', '남성복', '아동복', '유아복',
        # 소재
        '폴리에스터', '나일론', '면', '모직', '합성섬유', '기능성섬유',
    ],
    'products': ['의류', '직물', '원단', '니트', '우븐', '스웨터', '셔츠', '바지'],
    'sub_sectors': {
        'FASHION_BRAND': ['브랜드', '패션', '캐주얼', '스포츠웨어'],
        'FASHION_OEM': ['OEM', 'ODM', '수출', '봉제', '제조'],
        'TEXTILE': ['섬유', '원사', '직물', '방적', '염색'],
        'FASHION_RETAIL': ['유통', '백화점', '면세점']
    }
}
""")
        
        # 결과 저장
        result = {
            'total_companies': len(TOSS_FASHION),
            'found_in_db': found_count,
            'not_found': not_found,
            'sub_sector_distribution': dict(sub_sector_counts),
            'top_keywords': filtered_keywords.most_common(30),
            'top_products': product_counter.most_common(20),
            'companies': TOSS_FASHION
        }
        
        output_path = project_root / 'reports' / 'fashion_sector_analysis.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"\n결과 저장: {output_path}")
        
        return result
        
    finally:
        db.close()


if __name__ == "__main__":
    analyze_fashion_keywords()

