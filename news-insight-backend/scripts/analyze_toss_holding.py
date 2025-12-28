# -*- coding: utf-8 -*-
"""
토스증권 지주사 110개 기업 분석

1. 1,521개 패턴 매칭 문제 진단
2. 토스증권 지주사 리스트 기반 키워드/구조 분석
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

# 토스증권 지주사 110개 기업 리스트 (이미지에서 추출)
TOSS_HOLDING_COMPANIES = [
    # 1-23
    "HD한국조선해양", "POSCO홀딩스", "SK", "삼성에스에프씨홀딩스", "HD현대",
    "두산", "에코프로", "LG", "한진칼", "한화", "LS", "CJ", "GS", 
    "롯데지주", "영원무역홀딩스", "원익홀딩스", "한미사이언스", "한국앤컴퍼니",
    "아모레퍼시픽홀딩스", "효성", "한화비전", "대웅", "오리온홀딩스",
    # 24-46
    "하림지주", "SK디스커버리", "LS에코에너지", "HDC", "쿠쿠홀딩스",
    "SNT홀딩스", "DL", "솔브레인홀딩스", "녹십자홀딩스", "F&F홀딩스",
    "동아쏘시오홀딩스", "코오롱", "아세아", "LX홀딩스", "휴온스글로벌",
    "한일홀딩스", "풍산홀딩스", "세아제강지주", "세아홀딩스", "NICE",
    "풀무원", "삼양홀딩스", "농심홀딩스",
    # 47-69
    "HL홀딩스", "BGF", "INVENI", "KG케미칼", "KISCO홀딩스",
    "대상홀딩스", "대덕", "일진홀딩스", "넥센", "콜마홀딩스",
    "KPX홀딩스", "노루홀딩스", "이지홀딩스", "JW홀딩스", "종근당홀딩스",
    "하이트진로홀딩스", "서연", "골프존홀딩스", "HS효성", "동성케미컬",
    "네오위즈홀딩스", "웅진", "유비쿼스홀딩스",
    # 70-92
    "진양홀딩스", "한세예스24홀딩스", "미원홀딩스", "그래디언트", "유수홀딩스",
    "일동홀딩스", "매일홀딩스", "대성홀딩스", "한진중공업홀딩스", "샘표",
    "티와이홀딩스", "심텍홀딩스", "코아시아", "코스맥스비티아이", "제일파마홀딩스",
    "한솔홀딩스", "아이디스홀딩스", "경동인베스트", "디와이", "AK홀딩스",
    "현대코퍼레이션홀딩스", "비트플래닛", "크라운해태홀딩스",
    # 93-110
    "컴투스홀딩스", "성창기업지주", "솔본", "DRB동일", "HC홀센타",
    "이건홀딩스", "CS홀딩스", "이녹스", "신송홀딩스", "슈프리마에이치큐",
    "APS", "DSR", "우리산업홀딩스", "SJM홀딩스", "평화홀딩스",
    "원익푸드", "한국전자홀딩스", "휴맥스홀딩스"
]


def analyze_stock_count():
    """Stock 테이블 기업 수 확인"""
    db = SessionLocal()
    try:
        # 전체 Stock 수
        total = db.query(Stock).count()
        
        # 시장별 분류
        kospi = db.query(Stock).filter(Stock.market == 'KOSPI').count()
        kosdaq = db.query(Stock).filter(Stock.market == 'KOSDAQ').count()
        konex = db.query(Stock).filter(Stock.market == 'KONEX').count()
        
        print("=" * 60)
        print("[Stock 테이블 현황]")
        print("=" * 60)
        print(f"전체: {total:,}개")
        print(f"  KOSPI: {kospi:,}개")
        print(f"  KOSDAQ: {kosdaq:,}개")
        print(f"  KONEX: {konex:,}개")
        print(f"  기타: {total - kospi - kosdaq - konex:,}개")
        
        # 지주 패턴 재확인 (상장사만)
        patterns = ['지주', '홀딩스', '홀딩', 'Holdings']
        
        holding_listed = []
        for stock in db.query(Stock).filter(Stock.market.in_(['KOSPI', 'KOSDAQ'])).all():
            name = stock.stock_name or ''
            if any(p in name for p in patterns):
                holding_listed.append(stock)
        
        print(f"\n[지주 패턴 기업 (KOSPI+KOSDAQ만)]")
        print(f"  패턴 매칭: {len(holding_listed)}개")
        
        return len(holding_listed)
        
    finally:
        db.close()


def analyze_toss_holding():
    """토스증권 지주사 110개 분석"""
    db = SessionLocal()
    
    try:
        print("\n" + "=" * 60)
        print("[토스증권 지주사 110개 분석]")
        print("=" * 60)
        
        found_count = 0
        not_found = []
        
        keyword_counter = Counter()
        product_counter = Counter()
        krx_counter = Counter()
        sector_counter = Counter()
        
        # 회사명 패턴 분석
        name_pattern_counter = Counter()
        
        found_companies = []
        
        for company_name in TOSS_HOLDING_COMPANIES:
            # Stock에서 검색
            stock = db.query(Stock).filter(
                (Stock.stock_name == company_name) |
                (Stock.stock_name.contains(company_name))
            ).first()
            
            if not stock:
                # 약간 다른 이름으로 검색
                simplified = company_name.replace("홀딩스", "").replace("지주", "").strip()
                stock = db.query(Stock).filter(
                    Stock.stock_name.contains(simplified)
                ).first()
            
            if stock:
                found_count += 1
                ticker = stock.ticker
                name = stock.stock_name
                krx = stock.industry_raw or 'N/A'
                
                krx_counter[krx] += 1
                
                # 회사명 패턴 분석
                if '홀딩스' in name:
                    name_pattern_counter['홀딩스'] += 1
                elif '지주' in name:
                    name_pattern_counter['지주'] += 1
                elif '홀딩' in name:
                    name_pattern_counter['홀딩'] += 1
                else:
                    name_pattern_counter['패턴없음'] += 1
                
                # CompanyDetail
                detail = db.query(CompanyDetail).filter(
                    CompanyDetail.ticker == ticker
                ).first()
                
                # InvestorSector
                sector = db.query(InvestorSector).filter(
                    InvestorSector.ticker == ticker,
                    InvestorSector.is_primary == True
                ).first()
                
                if sector:
                    sector_counter[sector.major_sector] += 1
                else:
                    sector_counter['미분류'] += 1
                
                company_info = {
                    'name': name,
                    'ticker': ticker,
                    'krx': krx,
                    'major_sector': sector.major_sector if sector else None,
                    'keywords': [],
                    'products': [],
                    'biz_summary': ''
                }
                
                if detail:
                    company_info['keywords'] = detail.keywords or []
                    company_info['products'] = detail.products or []
                    company_info['biz_summary'] = (detail.biz_summary or '')[:150]
                    
                    for kw in detail.keywords or []:
                        if isinstance(kw, str) and len(kw) >= 2:
                            keyword_counter[kw] += 1
                    
                    for prod in detail.products or []:
                        if isinstance(prod, str) and len(prod) >= 2:
                            product_counter[prod] += 1
                
                found_companies.append(company_info)
            else:
                not_found.append(company_name)
        
        # 결과 출력
        print(f"\n매칭 결과: {found_count}/{len(TOSS_HOLDING_COMPANIES)}개")
        print(f"미발견: {len(not_found)}개")
        if not_found[:10]:
            print(f"  예시: {not_found[:10]}")
        
        print(f"\n[회사명 패턴 분포]")
        for pattern, count in name_pattern_counter.most_common():
            pct = count / found_count * 100 if found_count else 0
            print(f"  {pattern}: {count}개 ({pct:.1f}%)")
        
        print(f"\n[KRX 업종 분포 (Top 10)]")
        for krx, count in krx_counter.most_common(10):
            pct = count / found_count * 100 if found_count else 0
            print(f"  {krx}: {count}개 ({pct:.1f}%)")
        
        print(f"\n[현재 섹터 분류 분포]")
        for sector, count in sector_counter.most_common():
            pct = count / found_count * 100 if found_count else 0
            print(f"  {sector}: {count}개 ({pct:.1f}%)")
        
        # 키워드 필터링
        common_words = {'제조', '생산', '판매', '개발', '서비스', '사업', '회사', 
                       '영업', '매출', '수익', '비용', '기타', '부문', '분야',
                       '국내', '해외', '제품', '상품', '기업', '업체', '시장',
                       '연구개발', '수출', '연구', '투자', '정보없음'}
        
        print(f"\n[키워드 Top 20]")
        filtered_kw = {k: v for k, v in keyword_counter.items() 
                      if k.lstrip('#') not in common_words}
        for kw, count in Counter(filtered_kw).most_common(20):
            pct = count / found_count * 100 if found_count else 0
            print(f"  {kw}: {count}회 ({pct:.1f}%)")
        
        print(f"\n[제품/서비스 Top 15]")
        filtered_prod = {k: v for k, v in product_counter.items() 
                        if k not in common_words and len(k) >= 3}
        for prod, count in Counter(filtered_prod).most_common(15):
            pct = count / found_count * 100 if found_count else 0
            print(f"  {prod}: {count}회 ({pct:.1f}%)")
        
        # 패턴 없는 기업 상세 분석
        print(f"\n[패턴없음 기업 상세 (사업지주 후보)]")
        no_pattern_companies = [c for c in found_companies 
                                if not any(p in c['name'] for p in ['홀딩스', '지주', '홀딩', 'Holdings'])]
        for c in no_pattern_companies[:15]:
            print(f"  {c['name']}: KRX={c['krx'][:30] if c['krx'] else 'N/A'}, "
                  f"섹터={c['major_sector']}, 키워드={c['keywords'][:3]}")
        
        # 결과 저장
        result = {
            'total_toss': len(TOSS_HOLDING_COMPANIES),
            'found': found_count,
            'not_found': not_found,
            'name_pattern_dist': dict(name_pattern_counter),
            'krx_dist': dict(krx_counter.most_common(20)),
            'sector_dist': dict(sector_counter),
            'top_keywords': Counter(filtered_kw).most_common(30),
            'top_products': Counter(filtered_prod).most_common(20),
            'companies': found_companies
        }
        
        output_path = project_root / 'reports' / 'toss_holding_analysis.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"\n결과 저장: {output_path}")
        
        return result
        
    finally:
        db.close()


if __name__ == "__main__":
    analyze_stock_count()
    analyze_toss_holding()

