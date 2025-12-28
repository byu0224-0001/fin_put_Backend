# -*- coding: utf-8 -*-
"""
화장품 섹터 오분류 진단

토스증권 화장품 섹터에 없지만 DB SEC_COSMETIC으로 분류된 기업 분석
"""
import sys
from pathlib import Path
import json

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from app.db import SessionLocal
from app.models.investor_sector import InvestorSector
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock

# 토스증권 화장품 섹터 77개 기업 목록
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


def diagnose():
    db = SessionLocal()
    
    try:
        # 1. SEC_COSMETIC으로 분류된 기업 조회
        cosmetic_companies = db.query(
            InvestorSector.ticker,
            InvestorSector.major_sector,
            InvestorSector.sub_sector,
            InvestorSector.sector_l1,
            InvestorSector.sector_l2,
            InvestorSector.sector_l3_tags,
            InvestorSector.classification_method,
            InvestorSector.confidence,
            InvestorSector.classification_reasoning,
            Stock.stock_name,
            Stock.industry_raw
        ).join(
            Stock, InvestorSector.ticker == Stock.ticker
        ).filter(
            InvestorSector.major_sector == 'SEC_COSMETIC',
            InvestorSector.is_primary == True
        ).all()
        
        toss_set = set(TOSS_COSMETIC)
        
        print("=" * 80)
        print("[화장품 섹터 오분류 진단]")
        print("=" * 80)
        
        # 2. 토스증권에 없는 기업만 필터링
        misclassified = []
        for row in cosmetic_companies:
            if row.stock_name not in toss_set:
                misclassified.append(row)
        
        print(f"\n토스증권에 없지만 DB SEC_COSMETIC인 기업: {len(misclassified)}개\n")
        
        # 3. 오분류 패턴 분석
        method_count = {}
        industry_raw_count = {}
        
        results = []
        
        for i, row in enumerate(misclassified, 1):
            ticker = row.ticker
            name = row.stock_name
            method = row.classification_method or 'UNKNOWN'
            industry_raw = row.industry_raw or 'N/A'
            sub_sector = row.sub_sector or 'N/A'
            sector_l2 = row.sector_l2 or 'N/A'
            reasoning = row.classification_reasoning or ''
            
            # 분류 방법 카운트
            method_count[method] = method_count.get(method, 0) + 1
            
            # KRX 업종 카운트
            industry_raw_count[industry_raw] = industry_raw_count.get(industry_raw, 0) + 1
            
            # CompanyDetail 조회
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == ticker
            ).first()
            
            keywords = []
            products = []
            biz_summary = ''
            if detail:
                keywords = detail.keywords or []
                products = detail.products or []
                biz_summary = (detail.biz_summary or '')[:150]
            
            result = {
                'ticker': ticker,
                'name': name,
                'method': method,
                'industry_raw': industry_raw,
                'sub_sector': sub_sector,
                'sector_l2': sector_l2,
                'keywords': keywords[:5] if keywords else [],
                'products': products[:3] if products else [],
                'biz_summary': biz_summary,
                'reasoning': reasoning[:100] if reasoning else ''
            }
            results.append(result)
            
            # 상세 출력 (처음 30개만)
            if i <= 30:
                print(f"\n{i}. [{ticker}] {name}")
                print(f"   분류방법: {method}")
                print(f"   KRX업종: {industry_raw}")
                print(f"   sub_sector: {sub_sector}, L2: {sector_l2}")
                print(f"   키워드: {keywords[:5]}")
                print(f"   제품: {products[:3]}")
                if biz_summary:
                    print(f"   사업요약: {biz_summary[:80]}...")
        
        # 4. 패턴 분석 결과
        print("\n" + "=" * 80)
        print("[오분류 패턴 분석]")
        print("=" * 80)
        
        print("\n[분류 방법별 카운트]")
        for method, count in sorted(method_count.items(), key=lambda x: -x[1]):
            print(f"  {method}: {count}개")
        
        print("\n[KRX 업종별 카운트 (상위 15)]")
        for industry, count in sorted(industry_raw_count.items(), key=lambda x: -x[1])[:15]:
            print(f"  {industry}: {count}개")
        
        # 5. 의심스러운 패턴 식별
        print("\n" + "=" * 80)
        print("[의심스러운 오분류 패턴]")
        print("=" * 80)
        
        # 패션/의류가 화장품으로 분류된 경우
        fashion_keywords = ['의류', '섬유', '패션', '방직', '편직', '직물', '원단']
        fashion_misclassified = []
        
        # 바이오/제약이 화장품으로 분류된 경우
        bio_keywords = ['바이오', '제약', '의약', '신약', '세포', 'CDMO', 'CMO']
        bio_misclassified = []
        
        # 지주회사가 화장품으로 분류된 경우
        holding_keywords = ['지주', '홀딩스', '홀딩']
        holding_misclassified = []
        
        for r in results:
            text = ' '.join([
                r['industry_raw'] or '',
                ' '.join(r['keywords']),
                ' '.join(r['products']),
                r['biz_summary']
            ]).lower()
            
            name_lower = r['name'].lower()
            
            # 패션/의류
            if any(kw in text or kw in name_lower for kw in fashion_keywords):
                fashion_misclassified.append(r)
            
            # 바이오/제약
            if any(kw in text or kw in name_lower for kw in bio_keywords):
                bio_misclassified.append(r)
            
            # 지주회사
            if any(kw in text or kw in name_lower for kw in holding_keywords):
                holding_misclassified.append(r)
        
        print(f"\n1. 패션/의류 → SEC_COSMETIC 오분류 의심: {len(fashion_misclassified)}개")
        for r in fashion_misclassified[:10]:
            print(f"   - [{r['ticker']}] {r['name']} (KRX: {r['industry_raw']})")
        
        print(f"\n2. 바이오/제약 → SEC_COSMETIC 오분류 의심: {len(bio_misclassified)}개")
        for r in bio_misclassified[:10]:
            print(f"   - [{r['ticker']}] {r['name']} (KRX: {r['industry_raw']})")
        
        print(f"\n3. 지주회사 → SEC_COSMETIC 오분류 의심: {len(holding_misclassified)}개")
        for r in holding_misclassified[:10]:
            print(f"   - [{r['ticker']}] {r['name']} (KRX: {r['industry_raw']})")
        
        # 결과 저장
        output = {
            'total_misclassified': len(misclassified),
            'method_distribution': method_count,
            'industry_raw_distribution': industry_raw_count,
            'fashion_misclassified': [r['name'] for r in fashion_misclassified],
            'bio_misclassified': [r['name'] for r in bio_misclassified],
            'holding_misclassified': [r['name'] for r in holding_misclassified],
            'details': results
        }
        
        output_path = project_root / 'reports' / 'cosmetic_misclassification_diagnosis.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        print(f"\n결과 저장: {output_path}")
        
    finally:
        db.close()


if __name__ == "__main__":
    diagnose()

