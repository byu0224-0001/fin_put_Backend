# -*- coding: utf-8 -*-
"""
SEC_COSMETIC → SEC_FASHION 재분류 스크립트

29개 섬유/의류 기업을 SEC_FASHION으로 재분류
"""
import sys
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from app.db import SessionLocal
from app.models.investor_sector import InvestorSector
from app.models.stock import Stock

# 재분류 대상 기업 (KRX 업종 기반)
FASHION_KRX_INDUSTRIES = [
    '봉제의복 제조업', 
    '직물직조 및 직물제품 제조업', 
    '방적 및 가공사 제조업', 
    '섬유제품 염색, 정리 및 마무리 가공업',
    '섬유, 의복, 신발 및 가죽제품 소매업'
]

# Sub-sector 매핑
SUB_SECTOR_MAP = {
    '봉제의복 제조업': 'FASHION_OEM',
    '직물직조 및 직물제품 제조업': 'TEXTILE',
    '방적 및 가공사 제조업': 'TEXTILE',
    '섬유제품 염색, 정리 및 마무리 가공업': 'TEXTILE',
    '섬유, 의복, 신발 및 가죽제품 소매업': 'FASHION_BRAND',
}


def reclassify():
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("[SEC_COSMETIC → SEC_FASHION 재분류]")
        print(f"실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # SEC_COSMETIC 기업 중 섬유/의류 KRX 업종 조회
        cosmetic_companies = db.query(
            InvestorSector,
            Stock.stock_name,
            Stock.industry_raw
        ).join(
            Stock, InvestorSector.ticker == Stock.ticker
        ).filter(
            InvestorSector.major_sector == 'SEC_COSMETIC',
            InvestorSector.is_primary == True
        ).all()
        
        reclassified = []
        skipped = []
        
        for sector_record, stock_name, industry_raw in cosmetic_companies:
            industry_raw = industry_raw or ''
            
            # KRX 업종이 섬유/의류인 경우만 재분류
            matched_industry = None
            for fashion_industry in FASHION_KRX_INDUSTRIES:
                if fashion_industry in industry_raw:
                    matched_industry = fashion_industry
                    break
            
            if matched_industry:
                # 재분류 실행
                old_sector = sector_record.major_sector
                new_sub_sector = SUB_SECTOR_MAP.get(matched_industry, 'FASHION_OEM')
                
                sector_record.major_sector = 'SEC_FASHION'
                sector_record.sector_l1 = 'SEC_FASHION'
                sector_record.sub_sector = new_sub_sector
                sector_record.sector_l2 = new_sub_sector
                sector_record.classification_method = 'RULE_BASED_KRX'
                sector_record.classification_reasoning = f"KRX 업종 기반 재분류: {matched_industry} → SEC_FASHION > {new_sub_sector}"
                
                reclassified.append({
                    'ticker': sector_record.ticker,
                    'name': stock_name,
                    'industry_raw': industry_raw,
                    'old_sector': old_sector,
                    'new_sector': 'SEC_FASHION',
                    'sub_sector': new_sub_sector
                })
        
        # 변경 사항 커밋
        db.commit()
        
        print(f"\n[재분류 결과]")
        print(f"  - 재분류 완료: {len(reclassified)}개")
        print(f"  - 건너뜀: {len(skipped)}개")
        
        print(f"\n[재분류된 기업 목록]")
        for item in reclassified:
            print(f"  {item['ticker']} - {item['name']}")
            print(f"    KRX: {item['industry_raw']}")
            print(f"    {item['old_sector']} → {item['new_sector']} > {item['sub_sector']}")
        
        return reclassified
        
    except Exception as e:
        db.rollback()
        print(f"오류 발생: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    reclassify()

