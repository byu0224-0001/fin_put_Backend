# -*- coding: utf-8 -*-
"""
Override 기업의 매출 데이터 질적 검증 스크립트
데이터 존재 여부뿐만 아니라 내용의 유의미성 확인
"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.investor_sector import InvestorSector
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock

def check_override_revenue_quality():
    """Override 기업의 매출 데이터 질적 검증"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("Override 기업 매출 데이터 질적 검증")
        print("=" * 80)
        
        # Override 기업 조회
        all_sectors = db.query(InvestorSector).filter(
            InvestorSector.is_primary == True
        ).all()
        
        override_companies = []
        for sector in all_sectors:
            boosting_log = sector.boosting_log or {}
            classification_meta = boosting_log.get('classification_meta', {})
            override_hit = classification_meta.get('override_hit', False)
            
            if override_hit:
                stock = db.query(Stock).filter(Stock.ticker == sector.ticker).first()
                detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == sector.ticker).first()
                
                if detail:
                    revenue_data = detail.revenue_by_segment or {}
                    has_revenue = bool(revenue_data and isinstance(revenue_data, dict) and len(revenue_data) > 0)
                    
                    # 질적 검증
                    quality_issues = []
                    
                    if not has_revenue:
                        quality_issues.append("매출 데이터 없음")
                    else:
                        # 1. 세그먼트 개수 확인 (최소 2개 이상이어야 의미 있음)
                        segment_count = len(revenue_data)
                        if segment_count < 2:
                            quality_issues.append(f"세그먼트 부족 ({segment_count}개)")
                        
                        # 2. "기타" 또는 "단일부문" 비중 확인
                        other_keys = ['기타', '단일부문', '기타사업', '기타부문', '기타영업']
                        other_pct = sum(
                            pct for key, pct in revenue_data.items() 
                            if any(other in key for other in other_keys)
                        )
                        if other_pct >= 80:
                            quality_issues.append(f"'기타' 비중 과다 ({other_pct:.1f}%)")
                        
                        # 3. 구체적인 사업부문명 확인
                        specific_keys = [k for k in revenue_data.keys() if not any(other in k for other in other_keys)]
                        if len(specific_keys) < 2:
                            quality_issues.append(f"구체적 부문명 부족 ({len(specific_keys)}개)")
                    
                    override_companies.append({
                        'ticker': sector.ticker,
                        'name': stock.stock_name if stock else sector.ticker,
                        'has_revenue': has_revenue,
                        'revenue_data': revenue_data if has_revenue else {},
                        'segment_count': len(revenue_data) if has_revenue else 0,
                        'quality_issues': quality_issues
                    })
        
        print(f"\n[Override 기업] 총 {len(override_companies)}개")
        
        if len(override_companies) == 0:
            print("  Override 기업 없음")
            return True
        
        quality_ok = []
        quality_issues_list = []
        
        for company in override_companies:
            if len(company['quality_issues']) == 0:
                quality_ok.append(company)
            else:
                quality_issues_list.append(company)
        
        print(f"\n[질적 검증 결과]")
        print(f"  양호: {len(quality_ok)}개")
        print(f"  이슈: {len(quality_issues_list)}개")
        
        if len(quality_ok) > 0:
            print(f"\n  [양호 기업]")
            for company in quality_ok:
                print(f"    - {company['ticker']} ({company['name']}): {company['segment_count']}개 세그먼트")
                if company['revenue_data']:
                    top3 = sorted(company['revenue_data'].items(), key=lambda x: x[1], reverse=True)[:3]
                    print(f"      Top3: {', '.join([f'{k}({v:.1f}%)' for k, v in top3])}")
        
        if len(quality_issues_list) > 0:
            print(f"\n  [이슈 기업]")
            for company in quality_issues_list:
                print(f"    - {company['ticker']} ({company['name']}):")
                for issue in company['quality_issues']:
                    print(f"      * {issue}")
                if company['revenue_data']:
                    print(f"      데이터: {company['revenue_data']}")
            
            return False
        else:
            print(f"\n  [OK] 모든 Override 기업의 매출 데이터가 양호합니다.")
            return True
            
    finally:
        db.close()

if __name__ == '__main__':
    success = check_override_revenue_quality()
    sys.exit(0 if success else 1)

