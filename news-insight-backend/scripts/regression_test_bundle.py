# -*- coding: utf-8 -*-
"""
회귀 테스트 번들 생성 스크립트 (P0-5)

Top20 오분류 + 섹터별 대표기업 + 지배구조 특이군(홀딩스/스팩/리츠) 최소 60~100개
"""
import sys
import os
import json

# Windows 환경에서 UTF-8 인코딩 설정
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'
else:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.models.investor_sector import InvestorSector

def generate_regression_test_bundle():
    """회귀 테스트 번들 생성"""
    db = SessionLocal()
    
    try:
        print("=" * 80, flush=True)
        print("회귀 테스트 번들 생성", flush=True)
        print("=" * 80, flush=True)
        
        test_bundle = {
            'version': '1.0',
            'created_at': str(os.popen('date /t').read().strip()) if sys.platform == 'win32' else '',
            'test_cases': []
        }
        
        # 1. Top 20 오분류 후보
        print("\n[1/4] Top 20 오분류 후보 수집 중...", flush=True)
        misclassification_candidates = [
            '000210', '000230', '000480', '000650', '000680', '0008Z0',
            '001080', '001140', '001250', '001540', '001620', '001770',
            '001810', '002350', '002360', '002620', '002870', '002900',
            '003030', '003280'
        ]
        
        for ticker in misclassification_candidates:
            stock = db.query(Stock).filter(Stock.ticker == ticker).first()
            sector = db.query(InvestorSector).filter(
                InvestorSector.ticker == ticker,
                InvestorSector.is_primary == True
            ).first()
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == ticker
            ).order_by(CompanyDetail.updated_at.desc()).first()
            
            if stock and sector:
                test_bundle['test_cases'].append({
                    'ticker': ticker,
                    'name': stock.stock_name,
                    'category': 'MISCLASSIFICATION',
                    'current_sector': sector.major_sector,
                    'current_sub_sector': sector.sub_sector,
                    'expected_sector': None,  # 수동 검토 후 입력
                    'revenue_by_segment': detail.revenue_by_segment if detail else None,
                    'notes': '오분류 후보 - 수동 검토 필요'
                })
        
        print(f"  ✅ {len(misclassification_candidates)}개 수집 완료", flush=True)
        
        # 2. 섹터별 대표기업 (각 섹터 Top 5)
        print("\n[2/4] 섹터별 대표기업 수집 중...", flush=True)
        major_sectors = [
            'SEC_SEMI', 'SEC_AUTO', 'SEC_BIO', 'SEC_BATTERY', 'SEC_CHEM',
            'SEC_STEEL', 'SEC_CONST', 'SEC_BANK', 'SEC_INS', 'SEC_CARD',
            'SEC_ELECTRONICS', 'SEC_IT', 'SEC_RETAIL', 'SEC_FOOD',
            'SEC_COSMETIC', 'SEC_FASHION', 'SEC_ENT', 'SEC_TELECOM',
            'SEC_SHIP', 'SEC_MACH', 'SEC_UTIL', 'SEC_HOLDING'
        ]
        
        for sector in major_sectors:
            sectors = db.query(InvestorSector, Stock).join(
                Stock, InvestorSector.ticker == Stock.ticker
            ).filter(
                InvestorSector.major_sector == sector,
                InvestorSector.is_primary == True
            ).order_by(Stock.market_cap.desc()).limit(5).all()
            
            for inv_sector, stock in sectors:
                detail = db.query(CompanyDetail).filter(
                    CompanyDetail.ticker == stock.ticker
                ).order_by(CompanyDetail.updated_at.desc()).first()
                
                test_bundle['test_cases'].append({
                    'ticker': stock.ticker,
                    'name': stock.stock_name,
                    'category': 'SECTOR_REPRESENTATIVE',
                    'current_sector': inv_sector.major_sector,
                    'current_sub_sector': inv_sector.sub_sector,
                    'expected_sector': inv_sector.major_sector,  # 현재 분류가 정답
                    'market_cap': stock.market_cap,
                    'revenue_by_segment': detail.revenue_by_segment if detail else None,
                    'notes': f'{sector} 섹터 대표기업'
                })
        
        print(f"  ✅ 섹터별 대표기업 수집 완료", flush=True)
        
        # 3. 지배구조 특이군 (홀딩스/스팩/리츠)
        print("\n[3/4] 지배구조 특이군 수집 중...", flush=True)
        
        # 홀딩스
        holdings = db.query(Stock).filter(
            Stock.stock_name.like('%홀딩스%') | 
            Stock.stock_name.like('%지주%') |
            Stock.stock_name.like('%Holdings%')
        ).limit(30).all()
        
        for stock in holdings:
            sector = db.query(InvestorSector).filter(
                InvestorSector.ticker == stock.ticker,
                InvestorSector.is_primary == True
            ).first()
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == stock.ticker
            ).order_by(CompanyDetail.updated_at.desc()).first()
            
            if sector:
                test_bundle['test_cases'].append({
                    'ticker': stock.ticker,
                    'name': stock.stock_name,
                    'category': 'HOLDING_COMPANY',
                    'current_sector': sector.major_sector,
                    'current_sub_sector': sector.sub_sector,
                    'expected_sector': None,  # 수동 검토 후 입력
                    'revenue_by_segment': detail.revenue_by_segment if detail else None,
                    'notes': '지주회사 - entity_type 분리 필요'
                })
        
        # SPAC
        spacs = db.query(Stock).filter(
            Stock.stock_name.like('%SPAC%') | 
            Stock.stock_name.like('%스팩%')
        ).limit(10).all()
        
        for stock in spacs:
            sector = db.query(InvestorSector).filter(
                InvestorSector.ticker == stock.ticker,
                InvestorSector.is_primary == True
            ).first()
            
            if sector:
                test_bundle['test_cases'].append({
                    'ticker': stock.ticker,
                    'name': stock.stock_name,
                    'category': 'SPAC',
                    'current_sector': sector.major_sector,
                    'current_sub_sector': sector.sub_sector,
                    'expected_sector': 'SEC_HOLDING',  # SPAC은 지주회사로 분류
                    'notes': 'SPAC - 특수 구조'
                })
        
        # REIT
        reits = db.query(Stock).filter(
            Stock.stock_name.like('%REIT%') | 
            Stock.stock_name.like('%리츠%')
        ).limit(10).all()
        
        for stock in reits:
            sector = db.query(InvestorSector).filter(
                InvestorSector.ticker == stock.ticker,
                InvestorSector.is_primary == True
            ).first()
            
            if sector:
                test_bundle['test_cases'].append({
                    'ticker': stock.ticker,
                    'name': stock.stock_name,
                    'category': 'REIT',
                    'current_sector': sector.major_sector,
                    'current_sub_sector': sector.sub_sector,
                    'expected_sector': 'SEC_HOLDING',  # REIT는 지주회사로 분류
                    'notes': 'REIT - 특수 구조'
                })
        
        print(f"  ✅ 지배구조 특이군 수집 완료", flush=True)
        
        # 4. 중복 제거 및 통계
        print("\n[4/4] 중복 제거 및 통계 생성 중...", flush=True)
        
        # ticker 기준 중복 제거 (최신 것만 유지)
        seen = {}
        unique_cases = []
        for case in test_bundle['test_cases']:
            ticker = case['ticker']
            if ticker not in seen:
                seen[ticker] = case
                unique_cases.append(case)
        
        test_bundle['test_cases'] = unique_cases
        
        # 통계
        stats = {
            'total_cases': len(unique_cases),
            'by_category': {},
            'by_sector': {}
        }
        
        for case in unique_cases:
            cat = case['category']
            stats['by_category'][cat] = stats['by_category'].get(cat, 0) + 1
            
            sector = case['current_sector']
            stats['by_sector'][sector] = stats['by_sector'].get(sector, 0) + 1
        
        test_bundle['statistics'] = stats
        
        # 저장
        os.makedirs('reports', exist_ok=True)
        output_file = 'reports/regression_test_bundle.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(test_bundle, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n✅ 회귀 테스트 번들 생성 완료", flush=True)
        print(f"  파일: {output_file}", flush=True)
        print(f"  총 테스트 케이스: {stats['total_cases']}개", flush=True)
        print(f"\n카테고리별 통계:", flush=True)
        for cat, count in stats['by_category'].items():
            print(f"  {cat}: {count}개", flush=True)
        print("=" * 80, flush=True)
        
    except Exception as e:
        import traceback
        print(f"\n❌ 오류 발생: {e}", flush=True)
        traceback.print_exc()
        raise
    finally:
        db.close()

if __name__ == '__main__':
    generate_regression_test_bundle()

