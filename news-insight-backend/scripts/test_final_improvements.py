"""
최종 개선사항 검증 스크립트

4가지 위험 포인트 검증:
1. holding + revenue_top_sector 설정의 품질 게이트
2. primary_sector_source 값의 정규화
3. top1_score, margin의 스케일 일관성
4. 5튜플 통일의 호출부 전체 검증
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db import SessionLocal
from app.models.stock import Stock
from app.models.company_detail import CompanyDetail
from app.services.sector_classifier import classify_sector_rule_based

def test_holding_quality_gate():
    """1. holding + revenue_top_sector 설정의 품질 게이트 검증"""
    print("\n" + "="*80)
    print("1. Holding + Revenue Quality Gate 검증")
    print("="*80)
    
    db = SessionLocal()
    
    # POSCO홀딩스 테스트
    tickers = ['005490', '105560', '096770']  # POSCO홀딩스, KB금융, SK이노베이션
    company_names = ['POSCO홀딩스', 'KB금융', 'SK이노베이션']
    
    for ticker, company_name in zip(tickers, company_names):
        stock = db.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            print(f"⚠️  {ticker} ({company_name}): Stock not found")
            continue
            
        company_detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == ticker).first()
        if not company_detail:
            print(f"⚠️  {ticker} ({company_name}): CompanyDetail not found")
            continue
        
        major, sub, vc, conf, boosting_log = classify_sector_rule_based(company_detail, stock.stock_name, ticker=ticker)
        meta = boosting_log.get('classification_meta', {}) if boosting_log else {}
        
        entity_type = meta.get('entity_type', 'UNKNOWN')
        revenue_quality = meta.get('revenue_quality', 'UNKNOWN')
        primary_source = meta.get('primary_sector_source', 'UNKNOWN')
        
        print(f"\n{ticker} ({company_name}):")
        print(f"  - Major Sector: {major}")
        print(f"  - Entity Type: {entity_type}")
        print(f"  - Revenue Quality: {revenue_quality}")
        print(f"  - Primary Source: {primary_source}")
        
        # 품질 게이트 검증
        if entity_type in ('FINANCIAL_HOLDING', 'BIZ_HOLDCO', 'HOLDCO'):
            if revenue_quality == 'BAD':
                if primary_source == 'REVENUE':
                    print(f"  ❌ FAIL: Holding인데 revenue_quality=BAD인데도 primary_source=REVENUE")
                else:
                    print(f"  ✅ PASS: Holding인데 revenue_quality=BAD이므로 primary_source={primary_source} (품질 게이트 작동)")
            elif revenue_quality in ('OK', 'WARN'):
                if primary_source == 'REVENUE':
                    print(f"  ✅ PASS: Holding인데 revenue_quality={revenue_quality}이므로 primary_source=REVENUE (정상)")
                else:
                    print(f"  ⚠️  WARN: Holding인데 revenue_quality={revenue_quality}인데 primary_source={primary_source}")
    
    db.close()


def test_primary_sector_source_normalization():
    """2. primary_sector_source 값의 정규화 검증"""
    print("\n" + "="*80)
    print("2. Primary Sector Source 정규화 검증")
    print("="*80)
    
    db = SessionLocal()
    
    # 다양한 케이스 테스트
    test_cases = [
        ('005490', 'POSCO홀딩스'),  # Holding
        ('105560', 'KB금융'),  # Financial Holding
        ('000270', '기아'),  # Regular (single segment)
        ('005930', '삼성전자'),  # Major override
    ]
    
    sources_found = set()
    
    for ticker, company_name in test_cases:
        stock = db.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            continue
            
        company_detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == ticker).first()
        if not company_detail:
            continue
        
        major, sub, vc, conf, boosting_log = classify_sector_rule_based(company_detail, stock.stock_name, ticker=ticker)
        meta = boosting_log.get('classification_meta', {}) if boosting_log else {}
        primary_source = meta.get('primary_sector_source', 'UNKNOWN')
        
        sources_found.add(primary_source)
        print(f"{ticker} ({company_name}): primary_sector_source = {primary_source}")
    
    # 정규화 검증
    valid_sources = {'REVENUE', 'KEYWORD', 'OVERRIDE', 'HOLD', 'LLM', 'UNKNOWN'}
    invalid_sources = sources_found - valid_sources
    
    if invalid_sources:
        print(f"\n❌ FAIL: Invalid primary_sector_source values found: {invalid_sources}")
        print(f"   Valid values: {valid_sources}")
    else:
        print(f"\n✅ PASS: All primary_sector_source values are normalized")
        print(f"   Found values: {sources_found}")
    
    db.close()


def test_score_scale_consistency():
    """3. top1_score, margin의 스케일 일관성 검증"""
    print("\n" + "="*80)
    print("3. Score Scale 일관성 검증")
    print("="*80)
    
    db = SessionLocal()
    
    tickers = ['005490', '105560', '096770', '000270']
    
    for ticker in tickers:
        stock = db.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            continue
            
        company_detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == ticker).first()
        if not company_detail:
            continue
        
        major, sub, vc, conf, boosting_log = classify_sector_rule_based(company_detail, stock.stock_name, ticker=ticker)
        meta = boosting_log.get('classification_meta', {}) if boosting_log else {}
        
        top1_score = meta.get('top1_score', 0.0)
        margin = meta.get('margin', 0.0)
        
        print(f"\n{ticker} ({stock.stock_name}):")
        print(f"  - top1_score: {top1_score} (type: {type(top1_score).__name__})")
        print(f"  - margin: {margin} (type: {type(margin).__name__})")
        
        # 스케일 검증 (0.0~1.0 범위)
        if isinstance(top1_score, (int, float)):
            if 0.0 <= top1_score <= 1.0:
                print(f"  ✅ top1_score is in normalized range (0.0~1.0)")
            elif 0.0 <= top1_score <= 100.0:
                print(f"  ❌ FAIL: top1_score is in percentage range (0.0~100.0), should be normalized")
            else:
                print(f"  ❌ FAIL: top1_score is out of expected range: {top1_score}")
        
        if isinstance(margin, (int, float)):
            if -1.0 <= margin <= 1.0:  # margin은 음수일 수 있음
                print(f"  ✅ margin is in normalized range (-1.0~1.0)")
            else:
                print(f"  ❌ FAIL: margin is out of expected range: {margin}")
    
    db.close()


def test_tuple_unpacking():
    """4. 5튜플 통일의 호출부 전체 검증"""
    print("\n" + "="*80)
    print("4. 5튜플 통일 검증")
    print("="*80)
    
    db = SessionLocal()
    
    # 다양한 케이스로 호출 테스트
    test_cases = [
        ('005490', 'POSCO홀딩스'),
        ('105560', 'KB금융'),
        ('096770', 'SK이노베이션'),
        ('000270', '기아'),
    ]
    
    all_passed = True
    
    for ticker, company_name in test_cases:
        stock = db.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            continue
            
        company_detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == ticker).first()
        if not company_detail:
            continue
        
        try:
            # 5튜플 언패킹 테스트
            major, sub, vc, conf, boosting_log = classify_sector_rule_based(company_detail, stock.stock_name, ticker=ticker)
            
            # 타입 검증
            assert boosting_log is None or isinstance(boosting_log, dict), f"boosting_log should be dict or None, got {type(boosting_log)}"
            
            print(f"✅ {ticker} ({company_name}): 5튜플 언패킹 성공")
            
        except ValueError as e:
            print(f"❌ {ticker} ({company_name}): 5튜플 언패킹 실패 - {e}")
            all_passed = False
        except Exception as e:
            print(f"❌ {ticker} ({company_name}): 예외 발생 - {e}")
            all_passed = False
    
    if all_passed:
        print("\n✅ PASS: All tuple unpacking tests passed")
    else:
        print("\n❌ FAIL: Some tuple unpacking tests failed")
    
    db.close()


if __name__ == '__main__':
    print("="*80)
    print("최종 개선사항 검증 스크립트")
    print("="*80)
    
    test_holding_quality_gate()
    test_primary_sector_source_normalization()
    test_score_scale_consistency()
    test_tuple_unpacking()
    
    print("\n" + "="*80)
    print("검증 완료")
    print("="*80)

