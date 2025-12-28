"""
SK이노베이션 entity_type 검증 스크립트

필수-1 검증: entity_type_classifier 결과가 classification_meta.entity_type에 반영되는지 확인
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db import SessionLocal
from app.models.stock import Stock
from app.models.company_detail import CompanyDetail
from app.services.sector_classifier import classify_sector_rule_based

def test_sk_innovation():
    """SK이노베이션 entity_type 검증"""
    print("="*80)
    print("SK이노베이션 Entity Type 검증")
    print("="*80)
    
    db = SessionLocal()
    
    ticker = '096770'
    stock = db.query(Stock).filter(Stock.ticker == ticker).first()
    if not stock:
        print(f"⚠️  {ticker}: Stock not found")
        db.close()
        return
    
    company_detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == ticker).first()
    if not company_detail:
        print(f"⚠️  {ticker}: CompanyDetail not found")
        db.close()
        return
    
    major, sub, vc, conf, boosting_log = classify_sector_rule_based(company_detail, stock.stock_name, ticker=ticker)
    meta = boosting_log.get('classification_meta', {}) if boosting_log else {}
    
    print(f"\n{ticker} ({stock.stock_name}):")
    print(f"  - Major Sector: {major}")
    print(f"  - Sub Sector: {sub}")
    print(f"  - Value Chain: {vc}")
    print(f"  - Confidence: {conf}")
    print(f"  - Entity Type: {meta.get('entity_type', 'UNKNOWN')}")
    print(f"  - Entity Type Confidence: {meta.get('entity_type_confidence', 'N/A')}")
    print(f"  - Entity Type Evidence: {meta.get('entity_type_evidence', {})}")
    print(f"  - Override Hit: {meta.get('override_hit', False)}")  # R1: override_hit 확인
    print(f"  - Revenue Quality: {meta.get('revenue_quality', 'UNKNOWN')}")
    print(f"  - Quality Reason: {meta.get('quality_reason', 'UNKNOWN')}")
    print(f"  - Primary Source: {meta.get('primary_sector_source', 'UNKNOWN')}")
    print(f"  - Sector Candidates: {meta.get('sector_candidates', [])}")
    print(f"  - Sector Evidence: {meta.get('sector_evidence', {})}")
    
    # 🆕 P0 추가 확인: Primary Sector 확인
    print(f"\n  [Primary Sector 확인]")
    if major:
        print(f"    - Major Sector: {major} (정상 분류됨)")
        if major in ('SEC_ENERGY', 'SEC_CHEM', 'SEC_OIL'):
            print(f"    - [OK] 에너지/화학 섹터로 정상 분류됨")
        elif major == 'HOLD' or major is None:
            print(f"    - [WARN] Primary Sector가 HOLD/None (섹터 분류 필요)")
    else:
        print(f"    - [FAIL] Primary Sector가 None (섹터 분류 실패)")
    
    # P0 개선: evidence 상세 출력
    entity_type_evidence = meta.get('entity_type_evidence', {})
    if isinstance(entity_type_evidence, dict):
        print(f"\n  [Entity Type Evidence 상세]")
        print(f"    - Signals: {entity_type_evidence.get('signals', [])}")
        print(f"    - Holding Confidence: {entity_type_evidence.get('holding_confidence', 'N/A')}")
        print(f"    - Holding Type: {entity_type_evidence.get('holding_type', 'N/A')}")
    
    sector_evidence = meta.get('sector_evidence', {})
    if isinstance(sector_evidence, dict):
        print(f"\n  [Sector Evidence 상세]")
        print(f"    - Revenue Quality: {sector_evidence.get('revenue_quality', 'N/A')}")
        print(f"    - Has Revenue Data: {sector_evidence.get('has_revenue_data', 'N/A')}")
        print(f"    - Segments Count: {sector_evidence.get('segments_count', 'N/A')}")
    
    # biz_summary 확인 (자회사 패턴 확인용)
    if company_detail and company_detail.biz_summary:
        import re
        biz_summary = str(company_detail.biz_summary)
        subsidiary_patterns = [
            r'[가-힣A-Za-z0-9\s]+㈜',
            r'\[[가-힣A-Za-z0-9\s]+㈜\]',
            r'\(주\)[가-힣A-Za-z0-9\s]+',
        ]
        subsidiary_matches = set()
        for pattern in subsidiary_patterns:
            matches = re.findall(pattern, biz_summary)
            subsidiary_matches.update(matches)
        subsidiary_count = len(subsidiary_matches)
        
        control_keywords = [
            '지배', '종속', '자회사', '계열사', '연결대상', '지분율', 
            '최대주주', '계열회사', '연결조정', '종속회사', '관리', '경영'
        ]
        control_keyword_hits = sum(1 for kw in control_keywords if kw in biz_summary)
        
        print(f"\n  [biz_summary 분석]")
        print(f"    - 자회사 패턴 발견: {subsidiary_count}개")
        print(f"    - 지배구조 키워드 발견: {control_keyword_hits}개")
        print(f"    - biz_summary 길이: {len(biz_summary)}자")
    
    # 검증
    entity_type = meta.get('entity_type', '')
    if entity_type in ('BIZ_HOLDCO', 'HOLDCO', 'HOLDING_BUSINESS'):
        print(f"\n  [PASS] Entity Type이 지주회사로 정확히 분류됨 ({entity_type})")
    elif entity_type == 'REGULAR':
        print(f"\n  [FAIL] Entity Type이 REGULAR로 분류됨 (지주회사여야 함)")
    else:
        print(f"\n  [WARN] Entity Type이 예상과 다름 ({entity_type})")
    
    quality_reason = meta.get('quality_reason', '')
    if quality_reason:
        print(f"\n  [OK] Quality Reason 표준 코드: {quality_reason}")
    else:
        print(f"\n  [WARN] Quality Reason이 없음")
    
    # R1: override_hit 확인
    override_hit = meta.get('override_hit', False)
    entity_type_evidence = meta.get('entity_type_evidence', {})
    if isinstance(entity_type_evidence, dict):
        evidence_override_hit = entity_type_evidence.get('override_hit', False)
    else:
        evidence_override_hit = False
    
    if override_hit or evidence_override_hit:
        print(f"\n  [OK] Override Hit: {override_hit or evidence_override_hit} (하드코딩 적용됨)")
    else:
        print(f"\n  [WARN] Override Hit이 False (로직으로 분류됨 또는 DB 재분류 필요)")
    
    # biz_summary 내용 일부 출력
    if company_detail and company_detail.biz_summary:
        biz_summary = str(company_detail.biz_summary)
        print(f"\n  [biz_summary 내용 (처음 500자)]")
        print(f"    {biz_summary[:500]}")
    
    db.close()

if __name__ == '__main__':
    test_sk_innovation()

