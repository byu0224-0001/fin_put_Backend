"""
SK이노베이션 override OFF 테스트

로직만으로도 BIZ_HOLDCO가 나오는지 확인
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db import SessionLocal
from app.models.stock import Stock
from app.models.company_detail import CompanyDetail
from app.services.sector_classifier import classify_sector_rule_based

def test_sk_innovation_override_off():
    """SK이노베이션 override OFF 테스트"""
    print("="*80)
    print("SK이노베이션 override OFF 테스트")
    print("="*80)
    print("목적: 로직만으로도 BIZ_HOLDCO가 나오는지 확인")
    print("="*80)
    
    db = SessionLocal()
    ticker = '096770'
    stock = db.query(Stock).filter(Stock.ticker == ticker).first()
    company_detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == ticker).first()
    
    if not stock or not company_detail:
        print(f"[FAIL] {ticker}: 데이터 없음")
        db.close()
        return False
    
    # 현재 상태 확인 (override ON)
    print(f"\n[현재 상태 (override ON)]")
    major, sub, vc, conf, boosting_log = classify_sector_rule_based(company_detail, stock.stock_name, ticker=ticker)
    meta = boosting_log.get('classification_meta', {}) if boosting_log else {}
    print(f"  - Entity Type: {meta.get('entity_type', 'UNKNOWN')}")
    print(f"  - Override Hit: {meta.get('override_hit', False)}")
    print(f"  - Override Reason: {meta.get('override_reason', 'N/A')}")
    
    # override OFF 테스트는 수동으로 entity_type_classifier.py에서 주석 처리 후 재실행 필요
    print(f"\n[override OFF 테스트 방법]")
    print(f"  1. entity_type_classifier.py에서 SK이노베이션 특별 처리 주석 처리")
    print(f"  2. 이 스크립트 재실행")
    print(f"  3. Entity Type이 여전히 BIZ_HOLDCO인지 확인")
    print(f"     - BIZ_HOLDCO면: 로직으로도 잡힘 → override 제거 가능")
    print(f"     - REGULAR면: override 필요 → override 유지")
    
    db.close()
    return True

if __name__ == '__main__':
    test_sk_innovation_override_off()

