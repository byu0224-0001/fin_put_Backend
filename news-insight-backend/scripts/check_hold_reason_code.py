# -*- coding: utf-8 -*-
"""
hold_reason_code 누락 체크 스크립트
HOLD 경로에서 hold_reason_code가 classification_meta에 저장되었는지 확인
"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.investor_sector import InvestorSector
from sqlalchemy import or_

def check_hold_reason_code():
    """hold_reason_code 누락 체크"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("hold_reason_code 누락 체크")
        print("=" * 80)
        
        # confidence가 HOLD이거나 HOLD:로 시작하는 레코드 조회
        hold_sectors = db.query(InvestorSector).filter(
            or_(
                InvestorSector.confidence == 'HOLD',
                InvestorSector.confidence.like('HOLD:%')
            )
        ).all()
        
        print(f"\n[전체 HOLD 레코드]")
        print(f"  총 {len(hold_sectors)}개")
        
        missing_reason = []
        has_reason = []
        
        for sector in hold_sectors:
            boosting_log = sector.boosting_log or {}
            classification_meta = boosting_log.get('classification_meta', {})
            hold_reason_code = classification_meta.get('hold_reason_code')
            hold_reason = classification_meta.get('hold_reason')  # 하위 호환성
            
            if not hold_reason_code and not hold_reason:
                missing_reason.append(sector)
            else:
                has_reason.append(sector)
        
        print(f"\n[결과]")
        print(f"  hold_reason_code 있음: {len(has_reason)}개")
        print(f"  hold_reason_code 없음: {len(missing_reason)}개")
        
        if len(missing_reason) > 0:
            print(f"\n  [WARN] hold_reason_code 누락 레코드 (최대 20개):")
            for idx, sector in enumerate(missing_reason[:20], 1):
                boosting_log = sector.boosting_log or {}
                classification_meta = boosting_log.get('classification_meta', {})
                print(f"    {idx}. {sector.ticker}: confidence={sector.confidence}, primary_sector_source={classification_meta.get('primary_sector_source', 'N/A')}")
            
            if len(missing_reason) > 20:
                print(f"    ... 외 {len(missing_reason) - 20}개")
            
            return False
        else:
            print(f"  [OK] 모든 HOLD 레코드에 hold_reason_code가 저장되어 있습니다.")
            return True
            
    finally:
        db.close()

if __name__ == '__main__':
    success = check_hold_reason_code()
    sys.exit(0 if success else 1)

