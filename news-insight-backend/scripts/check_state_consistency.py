# -*- coding: utf-8 -*-
"""
상태 일관성 체크 스크립트 (확장 버전)
1. major_sector IS NOT NULL인데 confidence LIKE 'HOLD:%'인 레코드
2. override_hit=True인데 override_reason이 비어있는 레코드
3. confidence=HIGH인데 primary_sector_source가 HOLD로 남아있는 레코드
"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.investor_sector import InvestorSector
from sqlalchemy import and_, or_

def check_state_consistency():
    """상태 불일치 레코드 확인 (확장 버전)"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("상태 일관성 체크 (확장 버전)")
        print("=" * 80)
        
        all_issues = []
        
        # 1. major_sector IS NOT NULL인데 confidence LIKE 'HOLD:%'
        print("\n[체크 1] major_sector IS NOT NULL & confidence LIKE 'HOLD:%'")
        inconsistent1 = db.query(InvestorSector).filter(
            and_(
                InvestorSector.major_sector.isnot(None),
                InvestorSector.confidence.like('HOLD:%')
            )
        ).all()
        
        print(f"  발견: {len(inconsistent1)}개")
        if len(inconsistent1) > 0:
            all_issues.extend([('major_sector_with_HOLD', s) for s in inconsistent1])
            for idx, sector in enumerate(inconsistent1[:5], 1):
                boosting_log = sector.boosting_log or {}
                classification_meta = boosting_log.get('classification_meta', {})
                print(f"    {idx}. {sector.ticker}: {sector.major_sector} / {sector.confidence}")
        
        # 2. override_hit=True인데 override_reason이 비어있는 레코드
        print("\n[체크 2] override_hit=True인데 override_reason이 비어있는 레코드")
        # JSONB 쿼리는 복잡하므로 Python에서 필터링
        all_sectors = db.query(InvestorSector).filter(
            InvestorSector.boosting_log.isnot(None)
        ).all()
        
        inconsistent2 = []
        for sector in all_sectors:
            boosting_log = sector.boosting_log or {}
            classification_meta = boosting_log.get('classification_meta', {})
            override_hit = classification_meta.get('override_hit', False)
            override_reason = classification_meta.get('override_reason', '')
            
            if override_hit and not override_reason:
                inconsistent2.append(sector)
        
        print(f"  발견: {len(inconsistent2)}개")
        if len(inconsistent2) > 0:
            all_issues.extend([('override_hit_no_reason', s) for s in inconsistent2])
            for idx, sector in enumerate(inconsistent2[:5], 1):
                print(f"    {idx}. {sector.ticker}: override_hit=True, override_reason=''")
        
        # 3. confidence=HIGH인데 primary_sector_source가 HOLD로 남아있는 레코드
        print("\n[체크 3] confidence=HIGH인데 primary_sector_source가 HOLD로 남아있는 레코드")
        inconsistent3 = []
        for sector in all_sectors:
            if sector.confidence == 'HIGH':
                boosting_log = sector.boosting_log or {}
                classification_meta = boosting_log.get('classification_meta', {})
                primary_sector_source = classification_meta.get('primary_sector_source', '')
                
                if primary_sector_source == 'HOLD':
                    inconsistent3.append(sector)
        
        print(f"  발견: {len(inconsistent3)}개")
        if len(inconsistent3) > 0:
            all_issues.extend([('HIGH_with_HOLD_source', s) for s in inconsistent3])
            for idx, sector in enumerate(inconsistent3[:5], 1):
                boosting_log = sector.boosting_log or {}
                classification_meta = boosting_log.get('classification_meta', {})
                print(f"    {idx}. {sector.ticker}: confidence=HIGH, source={classification_meta.get('primary_sector_source', 'N/A')}")
        
        # 4. major_sector IS NULL인데 confidence=HIGH인 레코드 (역모순)
        print("\n[체크 4] major_sector IS NULL인데 confidence=HIGH인 레코드 (역모순)")
        inconsistent4 = []
        for sector in all_sectors:
            if sector.major_sector is None and sector.confidence == 'HIGH':
                inconsistent4.append(sector)
        
        print(f"  발견: {len(inconsistent4)}개")
        if len(inconsistent4) > 0:
            all_issues.extend([('NULL_sector_with_HIGH', s) for s in inconsistent4])
            for idx, sector in enumerate(inconsistent4[:5], 1):
                boosting_log = sector.boosting_log or {}
                classification_meta = boosting_log.get('classification_meta', {})
                override_hit = classification_meta.get('override_hit', False)
                print(f"    {idx}. {sector.ticker}: major_sector=NULL, confidence=HIGH, override_hit={override_hit}")
        
        # 종합 결과
        print("\n" + "=" * 80)
        print("[종합 결과]")
        print("=" * 80)
        
        total_issues = len(all_issues)
        if total_issues == 0:
            print(f"  [OK] 상태 일관성 정상 (불일치 레코드 없음)")
            return True
        else:
            print(f"  [WARN] 총 {total_issues}개의 불일치 레코드 발견")
            print(f"  -> 상세 리스트는 위를 참고하세요.")
            return False
            
    finally:
        db.close()

if __name__ == '__main__':
    success = check_state_consistency()
    sys.exit(0 if success else 1)

