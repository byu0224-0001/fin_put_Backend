"""
P0.5-4: SEC_* taxonomy 레벨 일관성 확인

industry_edges의 target_sector_code와 company의 sector 매핑 레벨이
같은 레벨끼리 매칭되는지 확인
"""
import sys
import os
from pathlib import Path

# Windows 인코딩 처리
if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from app.db import SessionLocal
from app.models.industry_edge import IndustryEdge
from app.models.investor_sector import InvestorSector


def check_sector_level_consistency():
    """
    섹터 레벨 일관성 확인
    
    industry_edges의 target_sector_code가 L1인지 확인하고,
    company의 sector_l1과 매칭 가능한지 확인
    """
    print("=" * 80)
    print("[P0.5-4: SEC_* Taxonomy 레벨 일관성 확인]")
    print("=" * 80)
    
    db = SessionLocal()
    
    try:
        # 1. industry_edges의 target_sector_code 분포 확인
        print("\n[1. Industry Edges 섹터 코드 분석]")
        print("-" * 80)
        
        all_edges = db.query(IndustryEdge).filter(
            IndustryEdge.is_active == "TRUE"
        ).all()
        
        if not all_edges:
            print("  ⚠️  DB에 저장된 industry_edges가 없습니다.")
            return
        
        sector_codes = {}
        for edge in all_edges:
            sector = edge.target_sector_code
            if sector:
                if sector not in sector_codes:
                    sector_codes[sector] = {
                        "count": 0,
                        "samples": []
                    }
                sector_codes[sector]["count"] += 1
                if len(sector_codes[sector]["samples"]) < 3:
                    sector_codes[sector]["samples"].append({
                        "id": edge.id,
                        "driver": edge.source_driver_code,
                        "logic": edge.logic_summary[:50] if edge.logic_summary else None
                    })
        
        print(f"  총 Industry Edges: {len(all_edges)}개")
        print(f"  고유 섹터 코드: {len(sector_codes)}개\n")
        
        for sector, info in sorted(sector_codes.items()):
            print(f"  - {sector}: {info['count']}개")
        
        # 2. 섹터 코드 레벨 추정 (L1/L2/L3)
        print("\n[2. 섹터 코드 레벨 분석]")
        print("-" * 80)
        
        # L1 패턴: SEC_로 시작하고 _가 하나만 있거나 없음 (예: SEC_SEMI, SEC_IT)
        # L2 패턴: SEC_로 시작하고 _가 2개 이상 (예: SEC_IT_HARDWARE)
        # L3 패턴: SEC_로 시작하고 _가 3개 이상 (예: SEC_IT_HARDWARE_CAMERA)
        
        l1_sectors = []
        l2_sectors = []
        l3_sectors = []
        unknown_sectors = []
        
        for sector in sector_codes.keys():
            if not sector or not sector.startswith("SEC_"):
                unknown_sectors.append(sector)
                continue
            
            parts = sector.split("_")
            if len(parts) == 2:  # SEC_SEMI
                l1_sectors.append(sector)
            elif len(parts) == 3:  # SEC_IT_HARDWARE
                l2_sectors.append(sector)
            elif len(parts) >= 4:  # SEC_IT_HARDWARE_CAMERA
                l3_sectors.append(sector)
            else:
                unknown_sectors.append(sector)
        
        print(f"  L1 섹터 (예: SEC_SEMI): {len(l1_sectors)}개")
        if l1_sectors:
            print(f"    예시: {', '.join(l1_sectors[:5])}")
        
        print(f"  L2 섹터 (예: SEC_IT_HARDWARE): {len(l2_sectors)}개")
        if l2_sectors:
            print(f"    예시: {', '.join(l2_sectors[:5])}")
        
        print(f"  L3 섹터 (예: SEC_IT_HARDWARE_CAMERA): {len(l3_sectors)}개")
        if l3_sectors:
            print(f"    예시: {', '.join(l3_sectors[:5])}")
        
        if unknown_sectors:
            print(f"  알 수 없는 형식: {len(unknown_sectors)}개")
            print(f"    예시: {', '.join(unknown_sectors[:5])}")
        
        # 3. Company 섹터 매핑 확인
        print("\n[3. Company 섹터 매핑 확인]")
        print("-" * 80)
        
        company_sectors = db.query(InvestorSector).filter(
            InvestorSector.sector_l1.isnot(None)
        ).limit(100).all()
        
        if not company_sectors:
            print("  ⚠️  DB에 저장된 company sector 매핑이 없습니다.")
        else:
            company_l1_sectors = set()
            company_l2_sectors = set()
            
            for cs in company_sectors:
                if cs.sector_l1:
                    company_l1_sectors.add(cs.sector_l1)
                if cs.sector_l2:
                    company_l2_sectors.add(cs.sector_l2)
            
            print(f"  Company L1 섹터: {len(company_l1_sectors)}개")
            print(f"  Company L2 섹터: {len(company_l2_sectors)}개")
            
            # 4. 매칭 가능성 확인
            print("\n[4. Industry-Company 매칭 가능성]")
            print("-" * 80)
            
            # L1 매칭
            industry_l1_set = set(l1_sectors)
            matchable_l1 = industry_l1_set.intersection(company_l1_sectors)
            unmatchable_l1 = industry_l1_set - company_l1_sectors
            
            print(f"  L1 레벨 매칭:")
            print(f"    - 매칭 가능: {len(matchable_l1)}개 ({', '.join(list(matchable_l1)[:5])})")
            if unmatchable_l1:
                print(f"    - 매칭 불가: {len(unmatchable_l1)}개 ({', '.join(list(unmatchable_l1)[:5])})")
            
            # L2 매칭
            if l2_sectors:
                industry_l2_set = set(l2_sectors)
                matchable_l2 = industry_l2_set.intersection(company_l2_sectors)
                unmatchable_l2 = industry_l2_set - company_l2_sectors
                
                print(f"  L2 레벨 매칭:")
                print(f"    - 매칭 가능: {len(matchable_l2)}개")
                if unmatchable_l2:
                    print(f"    - 매칭 불가: {len(unmatchable_l2)}개 ({', '.join(list(unmatchable_l2)[:5])})")
            
            # 5. 레벨 혼재 경고
            print("\n[5. 레벨 혼재 경고]")
            print("-" * 80)
            
            if l2_sectors or l3_sectors:
                print("  ⚠️  [WARNING] L2/L3 섹터 코드가 발견되었습니다.")
                print("     - 현재 정책: L1만 확정, L2/L3는 HOLD")
                print("     - 권장: industry_edges에 sector_code_level 컬럼 추가하여 레벨 명시")
                print("     - 또는: L2/L3는 L1로 변환하여 저장")
            else:
                print("  ✅ [OK] 모든 섹터 코드가 L1 레벨 (일관성 유지)")
        
        print("\n" + "=" * 80)
        print("[결론]")
        print("=" * 80)
        
        if l2_sectors or l3_sectors:
            print("  ⚠️  [권장] sector_code_level 컬럼 추가 고려")
            print("     - industry_edges에 레벨 정보 저장")
            print("     - 조인 시 레벨 일치 확인")
        else:
            print("  ✅ [OK] 레벨 일관성 양호 (모두 L1)")
        
    except Exception as e:
        print(f"\n  ❌ [ERROR] 확인 실패: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


if __name__ == "__main__":
    check_sector_level_consistency()

