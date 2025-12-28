# -*- coding: utf-8 -*-
"""
오분류 후보 자동 수정 스크립트
Revenue Score >= 0.8인 기업들의 섹터를 매출 기반 섹터로 업데이트
"""
import sys
import os

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
from app.models.investor_sector import InvestorSector
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.services.sector_classifier import calculate_revenue_sector_scores

# 자동 수정 대상 (Revenue Score >= 0.8)
AUTO_FIX_CANDIDATES = [
    {'ticker': '000230', 'name': '일동홀딩스', 'current': 'SEC_HOLDING', 'target': 'SEC_BIO'},
    {'ticker': '000480', 'name': 'CR홀딩스', 'current': 'SEC_HOLDING', 'target': 'SEC_MACH'},
    {'ticker': '002620', 'name': '제일파마홀딩스', 'current': 'SEC_HOLDING', 'target': 'SEC_BIO'},
    {'ticker': '000210', 'name': 'DL', 'current': 'SEC_CONST', 'target': 'SEC_MACH'},
    {'ticker': '001080', 'name': '만호제강', 'current': 'SEC_ELECTRONICS', 'target': 'SEC_STEEL'},
    {'ticker': '002350', 'name': '넥센타이어', 'current': 'SEC_TIRE', 'target': 'SEC_AUTO'},
    {'ticker': '002360', 'name': 'SH에너지화학', 'current': 'SEC_CHEM', 'target': 'SEC_MACH'},
    {'ticker': '003030', 'name': '세아제강지주', 'current': 'SEC_HOLDING', 'target': 'SEC_STEEL'},
    {'ticker': '002900', 'name': 'TYM', 'current': 'SEC_TELECOM', 'target': 'SEC_MACH'},
    {'ticker': '003280', 'name': '흥아해운', 'current': 'SEC_MACH', 'target': 'SEC_SHIP'},
    {'ticker': '0008Z0', 'name': '에스엔시스', 'current': 'SEC_MACH', 'target': 'SEC_IT'},
    {'ticker': '001140', 'name': '국보', 'current': 'SEC_MACH', 'target': 'SEC_RETAIL'},
    {'ticker': '001250', 'name': 'GS글로벌', 'current': 'SEC_MACH', 'target': 'SEC_RETAIL'},
    {'ticker': '001540', 'name': '안국약품', 'current': 'SEC_RETAIL', 'target': 'SEC_BIO'},
    {'ticker': '001620', 'name': '케이비아이동국실업', 'current': 'SEC_AUTO', 'target': 'SEC_MACH'},
    {'ticker': '001770', 'name': 'SHD', 'current': 'SEC_STEEL', 'target': 'SEC_MACH'},
    {'ticker': '001810', 'name': '무림SP', 'current': 'SEC_TIRE', 'target': 'SEC_MACH'},
    {'ticker': '000650', 'name': '천일고속', 'current': 'SEC_CARD', 'target': 'SEC_RETAIL'},
    {'ticker': '000680', 'name': 'LS네트웍스', 'current': 'SEC_AUTO', 'target': 'SEC_FINANCE'},
    {'ticker': '002870', 'name': '신풍', 'current': 'SEC_AUTO', 'target': 'SEC_RETAIL'},
]

def fix_misclassifications(dry_run=True):
    """오분류 후보 수정"""
    db = SessionLocal()
    
    try:
        print("=" * 80, flush=True)
        print("오분류 후보 자동 수정", flush=True)
        print("=" * 80, flush=True)
        
        if dry_run:
            print("\n⚠️  DRY RUN 모드 (실제 수정하지 않음)", flush=True)
        else:
            print("\n✅ 실제 수정 모드", flush=True)
        
        print(f"\n수정 대상: {len(AUTO_FIX_CANDIDATES)}개", flush=True)
        print("\n" + "=" * 80, flush=True)
        print("수정 계획", flush=True)
        print("=" * 80, flush=True)
        
        success_count = 0
        error_count = 0
        
        for candidate in AUTO_FIX_CANDIDATES:
            ticker = candidate['ticker']
            name = candidate['name']
            current_sector = candidate['current']
            target_sector = candidate['target']
            
            # 현재 섹터 조회
            sector = db.query(InvestorSector).filter(
                InvestorSector.ticker == ticker,
                InvestorSector.is_primary == True
            ).first()
            
            if not sector:
                print(f"❌ {name} ({ticker}): 섹터 정보 없음", flush=True)
                error_count += 1
                continue
            
            if sector.major_sector != current_sector:
                print(f"⚠️  {name} ({ticker}): 현재 섹터가 {sector.major_sector}로 이미 변경됨", flush=True)
                continue
            
            print(f"\n{name} ({ticker})", flush=True)
            print(f"  현재: {current_sector} → 목표: {target_sector}", flush=True)
            print(f"  Sub-sector: {sector.sub_sector}", flush=True)
            print(f"  Value-chain: {sector.value_chain}", flush=True)
            
            if not dry_run:
                # 섹터 업데이트
                sector.major_sector = target_sector
                # Sub-sector와 Value-chain은 유지하거나 기본값으로 설정
                # 필요시 추가 로직 구현
                
                try:
                    db.commit()
                    print(f"  ✅ 수정 완료", flush=True)
                    success_count += 1
                except Exception as e:
                    db.rollback()
                    print(f"  ❌ 수정 실패: {e}", flush=True)
                    error_count += 1
            else:
                print(f"  [DRY RUN] 수정 예정", flush=True)
                success_count += 1
        
        print("\n" + "=" * 80, flush=True)
        print("수정 결과", flush=True)
        print("=" * 80, flush=True)
        print(f"성공: {success_count}개", flush=True)
        print(f"실패: {error_count}개", flush=True)
        
        if dry_run:
            print("\n⚠️  실제 수정을 원하시면 --apply 플래그를 사용하세요:", flush=True)
            print("  python scripts/fix_misclassifications.py --apply", flush=True)
        
    except Exception as e:
        import traceback
        print(f"\n❌ 오류 발생: {e}", flush=True)
        traceback.print_exc()
        db.rollback()
        raise
    finally:
        db.close()

def main():
    import sys
    dry_run = '--apply' not in sys.argv
    
    fix_misclassifications(dry_run=dry_run)

if __name__ == '__main__':
    main()

