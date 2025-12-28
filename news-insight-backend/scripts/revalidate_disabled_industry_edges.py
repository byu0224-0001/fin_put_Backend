"""
Soft Delete 데이터 재활성화 스크립트

P0+: 룰 변경 후 이전에 disable된 데이터를 재검증 → 재활성화
"""
import sys
import os
from pathlib import Path
from datetime import datetime

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
from extractors.naver_report_router import check_sector_sanity


def revalidate_disabled_industry_edges(rule_version: str = "v2"):
    """
    Disable된 industry_edges 재검증 및 재활성화
    
    Args:
        rule_version: 룰 버전 (예: "v2", "v3")
    """
    print("=" * 80)
    print(f"[Soft Delete 데이터 재활성화] (룰 버전: {rule_version})")
    print("=" * 80)
    
    db = SessionLocal()
    
    try:
        # 1. Disable된 edges 조회
        disabled_edges = db.query(IndustryEdge).filter(
            IndustryEdge.is_active != "TRUE"
        ).all()
        
        print(f"\n[Disable된 Industry Edges] {len(disabled_edges)}개\n")
        
        if not disabled_edges:
            print("  ✅ Disable된 데이터 없음")
            return
        
        # 2. 재검증
        print("[재검증 진행]")
        print("-" * 80)
        
        reactivated_count = 0
        still_disabled_count = 0
        
        for edge in disabled_edges:
            # Sanity Check 재실행
            # 주의: 원본 리포트 데이터가 없으므로 logic_summary와 key_sentence로 재검증
            title = edge.key_sentence[:100] if edge.key_sentence else ""
            full_text = edge.logic_summary or ""
            
            is_valid, reason, rule_id = check_sector_sanity(
                title=title,
                full_text=full_text,
                target_sector_code=edge.target_sector_code,
                company_name=None
            )
            
            if is_valid:
                # 재활성화
                edge.is_active = "TRUE"
                edge.disabled_reason = None
                edge.disabled_at = None
                reactivated_count += 1
                print(f"  ✅ 재활성화: ID={edge.id}, Sector={edge.target_sector_code}")
            else:
                # 여전히 invalid
                still_disabled_count += 1
                print(f"  ⚠️  여전히 Disable: ID={edge.id}, Reason={reason}")
        
        # 3. 커밋
        if reactivated_count > 0:
            db.commit()
            print(f"\n[재활성화 완료] {reactivated_count}개")
        
        print(f"\n[최종 결과]")
        print(f"  - 재활성화: {reactivated_count}개")
        print(f"  - 여전히 Disable: {still_disabled_count}개")
        
        # 4. 재활성화된 edges의 disabled_reason 기록
        if reactivated_count > 0:
            print(f"\n[재활성화 사유]")
            print(f"  - 룰 버전 업데이트: {rule_version}")
            print(f"  - 재검증 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        db.rollback()
        print(f"\n  ❌ [ERROR] 재활성화 실패: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Disable된 industry_edges 재활성화")
    parser.add_argument("--rule_version", default="v2", help="룰 버전 (예: v2, v3)")
    args = parser.parse_args()
    
    revalidate_disabled_industry_edges(rule_version=args.rule_version)

