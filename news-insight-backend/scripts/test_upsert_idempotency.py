"""
Upsert 회귀 테스트 스크립트

동일 리포트 데이터로 파이프라인을 3회 연속 실행하여:
1. edge row 수가 동일하게 유지되는지 (증식 여부)
2. logic_summary가 업데이트되는지
3. driver/evidence가 누적/덮어쓰기 정책대로 동작하는지

확인
"""
import sys
from pathlib import Path
import json
import hashlib
from datetime import datetime
from typing import Dict, List, Optional

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from app.db import SessionLocal
from app.models.industry_edge import IndustryEdge
from sqlalchemy import func

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_edge_count_by_report_id(db, report_id: str) -> int:
    """특정 report_id의 edge 개수 조회"""
    return db.query(IndustryEdge).filter(
        IndustryEdge.report_id == report_id
    ).count()


def get_edge_details(db, report_id: str) -> List[Dict]:
    """특정 report_id의 edge 상세 정보 조회 (row id 포함)"""
    edges = db.query(IndustryEdge).filter(
        IndustryEdge.report_id == report_id
    ).order_by(IndustryEdge.id).all()  # ⭐ id 순서 고정
    
    return [
        {
            "id": e.id,  # ⭐ row id
            "report_id": e.report_id,
            "source_driver_code": e.source_driver_code,
            "target_sector_code": e.target_sector_code,
            "logic_summary_len": len(e.logic_summary) if e.logic_summary else 0,
            "logic_summary_hash": hashlib.md5(str(e.logic_summary).encode('utf-8')).hexdigest()[:8] if e.logic_summary else None,
            "logic_fingerprint": e.logic_fingerprint,
            "created_at": e.created_at.isoformat() if e.created_at else None,
            "updated_at": e.updated_at.isoformat() if e.updated_at else None,
        }
        for e in edges
    ]


def run_enrichment_once(report_file: str, limit: Optional[int] = None) -> Dict:
    """Enrichment 1회 실행"""
    import subprocess
    
    cmd = [
        sys.executable,
        str(project_root / "scripts" / "enrich_edges_from_reports.py"),
        "--input", report_file,
    ]
    if limit:
        cmd.extend(["--limit", str(limit)])
    
    logger.info(f"Enrichment 실행: {' '.join(cmd)}")
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace'
    )
    
    return {
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr
    }


def test_upsert_idempotency(
    report_file: str,
    test_report_id: Optional[str] = None,
    runs: int = 3
) -> Dict:
    """Upsert 회귀 테스트"""
    db = SessionLocal()
    
    try:
        # 초기 상태 확인
        if test_report_id:
            initial_count = get_edge_count_by_report_id(db, test_report_id)
            initial_details = get_edge_details(db, test_report_id)
        else:
            # 전체 edge 수 확인
            initial_count = db.query(IndustryEdge).count()
            initial_details = []
        
        logger.info(f"\n[초기 상태]")
        logger.info(f"  Edge 개수: {initial_count}")
        if initial_details:
            logger.info(f"  테스트 리포트 Edge 개수: {len(initial_details)}")
        
        results = []
        
        # N회 실행
        for run_num in range(1, runs + 1):
            logger.info(f"\n[실행 {run_num}/{runs}]")
            
            # Enrichment 실행
            enrichment_result = run_enrichment_once(report_file, limit=1)
            
            if enrichment_result["returncode"] != 0:
                logger.error(f"Enrichment 실패: {enrichment_result['stderr']}")
                continue
            
            # 실행 후 상태 확인
            if test_report_id:
                after_count = get_edge_count_by_report_id(db, test_report_id)
                after_details = get_edge_details(db, test_report_id)
            else:
                after_count = db.query(IndustryEdge).count()
                after_details = []
            
            # ⭐ row id 고정 확인
            row_ids_stable = True
            row_id_changes = set()
            row_id_count = 0
            if test_report_id and initial_details:
                initial_ids = sorted([e["id"] for e in initial_details])
                after_ids = sorted([e["id"] for e in after_details])
                row_ids_stable = (initial_ids == after_ids)
                row_id_changes = set(after_ids) - set(initial_ids) if after_ids != initial_ids else set()
                row_id_count = len(after_ids)
            
            # ⭐ updated_at 갱신 확인
            updated_at_changed = False
            updated_at_prev = None
            updated_at_curr = None
            if run_num > 1 and after_details:
                prev_updated = results[run_num - 2]["edge_details"][0]["updated_at"] if results[run_num - 2]["edge_details"] else None
                curr_updated = after_details[0]["updated_at"] if after_details else None
                updated_at_changed = (prev_updated != curr_updated)
                updated_at_prev = prev_updated
                updated_at_curr = curr_updated
            
            result = {
                "run": run_num,
                "edge_count": after_count,
                "edge_count_change": after_count - initial_count,
                "edge_details": after_details,
                "enrichment_success": enrichment_result["returncode"] == 0,
                "row_ids_stable": row_ids_stable,
                "row_id_changes": list(row_id_changes),
                "row_id_count": row_id_count,
                "updated_at_changed": updated_at_changed,
                "updated_at_prev": updated_at_prev,
                "updated_at_curr": updated_at_curr
            }
            results.append(result)
            
            logger.info(f"  실행 후 Edge 개수: {after_count} (변화: {result['edge_count_change']})")
            logger.info(f"  Row ID 안정: {row_ids_stable} (개수: {row_id_count})")
            if row_id_changes:
                logger.warning(f"  ⚠️ Row ID 변화: {row_id_changes}")
            if after_details:
                logger.info(f"  테스트 리포트 Edge 개수: {len(after_details)}")
                for i, detail in enumerate(after_details, 1):
                    logger.info(f"    Edge {i}: id={detail['id']}, "
                              f"len={detail['logic_summary_len']}, "
                              f"hash={detail.get('logic_summary_hash', 'N/A')}, "
                              f"fp={detail['logic_fingerprint']}, "
                              f"updated={detail['updated_at']}")
                if updated_at_changed:
                    logger.info(f"  ⚠️ Updated_at 변경: {updated_at_prev} → {updated_at_curr}")
        
        # 결과 분석
        analysis = {
            "initial_count": initial_count,
            "final_count": results[-1]["edge_count"] if results else initial_count,
            "runs": runs,
            "edge_count_stable": all(
                r["edge_count"] == results[0]["edge_count"] 
                for r in results
            ) if results else False,
            "edge_count_changes": [r["edge_count_change"] for r in results],
            "all_successful": all(r["enrichment_success"] for r in results),
            "row_ids_stable": all(r.get("row_ids_stable", True) for r in results) if results else True,
            "updated_at_changed": any(r.get("updated_at_changed", False) for r in results[1:]) if len(results) > 1 else False,
            "upsert_strategy": "UPDATE_IN_PLACE"  # 현재 정책
        }
        
        return {
            "test_report_id": test_report_id,
            "results": results,
            "analysis": analysis
        }
        
    finally:
        db.close()


def print_test_summary(test_result: Dict):
    """테스트 결과 요약 출력"""
    print("\n" + "=" * 80)
    print("[Upsert 회귀 테스트 결과]")
    print("=" * 80)
    
    analysis = test_result["analysis"]
    
    print(f"초기 Edge 개수: {analysis['initial_count']}")
    print(f"최종 Edge 개수: {analysis['final_count']}")
    print(f"실행 횟수: {analysis['runs']}")
    print(f"Edge 개수 변화: {analysis['edge_count_changes']}")
    
    print("\n[판정]")
    if analysis["edge_count_stable"]:
        print("  ✅ Edge 개수 안정 (증식 없음)")
    else:
        print("  ❌ Edge 개수 불안정 (증식 가능성)")
        print(f"     변화: {analysis['edge_count_changes']}")
    
    if analysis["row_ids_stable"]:
        print(f"  ✅ Row ID 안정 (전략: {analysis['upsert_strategy']})")
    else:
        print(f"  ⚠️ Row ID 변화 (전략: {analysis['upsert_strategy']})")
        print("     → UPDATE_IN_PLACE 전략이면 ID 고정 기대, REPLACE 전략이면 ID 변동 허용")
    
    if analysis["updated_at_changed"]:
        print("  ✅ Updated_at 갱신 확인됨")
    else:
        print("  ⚠️ Updated_at 갱신 없음 (내용 변경 없음 또는 업데이트 정책 미작동)")
    
    if analysis["all_successful"]:
        print("  ✅ 모든 실행 성공")
    else:
        print("  ❌ 일부 실행 실패")
    
    # Edge 상세 정보
    if test_result.get("test_report_id") and test_result["results"]:
        print("\n[Edge 상세 정보]")
        last_details = test_result["results"][-1]["edge_details"]
        for i, detail in enumerate(last_details, 1):
            print(f"  Edge {i}:")
            print(f"    - ID: {detail['id']}")
            print(f"    - Driver: {detail['source_driver_code']}")
            print(f"    - Sector: {detail['target_sector_code']}")
            print(f"    - Summary 길이: {detail['logic_summary_len']}")
            print(f"    - Fingerprint: {detail['logic_fingerprint']}")
            print(f"    - Created: {detail['created_at']}")
            print(f"    - Updated: {detail['updated_at']}")


def main():
    """메인 함수"""
    import argparse
    parser = argparse.ArgumentParser(description="Upsert 회귀 테스트")
    parser.add_argument("--report-file", type=str, required=True, help="파싱된 리포트 JSON 파일")
    parser.add_argument("--test-report-id", type=str, help="테스트할 특정 report_id (선택)")
    parser.add_argument("--runs", type=int, default=3, help="실행 횟수 (기본: 3)")
    args = parser.parse_args()
    
    # 리포트 파일 확인
    report_path = Path(args.report_file)
    if not report_path.exists():
        # 상대 경로 시도
        report_path = project_root / "reports" / args.report_file
        if not report_path.exists():
            logger.error(f"리포트 파일을 찾을 수 없습니다: {args.report_file}")
            return
    
    logger.info(f"테스트 리포트 파일: {report_path}")
    
    test_result = test_upsert_idempotency(
        str(report_path),
        test_report_id=args.test_report_id,
        runs=args.runs
    )
    
    print_test_summary(test_result)


if __name__ == "__main__":
    main()

