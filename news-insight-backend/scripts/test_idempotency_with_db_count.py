"""
멱등성 테스트 (DB count 기준)

동일 입력으로 enrichment를 2회 실행하여:
1. broker_reports row 증가 = 0
2. industry_edges row 증가 = 0
3. partial unique index 충돌 없이 스킵으로 처리되는지
4. 같은 리포트의 evidence_id가 동일한지 (샘플 3개)
"""
import sys
from pathlib import Path

# Windows 인코딩 처리
if sys.platform == 'win32':
    import codecs
    import os
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.db import SessionLocal
from app.models.industry_edge import IndustryEdge
from app.models.edge import Edge
from sqlalchemy import func

# broker_reports 테이블 존재 여부 확인
try:
    from app.models.broker_report import BrokerReport
    BROKER_REPORTS_EXISTS = True
except:
    BROKER_REPORTS_EXISTS = False

def test_idempotency_with_db_count():
    """멱등성 테스트 (DB count 기준)"""
    print("=" * 80)
    print("[멱등성 테스트 - DB Count 기준]")
    print("=" * 80)
    print("\n[안내]")
    print("1. 동일 입력 파일로 enrichment를 2회 실행하세요:")
    print("   python scripts/enrich_edges_from_reports.py --input <파일> --limit 5")
    print("   python scripts/enrich_edges_from_reports.py --input <파일> --limit 5")
    print("\n2. 1회차 실행 후 이 스크립트를 실행하여 초기 카운트를 기록하세요.")
    print("3. 2회차 실행 후 이 스크립트를 다시 실행하여 증가량을 확인하세요.")
    
    db = SessionLocal()
    
    try:
        # 1. 현재 상태 기록
        print("\n" + "=" * 80)
        print("[1단계] 현재 상태 기록")
        print("=" * 80)
        
        if BROKER_REPORTS_EXISTS:
            try:
                broker_count = db.query(BrokerReport).count()
                print(f"  broker_reports: {broker_count}개")
            except:
                db.rollback()
                print(f"  broker_reports: 테이블 없음 또는 오류")
                broker_count = 0
        else:
            print(f"  broker_reports: 모델 없음")
            broker_count = 0
        
        industry_count = db.query(IndustryEdge).count()
        edge_count = db.query(Edge).filter(Edge.relation_type == "DRIVEN_BY").count()
        
        print(f"  industry_edges: {industry_count}개")
        print(f"  DRIVEN_BY edges: {edge_count}개")
        
        # 2. 최신 industry_edges의 report_id 샘플 확인
        print("\n[2단계] 최신 industry_edges의 report_id 샘플")
        recent_edges = db.query(IndustryEdge).order_by(IndustryEdge.id.desc()).limit(5).all()
        report_ids = []
        for edge in recent_edges:
            report_id = edge.report_id if edge.report_id else "NULL"
            report_ids.append(report_id)
            print(f"  ID: {edge.id}, report_id: {report_id[:20] if report_id != 'NULL' else 'NULL'}, sector: {edge.target_sector_code}")
        
        # 3. evidence_id 샘플 확인 (DRIVEN_BY edges)
        print("\n[3단계] evidence_id 샘플 확인")
        sample_edges = db.query(Edge).filter(
            Edge.relation_type == "DRIVEN_BY"
        ).order_by(Edge.id.desc()).limit(3).all()
        
        evidence_samples = []
        for edge in sample_edges:
            evidence_ids = []
            if edge.properties:
                evidence_layer = edge.properties.get("evidence_layer", [])
                if isinstance(evidence_layer, list):
                    for evidence in evidence_layer:
                        if isinstance(evidence, dict):
                            fp = evidence.get("report_fingerprint", "")
                            if fp:
                                evidence_ids.append(fp[:16])
            
            evidence_samples.append({
                "edge_id": str(edge.id)[:50],
                "evidence_ids": evidence_ids,
                "count": len(evidence_ids)
            })
            print(f"  Edge: {edge.id[:50]}")
            print(f"    evidence_ids: {evidence_ids}")
            print(f"    evidence_count: {len(evidence_ids)}")
        
        # 4. 결과 요약
        print("\n" + "=" * 80)
        print("[테스트 결과 요약]")
        print("=" * 80)
        print(f"[OK] broker_reports: {broker_count}개")
        print(f"[OK] industry_edges: {industry_count}개")
        print(f"[OK] DRIVEN_BY edges: {edge_count}개")
        
        non_null_count = sum(1 for rid in report_ids if rid != "NULL")
        print(f"[OK] report_id 채워진 비율: {non_null_count}/{len(report_ids)} ({non_null_count*100//len(report_ids) if report_ids else 0}%)")
        
        # 5. 다음 단계 안내
        print("\n" + "=" * 80)
        print("[다음 단계]")
        print("=" * 80)
        print("1. 위 숫자를 기록하세요.")
        print("2. 동일 입력 파일로 enrichment를 2회차 실행하세요.")
        print("3. 이 스크립트를 다시 실행하여 증가량을 확인하세요.")
        print("\n[검증 기준]")
        print("  - broker_reports 증가 = 0")
        print("  - industry_edges 증가 = 0")
        print("  - evidence_id 동일 (샘플 3개 비교)")
        
        return {
            "broker_count": broker_count,
            "industry_count": industry_count,
            "edge_count": edge_count,
            "report_ids": report_ids,
            "evidence_samples": evidence_samples
        }
        
    except Exception as e:
        print(f"\n[ERROR] 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        db.close()

if __name__ == "__main__":
    test_idempotency_with_db_count()

