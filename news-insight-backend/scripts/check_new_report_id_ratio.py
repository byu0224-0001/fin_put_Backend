"""
신규 생성 industry_edges만 대상으로 report_id 채움 비율 확인

이번 실행에서 새로 생성된 industry_edges만 필터해서
report_id NOT NULL 비율이 80%+인지 확인
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

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

def check_new_report_id_ratio(hours_threshold=2):
    """
    신규 생성 industry_edges만 대상으로 report_id 채움 비율 확인
    
    Args:
        hours_threshold: 몇 시간 이내 생성된 데이터를 신규로 볼지 (기본 2시간)
    """
    print("=" * 80)
    print("[신규 industry_edges report_id 채움 비율 확인]")
    print("=" * 80)
    
    db = SessionLocal()
    
    try:
        # 최근 N시간 이내 생성된 industry_edges 필터
        threshold_time = datetime.utcnow() - timedelta(hours=hours_threshold)
        
        print(f"\n[1단계] 신규 데이터 필터링 (최근 {hours_threshold}시간 이내)")
        print(f"  기준 시간: {threshold_time}")
        
        # 신규 industry_edges 조회
        new_edges = db.query(IndustryEdge).filter(
            IndustryEdge.created_at >= threshold_time
        ).all()
        
        print(f"  신규 industry_edges: {len(new_edges)}개")
        
        if len(new_edges) == 0:
            print("\n[WARNING] 신규 데이터가 없습니다.")
            print("  최근 enrichment 실행이 없거나, hours_threshold를 늘려야 합니다.")
            return None
        
        # report_id 채움 비율 계산
        filled_count = 0
        null_count = 0
        
        filled_edges = []
        null_edges = []
        
        for edge in new_edges:
            if edge.report_id and edge.report_id.strip() != "":
                filled_count += 1
                filled_edges.append(edge)
            else:
                null_count += 1
                null_edges.append(edge)
        
        ratio = (filled_count / len(new_edges) * 100) if len(new_edges) > 0 else 0
        
        print(f"\n[2단계] report_id 채움 비율")
        print(f"  채워진 개수: {filled_count}/{len(new_edges)}")
        print(f"  비어있는 개수: {null_count}/{len(new_edges)}")
        print(f"  채움 비율: {ratio:.1f}%")
        
        # 샘플 출력
        print(f"\n[3단계] 채워진 데이터 샘플 (최대 3개)")
        for i, edge in enumerate(filled_edges[:3], 1):
            print(f"  {i}. ID: {edge.id}, report_id: {edge.report_id[:20]}, sector: {edge.target_sector_code}")
        
        if null_edges:
            print(f"\n[4단계] 비어있는 데이터 샘플 (최대 3개)")
            for i, edge in enumerate(null_edges[:3], 1):
                print(f"  {i}. ID: {edge.id}, report_id: NULL, sector: {edge.target_sector_code}, created_at: {edge.created_at}")
        
        # 결과 평가
        print("\n" + "=" * 80)
        print("[결과 평가]")
        print("=" * 80)
        
        if ratio >= 80:
            print(f"[OK] report_id 채움 비율이 80% 이상입니다 ({ratio:.1f}%)")
            print("  rc1 기준 통과")
            return True
        else:
            print(f"[ERROR] report_id 채움 비율이 80% 미만입니다 ({ratio:.1f}%)")
            print("  저장 로직에서 report_id를 설정하는 경로가 일부 케이스에서 빠졌을 수 있습니다.")
            print("  MIXED/INDUSTRY/MACRO 중 특정 분기에서 broker_report upsert가 누락됐을 가능성이 있습니다.")
            return False
        
    except Exception as e:
        print(f"\n[ERROR] 확인 실패: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        db.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=2, help="최근 N시간 이내 생성된 데이터를 신규로 볼지 (기본 2시간)")
    args = parser.parse_args()
    
    check_new_report_id_ratio(args.hours)

