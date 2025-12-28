"""
테스트 데이터 선택적 초기화 스크립트

증권사 리포트 로직 테스트를 통해서 쌓인 DB만 초기화
- source='naver' 또는 source='kirs' (네이버 리포트, 한국IR협의회 리포트)
- 최근 N일 이내 생성된 데이터 (기본: 2일)
- 또는 특정 날짜 이후 데이터

⭐ P0-X3: 안전장치 강화
- ENV/DB 가드 (프로덕션 환경 차단)
- dry-run 기본값
- 2일 기준 (확인 가능한 최근 데이터만)
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy.orm import Session
from sqlalchemy import and_
from app.db import SessionLocal
from app.models.broker_report import BrokerReport
from app.models.industry_edge import IndustryEdge
from app.models.driver_candidate import DriverCandidate


def check_environment_safety():
    """
    환경 안전성 체크
    
    ⭐ P0-X3: 프로덕션 환경에서 실행 방지
    """
    # 환경 변수 체크
    env = os.getenv('ENVIRONMENT', '').lower()
    db_name = os.getenv('DB_NAME', '')
    
    # 프로덕션 환경 차단
    if env in ['prod', 'production', 'live']:
        print("[오류] 프로덕션 환경에서는 실행할 수 없습니다.")
        print(f"   ENVIRONMENT={env}")
        return False
    
    # DB 이름 체크 (newsdb는 테스트 DB로 가정)
    if db_name and 'prod' in db_name.lower() and 'test' not in db_name.lower():
        print("[오류] 프로덕션 DB에서는 실행할 수 없습니다.")
        print(f"   DB_NAME={db_name}")
        return False
    
    return True


def reset_test_data_selective(
    source: str = None,  # None이면 'naver'와 'kirs' 모두
    days_back: int = 2,  # ⭐ P0-X3: 2일 기준
    specific_date: str = None,
    dry_run: bool = True,
    auto_confirm: bool = False,  # ⭐ 자동 확인 (비대화형 환경용)
    include_orphan_edges: bool = False  # ⭐ P0-D: Orphan industry_edges 포함 여부
):
    """
    테스트 데이터 선택적 초기화
    
    Args:
        source: 수집 소스 (None이면 'naver'와 'kirs' 모두, 기본: None)
        days_back: 최근 N일 이내 데이터 (기본: 2일)
        specific_date: 특정 날짜 이후 (YYYY-MM-DD 형식, days_back보다 우선)
        dry_run: True면 실제 삭제 안 함 (기본: True)
    """
    # ⭐ P0-X3: 환경 안전성 체크
    if not check_environment_safety():
        print("\n[경고] 환경 안전성 체크 실패. 실행을 중단합니다.")
        return
    
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("[테스트 데이터 선택적 초기화]")
        print("=" * 80)
        
        # 날짜 기준 계산
        if specific_date:
            try:
                cutoff_date = datetime.strptime(specific_date, "%Y-%m-%d")
            except ValueError:
                print(f"[오류] 날짜 형식 오류: {specific_date} (YYYY-MM-DD 형식 필요)")
                return
        else:
            cutoff_date = datetime.now() - timedelta(days=days_back)
        
        # source 필터 (None이면 'naver'와 'kirs' 모두)
        sources = ['naver', 'kirs'] if source is None else [source]
        
        print(f"소스: {', '.join(sources)}")
        print(f"기준 날짜: {cutoff_date.strftime('%Y-%m-%d %H:%M:%S')} 이후")
        print(f"모드: {'DRY-RUN (실제 삭제 안 함)' if dry_run else '[경고] 실제 삭제'}")
        if include_orphan_edges:
            print(f"Orphan edges 포함: 예 (report_id NULL인 industry_edges도 삭제)")
        print("")
        
        # 1. broker_reports 조회 및 카운트
        broker_reports_query = db.query(BrokerReport).filter(
            and_(
                BrokerReport.source.in_(sources),
                BrokerReport.created_at >= cutoff_date
            )
        )
        broker_reports_count = broker_reports_query.count()
        broker_reports_list = broker_reports_query.all()
        broker_report_ids = [r.report_id for r in broker_reports_list]
        
        print(f"[1] broker_reports: {broker_reports_count}개 발견")
        if broker_reports_count > 0:
            print(f"    예시 report_id: {broker_report_ids[:3]}")
            print(f"    소스별 분포:")
            for src in sources:
                count = sum(1 for r in broker_reports_list if r.source == src)
                print(f"      - {src}: {count}개")
        
        # 2. industry_edges 조회 및 카운트
        # 2-1. broker_reports와 연결된 industry_edges
        industry_edges_count = 0
        if broker_report_ids:
            industry_edges_count = db.query(IndustryEdge).filter(
                IndustryEdge.report_id.in_(broker_report_ids)
            ).count()
        
        # 2-2. ⭐ P0-D: Orphan industry_edges (report_id IS NULL) 조회
        orphan_edges_count = 0
        orphan_edges = []
        if include_orphan_edges:
            orphan_edges = db.query(IndustryEdge).filter(
                and_(
                    IndustryEdge.report_id.is_(None),
                    IndustryEdge.created_at >= cutoff_date
                )
            ).all()
            orphan_edges_count = len(orphan_edges)
        
        total_industry_edges_count = industry_edges_count + orphan_edges_count
        
        print(f"[2] industry_edges: {total_industry_edges_count}개 발견")
        if industry_edges_count > 0:
            print(f"    - broker_reports와 연결된 것: {industry_edges_count}개")
        if orphan_edges_count > 0:
            print(f"    - orphan (report_id NULL): {orphan_edges_count}개")
        
        # 3. driver_candidates 조회 및 카운트 (source_report_id로 연결)
        driver_candidates_count = 0
        if broker_report_ids:
            driver_candidates_count = db.query(DriverCandidate).filter(
                DriverCandidate.source_report_id.in_(broker_report_ids)
            ).count()
        
        print(f"[3] driver_candidates: {driver_candidates_count}개 발견")
        
        total_count = broker_reports_count + total_industry_edges_count + driver_candidates_count
        
        print("")
        print(f"총 삭제 대상: {total_count}개")
        print("")
        
        if total_count == 0:
            print("[완료] 삭제할 데이터가 없습니다.")
            return
        
        # Dry-run이면 여기서 종료
        if dry_run:
            print("[경고] DRY-RUN 모드: 실제 삭제는 하지 않습니다.")
            print("   실제 삭제하려면 --execute 플래그를 사용하세요.")
            return
        
        # ⭐ P0-X3: 실제 삭제 전 최종 확인
        print("=" * 80)
        print("[경고] 실제 삭제를 진행합니다.")
        print("=" * 80)
        print(f"삭제 대상:")
        print(f"  - broker_reports: {broker_reports_count}개")
        print(f"  - industry_edges: {total_industry_edges_count}개")
        if orphan_edges_count > 0:
            print(f"    (연결된 것: {industry_edges_count}개, orphan: {orphan_edges_count}개)")
        print(f"  - driver_candidates: {driver_candidates_count}개")
        print(f"  - 총계: {total_count}개")
        print("")
        
        # ⭐ 자동 확인 또는 수동 확인
        if not auto_confirm:
            try:
                confirm = input(f"정말로 {total_count}개 데이터를 삭제하시겠습니까? (yes 입력 필요): ")
                if confirm.lower() != 'yes':
                    print("취소되었습니다.")
                    return
            except EOFError:
                print("[오류] 비대화형 환경입니다. --yes 플래그를 사용하세요.")
                return
        else:
            print(f"[자동 확인] {total_count}개 데이터 삭제를 진행합니다.")
        
        deleted_counts = {}
        
        # 3. driver_candidates 삭제 (먼저 - FK 제약 때문)
        if driver_candidates_count > 0:
            deleted = db.query(DriverCandidate).filter(
                DriverCandidate.source_report_id.in_(broker_report_ids)
            ).delete(synchronize_session=False)
            deleted_counts['driver_candidates'] = deleted
            print(f"[완료] driver_candidates: {deleted}개 삭제")
        
        # 2. industry_edges 삭제
        # 2-1. broker_reports와 연결된 industry_edges 삭제
        if industry_edges_count > 0:
            deleted = db.query(IndustryEdge).filter(
                IndustryEdge.report_id.in_(broker_report_ids)
            ).delete(synchronize_session=False)
            deleted_counts['industry_edges'] = deleted
            print(f"[완료] industry_edges (연결된 것): {deleted}개 삭제")
        
        # 2-2. ⭐ P0-D: Orphan industry_edges 삭제
        if orphan_edges_count > 0:
            orphan_ids = [e.id for e in orphan_edges]
            deleted_orphan = db.query(IndustryEdge).filter(
                IndustryEdge.id.in_(orphan_ids)
            ).delete(synchronize_session=False)
            deleted_counts['industry_edges_orphan'] = deleted_orphan
            print(f"[완료] industry_edges (orphan): {deleted_orphan}개 삭제")
        
        # 1. broker_reports 삭제 (마지막)
        if broker_reports_count > 0:
            deleted = broker_reports_query.delete(synchronize_session=False)
            deleted_counts['broker_reports'] = deleted
            print(f"[완료] broker_reports: {deleted}개 삭제")
        
        db.commit()
        
        print("")
        print("=" * 80)
        print("[삭제 완료]")
        print("=" * 80)
        for table, count in deleted_counts.items():
            print(f"  {table}: {count}개")
        print(f"  총계: {sum(deleted_counts.values())}개")
        
    except Exception as e:
        db.rollback()
        print(f"[오류] 초기화 실패: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        db.close()


def verify_deletion():
    """
    삭제 후 검증 (DB 조회)
    """
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("[삭제 검증]")
        print("=" * 80)
        
        # 최근 2일 이내 데이터 확인
        cutoff_date = datetime.now() - timedelta(days=2)
        
        # broker_reports 확인
        broker_reports_count = db.query(BrokerReport).filter(
            and_(
                BrokerReport.source.in_(['naver', 'kirs']),
                BrokerReport.created_at >= cutoff_date
            )
        ).count()
        
        # industry_edges 확인 (최근 2일)
        industry_edges_count = db.query(IndustryEdge).filter(
            IndustryEdge.created_at >= cutoff_date
        ).count()
        
        # driver_candidates 확인 (최근 2일)
        driver_candidates_count = db.query(DriverCandidate).filter(
            DriverCandidate.created_at >= cutoff_date
        ).count()
        
        print(f"최근 2일 이내 남은 데이터:")
        print(f"  - broker_reports: {broker_reports_count}개")
        print(f"  - industry_edges: {industry_edges_count}개")
        print(f"  - driver_candidates: {driver_candidates_count}개")
        
        if broker_reports_count == 0 and industry_edges_count == 0 and driver_candidates_count == 0:
            print("\n[완료] 테스트 데이터가 모두 삭제되었습니다.")
        else:
            print(f"\n[경고] 일부 데이터가 남아있습니다. (최근 2일 이내 생성된 데이터)")
        
    finally:
        db.close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="테스트 데이터 선택적 초기화")
    parser.add_argument('--source', type=str, default=None,
                        help='수집 소스 (naver/kirs, 기본: None=모두)')
    parser.add_argument('--days-back', type=int, default=2,
                        help='최근 N일 이내 데이터 (기본: 2일)')
    parser.add_argument('--since-date', type=str, default=None,
                        help='특정 날짜 이후 (YYYY-MM-DD 형식, days-back보다 우선)')
    parser.add_argument('--execute', action='store_true',
                        help='실제 삭제 실행 (기본은 dry-run)')
    parser.add_argument('--yes', action='store_true',
                        help='자동 확인 (비대화형 환경용)')
    parser.add_argument('--include-orphan-edges', action='store_true',
                        help='Orphan industry_edges (report_id NULL)도 삭제 대상에 포함')
    parser.add_argument('--verify', action='store_true',
                        help='삭제 후 검증 (DB 조회)')
    
    args = parser.parse_args()
    
    reset_test_data_selective(
        source=args.source,
        days_back=args.days_back,
        specific_date=args.since_date,
        dry_run=not args.execute,
        auto_confirm=args.yes,
        include_orphan_edges=args.include_orphan_edges
    )
    
    if args.verify:
        verify_deletion()


if __name__ == "__main__":
    main()

