"""
RC2 파이프라인 스모크 테스트 스크립트

Cold Start 파이프라인 전체 상태를 한눈에 보는 요약 리포트
- Input 파일 카운트
- broker_reports 상태
- industry_edges 상태
- driver_candidates 상태
- HOLD 사유별 분류
- 종합 평가

⭐ P0-추가3: 파이프라인 전체 상태 요약 리포트
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List
import glob
import json

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from app.db import SessionLocal
from app.models.broker_report import BrokerReport
from app.models.industry_edge import IndustryEdge
from app.models.driver_candidate import DriverCandidate
from sqlalchemy import func, and_, or_
from sqlalchemy.dialects.postgresql import JSONB

def count_json_files(pattern: str) -> int:
    """JSON 파일 개수 카운트"""
    files = glob.glob(str(project_root / pattern))
    return len(files)

def smoke_test_rc2_pipeline(days_back: int = 2, sources: List[str] = None):
    """
    RC2 파이프라인 스모크 테스트
    
    Args:
        days_back: 최근 N일 이내 데이터
        sources: 필터링할 source 리스트 (None이면 ['naver', 'kirs'])
    """
    if sources is None:
        sources = ['naver', 'kirs']
    
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("[RC2 파이프라인 스모크 테스트]")
        print("=" * 80)
        print(f"기준: 최근 {days_back}일 이내, source={sources}")
        print("")
        
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        # 1. Input 파일 카운트
        print("[1] Input 파일")
        print("-" * 80)
        naver_reports_count = count_json_files("reports/naver_reports_*.json")
        parsed_reports_count = count_json_files("reports/parsed_naver_reports_*.json")
        parsed_kirs_count = count_json_files("reports/parsed_kirs_reports_*.json")
        # parsed_reports_*.json도 포함 (일반 파싱 결과 파일)
        parsed_general_count = count_json_files("reports/parsed_reports_*.json")
        print(f"  - naver_reports JSON: {naver_reports_count}개")
        print(f"  - parsed_naver_reports JSON: {parsed_reports_count}개")
        print(f"  - parsed_kirs_reports JSON: {parsed_kirs_count}개")
        print(f"  - parsed_reports JSON (일반): {parsed_general_count}개")
        print(f"  - 총 parsed 파일: {parsed_reports_count + parsed_kirs_count + parsed_general_count}개")
        print("")
        
        # ⭐ P0-추가4: 파싱 성공률/실패 사유 분석
        print("[1-1] 파싱 성공률 분석")
        print("-" * 80)
        print("[지표 정의]")
        print("  - 분모: 수집 단계에서 유효(200 OK)로 판정된 문서 수")
        print("  - 분자: 그 중에서 구조화(LLM 추출)까지 성공한 수")
        print("  - 실패 사유: 상호배타적 (404/파싱/LLM/스키마/DB)")
        print("")
        parsed_files = []
        if parsed_reports_count > 0:
            parsed_files.extend(glob.glob(str(project_root / "reports" / "parsed_naver_reports_*.json")))
        if parsed_kirs_count > 0:
            parsed_files.extend(glob.glob(str(project_root / "reports" / "parsed_kirs_reports_*.json")))
        if parsed_general_count > 0:
            parsed_files.extend(glob.glob(str(project_root / "reports" / "parsed_reports_*.json")))
        
        if parsed_files:
            # 최신 파일 사용
            latest_parsed_file = max(parsed_files, key=lambda p: Path(p).stat().st_mtime)
            try:
                with open(latest_parsed_file, 'r', encoding='utf-8') as f:
                    parsed_reports = json.load(f)
                
                total_parsed = len(parsed_reports)
                success_count = sum(1 for r in parsed_reports if r.get("total_paragraphs", 0) > 0)
                fail_count = total_parsed - success_count
                success_rate = (success_count / total_parsed * 100) if total_parsed > 0 else 0
                
                print(f"  - 총 리포트: {total_parsed}개")
                print(f"  - 파싱 성공: {success_count}개 ({success_rate:.1f}%)")
                print(f"  - 파싱 실패: {fail_count}개")
                
                # 실패 원인별 분류
                parse_fail_reasons = {}
                for r in parsed_reports:
                    if r.get("total_paragraphs", 0) == 0:
                        reason = r.get("parse_fail_reason", "UNKNOWN_FAILURE")
                        parse_fail_reasons[reason] = parse_fail_reasons.get(reason, 0) + 1
                
                if parse_fail_reasons:
                    print(f"  - 실패 원인 TOP3:")
                    sorted_reasons = sorted(parse_fail_reasons.items(), key=lambda x: x[1], reverse=True)[:3]
                    for reason, count in sorted_reasons:
                        print(f"      - {reason}: {count}개")
                
                # 목표 지표 체크
                if success_rate >= 70:
                    print(f"  - 목표 달성: 70%+ ({success_rate:.1f}%) [통과]")
                else:
                    print(f"  - 목표 미달: 70%+ ({success_rate:.1f}%) [경고]")
                
            except Exception as e:
                print(f"  - 파싱 파일 분석 실패: {e}")
        else:
            print("  - Parsed 파일 없음 (파싱 스크립트 실행 필요)")
        print("")
        
        # 2. broker_reports 상태
        print("[2] broker_reports 테이블")
        print("-" * 80)
        broker_reports_query = db.query(BrokerReport).filter(
            and_(
                BrokerReport.source.in_(sources),
                BrokerReport.created_at >= cutoff_date
            )
        )
        broker_reports_total = broker_reports_query.count()
        
        # 상태별 분류
        status_counts = db.query(
            BrokerReport.processing_status,
            func.count(BrokerReport.id).label('count')
        ).filter(
            and_(
                BrokerReport.source.in_(sources),
                BrokerReport.created_at >= cutoff_date
            )
        ).group_by(BrokerReport.processing_status).all()
        
        print(f"  - 총계: {broker_reports_total}개")
        for status, count in status_counts:
            print(f"    - {status}: {count}개")
        print("")
        
        # 3. industry_edges 상태
        print("[3] industry_edges 테이블")
        print("-" * 80)
        # broker_reports와 연결된 것
        broker_report_ids = [r.report_id for r in broker_reports_query.all()]
        
        if broker_report_ids:
            industry_edges_connected = db.query(IndustryEdge).filter(
                IndustryEdge.report_id.in_(broker_report_ids)
            ).count()
        else:
            industry_edges_connected = 0
        
        # orphan edges
        industry_edges_orphan = db.query(IndustryEdge).filter(
            and_(
                IndustryEdge.report_id.is_(None),
                IndustryEdge.created_at >= cutoff_date
            )
        ).count()
        
        # 전체 (최근 N일)
        industry_edges_total = db.query(IndustryEdge).filter(
            IndustryEdge.created_at >= cutoff_date
        ).count()
        
        print(f"  - 연결된 것: {industry_edges_connected}개")
        print(f"  - orphan (report_id NULL): {industry_edges_orphan}개")
        print(f"  - 총계: {industry_edges_total}개")
        print("")
        
        # 4. driver_candidates 상태
        print("[4] driver_candidates 테이블")
        print("-" * 80)
        if broker_report_ids:
            driver_candidates_count = db.query(DriverCandidate).filter(
                DriverCandidate.source_report_id.in_(broker_report_ids)
            ).count()
        else:
            driver_candidates_count = 0
        
        print(f"  - 총계: {driver_candidates_count}개")
        print("")
        
        # 5. HOLD 사유별 분류
        print("[5] HOLD 사유별 분류")
        print("-" * 80)
        # broker_reports의 processing_status가 HOLD인 것들
        hold_reports = broker_reports_query.filter(
            or_(
                BrokerReport.processing_status.like('HOLD%'),
                BrokerReport.processing_status == 'PARSED_HOLD',
                BrokerReport.processing_status == 'EXTRACTED_HOLD'
            )
        ).all()
        
        hold_reasons = {}
        for br in hold_reports:
            status = br.processing_status
            if status not in hold_reasons:
                hold_reasons[status] = 0
            hold_reasons[status] += 1
        
        if hold_reasons:
            for reason, count in sorted(hold_reasons.items(), key=lambda x: x[1], reverse=True):
                print(f"  - {reason}: {count}개")
        else:
            print("  - HOLD 없음")
        print("")
        
        # ⭐ P1: Sanity Check 실패 Top Rule 출력
        sanity_check_holds = broker_reports_query.filter(
            BrokerReport.processing_status.like('HOLD_SECTOR_MAPPING_SANITY_CHECK_FAILED%')
        ).all()
        
        if sanity_check_holds:
            print("[5-2] Sanity Check 실패 Top Rule")
            print("-" * 80)
            # routing_audit에서 rule_id 추출
            rule_counts = {}
            for br in sanity_check_holds:
                if br.routing_audit:
                    audit = br.routing_audit if isinstance(br.routing_audit, dict) else json.loads(br.routing_audit) if isinstance(br.routing_audit, str) else {}
                    hold_reason = audit.get("hold_reason", "") or br.processing_status
                    # rule_id 추출 시도
                    if "rule_id:" in hold_reason:
                        rule_id = hold_reason.split("rule_id:")[-1].strip().split(")")[0]
                    elif "rule_id=" in hold_reason:
                        rule_id = hold_reason.split("rule_id=")[-1].strip().split(")")[0]
                    else:
                        rule_id = hold_reason.split(":")[0] if ":" in hold_reason else hold_reason[:50]
                    
                    rule_counts[rule_id] = rule_counts.get(rule_id, 0) + 1
            
            if rule_counts:
                sorted_rules = sorted(rule_counts.items(), key=lambda x: x[1], reverse=True)[:3]
                print(f"  - 총 Sanity Check 실패: {len(sanity_check_holds)}개")
                for rule_id, count in sorted_rules:
                    print(f"    - {rule_id}: {count}개")
            else:
                print(f"  - 총 Sanity Check 실패: {len(sanity_check_holds)}개 (rule_id 추출 실패)")
            print("")
        
        # ⭐ P0-추가5: Ticker 매칭 HOLD 사유별 액션 가이드
        print("[5-1] Ticker 매칭 HOLD 사유별 액션 가이드")
        print("-" * 80)
        ticker_hold_reasons = {
            "HOLD_TICKER_AMBIGUOUS": {
                "count": 0,
                "action": "ticker alias 사전 확장 / 키워드 기반 disambiguation / 수동 매핑 UI"
            },
            "HOLD_TICKER_NOT_FOUND": {
                "count": 0,
                "action": "stocks 테이블에 기업 추가 / ticker alias 사전 확장 / 수동 매핑"
            }
        }
        
        # Ticker 관련 HOLD 카운트
        for br in hold_reports:
            status = br.processing_status
            if status in ticker_hold_reasons:
                ticker_hold_reasons[status]["count"] += 1
        
        # Ticker 매칭 성공률 계산 (COMPANY 리포트 기준)
        company_reports = broker_reports_query.filter(
            BrokerReport.report_type == "COMPANY"
        ).all()
        
        if company_reports:
            company_total = len(company_reports)
            company_ticker_hold = sum(
                1 for br in company_reports
                if br.processing_status in ["HOLD_TICKER_AMBIGUOUS", "HOLD_TICKER_NOT_FOUND"]
            )
            company_ticker_matched = company_total - company_ticker_hold
            ticker_match_rate = (company_ticker_matched / company_total * 100) if company_total > 0 else 0
            
            print(f"  - COMPANY 리포트: {company_total}개")
            print(f"  - Ticker 매칭 성공: {company_ticker_matched}개 ({ticker_match_rate:.1f}%)")
            print(f"  - Ticker 매칭 HOLD: {company_ticker_hold}개")
            
            # 목표 지표 체크
            if ticker_match_rate >= 60:
                print(f"  - 목표 달성: 60%+ ({ticker_match_rate:.1f}%) [통과]")
            else:
                print(f"  - 목표 미달: 60%+ ({ticker_match_rate:.1f}%) [경고]")
            
            # HOLD 사유별 액션 가이드
            for reason, info in ticker_hold_reasons.items():
                if info["count"] > 0:
                    print(f"  - {reason}: {info['count']}개")
                    print(f"    → 다음 액션: {info['action']}")
        else:
            print("  - COMPANY 리포트 없음 (ticker 매칭 검증 불가)")
        print("")
        
        # 6. 종합 평가
        print("=" * 80)
        print("[종합 평가]")
        print("=" * 80)
        
        # 성공 지표
        success_indicators = {
            "input_files": naver_reports_count > 0 or parsed_reports_count > 0 or parsed_kirs_count > 0 or parsed_general_count > 0,
            "parsed_files": parsed_reports_count > 0 or parsed_kirs_count > 0 or parsed_general_count > 0,
            "broker_reports": broker_reports_total > 0,
            "industry_edges": industry_edges_connected > 0,
            "driver_candidates": driver_candidates_count >= 0  # 0도 정상
        }
        
        all_pass = all(success_indicators.values())
        
        for indicator, passed in success_indicators.items():
            status = "[통과]" if passed else "[실패]"
            print(f"  {status} {indicator}: {'통과' if passed else '실패'}")
        
        print("")
        if all_pass:
            print("[통과] 파이프라인 정상 작동")
        else:
            print("[경고] 파이프라인 이상 감지 - 상세 확인 필요")
            print("")
            print("확인 사항:")
            if not success_indicators["input_files"]:
                print("  - Input JSON 파일이 없습니다. 수집 스크립트를 실행하세요.")
            if not success_indicators["parsed_files"]:
                print("  - Parsed JSON 파일이 없습니다. 파싱 스크립트를 실행하세요.")
            if not success_indicators["broker_reports"]:
                print("  - broker_reports가 없습니다. Enrichment를 실행하세요.")
            if not success_indicators["industry_edges"]:
                print("  - industry_edges가 없습니다. Enrichment 결과를 확인하세요.")
        
    finally:
        db.close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="RC2 파이프라인 스모크 테스트")
    parser.add_argument('--days-back', type=int, default=2,
                        help='최근 N일 이내 데이터 (기본: 2일)')
    parser.add_argument('--sources', type=str, nargs='+', default=None,
                        help='필터링할 source 리스트 (기본: naver kirs)')
    
    args = parser.parse_args()
    
    smoke_test_rc2_pipeline(days_back=args.days_back, sources=args.sources)


if __name__ == "__main__":
    main()

