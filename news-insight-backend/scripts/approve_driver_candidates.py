"""
드라이버 후보 승인 CLI 스크립트

기능:
1. 대기 중인 후보 목록 표시 (Top N)
2. (A)승인/(M)병합/(R)거절 선택
3. DB 반영
4. 관련 리포트 자동 재처리(Re-enrichment) 트리거
"""
import sys
import os
from pathlib import Path
from typing import List, Dict, Optional, Any
import json
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

from sqlalchemy.orm import Session
from app.db import SessionLocal
from app.models.driver_candidate import DriverCandidate
from app.models.economic_variable import EconomicVariable
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def list_pending_candidates(db: Session, limit: int = 20) -> List[DriverCandidate]:
    """
    대기 중인 드라이버 후보 목록 조회 (Top N)
    
    Args:
        db: DB 세션
        limit: 최대 개수
    
    Returns:
        후보 리스트 (발견 횟수 내림차순)
    """
    candidates = db.query(DriverCandidate).filter(
        DriverCandidate.status == 'PENDING'
    ).order_by(
        DriverCandidate.occurrence_count.desc(),
        DriverCandidate.last_seen_at.desc()
    ).limit(limit).all()
    
    return candidates


def approve_candidate(
    db: Session,
    candidate_id: int,
    approved_driver_code: str,
    approved_by: str = "admin"
) -> bool:
    """
    후보 승인 (신규 driver로 create)
    
    ⭐ 개선: 트랜잭션 명시화 및 멱등성 강화
    - 이미 존재하는 driver인 경우 멱등성 보장
    - 트랜잭션 경계 명확화
    
    Args:
        db: DB 세션
        candidate_id: 후보 ID
        approved_driver_code: 승인된 드라이버 코드
        approved_by: 승인자
    
    Returns:
        성공 여부
    """
    try:
        # 트랜잭션 시작 (명시적)
        db.begin()
        
        candidate = db.query(DriverCandidate).filter(
            DriverCandidate.id == candidate_id
        ).first()
        
        if not candidate:
            logger.error(f"후보를 찾을 수 없습니다: {candidate_id}")
            db.rollback()
            return False
        
        # ⭐ 멱등성 보장: 이미 처리된 후보는 성공으로 반환
        if candidate.status != 'PENDING':
            if candidate.status == 'APPROVED' and candidate.approved_driver_code == approved_driver_code:
                logger.info(f"이미 승인된 후보 (멱등성 보장): {candidate.candidate_text} → {approved_driver_code}")
                db.rollback()  # 변경사항 없으므로 롤백
                return True
            else:
                logger.error(f"이미 처리된 후보입니다: {candidate.status}")
                db.rollback()
                return False
        
        # ⭐ 중복 체크 강화: economic_variables에 이미 존재하는지 확인
        existing_var = db.query(EconomicVariable).filter(
            EconomicVariable.code == approved_driver_code
        ).first()
        
        if existing_var:
            # 이미 존재하면 멱등성 보장: 후보만 업데이트
            candidate.status = 'APPROVED'
            candidate.approved_driver_code = approved_driver_code
            candidate.approved_by = approved_by
            candidate.approved_at = datetime.utcnow()
            
            # 동의어 추가 (없는 경우만)
            synonyms = existing_var.synonyms or []
            if candidate.candidate_text not in synonyms:
                synonyms.append(candidate.candidate_text)
                existing_var.synonyms = synonyms
            
            db.commit()
            logger.info(f"후보 승인 완료 (이미 존재하는 driver): {approved_driver_code}")
            return True
        
        # 1. economic_variables에 신규 드라이버 추가
        new_var = EconomicVariable(
            code=approved_driver_code,
            name_ko=candidate.candidate_text,
            synonyms=[candidate.candidate_text],  # 원본 텍스트를 동의어로 추가
            description=f"후보 승인: {candidate.candidate_text}",
            source="DRIVER_CANDIDATE_APPROVAL"
        )
        db.add(new_var)
        logger.info(f"신규 드라이버 추가: {approved_driver_code} ({candidate.candidate_text})")
        
        # 2. 후보 상태 업데이트
        candidate.status = 'APPROVED'
        candidate.approved_driver_code = approved_driver_code
        candidate.approved_by = approved_by
        candidate.approved_at = datetime.utcnow()
        
        # 명시적 커밋
        db.commit()
        logger.info(f"후보 승인 완료: {candidate.candidate_text} → {approved_driver_code}")
        return True
        
    except Exception as e:
        # 명시적 롤백
        db.rollback()
        logger.error(f"후보 승인 실패: {e}", exc_info=True)
        return False


def merge_candidate(
    db: Session,
    candidate_id: int,
    merged_to_driver_code: str,
    add_as_synonym: bool = True,
    approved_by: str = "admin"
) -> bool:
    """
    후보 병합 (기존 driver에 merge hookup 또는 synonym 추가)
    
    ⭐ 개선: 트랜잭션 명시화 및 멱등성 강화
    
    Args:
        db: DB 세션
        candidate_id: 후보 ID
        merged_to_driver_code: 병합 대상 드라이버 코드
        add_as_synonym: 동의어로 추가할지 여부
        approved_by: 승인자
    
    Returns:
        성공 여부
    """
    try:
        # 트랜잭션 시작 (명시적)
        db.begin()
        
        candidate = db.query(DriverCandidate).filter(
            DriverCandidate.id == candidate_id
        ).first()
        
        if not candidate:
            logger.error(f"후보를 찾을 수 없습니다: {candidate_id}")
            db.rollback()
            return False
        
        # ⭐ 멱등성 보장: 이미 처리된 후보는 성공으로 반환
        if candidate.status != 'PENDING':
            if candidate.status == 'MERGED' and candidate.merged_to_driver_code == merged_to_driver_code:
                logger.info(f"이미 병합된 후보 (멱등성 보장): {candidate.candidate_text} → {merged_to_driver_code}")
                db.rollback()
                return True
            else:
                logger.error(f"이미 처리된 후보입니다: {candidate.status}")
                db.rollback()
                return False
        
        # 기존 드라이버 확인
        existing_var = db.query(EconomicVariable).filter(
            EconomicVariable.code == merged_to_driver_code
        ).first()
        
        if not existing_var:
            logger.error(f"드라이버를 찾을 수 없습니다: {merged_to_driver_code}")
            db.rollback()
            return False
        
        # 동의어 추가
        if add_as_synonym:
            synonyms = existing_var.synonyms or []
            if candidate.candidate_text not in synonyms:
                synonyms.append(candidate.candidate_text)
                existing_var.synonyms = synonyms
                logger.info(f"동의어 추가: {merged_to_driver_code} ← {candidate.candidate_text}")
            else:
                logger.debug(f"동의어 이미 존재: {merged_to_driver_code} ← {candidate.candidate_text}")
        
        # 후보 상태 업데이트
        candidate.status = 'MERGED'
        candidate.merged_to_driver_code = merged_to_driver_code
        candidate.synonym_for_driver_code = merged_to_driver_code if add_as_synonym else None
        candidate.approved_by = approved_by
        candidate.approved_at = datetime.utcnow()
        
        # 명시적 커밋
        db.commit()
        logger.info(f"후보 병합 완료: {candidate.candidate_text} → {merged_to_driver_code}")
        return True
        
    except Exception as e:
        # 명시적 롤백
        db.rollback()
        logger.error(f"후보 병합 실패: {e}", exc_info=True)
        return False


def reject_candidate(
    db: Session,
    candidate_id: int,
    rejection_reason: str,
    approved_by: str = "admin"
) -> bool:
    """
    후보 거절
    
    ⭐ 개선: 트랜잭션 명시화 및 멱등성 강화
    
    Args:
        db: DB 세션
        candidate_id: 후보 ID
        rejection_reason: 거절 사유
        approved_by: 승인자
    
    Returns:
        성공 여부
    """
    try:
        # 트랜잭션 시작 (명시적)
        db.begin()
        
        candidate = db.query(DriverCandidate).filter(
            DriverCandidate.id == candidate_id
        ).first()
        
        if not candidate:
            logger.error(f"후보를 찾을 수 없습니다: {candidate_id}")
            db.rollback()
            return False
        
        # ⭐ 멱등성 보장: 이미 처리된 후보는 성공으로 반환
        if candidate.status != 'PENDING':
            if candidate.status == 'REJECTED':
                logger.info(f"이미 거절된 후보 (멱등성 보장): {candidate.candidate_text}")
                db.rollback()
                return True
            else:
                logger.error(f"이미 처리된 후보입니다: {candidate.status}")
                db.rollback()
                return False
        
        candidate.status = 'REJECTED'
        candidate.rejection_reason = rejection_reason
        candidate.approved_by = approved_by
        candidate.approved_at = datetime.utcnow()
        
        # 명시적 커밋
        db.commit()
        logger.info(f"후보 거절 완료: {candidate.candidate_text} (사유: {rejection_reason})")
        return True
        
    except Exception as e:
        # 명시적 롤백
        db.rollback()
        logger.error(f"후보 거절 실패: {e}", exc_info=True)
        return False


def re_enrich_affected_reports(
    db: Session,
    candidate_id: int
) -> List[str]:
    """
    승인/병합된 후보와 관련된 리포트 재처리
    
    Args:
        db: DB 세션
        candidate_id: 후보 ID
    
    Returns:
        재처리 대상 리포트 ID 리스트
    """
    candidate = db.query(DriverCandidate).filter(
        DriverCandidate.id == candidate_id
    ).first()
    
    if not candidate:
        return []
    
    # source_report_id가 있는 리포트들 찾기
    # (실제로는 broker_reports 테이블에서 status를 WAITING으로 변경)
    affected_report_ids = []
    
    if candidate.source_report_id:
        affected_report_ids.append(candidate.source_report_id)
    
    # 같은 candidate_text를 가진 다른 후보들도 찾기
    related_candidates = db.query(DriverCandidate).filter(
        DriverCandidate.candidate_text == candidate.candidate_text,
        DriverCandidate.status.in_(['APPROVED', 'MERGED'])
    ).all()
    
    for related in related_candidates:
        if related.source_report_id and related.source_report_id not in affected_report_ids:
            affected_report_ids.append(related.source_report_id)
    
    logger.info(f"재처리 대상 리포트: {len(affected_report_ids)}개")
    return affected_report_ids


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="드라이버 후보 승인 CLI")
    parser.add_argument("--list", action="store_true", help="대기 중인 후보 목록 표시")
    parser.add_argument("--limit", type=int, default=20, help="목록 최대 개수")
    parser.add_argument("--approve", type=int, help="후보 ID 승인 (신규 driver로 create)")
    parser.add_argument("--driver-code", type=str, help="승인된 드라이버 코드 (--approve와 함께 사용)")
    parser.add_argument("--merge", type=int, help="후보 ID 병합 (기존 driver에 merge)")
    parser.add_argument("--merge-to", type=str, help="병합 대상 드라이버 코드 (--merge와 함께 사용)")
    parser.add_argument("--reject", type=int, help="후보 ID 거절")
    parser.add_argument("--reason", type=str, help="거절 사유 (--reject와 함께 사용)")
    parser.add_argument("--re-enrich", type=int, help="후보 ID와 관련된 리포트 재처리")
    parser.add_argument("--approved-by", type=str, default="admin", help="승인자 이름")
    
    args = parser.parse_args()
    
    db = SessionLocal()
    
    try:
        if args.list:
            # 목록 표시
            candidates = list_pending_candidates(db, args.limit)
            
            print("=" * 80)
            print(f"[대기 중인 드라이버 후보] {len(candidates)}개")
            print("=" * 80)
            
            for i, candidate in enumerate(candidates, 1):
                print(f"\n[{i}] ID: {candidate.id}")
                print(f"    후보 텍스트: {candidate.candidate_text}")
                print(f"    제안 드라이버: {candidate.suggested_driver_code or '없음'}")
                print(f"    신뢰도: {candidate.confidence:.2f}")
                print(f"    발견 횟수: {candidate.occurrence_count}회")
                print(f"    최초 발견: {candidate.first_seen_at}")
                print(f"    최근 발견: {candidate.last_seen_at}")
                if candidate.source_report_title:
                    print(f"    출처 리포트: {candidate.source_report_title[:50]}...")
                if candidate.context_sentence:
                    print(f"    컨텍스트: {candidate.context_sentence[:100]}...")
                print()
        
        elif args.approve:
            # 승인
            if not args.driver_code:
                print("❌ --driver-code가 필요합니다.")
                return
            
            success = approve_candidate(
                db, args.approve, args.driver_code, args.approved_by
            )
            
            if success:
                print(f"✅ 후보 승인 완료: ID {args.approve} → {args.driver_code}")
                # 재처리 트리거
                affected_reports = re_enrich_affected_reports(db, args.approve)
                if affected_reports:
                    print(f"📋 재처리 대상 리포트: {len(affected_reports)}개")
                    print("   다음 명령으로 재처리하세요:")
                    print(f"   python scripts/enrich_edges_from_reports.py --input reports/parsed_*.json --limit {len(affected_reports)}")
            else:
                print(f"❌ 후보 승인 실패: ID {args.approve}")
        
        elif args.merge:
            # 병합
            if not args.merge_to:
                print("❌ --merge-to가 필요합니다.")
                return
            
            success = merge_candidate(
                db, args.merge, args.merge_to, add_as_synonym=True, approved_by=args.approved_by
            )
            
            if success:
                print(f"✅ 후보 병합 완료: ID {args.merge} → {args.merge_to}")
                # 재처리 트리거
                affected_reports = re_enrich_affected_reports(db, args.merge)
                if affected_reports:
                    print(f"📋 재처리 대상 리포트: {len(affected_reports)}개")
            else:
                print(f"❌ 후보 병합 실패: ID {args.merge}")
        
        elif args.reject:
            # 거절
            if not args.reason:
                print("❌ --reason이 필요합니다.")
                return
            
            success = reject_candidate(
                db, args.reject, args.reason, args.approved_by
            )
            
            if success:
                print(f"✅ 후보 거절 완료: ID {args.reject}")
            else:
                print(f"❌ 후보 거절 실패: ID {args.reject}")
        
        elif args.re_enrich:
            # 재처리
            affected_reports = re_enrich_affected_reports(db, args.re_enrich)
            if affected_reports:
                print(f"📋 재처리 대상 리포트: {len(affected_reports)}개")
                print("   다음 명령으로 재처리하세요:")
                print(f"   python scripts/enrich_edges_from_reports.py --input reports/parsed_*.json --limit {len(affected_reports)}")
            else:
                print("재처리 대상 리포트가 없습니다.")
        
        else:
            parser.print_help()
    
    finally:
        db.close()


if __name__ == "__main__":
    main()

