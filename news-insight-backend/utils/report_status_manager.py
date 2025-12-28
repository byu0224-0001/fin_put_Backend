"""
Report Status Manager (P0+)

리포트 처리 상태 관리 (Idempotency 2단계 - Report-level)
DB 기반 영속화 (파일 기반에서 전환)
"""
import sys
from pathlib import Path
from typing import Dict, Optional, Any
import json
import hashlib
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
from sqlalchemy.orm import Session

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ReportStatusManager:
    """
    리포트 처리 상태 관리 (DB 기반)
    
    상태: WAITING, PARSED_OK, PARSED_HOLD, MATCHED_OK, MATCHED_HOLD,
          EXTRACTED_OK, EXTRACTED_HOLD, ENRICHED, FAILED_RETRYABLE, FAILED_FATAL
    
    ENRICHED면 LLM 호출 없이 스킵 (비용 절감)
    """
    
    def __init__(self, db: Optional[Session] = None, status_file: Optional[Path] = None):
        """
        Args:
            db: DB 세션 (None이면 자동 생성)
            status_file: 상태 파일 경로 (백업/디버깅용, 선택)
        """
        self.db = db
        self.status_file = status_file
        if status_file:
            self.status_file.parent.mkdir(parents=True, exist_ok=True)
    
    def _get_db(self) -> Session:
        """DB 세션 가져오기"""
        if self.db:
            return self.db
        from app.db import SessionLocal
        return SessionLocal()
    
    def _close_db_if_created(self, db: Session):
        """자동 생성한 DB 세션만 닫기"""
        if not self.db:
            db.close()
    
    def generate_report_uid(self, report: Dict[str, Any]) -> str:
        """
        리포트 원본 단위 식별자 생성 (report_uid)
        
        Args:
            report: 리포트 데이터
        
        Returns:
            report_uid (sha256(pdf_url or naver_doc_id))
        """
        from extractors.edge_enricher import generate_report_uid
        
        pdf_url = report.get("pdf_url")
        naver_doc_id = report.get("url")  # 임시로 URL 사용
        
        return generate_report_uid(pdf_url=pdf_url, naver_doc_id=naver_doc_id)
    
    def get_status(self, report: Dict[str, Any]) -> Optional[str]:
        """
        리포트 처리 상태 조회 (DB에서)
        
        Args:
            report: 리포트 데이터
        
        Returns:
            상태 또는 None
        """
        db = self._get_db()
        try:
            # broker_reports 테이블이 없을 수 있으므로 예외 처리
            from sqlalchemy import inspect
            inspector = inspect(db.bind)
            if "broker_reports" not in inspector.get_table_names():
                logger.debug("broker_reports 테이블이 없습니다. 상태 조회 스킵")
                return None
            
            from app.models.broker_report import BrokerReport
            
            # report_uid로 조회
            report_uid = self.generate_report_uid(report)
            broker_report = db.query(BrokerReport).filter(
                BrokerReport.report_uid == report_uid
            ).first()
            
            if broker_report:
                return broker_report.processing_status
            
            # report_id로 조회 (fallback)
            report_id = report.get("report_id")
            if report_id:
                broker_report = db.query(BrokerReport).filter(
                    BrokerReport.report_id == report_id
                ).first()
                if broker_report:
                    return broker_report.processing_status
            
            return None
        finally:
            self._close_db_if_created(db)
    
    def set_status(self, report: Dict[str, Any], status: str):
        """
        리포트 처리 상태 설정 (DB에 저장)
        
        Args:
            report: 리포트 데이터
            status: 상태
        """
        db = self._get_db()
        try:
            # broker_reports 테이블이 없을 수 있으므로 예외 처리
            try:
                from sqlalchemy import inspect
                inspector = inspect(db.bind)
                if "broker_reports" not in inspector.get_table_names():
                    logger.debug(f"broker_reports 테이블이 없습니다. 상태 설정 스킵 (status: {status})")
                    return
            except Exception as e:
                logger.debug(f"테이블 체크 실패 (정상일 수 있음): {e}")
                return
            
            from app.models.broker_report import BrokerReport
            
            report_uid = self.generate_report_uid(report)
            report_id = report.get("report_id")
            
            # 기존 리포트 조회 또는 생성
            broker_report = db.query(BrokerReport).filter(
                BrokerReport.report_uid == report_uid
            ).first()
            
            if not broker_report and report_id:
                broker_report = db.query(BrokerReport).filter(
                    BrokerReport.report_id == report_id
                ).first()
            
            if broker_report:
                old_status = broker_report.processing_status
                broker_report.processing_status = status
                broker_report.updated_at = datetime.utcnow()
                
                if old_status != status:
                    logger.info(f"리포트 상태 변경: {report_id or report_uid[:16]}... {old_status} → {status}")
            else:
                # 새 리포트 생성 (최소 정보만)
                broker_report = BrokerReport(
                    report_id=report_id or f"temp_{report_uid[:16]}",
                    report_uid=report_uid,
                    report_title=report.get("title", "")[:200],
                    processing_status=status,
                    parser_version="v1.0"
                )
                db.add(broker_report)
                logger.info(f"새 리포트 상태 저장: {report_id or report_uid[:16]}... → {status}")
            
            db.commit()
        except Exception as e:
            logger.error(f"상태 저장 실패: {e}", exc_info=True)
            db.rollback()
        finally:
            self._close_db_if_created(db)
    
    def is_enriched(self, report: Dict[str, Any]) -> bool:
        """
        리포트가 이미 Enriched 상태인지 확인
        
        Args:
            report: 리포트 데이터
        
        Returns:
            True면 이미 Enriched (스킵 가능)
        """
        status = self.get_status(report)
        return status == "ENRICHED"
    
    def should_skip_llm(self, report: Dict[str, Any]) -> bool:
        """
        LLM 호출을 스킵해야 하는지 확인
        
        Args:
            report: 리포트 데이터
        
        Returns:
            True면 LLM 호출 스킵 (비용 절감)
        """
        status = self.get_status(report)
        # ENRICHED면 스킵
        return status == "ENRICHED"

