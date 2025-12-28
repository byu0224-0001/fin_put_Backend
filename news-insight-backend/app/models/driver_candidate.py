"""
드라이버 후보 모델
모르는 단어/표현이 나오면 후보로 등록하고, 승인 후 master에 반영
"""
from sqlalchemy import Column, Integer, String, Text, Float, DateTime, Index
from sqlalchemy.dialects.postgresql import TIMESTAMP
from datetime import datetime
from app.db import Base


class DriverCandidate(Base):
    """드라이버 후보 모델"""
    __tablename__ = "driver_candidates"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    candidate_text = Column(String(200), nullable=False, index=True)  # 원본 텍스트
    suggested_driver_code = Column(String(50), nullable=True, index=True)  # 제안된 드라이버 코드
    confidence = Column(Float, default=0.0)  # 제안 신뢰도
    method = Column(String(50))  # 제안 방법 (LLM, RULE, MANUAL)
    
    # 승인 상태
    status = Column(String(20), default='PENDING', index=True)  # PENDING, APPROVED, REJECTED, MERGED
    approved_driver_code = Column(String(50), nullable=True)  # 승인된 드라이버 코드
    merged_to_driver_code = Column(String(50), nullable=True)  # 병합 대상 드라이버 코드
    synonym_for_driver_code = Column(String(50), nullable=True)  # 동의어로 추가할 드라이버 코드
    
    # 메타데이터
    source_report_id = Column(String(100), nullable=True, index=True)  # 출처 리포트 ID
    source_report_title = Column(Text, nullable=True)  # 출처 리포트 제목
    source_report_text = Column(Text, nullable=True)  # 출처 리포트 텍스트
    context_sentence = Column(Text, nullable=True)  # 후보가 발견된 문장
    
    # 승인 정보
    approved_by = Column(String(100), nullable=True)  # 승인자
    approved_at = Column(DateTime, nullable=True)  # 승인 시각
    rejection_reason = Column(Text, nullable=True)  # 거절 사유
    
    # 통계
    occurrence_count = Column(Integer, default=1)  # 발견 횟수
    first_seen_at = Column(DateTime, default=datetime.utcnow)  # 최초 발견 시각
    last_seen_at = Column(DateTime, default=datetime.utcnow)  # 최근 발견 시각
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 복합 인덱스
    __table_args__ = (
        Index('uq_driver_candidates_text_report', 'candidate_text', 'source_report_id', unique=True),
    )
    
    def __repr__(self):
        return f"<DriverCandidate(id={self.id}, text='{self.candidate_text}', status='{self.status}')>"

