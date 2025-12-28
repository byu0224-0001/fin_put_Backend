"""
증권사 리포트 모델 (Phase 2.0)
"""
from sqlalchemy import Column, Integer, String, Text, Date, DateTime, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db import Base


class BrokerReport(Base):
    """증권사 리포트 모델"""
    __tablename__ = "broker_reports"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    ticker = Column(String(10), ForeignKey('stocks.ticker'), nullable=True, index=True)  # NULL 허용 (산업 리포트)
    report_id = Column(String(100), unique=True, nullable=False, index=True)  # 고유 ID
    broker_name = Column(String(100))  # 증권사명
    analyst_name = Column(String(100))  # 애널리스트명
    report_title = Column(Text)  # 리포트 제목
    report_date = Column(Date, index=True)  # 리포트 발행일
    report_type = Column(String(50))  # 리포트 유형
    target_price = Column(Integer)  # 목표주가
    rating = Column(String(20))  # 투자의견
    report_url = Column(Text)  # 리포트 원본 URL
    report_content = Column(Text)  # 리포트 본문
    summary = Column(Text)  # 리포트 요약
    key_points = Column(JSONB)  # 핵심 포인트 (JSONB)
    source = Column(String(50), default='naver')  # 수집 소스
    
    # P0+ 보강: 처리 상태 및 메타데이터
    processing_status = Column(String(20), default='WAITING', index=True)  # 처리 상태
    report_uid = Column(String(64), index=True)  # 원본 단위 식별자
    parser_version = Column(String(20), default='v1.0')  # 파서 버전
    
    extracted_at = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 관계 설정
    stock = relationship("Stock", foreign_keys=[ticker])
    
    # 복합 인덱스
    __table_args__ = (
        Index('idx_broker_reports_processing_status', 'processing_status'),
        Index('idx_broker_reports_uid', 'report_uid'),
    )
    
    def __repr__(self):
        return f"<BrokerReport(report_id={self.report_id}, status={self.processing_status})>"

