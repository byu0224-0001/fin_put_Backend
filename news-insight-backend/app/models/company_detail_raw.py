"""
보완 테이블: Raw 데이터 저장
원본 데이터 보관 (재처리 가능하도록)
"""
from sqlalchemy import Column, String, Text, DateTime, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db import Base


class CompanyDetailRaw(Base):
    """Raw 데이터 저장 모델 (원본 보관)"""
    __tablename__ = "company_details_raw"
    
    id = Column(String(100), primary_key=True)  # ticker_source_year 형식 (예: "005930_DART_2024")
    ticker = Column(String(20), ForeignKey('stocks.ticker'), nullable=False, index=True)
    source = Column(String(50), nullable=False)  # 예: "DART_2024"
    year = Column(String(4), nullable=False, index=True)  # 예: "2024"
    raw_html = Column(Text)  # 원본 HTML (TOAST 자동 압축)
    raw_markdown = Column(Text)  # 변환된 Markdown
    raw_json = Column(JSONB)  # LLM 응답 원본 JSON
    processing_status = Column(String(20), default="PENDING", index=True)  # PENDING, PROCESSING, COMPLETED, FAILED
    fetched_at = Column(DateTime, default=datetime.utcnow)
    processed_at = Column(DateTime)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 관계 설정
    stock = relationship("Stock", foreign_keys=[ticker])
    
    # 복합 인덱스
    __table_args__ = (
        Index('idx_raw_ticker_source_year', 'ticker', 'source', 'year'),
        Index('idx_raw_processing_status', 'processing_status'),
    )
    
    def __repr__(self):
        return f"<CompanyDetailRaw(ticker={self.ticker}, source='{self.source}', status='{self.processing_status}')>"

