"""
보완 테이블: 버전 관리
기업 정보의 버전 추적
"""
from sqlalchemy import Column, String, Integer, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db import Base


class CompanyDetailVersion(Base):
    """기업 정보 버전 관리 모델"""
    __tablename__ = "company_details_version"
    
    id = Column(String(100), primary_key=True)  # ticker_year_version 형식 (예: "005930_2024_v1")
    ticker = Column(String(20), ForeignKey('stocks.ticker'), nullable=False, index=True)
    year = Column(String(4), nullable=False, index=True)  # 예: "2024"
    version = Column(Integer, nullable=False, default=1)  # 버전 번호
    model_version = Column(String(50))  # 예: "gpt-4o-mini", "gpt-5-mini"
    processed_at = Column(DateTime, default=datetime.utcnow)
    is_current = Column(String(1), default="Y", index=True)  # Y, N
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 관계 설정
    stock = relationship("Stock", foreign_keys=[ticker])
    
    # 복합 인덱스
    __table_args__ = (
        Index('idx_version_ticker_year', 'ticker', 'year'),
        Index('idx_version_current', 'ticker', 'year', 'is_current'),
    )
    
    def __repr__(self):
        return f"<CompanyDetailVersion(ticker={self.ticker}, year={self.year}, version={self.version})>"

