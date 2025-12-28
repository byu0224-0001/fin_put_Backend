"""
기업명 별칭 매핑 모델

DART 보고서의 다양한 기업명 표기를 정규화하기 위한 별칭 테이블
"""
from sqlalchemy import Column, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db import Base


class CompanyAlias(Base):
    """기업명 별칭 매핑 모델"""
    __tablename__ = "company_aliases"
    
    id = Column(String(100), primary_key=True)  # alias_name_ticker 형식
    alias_name = Column(String(200), nullable=False, index=True)  # "SEMCO", "에스이엠"
    official_name = Column(String(200), nullable=False)  # "삼성전기"
    ticker = Column(String(20), ForeignKey('stocks.ticker'), nullable=True)  # "009150" 또는 NULL(비상장)
    company_type = Column(String(20), default="LISTED")  # LISTED, UNLISTED
    confidence = Column(String(20), default="HIGH")  # HIGH, MEDIUM, LOW
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 관계 설정
    stock = relationship("Stock", foreign_keys=[ticker])
    
    def __repr__(self):
        return f"<CompanyAlias(alias='{self.alias_name}', official='{self.official_name}', ticker={self.ticker})>"

