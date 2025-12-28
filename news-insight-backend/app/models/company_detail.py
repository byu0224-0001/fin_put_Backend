"""
Axis 3: Qualitative Data (The Context)
기업 정성 정보 모델
"""
from sqlalchemy import Column, String, Text, DateTime, Date, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db import Base


class CompanyDetail(Base):
    """기업 정성 정보 모델 (The Context)"""
    __tablename__ = "company_details"
    
    id = Column(String(50), primary_key=True)  # ticker_source_year 형식 (예: "005930_DART_2024")
    ticker = Column(String(20), ForeignKey('stocks.ticker'), nullable=False, index=True)
    source = Column(String(50), nullable=False)  # 예: "DART_2024", "WIKI"
    latest_report_date = Column(Date, nullable=True)  # 최신 보고서 접수일자 (YYYY-MM-DD)
    biz_summary = Column(Text)  # 사업 요약 (3줄)
    products = Column(JSONB, default=[])  # 주요 제품 리스트
    clients = Column(JSONB, default=[])  # 주요 고객사 리스트
    supply_chain = Column(JSONB, default=[])  # 공급망 정보: [{"item": "원재료명", "supplier": "공급사명1, 공급사명2"}]
    raw_materials = Column(JSONB, default=[])  # 핵심 원재료 리스트 (하위 호환성 유지, supply_chain에서 파생 가능)
    financial_value_chain = Column(JSONB, default=None)  # 금융사 전용 밸류체인 정보
    risk_factors = Column(Text)  # 리스크 요인
    cost_structure = Column(Text)  # 비용 구조
    keywords = Column(JSONB, default=[])  # 핵심 키워드 리스트
    revenue_by_segment = Column(JSONB, default={})  # 사업부문별 매출 비중: {"부문명": 비중(%)}
    # notes 컬럼은 DB에 없으므로 제거 (필요시 DB 마이그레이션으로 추가 후 활성화)
    extracted_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 관계 설정
    stock = relationship("Stock", foreign_keys=[ticker])
    
    def __repr__(self):
        return f"<CompanyDetail(ticker={self.ticker}, source='{self.source}')>"

