"""
Axis 4: Quantitative Data (The Fuel) - Macro
경제 지표 시계열 모델
"""
from sqlalchemy import Column, String, Float, DateTime, Date, ForeignKey, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db import Base


class EconomicHistory(Base):
    """경제 지표 시계열 모델 (Macro Indices)"""
    __tablename__ = "economic_history"
    
    id = Column(String(100), primary_key=True)  # code_date 형식 (예: "RATE_FED_2024-01-01")
    code = Column(String(50), ForeignKey('economic_variables.code'), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    value = Column(Float, nullable=False)
    unit = Column(String(20))  # 예: "PERCENT", "USD", "INDEX"
    source = Column(String(50))  # 예: "FRED", "BOK", "MANUAL"
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 관계 설정
    variable = relationship("EconomicVariable", foreign_keys=[code])
    
    # 복합 인덱스 (조회 성능 최적화)
    __table_args__ = (
        Index('idx_economic_history_code_date', 'code', 'date'),
    )
    
    def __repr__(self):
        return f"<EconomicHistory(code={self.code}, date={self.date}, value={self.value})>"

