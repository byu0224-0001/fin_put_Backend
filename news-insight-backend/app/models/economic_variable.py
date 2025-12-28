"""
Axis 1: Ontology (The Brain)
경제 변수 온톨로지 모델
"""
from sqlalchemy import Column, String, Text, DateTime
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime
from app.db import Base


class EconomicVariable(Base):
    """경제 변수 모델 (The Brain)"""
    __tablename__ = "economic_variables"
    
    code = Column(String(50), primary_key=True, index=True)  # 예: RATE_FED, CPI_US, DRAM_ASP
    name_ko = Column(String(200), nullable=False)
    category = Column(String(50), index=True)  # MACRO, POLICY, RISK, RATE, SECTOR 등
    layer = Column(String(20), index=True)  # global, domestic
    synonyms = Column(JSONB, default=[])  # JSON 배열: ["FFR", "Fed 금리", "연준 금리"]
    description = Column(Text)
    
    # ⭐ GPT 제안 필드 추가 (P/Q/C 분류용)
    type = Column(String(1), index=True)  # 'P' (Price), 'Q' (Quantity), 'C' (Cost)
    source_hint = Column(String(100))  # 'FRED', 'ECOS', 'BLOOMBERG', 'CUSTOM' 등
    unit = Column(String(50))  # 'USD/GB', 'USD/ton', '%' 등
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<EconomicVariable(code={self.code}, name_ko='{self.name_ko}', type={self.type})>"

