"""
EconVar ↔ Granular Sector 연결 매핑 모델
경제 변수와 Granular Sector 간의 인과 관계 정의
"""
from sqlalchemy import Column, String, Text, DateTime, Numeric, ForeignKey, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db import Base


class EconVarGranularLink(Base):
    """경제 변수 ↔ Granular Sector 연결 매핑"""
    __tablename__ = "econvar_granular_link"
    
    econvar_code = Column(String(50), ForeignKey('economic_variables.code', ondelete='CASCADE'), 
                         primary_key=True, index=True)
    granular_code = Column(String(100), ForeignKey('sector_granular.code', ondelete='CASCADE'), 
                          primary_key=True, index=True)
    polarity = Column(String(1))  # '+' or '-' (변수↑ 시 긍정/부정 영향)
    sensitivity = Column(Numeric(5, 2))  # 0.0 ~ 1.0 (민감도 가중치)
    note = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 관계 설정 (lazy loading으로 순환 참조 방지)
    economic_variable = relationship("EconomicVariable", backref="granular_links", lazy="select")
    granular_sector = relationship("SectorGranular", back_populates="econvar_links", lazy="select")
    
    # 인덱스
    __table_args__ = (
        Index('idx_econvar_granular_polarity', 'polarity'),
    )
    
    def __repr__(self):
        return f"<EconVarGranularLink(econvar={self.econvar_code}, granular={self.granular_code}, polarity={self.polarity})>"

