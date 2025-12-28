"""
Granular Sector 모델
Sub-sector 하위의 세분화된 산업 분류 정의
"""
from sqlalchemy import Column, String, Text, DateTime, Boolean, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db import Base


class SectorGranular(Base):
    """Granular Sector 모델 (Sub-sector 하위 세분화)"""
    __tablename__ = "sector_granular"
    
    code = Column(String(100), primary_key=True)  # 예: 'SEMI_MEMORY_HBM', 'BATTERY_CATHODE_NCM'
    major_sector = Column(String(50), nullable=False, index=True)  # 예: 'SEC_SEMI'
    sub_sector = Column(String(50), nullable=False, index=True)  # 예: 'MEMORY'
    display_name_ko = Column(String(200), nullable=False)  # 예: 'HBM 메모리'
    display_name_en = Column(String(200))  # 예: 'HBM Memory'
    value_chain = Column(String(20), index=True)  # 'UPSTREAM', 'MIDSTREAM', 'DOWNSTREAM'
    description = Column(Text)
    keywords = Column(JSONB, default=[])  # 키워드 배열
    is_active = Column(Boolean, default=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 관계 설정
    econvar_links = relationship(
        "EconVarGranularLink",
        back_populates="granular_sector",
        cascade="all, delete-orphan"
    )
    
    # 복합 인덱스
    __table_args__ = (
        Index('idx_sector_granular_major_sub', 'major_sector', 'sub_sector'),
    )
    
    def __repr__(self):
        return f"<SectorGranular(code={self.code}, display_name_ko='{self.display_name_ko}')>"

