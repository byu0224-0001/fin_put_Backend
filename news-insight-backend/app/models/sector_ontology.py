"""
보완 테이블: 섹터 온톨로지 확장
섹터 계층 구조 및 Value Chain 관리
"""
from sqlalchemy import Column, String, Integer, Text, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db import Base


class SectorOntology(Base):
    """섹터 온톨로지 모델 (확장 가능한 구조)"""
    __tablename__ = "sector_ontology"
    
    sector_code = Column(String(50), primary_key=True)  # 예: "SEC_SEMI", "SEC_FINANCE"
    parent_sector = Column(String(50), ForeignKey('sector_ontology.sector_code'), nullable=True)  # 상위 섹터
    sector_name_ko = Column(String(200), nullable=False)
    sector_name_en = Column(String(200))
    value_chain_position = Column(String(50))  # UPSTREAM, MIDSTREAM, DOWNSTREAM
    description = Column(Text)
    gics_code = Column(String(20))  # GICS 코드 (선택적)
    level = Column(Integer, default=1)  # 계층 레벨 (1=최상위)
    is_active = Column(String(1), default="Y", index=True)  # Y, N
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 자기 참조 관계 (계층 구조)
    parent = relationship("SectorOntology", remote_side=[sector_code], backref="children")
    
    # 인덱스
    __table_args__ = (
        Index('idx_sector_parent', 'parent_sector'),
        Index('idx_sector_level', 'level'),
    )
    
    def __repr__(self):
        return f"<SectorOntology(code={self.sector_code}, name_ko='{self.sector_name_ko}')>"

