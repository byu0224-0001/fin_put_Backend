"""
Industry Sector Mapping Model

KRX 업종(Sector) → 투자 섹터 매핑 테이블
"""
from sqlalchemy import Column, String, Text, DateTime, Index
from datetime import datetime
from app.db import Base


class IndustrySectorMapping(Base):
    """KRX 업종 → 투자 섹터 매핑 모델"""
    __tablename__ = "industry_sector_mapping"
    
    id = Column(String(100), primary_key=True)  # krx_sector 형식 (예: "반도체 제조업")
    krx_sector = Column(String(200), nullable=False, unique=True, index=True)  # KRX 업종명
    krx_industry = Column(String(200), nullable=True)  # KRX 산업명 (선택적, 더 세부 분류)
    
    # 투자 섹터 매핑
    major_sector = Column(String(50), nullable=True, index=True)  # 주요 섹터 (예: "테크")
    sub_sector = Column(String(50), nullable=True)  # 서브 섹터 (예: "반도체/디스플레이")
    
    # 매핑 메타데이터
    mapping_method = Column(String(20), default="RULE_BASED")  # "RULE_BASED", "MANUAL", "LLM"
    confidence = Column(String(20), default="MEDIUM")  # "HIGH", "MEDIUM", "LOW"
    notes = Column(Text, nullable=True)  # 매핑 노트 (예: "일부 기업은 DART 내용 기반 재분류 필요")
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 인덱스
    __table_args__ = (
        Index('idx_mapping_sector', 'major_sector', 'sub_sector'),
    )
    
    def __repr__(self):
        return f"<IndustrySectorMapping(krx_sector='{self.krx_sector}', major_sector='{self.major_sector}')>"

