"""
Axis 5: Logic & Links (The KG Layer)
Knowledge Graph 관계 모델
"""
from sqlalchemy import Column, String, Float, DateTime, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db import Base


class Edge(Base):
    """Knowledge Graph 관계 모델 (Entity Relationships)"""
    __tablename__ = "edges"
    
    id = Column(String(150), primary_key=True)  # source_target_relation 형식
    source_id = Column(String(100), nullable=False, index=True)  # 예: "RATE_FED", "005930"
    target_id = Column(String(100), nullable=False, index=True)  # 예: "SEC_FINANCE", "AAPL"
    relation_type = Column(String(50), nullable=False, index=True)  # 예: SUPPLIES_TO, AFFECTS, HEDGES_AGAINST
    
    # ⭐ 엔티티 타입 (KG 연결 명확화)
    source_type = Column(String(20), nullable=True, index=True)  # COMPANY, DRIVER, SECTOR, TAG, ECONVAR
    target_type = Column(String(20), nullable=True, index=True)  # COMPANY, DRIVER, SECTOR, TAG, ECONVAR
    
    # ⭐ 속성 (JSONB) - Driver Tags, weight, direction 등 포함
    properties = Column(JSONB, default=dict, nullable=True)  # {"driver_tags": [...], "weight": 0.8, ...}
    
    weight = Column(Float, default=1.0)  # 관계 강도 (0.0 ~ 1.0)
    evidence = Column(JSONB, nullable=True)  # 증거 정보 (구조화된 JSON 데이터)
    source = Column(String(50))  # 예: "DART", "LLM", "RULE_BASED"
    direction = Column(String(10), default="DIRECTED")  # DIRECTED, UNDIRECTED
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 복합 인덱스 (조회 성능 최적화)
    __table_args__ = (
        Index('idx_edges_source_target', 'source_id', 'target_id'),
        Index('idx_edges_relation_type', 'relation_type'),
        Index('idx_edges_source_type', 'source_type'),
        Index('idx_edges_target_type', 'target_type'),
    )
    
    def __repr__(self):
        return f"<Edge({self.source_id} -[{self.relation_type}]-> {self.target_id})>"

