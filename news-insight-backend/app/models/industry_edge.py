"""
산업 리포트 인사이트 모델 (Phase 2.0 P0+)
"""
from sqlalchemy import Column, Integer, String, Text, Date, DateTime, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime
from app.db import Base


class IndustryEdge(Base):
    """산업 리포트 인사이트 모델"""
    __tablename__ = "industry_edges"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    report_id = Column(String(100), ForeignKey('broker_reports.report_id', ondelete='SET NULL'), nullable=False, index=True)
    # ⭐ 키 구조 명확화: source → target 관계
    source_driver_code = Column(String(50), index=True)  # 소스: 경제 변수 (driver_code)
    target_sector_code = Column(String(50), index=True)  # 타겟: 섹터 코드
    target_type = Column(String(50))  # SECTOR, VALUE_CHAIN, DRIVER
    relation_type = Column(String(50), default='INDUSTRY_DRIVEN_BY')  # 관계 타입
    logic_summary = Column(Text)  # 인사이트 요약 (analyst_logic)
    conditions = Column(JSONB)  # 조건부 설명 (positive/negative)
    key_sentence = Column(Text)  # 핵심 문장
    extraction_confidence = Column(String(20))  # HIGH, MED, LOW
    valid_from = Column(Date, index=True)  # 유효 시작일
    valid_to = Column(Date)  # 유효 종료일
    logic_fingerprint = Column(String(32), index=True)  # ⭐ 추가: 논리 요약 해시 (중복 체크용)
    # ⭐ P0+: Soft delete 컬럼 (과거 오매핑 데이터 회수용)
    is_active = Column(String(10), default='TRUE', index=True)  # TRUE/FALSE (문자열로 저장, 기본값 TRUE)
    disabled_reason = Column(Text)  # 비활성화 사유
    disabled_at = Column(DateTime)  # 비활성화 시각
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # ⭐ 중복 방지: (target_sector_code, logic_fingerprint) 조합으로 중복 체크 (driver_code 무관)
    __table_args__ = (
        Index('idx_industry_edges_source_target', 'source_driver_code', 'target_sector_code'),
        Index('idx_industry_edges_sector_logic', 'target_sector_code', 'logic_fingerprint'),
    )
    
    def __repr__(self):
        return f"<IndustryEdge(report_id={self.report_id}, {self.source_driver_code}→{self.target_sector_code})>"

