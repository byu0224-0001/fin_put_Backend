"""
Axis 2: Company Master (The Body) - 확장
투자 섹터 분류 모델 (Multi-Sector 지원)
"""
from sqlalchemy import Column, String, Text, DateTime, ForeignKey, Float, Boolean, Integer, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db import Base


class InvestorSector(Base):
    """투자 섹터 분류 모델 (Multi-Sector + PSW)"""
    __tablename__ = "investor_sector"
    
    # Primary Key: ticker_sector 조합 (Multi-Sector 지원)
    id = Column(String(100), primary_key=True)  # "005930_SEC_SEMI_MEMORY"
    ticker = Column(String(20), ForeignKey('stocks.ticker'), nullable=False, index=True)  # unique 제거
    
    # 기존 필드 (하위 호환성 유지)
    major_sector = Column(String(50), nullable=False, index=True)  # 예: "SEC_SEMI" (반도체)
    sub_sector = Column(String(50), nullable=True)  # 예: "MEMORY" (메모리)
    
    # ⭐ Phase 2: 5단계 밸류체인 세분화 (단순화 버전)
    # 기존 value_chain 컬럼을 재사용: "UPSTREAM", "MIDSTREAM", "DOWNSTREAM" (하위 호환성)
    # 새로운 5단계: "UPSTREAM", "MID_HARD", "MID_SOFT", "DOWN_BIZ", "DOWN_SERVICE" (top1)
    value_chain = Column(String(50), nullable=True)  # 주요 밸류체인 (top1)
    value_chain_detail = Column(String(50), nullable=True)  # 보조 라벨 (top2, gap < 0.1일 때만)
    value_chain_confidence = Column(Float, nullable=True)  # top1_score - top2_score (0.0~1.0)
    
    # ⭐ 새로운 계층 구조 필드 (L1 → L2 → L3)
    sector_l1 = Column(String(50), nullable=True, index=True)  # 예: "SEC_FINANCE", "SEC_SEMI" (대분류)
    sector_l2 = Column(String(50), nullable=True, index=True)  # 예: "PG", "MEMORY" (중분류)
    sector_l3_tags = Column(JSONB, nullable=True, default=[])  # 예: ["ONLINE_PG", "VAN"] (소분류 태그 리스트)
    confidence_l2 = Column(Float, nullable=True)  # L2 분류 신뢰도 (0.0~1.0)
    confidence_l3 = Column(JSONB, nullable=True)  # L3 태그별 신뢰도 (선택적)
    business_model_role = Column(String(50), nullable=True)  # 금융 섹터용: "SERVICE", "INVESTMENT", "HOLDING"
    
    # ⭐ 핵심: PSW (Price Sensitivity Weight)
    psw = Column(Float, default=0.0)  # 0.0 ~ 1.0 (주가 민감도 가중치)
    is_primary = Column(Boolean, default=False)  # Primary 섹터 여부
    
    # ⭐ Multi-label 지원: 섹터별 가중치
    sector_weight = Column(Float, default=0.5)  # 0.0 ~ 1.0 (섹터별 가중치)
    
    # 분류 메타데이터
    classification_method = Column(String(20))  # "RULE_BASED", "LLM", "MANUAL", "PRODUCT_BASED", "PSW_CALCULATED", "ENSEMBLE", "RULE_BASED_HOLDING", "RULE_BASED_REIT", "RULE_BASED_SPAC"
    confidence = Column(String(50))  # "HIGH", "MEDIUM", "LOW", "VERY_LOW", "HOLD:HOLD_*" (HOLD 사유 포함)
    fallback_used = Column(String(20), nullable=True)  # ⭐ Fallback 사용 여부: True/False (현재 VARCHAR, BOOLEAN 용도)
    fallback_type = Column(String(20), nullable=True)  # ⭐ Fallback 타입: "RULE", "TOP1", "KRX", "UNKNOWN" (NULL = 정상 분류)
    
    # Rule-based 분류 메타데이터 (학습 데이터 수집용)
    rule_version = Column(String(20), nullable=True)  # Rule 버전 (예: "v1.0")
    rule_confidence = Column(Float, nullable=True)  # Rule 신뢰도 (0.0~1.0)
    training_label = Column(Boolean, default=False)  # 학습용 라벨 여부 (신뢰도 0.7 이상만 True)
    
    # ⭐ 앙상블 메타데이터 (멀티 모델 앙상블 파이프라인)
    rule_score = Column(Float, nullable=True)  # Rule-based 점수
    embedding_score = Column(Float, nullable=True)  # KorFinMTEB 점수
    bge_score = Column(Float, nullable=True)  # BGE-M3 점수
    gpt_score = Column(Float, nullable=True)  # GPT 점수
    ensemble_score = Column(Float, nullable=True)  # 최종 앙상블 점수
    classification_reasoning = Column(Text, nullable=True)  # 분류 근거 (GPT가 제공)
    
    # ⭐ 레벨 2: 인과 구조 분석 메타데이터 (GPT 인과 Reasoning 생성)
    causal_structure = Column(JSONB, nullable=True)  # 인과 구조 분석 결과
    # 예: {
    #   "upstream_impacts": ["웨이퍼 공급사", "장비 제조사"],
    #   "downstream_impacts": ["AI 서버 제조사", "스마트폰 제조사"],
    #   "key_drivers": [{"var": "DRAM ASP", "type": "P", "description": "메모리 가격이 매출에 직접 영향"}],
    #   "granular_tags": ["HBM", "AI 서버"],
    #   "risk_factors": ["재고 과다", "가격 하락 사이클"],
    #   "opportunity_factors": ["AI 데이터센터 확대", "HBM 수요 증가"]
    # }
    investment_insights = Column(Text, nullable=True)  # 투자 관점 시사점
    # 예: "반도체 섹터 중에서도 AI 서버용 메모리에 집중되어 있어, AI 데이터센터 투자 확대 시 직접적 수혜 가능..."
    
    # ⭐ 레벨 2.5: Exposure Drivers (경제 변수 노출도)
    exposure_drivers = Column(JSONB, nullable=True, default=[])  # 표준 드라이버 (정확한 경제 변수)
    # 예: [
    #   {"var": "DRAM ASP", "type": "P", "code": "DRAM_ASP", "description": "메모리 가격이 매출에 직접 영향"},
    #   {"var": "AI 서버 CAPEX", "type": "Q", "code": "AI_SERVER_CAPEX", "description": "AI 투자 확대로 수요 증가"}
    # ]
    supporting_drivers = Column(JSONB, nullable=True, default=[])  # GPT 설명용 드라이버 (일반적인 설명)
    # 예: [
    #   {"var": "AI 서버 투자 증가", "type": "Q", "description": "AI 데이터센터 확대로 수요 증가"}
    # ]
    
    # ⭐ Boosting 로그 (Anchor/KG Edge Boosting 메타데이터)
    boosting_log = Column(JSONB, nullable=True)  # Boosting 적용 로그
    # 예: {
    #   "anchor_applied": true,
    #   "kg_applied": false,
    #   "reason": "Top-2 gap (0.02) < threshold, Anchor 발견",
    #   "multiplier": 0.2,
    #   "final_boost": 0.012
    # }
    
    # 계산 메타데이터 (PSW 계산용 - Phase 3에서 사용)
    psw_calculation_date = Column(DateTime, nullable=True)  # PSW 계산 일자
    psw_correlation = Column(Float, nullable=True)  # corr(주가, 섹터 지표)
    psw_sample_size = Column(Integer, nullable=True)  # 상관관계 계산 샘플 수
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 관계 설정
    stock = relationship("Stock", foreign_keys=[ticker])
    
    # 복합 인덱스
    __table_args__ = (
        Index('idx_ticker_sector', 'ticker', 'major_sector', 'sub_sector'),
        Index('idx_ticker_primary', 'ticker', 'is_primary'),
    )
    
    def get_sector_l1(self) -> str:
        """하위 호환성: major_sector → sector_l1"""
        return self.sector_l1 or self.major_sector
    
    def get_sector_l2(self) -> str:
        """하위 호환성: sub_sector → sector_l2"""
        return self.sector_l2 or self.sub_sector
    
    def get_sector_l3_tags(self) -> list:
        """L3 태그 리스트 반환 (granular_tags에서도 가져올 수 있음)"""
        if self.sector_l3_tags:
            return self.sector_l3_tags
        # causal_structure에서 granular_tags 가져오기 (하위 호환성)
        if self.causal_structure and isinstance(self.causal_structure, dict):
            return self.causal_structure.get('granular_tags', [])
        return []
    
    def __repr__(self):
        return f"<InvestorSector(ticker={self.ticker}, major_sector='{self.major_sector}', sub_sector='{self.sub_sector}', is_primary={self.is_primary})>"

