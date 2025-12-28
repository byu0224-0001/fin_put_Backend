from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy.dialects.postgresql import ARRAY
from datetime import datetime
from app.db import Base


class Stock(Base):
    """상장기업 정보 모델"""
    __tablename__ = "stocks"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    stock_name = Column(String(200), nullable=False, index=True)
    ticker = Column(String(20), nullable=False, unique=True, index=True)
    market = Column(String(20))  # KOSPI, KOSDAQ, NASDAQ, NYSE
    industry_raw = Column(String(200), nullable=True, index=True)  # KRX 업종명 (예: "반도체 제조업", "통신 및 방송 장비 제조업")
    synonyms = Column(ARRAY(String))  # 약칭, 브랜드명 등 (선택적)
    country = Column(String(10))  # KR, US
    
    # DART 고유번호 (8자리) - 이중 매핑 전략용
    dart_corp_code = Column(String(8), nullable=True, index=True)  # DART 고유번호 (캐싱용)
    
    # 시가총액 (원 단위) - V1.5.5: 설명용 메타 정보
    # ⚠️ 정책: Exposure는 '구조적 민감도'이며, market_cap은 '해석을 돕는 맥락 정보'다.
    # 두 값은 계산적으로 결합하지 않는다.
    # - ✅ 허용: 해석 문장, Tie-breaking (순위 변경 없음)
    # - ❌ 금지: 노출도 계산에 곱하기, Top-N 선정에 직접 반영
    market_cap = Column(Integer, nullable=True, index=True)  # 시가총액 (원 단위, NULL 허용)
    
    # 우선주 관리 필드 (Phase 2) - DB에 컬럼이 없으므로 주석 처리
    # is_preferred_stock = Column(Boolean, default=False, nullable=False, index=True)  # 우선주 여부
    # parent_ticker = Column(String(20), ForeignKey('stocks.ticker'), nullable=True, index=True)  # 본주 티커 (Self-referencing)
    
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<Stock(ticker={self.ticker}, name='{self.stock_name}')>"

