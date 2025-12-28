"""
Axis 4: Quantitative Data (The Fuel) - Stock
주가 시계열 모델
"""
from sqlalchemy import Column, String, Float, DateTime, Date, ForeignKey, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db import Base


class StockPrice(Base):
    """주가 시계열 모델 (Market Data)"""
    __tablename__ = "stock_prices"
    
    id = Column(String(100), primary_key=True)  # ticker_date 형식 (예: "005930_2024-01-01")
    ticker = Column(String(20), ForeignKey('stocks.ticker'), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float, nullable=False)
    volume = Column(Float)  # 거래량
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 관계 설정
    stock = relationship("Stock", foreign_keys=[ticker])
    
    # 복합 인덱스 (조회 성능 최적화)
    __table_args__ = (
        Index('idx_stock_prices_ticker_date', 'ticker', 'date'),
    )
    
    def __repr__(self):
        return f"<StockPrice(ticker={self.ticker}, date={self.date}, close={self.close})>"

