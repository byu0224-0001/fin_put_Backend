"""
보완 테이블: Traceability
처리 과정 추적 및 로깅
"""
from sqlalchemy import Column, String, Integer, Text, DateTime, ForeignKey, Index
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db import Base


class ProcessingLog(Base):
    """처리 로그 모델 (Traceability)"""
    __tablename__ = "processing_log"
    
    id = Column(String(150), primary_key=True)  # ticker_step_timestamp 형식
    ticker = Column(String(20), ForeignKey('stocks.ticker'), nullable=False, index=True)
    step = Column(String(50), nullable=False, index=True)  # 예: "DART_FETCH", "HTML_CLEAN", "LLM_EXTRACT", "DB_SAVE"
    status = Column(String(20), nullable=False, index=True)  # SUCCESS, FAILED, RETRY
    error_message = Column(Text)  # 에러 메시지 (실패 시)
    retry_count = Column(Integer, default=0)  # 재시도 횟수
    duration_ms = Column(Integer)  # 처리 시간 (밀리초)
    extra_metadata = Column(Text)  # 추가 메타데이터 (JSON 문자열) - metadata는 예약어라서 변경
    created_at = Column(DateTime, default=datetime.utcnow, index=True)  # 인덱스 추가 (조회 성능)
    
    # 관계 설정
    stock = relationship("Stock", foreign_keys=[ticker])
    
    # 복합 인덱스
    __table_args__ = (
        Index('idx_log_ticker_step', 'ticker', 'step'),
        Index('idx_log_status_created', 'status', 'created_at'),
        Index('idx_log_ticker_created', 'ticker', 'created_at'),
    )
    
    def __repr__(self):
        return f"<ProcessingLog(ticker={self.ticker}, step='{self.step}', status='{self.status}')>"

