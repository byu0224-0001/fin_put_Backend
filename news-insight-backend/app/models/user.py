from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from datetime import datetime
from app.db import Base


class UserInsight(Base):
    """사용자 인사이트 모델 (태그, 메모 등)"""
    __tablename__ = "user_insights"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    article_id = Column(Integer, ForeignKey("articles.id", ondelete="CASCADE"), nullable=False, index=True)
    memo = Column(Text)  # 사용자 메모
    tags = Column(String(500))  # 사용자 태그 (쉼표로 구분)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 관계
    article = relationship("Article", back_populates="insights")
    
    def __repr__(self):
        return f"<UserInsight(article_id={self.article_id}, tags='{self.tags}')>"

