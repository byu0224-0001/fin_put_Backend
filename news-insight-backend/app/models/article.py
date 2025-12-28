from sqlalchemy import Column, Integer, String, Text, DateTime, JSON, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime
from app.db import Base


class Article(Base):
    """뉴스 기사 모델"""
    __tablename__ = "articles"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    title = Column(String(500), nullable=False, index=True)
    source = Column(String(200), nullable=False)
    link = Column(String(1000), unique=True, nullable=False, index=True)
    summary = Column(Text)  # RSS에서 제공하는 요약
    image_url = Column(String(1000), nullable=True)  # RSS에서 추출한 이미지 URL
    published_at = Column(DateTime, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 관계
    summary_data = relationship("Summary", back_populates="article", uselist=False)
    insights = relationship("UserInsight", back_populates="article")
    
    def __repr__(self):
        return f"<Article(id={self.id}, title='{self.title[:50]}...')>"


class Summary(Base):
    """AI 분석 요약 모델"""
    __tablename__ = "summaries"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    article_id = Column(Integer, ForeignKey("articles.id", ondelete="CASCADE"), unique=True, nullable=False)
    summary = Column(Text)  # AI 생성 요약
    keywords = Column(JSONB)  # 키워드 리스트 (JSON)
    entities = Column(JSONB)  # 개체명 (JSON)
    bullet_points = Column(JSONB)  # 핵심 bullet points (JSON)
    sentiment = Column(String(50))  # 감성 분석 (positive/negative/neutral)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 관계
    article = relationship("Article", back_populates="summary_data")
    
    def __repr__(self):
        return f"<Summary(article_id={self.article_id})>"

