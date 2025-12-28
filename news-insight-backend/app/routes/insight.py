from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional
from app.db import get_db, get_neo4j
from app.models.article import Article, Summary
from app.models.user import UserInsight
from app.services.graph import get_related_articles
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/insight", tags=["Insight"])


class FeedbackRequest(BaseModel):
    """피드백 요청 모델"""
    article_id: int
    memo: Optional[str] = None
    tags: Optional[str] = None  # 쉼표로 구분된 태그


@router.get("/")
def list_insights(
    limit: int = 20,
    offset: int = 0,
    db: Session = Depends(get_db)
):
    """
    내 인사이트 목록 조회
    
    Args:
        limit: 반환할 개수
        offset: 페이지 오프셋
        db: 데이터베이스 세션
    
    Returns:
        인사이트 목록
    """
    summaries = db.query(Summary).join(Article).order_by(Summary.created_at.desc()).offset(offset).limit(limit).all()
    
    return {
        "insights": [
            {
                "article_id": s.article_id,
                "title": s.article.title if s.article else None,
                "summary": s.summary,
                "keywords": s.keywords or [],
                "sentiment": s.sentiment,
                "created_at": s.created_at.isoformat(),
            }
            for s in summaries
        ],
        "total": db.query(Summary).count(),
        "limit": limit,
        "offset": offset
    }


@router.get("/{article_id}")
def get_insight(
    article_id: int,
    db: Session = Depends(get_db)
):
    """
    특정 기사의 인사이트 조회
    
    Args:
        article_id: 기사 ID
        db: 데이터베이스 세션
    
    Returns:
        인사이트 정보
    """
    summary = db.query(Summary).filter(Summary.article_id == article_id).first()
    
    if not summary:
        raise HTTPException(status_code=404, detail="인사이트를 찾을 수 없습니다.")
    
    article = db.query(Article).filter(Article.id == article_id).first()
    user_insight = db.query(UserInsight).filter(UserInsight.article_id == article_id).first()
    
    return {
        "article_id": article_id,
        "article": {
            "title": article.title if article else None,
            "source": article.source if article else None,
            "link": article.link if article else None,
            "published_at": article.published_at.isoformat() if article and article.published_at else None,
        },
        "summary": summary.summary,
        "keywords": summary.keywords or [],
        "entities": summary.entities or {},
        "bullet_points": summary.bullet_points or [],
        "sentiment": summary.sentiment,
        "user_feedback": {
            "memo": user_insight.memo if user_insight else None,
            "tags": user_insight.tags.split(",") if user_insight and user_insight.tags else [],
        } if user_insight else None,
        "created_at": summary.created_at.isoformat(),
    }


@router.post("/feedback")
def add_feedback(
    request: FeedbackRequest,
    db: Session = Depends(get_db)
):
    """
    태그/메모 추가 (피드백)
    
    Args:
        request: 피드백 요청
        db: 데이터베이스 세션
    
    Returns:
        업데이트 결과
    """
    # 기사 존재 확인
    article = db.query(Article).filter(Article.id == request.article_id).first()
    if not article:
        raise HTTPException(status_code=404, detail="기사를 찾을 수 없습니다.")
    
    # 기존 피드백 확인
    user_insight = db.query(UserInsight).filter(UserInsight.article_id == request.article_id).first()
    
    if user_insight:
        # 업데이트
        if request.memo is not None:
            user_insight.memo = request.memo
        if request.tags is not None:
            user_insight.tags = request.tags
        db.commit()
        db.refresh(user_insight)
    else:
        # 생성
        user_insight = UserInsight(
            article_id=request.article_id,
            memo=request.memo,
            tags=request.tags
        )
        db.add(user_insight)
        db.commit()
        db.refresh(user_insight)
    
    return {
        "updated": True,
        "article_id": request.article_id,
        "feedback": {
            "memo": user_insight.memo,
            "tags": user_insight.tags,
        }
    }


@router.get("/recommend/related")
def get_related_recommendations(
    entity: str,
    limit: int = 10,
    db: Session = Depends(get_db)
):
    """
    연관 뉴스 추천 (엔티티 기반)
    
    Args:
        entity: 엔티티 이름
        limit: 최대 반환 개수
        db: 데이터베이스 세션
    
    Returns:
        연관 기사 목록
    """
    try:
        # Neo4j에서 관련 기사 ID 조회
        neo4j_session = next(get_neo4j())
        article_ids = get_related_articles(neo4j_session, entity, limit)
        neo4j_session.close()
        
        if not article_ids:
            return {
                "entity": entity,
                "articles": [],
                "message": "관련 기사를 찾을 수 없습니다."
            }
        
        # PostgreSQL에서 기사 정보 조회
        articles = db.query(Article).filter(Article.id.in_(article_ids)).all()
        summaries = db.query(Summary).filter(Summary.article_id.in_(article_ids)).all()
        
        summary_dict = {s.article_id: s for s in summaries}
        
        return {
            "entity": entity,
            "articles": [
                {
                    "id": a.id,
                    "title": a.title,
                    "source": a.source,
                    "link": a.link,
                    "summary": summary_dict.get(a.id).summary if summary_dict.get(a.id) else None,
                    "keywords": summary_dict.get(a.id).keywords if summary_dict.get(a.id) else [],
                    "published_at": a.published_at.isoformat() if a.published_at else None,
                }
                for a in articles
            ]
        }
        
    except Exception as e:
        logger.error(f"연관 뉴스 추천 실패: {e}")
        raise HTTPException(status_code=500, detail=f"추천 실패: {str(e)}")

