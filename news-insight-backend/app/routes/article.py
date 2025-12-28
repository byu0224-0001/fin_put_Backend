from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional
from app.db import get_db
from app.models.article import Article, Summary
from app.services.parser import parse_article_content
from app.services.summarizer import summarize_text
from app.celery_worker import analyze_article_task
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/article", tags=["Article"])


class ParseArticleRequest(BaseModel):
    """기사 파싱 요청 모델"""
    url: str
    text: Optional[str] = None  # WebView에서 추출한 텍스트


class TestSummarizeRequest(BaseModel):
    """요약 테스트 요청 모델"""
    text: str
    
    class Config:
        json_schema_extra = {
            "example": {
                "text": "삼성전자가 13일 2024년 4분기 실적을 발표했다. 영업이익은 전년 대비 15% 증가한 12조원을 기록했다. 반도체 부문이 호조를 보였으며, 특히 HBM(고대역폭메모리) 수요가 급증했다. 애플과의 협력도 확대될 전망이다. 시장 전문가들은 삼성전자의 성장세가 지속될 것으로 전망하고 있다."
            }
        }
        
        # JSON 파싱 오류 방지
        json_encoders = {
            str: lambda v: v
        }


class ParseArticleResponse(BaseModel):
    """기사 파싱 응답 모델"""
    task_id: str
    article_id: Optional[int] = None
    status: str


@router.post("/parse")
def parse_article(
    request: ParseArticleRequest,
    db: Session = Depends(get_db)
):
    """
    클라이언트(WebView)에서 기사 본문 전송 및 비동기 분석 시작
    
    Args:
        request: 파싱 요청 (url, text)
        db: 데이터베이스 세션
    
    Returns:
        task_id: Celery 작업 ID
    """
    try:
        # 기사 찾기 (DB에서)
        article = db.query(Article).filter(Article.link == request.url).first()
        
        if not article:
            raise HTTPException(status_code=404, detail="기사를 찾을 수 없습니다. 먼저 /feed에서 기사를 수집해주세요.")
        
        # 본문 텍스트 가져오기
        text = request.text
        if not text:
            # URL에서 직접 파싱
            text = parse_article_content(request.url)
            if not text:
                raise HTTPException(status_code=400, detail="기사 본문을 추출할 수 없습니다.")
        
        # Celery 작업 시작 (비동기)
        task = analyze_article_task.delay(article.id, text)
        
        return {
            "task_id": task.id,
            "article_id": article.id,
            "status": "pending"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"기사 파싱 요청 실패: {e}")
        raise HTTPException(status_code=500, detail=f"기사 파싱 실패: {str(e)}")


@router.get("/status/{task_id}")
def get_task_status(task_id: str):
    """
    비동기 작업 상태 확인
    
    Args:
        task_id: Celery 작업 ID
    
    Returns:
        작업 상태
    """
    from celery.result import AsyncResult
    from app.celery_worker import celery_app
    
    task = AsyncResult(task_id, app=celery_app)
    
    if task.state == 'PENDING':
        response = {
            'status': 'pending',
            'message': '작업 대기 중'
        }
    elif task.state == 'PROGRESS':
        response = {
            'status': 'processing',
            'message': '분석 진행 중',
            'progress': task.info.get('progress', 0)
        }
    elif task.state == 'SUCCESS':
        response = {
            'status': 'done',
            'message': '분석 완료',
            'result': task.result
        }
    else:  # FAILURE
        response = {
            'status': 'failed',
            'message': str(task.info) if isinstance(task.info, str) else '작업 실패'
        }
    
    return response


@router.get("/{article_id}")
def get_article(
    article_id: int,
    db: Session = Depends(get_db)
):
    """
    기사 상세 정보 조회
    
    Args:
        article_id: 기사 ID
        db: 데이터베이스 세션
    
    Returns:
        기사 정보 및 요약
    """
    article = db.query(Article).filter(Article.id == article_id).first()
    
    if not article:
        raise HTTPException(status_code=404, detail="기사를 찾을 수 없습니다.")
    
    summary = db.query(Summary).filter(Summary.article_id == article_id).first()
    
    # 키워드 검증 및 정리
    keywords = []
    if summary and summary.keywords:
        if isinstance(summary.keywords, list):
            keywords = summary.keywords
        elif isinstance(summary.keywords, dict):
            # JSONB가 dict로 파싱된 경우 처리
            keywords = list(summary.keywords.values()) if summary.keywords else []
    
    return {
        "article": {
            "id": article.id,
            "title": article.title,
            "source": article.source,
            "link": article.link,
            "summary": article.summary,
            "published_at": article.published_at.isoformat() if article.published_at else None,
        },
        "ai_summary": {
            "summary": summary.summary if summary else None,
            "keywords": keywords,  # 검증된 키워드 사용
            "entities": summary.entities if summary else {},
            "bullet_points": summary.bullet_points if summary else [],
            "sentiment": summary.sentiment if summary else None,
            "created_at": summary.created_at.isoformat() if summary else None,
        } if summary else None
    }


@router.get("/{article_id}/insight")
def get_article_insight(
    article_id: int,
    db: Session = Depends(get_db)
):
    """
    기사 인사이트 조회 (요약 결과)
    
    Args:
        article_id: 기사 ID
        db: 데이터베이스 세션
    
    Returns:
        인사이트 정보
    """
    summary = db.query(Summary).filter(Summary.article_id == article_id).first()
    
    if not summary:
        raise HTTPException(status_code=404, detail="인사이트가 아직 생성되지 않았습니다.")
    
    article = db.query(Article).filter(Article.id == article_id).first()
    
    # 키워드 검증 및 정리
    keywords = []
    if summary.keywords:
        if isinstance(summary.keywords, list):
            keywords = summary.keywords
        elif isinstance(summary.keywords, dict):
            keywords = list(summary.keywords.values()) if summary.keywords else []
    
    return {
        "article_id": article_id,
        "title": article.title if article else None,
        "link": article.link if article else None,
        "summary": summary.summary,
        "keywords": keywords,  # 검증된 키워드 사용
        "entities": summary.entities or {},
        "bullet_points": summary.bullet_points or [],
        "sentiment": summary.sentiment,
        "created_at": summary.created_at.isoformat(),
    }


@router.post("/test/summarize")
def test_summarize(request: TestSummarizeRequest):
    """
    텍스트 요약 및 키워드 추출 테스트 (직접 텍스트 입력)
    
    이 엔드포인트는 Swagger UI에서 빠르게 테스트하기 위한 용도입니다.
    실제 기사 데이터 없이 텍스트만으로 요약, 키워드, 엔티티, 감성 분석을 테스트할 수 있습니다.
    
    Args:
        request: 테스트 요청 (text 필수)
    
    Returns:
        요약 결과:
        {
            "summary": "요약문",
            "keywords": ["키워드1", "키워드2", ...],
            "entities": {"ORG": [...], "PERSON": [...], "LOCATION": [...]},
            "bullet_points": ["핵심1", "핵심2", ...],
            "sentiment": "positive/negative/neutral"
        }
    """
    try:
        if not request.text or len(request.text.strip()) < 100:
            raise HTTPException(
                status_code=400,
                detail="텍스트가 너무 짧습니다. 최소 100자 이상 입력해주세요."
            )
        
        # 요약 실행
        result = summarize_text(request.text)
        
        if not result:
            raise HTTPException(
                status_code=500,
                detail="요약 생성에 실패했습니다."
            )
        
        return {
            "success": True,
            "input_length": len(request.text),
            "result": result
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"요약 테스트 실패: {e}")
        raise HTTPException(status_code=500, detail=f"요약 테스트 실패: {str(e)}")

