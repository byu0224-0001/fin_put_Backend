from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from urllib.parse import urlparse
from app.db import get_db
from app.config import settings
from app.services.rss_collector import fetch_rss_articles
from app.services.deduplicator import deduplicate_articles
from app.models.article import Article
from app.utils.cache import get_from_cache, set_to_cache, get_cache_key
from datetime import datetime
import logging
import html

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/feed", tags=["Feed"])


def format_article_response(article: Article, cluster_info: dict = None, db: Session = None) -> dict:
    """
    기사 응답 포맷 통일
    
    Args:
        article: Article 모델 객체
        cluster_info: 클러스터 정보 (cluster_id, representative)
        db: 데이터베이스 세션 (AI 분석 여부 확인용)
    
    Returns:
        통일된 형식의 기사 데이터
    """
    if cluster_info is None:
        cluster_info = {}
    
    # 이미지 URL 추출 (DB에 저장된 값 사용)
    image_url = article.image_url if hasattr(article, 'image_url') and article.image_url else None
    
    related_articles = cluster_info.get("related_articles", []) if cluster_info else []

    title = html.unescape(article.title or "")
    
    # AI 분석 완료 여부 확인
    ai_summary_exists = False
    summary = None
    if db:
        from app.models.article import Summary
        summary_data = db.query(Summary).filter(Summary.article_id == article.id).first()
        if summary_data:
            ai_summary_exists = True
            summary = summary_data.summary

    return {
        "id": article.id,
        "title": title,
        "summary": summary,  # AI 분석 완료 시에만 값이 있음, 아니면 null
        "source": article.source,
        "url": article.link,  # 프론트엔드용 필드
        "link": article.link,  # 하위 호환성
        "published_at": article.published_at.isoformat() if article.published_at else None,
        "image_url": image_url,  # 이미지 URL
        "cluster_id": cluster_info.get("cluster_id"),  # 클러스터 ID
        "representative": cluster_info.get("representative", False),  # 대표 기사 여부
        "related_articles": related_articles,
        "ai_analysis_available": ai_summary_exists  # AI 분석 완료 여부 플래그
    }


@router.get("/")
def get_feed(
    limit: int = 20,
    offset: int = 0,
    source: Optional[str] = None,
    deduplicate: bool = True,
    similarity_threshold: float = Query(0.75, ge=0.0, le=1.0, description="유사도 임계값 (0.0~1.0)"),
    enable_bert: bool = Query(True, description="BERT Embedding 사용 여부"),
    db: Session = Depends(get_db)
):
    """
    RSS 피드에서 최신 뉴스 기사 수집 및 반환 (중복 제거 및 다양성 확보)
    
    Args:
        limit: 반환할 기사 수
        offset: 페이지 오프셋
        source: 특정 출처 필터링
        deduplicate: 중복 제거 사용 여부
        similarity_threshold: 유사도 임계값 (0.75 권장)
        enable_bert: BERT Embedding 사용 여부
        db: 데이터베이스 세션
    """
    try:
        # 캐시 키 생성 (파라미터 기반)
        cache_key = get_cache_key(
            "feed",
            limit=limit,
            offset=offset,
            source=source,
            deduplicate=deduplicate,
            similarity_threshold=similarity_threshold,
            enable_bert=enable_bert
        )
        
        # 캐시에서 가져오기 시도
        cached_result = get_from_cache(cache_key)
        if cached_result:
            logger.info(f"캐시 히트: {cache_key}")
            # DB에서 최신 정보 가져오기 (ID 기준)
            article_ids = [a["id"] for a in cached_result["articles"]]
            saved_articles = db.query(Article).filter(Article.id.in_(article_ids)).all()
            
            # ID 순서 유지
            article_dict = {a.id: a for a in saved_articles}
            saved_articles = [article_dict[aid] for aid in article_ids if aid in article_dict]
            
            # 클러스터 정보는 캐시에서 가져오기
            # 캐시된 articles에서 cluster_id, representative, image_url 정보 추출
            cached_articles_dict = {a["id"]: a for a in cached_result["articles"]}
            
            # 캐시된 데이터에서 AI 분석 여부 확인
            from app.models.article import Summary
            article_ids = [a.id for a in saved_articles]
            summaries = {s.article_id: s for s in db.query(Summary).filter(Summary.article_id.in_(article_ids)).all()}
            
            return {
                "articles": [
                    {
                        "id": a.id,
                        "title": a.title,
                        "summary": summaries.get(a.id).summary if summaries.get(a.id) is not None else None,  # AI 분석 완료 시에만 값이 있음
                        "source": a.source,
                        "url": a.link,
                        "link": a.link,
                        "published_at": a.published_at.isoformat() if a.published_at else None,
                        "image_url": a.image_url if hasattr(a, 'image_url') and a.image_url else cached_articles_dict.get(a.id, {}).get("image_url"),
                        "cluster_id": cached_articles_dict.get(a.id, {}).get("cluster_id"),
                        "representative": cached_articles_dict.get(a.id, {}).get("representative", False),
                        "related_articles": cached_articles_dict.get(a.id, {}).get("related_articles", []),
                        "ai_analysis_available": summaries.get(a.id) is not None,  # AI 분석 완료 여부 플래그
                    }
                    for a in saved_articles
                ],
                "total": cached_result["total"],
                "original_count": cached_result["original_count"],
                "deduplicated_count": cached_result["deduplicated_count"],
                "limit": cached_result["limit"],
                "offset": cached_result["offset"],
                "deduplication_enabled": cached_result["deduplication_enabled"],
                "cached": True
            }
        
        logger.info(f"캐시 미스: {cache_key}. 새로 수집 시작.")
        
        # RSS에서 기사 수집
        articles_data = fetch_rss_articles(settings.RSS_FEEDS)
        
        # 필터링
        if source:
            articles_data = [a for a in articles_data if source in a.get("source", "")]
        
        original_count = len(articles_data)
        
        # 중복 제거 적용
        cluster_info_map = {}  # 클러스터 정보 저장용
        if deduplicate and len(articles_data) > 1:
            logger.info(f"중복 제거 시작: {len(articles_data)}개 기사")
            # 모든 기사에 대해 중복 제거 수행 (충분히 큰 max_results 설정)
            articles_data = deduplicate_articles(
                articles_data,
                similarity_threshold=similarity_threshold,
                max_results=max(limit * 10, 500),  # 충분히 큰 값으로 설정 (최소 500개 이상)
                enable_bert=enable_bert,
                tfidf_weight=settings.TFIDF_WEIGHT,
                bert_weight=settings.BERT_WEIGHT,
                max_same_source=settings.MAX_SAME_SOURCE
            )
            logger.info(f"중복 제거 완료: {original_count} -> {len(articles_data)}개 기사")
            
            # 클러스터 정보 추출
            for article in articles_data:
                if "cluster_id" in article and "representative" in article:
                    cluster_info_map[article.get("link")] = {
                        "cluster_id": article.get("cluster_id"),
                        "representative": article.get("representative"),
                        "related_articles": article.get("related_articles", []),
                    }
        
        # 페이징
        paginated = articles_data[offset:offset + limit]
        
        # DB에 기사 저장 (중복 체크 - link 기반)
        saved_articles = []
        article_cluster_info = {}  # DB 저장 후 ID 기준 클러스터 정보
        article_image_map = {}  # 이미지 URL 매핑 (link -> image_url)
        
        for article_data in paginated:
            # 이미지 URL 저장
            image_url = article_data.get("image_url")
            link = article_data.get("link")
            if image_url:
                article_image_map[link] = image_url
            else:
                # image_url이 없으면 None으로 명시
                article_image_map[link] = None
            
            # 이미 존재하는지 확인
            existing = db.query(Article).filter(Article.link == link).first()
            
            if not existing:
                # 새 기사 저장
                article = Article(
                    title=article_data["title"],
                    source=article_data["source"],
                    link=link,
                    summary=article_data.get("summary"),
                    image_url=image_url,  # 이미지 URL 저장
                    published_at=datetime.fromisoformat(article_data["published_at"])
                )
                db.add(article)
                db.commit()
                db.refresh(article)
                saved_articles.append(article)
            else:
                # 기존 기사가 있으면 이미지 URL 업데이트 (없는 경우에만)
                if image_url and not existing.image_url:
                    existing.image_url = image_url
                    db.commit()
                    db.refresh(existing)
                saved_articles.append(existing)
        
            # 클러스터 정보 저장 (link -> id 매핑)
            if link in cluster_info_map:
                article_id = existing.id if existing else article.id
                article_cluster_info[article_id] = cluster_info_map[link]
        
        # 이미지 URL 확인 (DB에 저장된 것이 우선, 없으면 RSS에서 가져온 것 사용)
        for article in saved_articles:
            # DB에 이미지 URL이 있으면 사용, 없으면 RSS에서 추출한 것 사용
            if not article.image_url:
                article.image_url = article_image_map.get(article.link)
            # article_image_map에도 업데이트 (응답 포맷에 사용)
            if article.image_url:
                article_image_map[article.link] = article.image_url
        
        # 응답 형식 통일
        formatted_articles = [
            format_article_response(a, article_cluster_info.get(a.id, {}), db)
                for a in saved_articles
        ]
        
        result = {
            "articles": formatted_articles,
            "total": len(articles_data),
            "original_count": original_count,
            "deduplicated_count": len(articles_data) if deduplicate else original_count,
            "limit": limit,
            "offset": offset,
            "deduplication_enabled": deduplicate,
            "cached": False
        }
        
        # 캐시 저장 (TTL: 10분) - 이미지 URL 포함
        set_to_cache(cache_key, {
            "articles": formatted_articles,
            "total": len(articles_data),
            "original_count": original_count,
            "deduplicated_count": len(articles_data) if deduplicate else original_count,
            "limit": limit,
            "offset": offset,
            "deduplication_enabled": deduplicate,
            "cluster_info": article_cluster_info
        }, ttl=600)
        
        return result
        
    except Exception as e:
        logger.error(f"피드 수집 실패: {e}")
        raise HTTPException(status_code=500, detail=f"피드 수집 실패: {str(e)}")


@router.get("/sources")
def get_sources():
    """사용 가능한 RSS 출처 목록 반환"""
    return {
        "sources": [
            {
                "name": urlparse(url).netloc or url,
                "url": url
            }
            for url in settings.RSS_FEEDS
        ]
    }

