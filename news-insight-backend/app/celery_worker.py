from celery import Celery
from celery.signals import worker_process_init
from app.config import settings
from app.db import SessionLocal, neo4j_driver
from app.models.article import Article, Summary
from app.services.summarizer import summarize_text
from app.services.graph import update_article_graph
from app.services.pipelines.model_loader import warm_up_models
from app.utils.logging import setup_logging
import logging

# 로깅 설정
setup_logging()

logger = logging.getLogger(__name__)

# Celery 앱 생성
celery_app = Celery(
    "news_insight",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND
)

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

# Celery 워커 시작 시 모델 Warm-up
@worker_process_init.connect
def on_worker_process_init(**kwargs):
    """Celery 워커 프로세스 시작 시 모델 Warm-up"""
    logger.info("=" * 50)
    logger.info("Celery 워커 시작 중...")
    logger.info("=" * 50)
    warm_up_models()
    logger.info("Celery 워커 준비 완료")
    logger.info("=" * 50)


@celery_app.task(name="analyze_article", bind=True)
def analyze_article_task(self, article_id: int, text: str):
    """
    기사 분석 비동기 작업
    
    Args:
        article_id: 기사 ID
        text: 기사 본문 텍스트
    
    Returns:
        분석 결과
    """
    db = SessionLocal()
    neo4j_session = None
    
    try:
        # 진행 상태 업데이트
        self.update_state(state='PROGRESS', meta={'progress': 10, 'message': 'AI 요약 시작'})
        
        # AI 요약 실행
        summary_result = summarize_text(text)
        
        if not summary_result:
            raise Exception("AI 요약 실패")
        
        self.update_state(state='PROGRESS', meta={'progress': 50, 'message': '데이터베이스 저장 중'})
        
        # 요약 결과 DB 저장
        existing_summary = db.query(Summary).filter(Summary.article_id == article_id).first()
        
        # 키워드 검증 및 로깅
        keywords = summary_result.get("keywords", [])
        if not isinstance(keywords, list):
            logger.warning(f"키워드가 리스트가 아님: {type(keywords)}, 빈 배열로 변환")
            keywords = []
        
        logger.info(f"DB 저장 전 키워드 확인: {len(keywords)}개 - {keywords[:3] if keywords else '없음'}")
        
        if existing_summary:
            # 업데이트
            existing_summary.summary = summary_result.get("summary", "")
            existing_summary.keywords = keywords
            existing_summary.entities = summary_result.get("entities", {})
            existing_summary.bullet_points = summary_result.get("bullet_points", [])
            existing_summary.sentiment = summary_result.get("sentiment", "neutral")
            summary = existing_summary
        else:
            # 생성
            summary = Summary(
                article_id=article_id,
                summary=summary_result.get("summary", ""),
                keywords=keywords,
                entities=summary_result.get("entities", {}),
                bullet_points=summary_result.get("bullet_points", []),
                sentiment=summary_result.get("sentiment", "neutral")
            )
            db.add(summary)
        
        db.commit()
        db.refresh(summary)
        
        # 저장 후 검증
        logger.info(f"DB 저장 완료 - 키워드: {len(summary.keywords) if summary.keywords else 0}개")
        
        self.update_state(state='PROGRESS', meta={'progress': 80, 'message': '그래프 업데이트 중'})
        
        # Neo4j 그래프 업데이트
        try:
            neo4j_session = neo4j_driver.get_driver().session()
            update_article_graph(
                neo4j_session,
                article_id,
                summary_result.get("entities", {}),
                summary_result.get("keywords", [])
            )
        except Exception as e:
            logger.warning(f"그래프 업데이트 실패 (계속 진행): {e}")
        
        self.update_state(state='PROGRESS', meta={'progress': 100, 'message': '완료'})
        
        return {
            "article_id": article_id,
            "summary": summary_result.get("summary", ""),
            "keywords": summary_result.get("keywords", []),
            "entities": summary_result.get("entities", {}),
            "bullet_points": summary_result.get("bullet_points", []),
            "sentiment": summary_result.get("sentiment", "neutral"),
            "status": "success"
        }
        
    except Exception as e:
        logger.error(f"기사 분석 실패 (article_id={article_id}): {e}")
        raise
    finally:
        db.close()
        if neo4j_session:
            neo4j_session.close()

