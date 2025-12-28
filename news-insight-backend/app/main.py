import os
# TensorFlow 비활성화 (PyTorch만 사용)
os.environ['TRANSFORMERS_NO_TF'] = '1'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'  # TensorFlow 경고 숨김

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from app.config import settings
from app.routes import feed, article, insight, scenario
from app.db import Base, engine
from app.utils.logging import setup_logging
from app.services.pipelines.entities import load_company_dict_from_db
from app.services.pipelines.model_loader import warm_up_models
import logging

logger = logging.getLogger(__name__)

# 로깅 설정
setup_logging()

# 데이터베이스 테이블 생성 (연결 실패 시에도 서버는 시작됨)
try:
    Base.metadata.create_all(bind=engine)
    logger.info("데이터베이스 테이블 생성 완료")
except Exception as e:
    logger.warning(f"데이터베이스 테이블 생성 실패 (서버는 계속 시작됨): {e}")
    logger.warning("PostgreSQL 서버가 실행 중인지 확인해주세요. (docker-compose up db 또는 PostgreSQL 직접 설치)")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """서버 시작/종료 시 실행"""
    # 서버 시작 시
    logger.info("=" * 50)
    logger.info("서버 시작 중...")
    logger.info("=" * 50)
    
    # 기업명 딕셔너리 로딩 (서버 시작 시 1회만)
    # DB 연결 실패 시에도 서버는 계속 시작됨 (load_company_dict_from_db 내부에서 예외 처리됨)
    logger.info("기업명 딕셔너리 로딩 시작...")
    try:
        load_company_dict_from_db()
    except Exception as e:
        logger.warning(f"기업명 딕셔너리 로딩 실패 (서버는 계속 시작됨): {e}")
        logger.warning("DB 연결이 실패해도 서버는 정상 작동하지만, 엔티티 추출 기능이 제한될 수 있습니다.")
    
    # AI 모델 Warm-up (첫 요청 지연 방지)
    logger.info("AI 모델 Warm-up 시작...")
    try:
        warm_up_models()
    except Exception as e:
        logger.error(f"AI 모델 Warm-up 실패: {e}")
        logger.error("첫 요청 시 모델 로드로 인한 지연이 발생할 수 있습니다.")
    
    logger.info("서버 시작 완료")
    logger.info("=" * 50)
    
    yield
    
    # 서버 종료 시 (필요시 정리 작업)
    logger.info("서버 종료 중...")


app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    debug=settings.DEBUG,
    lifespan=lifespan,
    # JSON 파싱 오류 방지 설정
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 라우트 등록
app.include_router(feed.router, prefix="/api", tags=["Feed"])
app.include_router(article.router, prefix="/api", tags=["Article"])
app.include_router(insight.router, prefix="/api", tags=["Insight"])
app.include_router(scenario.router, prefix="/api/v1", tags=["Scenario Analysis"])


@app.get("/")
def root():
    """루트 엔드포인트"""
    return {
        "message": "News Insight API is running 🚀",
        "version": settings.API_VERSION,
        "docs": "/docs"
    }


@app.get("/health")
def health_check():
    """헬스 체크 엔드포인트"""
    return {
        "status": "healthy",
        "service": "news-insight-backend"
    }


if __name__ == "__main__":
    import uvicorn
    # Docker 환경에서는 reload 사용 안 함
    # 로컬 개발 시에만 reload 사용 (import string 필요)
    use_reload = settings.DEBUG and not os.getenv("DOCKER_ENV")
    if use_reload:
        # reload 사용 시 import string 필요
        uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
    else:
        # Docker나 프로덕션에서는 reload 없이 실행
        uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)

