"""
pgvector 확장 설치 확인 및 설치 스크립트
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from app.db import engine
from sqlalchemy import text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_pgvector():
    """pgvector 확장 설치 여부 확인"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT EXISTS(
                    SELECT 1 
                    FROM pg_extension 
                    WHERE extname = 'vector'
                );
            """))
            exists = result.scalar()
            
            if exists:
                logger.info("✅ pgvector 확장이 이미 설치되어 있습니다.")
                return True
            else:
                logger.warning("⚠️ pgvector 확장이 설치되지 않았습니다.")
                return False
    except Exception as e:
        logger.error(f"❌ pgvector 확인 중 오류: {e}")
        return False


def install_pgvector():
    """pgvector 확장 설치 시도"""
    try:
        with engine.connect() as conn:
            logger.info("📥 pgvector 확장 설치 시도 중...")
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector;"))
            conn.commit()
            logger.info("✅ pgvector 확장 설치 완료")
            return True
    except Exception as e:
        logger.error(f"❌ pgvector 확장 설치 실패: {e}")
        logger.info("💡 수동 설치 방법:")
        logger.info("   1. PostgreSQL 서버에 pgvector 확장 설치 필요")
        logger.info("   2. Docker를 사용하는 경우: pgvector 이미지 사용 또는 수동 설치")
        logger.info("   3. 참고: https://github.com/pgvector/pgvector")
        return False


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("pgvector 확장 확인 및 설치")
    logger.info("=" * 60)
    
    if not check_pgvector():
        install_pgvector()
        check_pgvector()
    
    logger.info("=" * 60)

