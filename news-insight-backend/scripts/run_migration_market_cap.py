# -*- coding: utf-8 -*-
"""
시가총액 컬럼 추가 마이그레이션 실행 스크립트
"""

import sys
import os
import codecs
from pathlib import Path

# 인코딩 설정
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')

# 프로젝트 루트 추가
project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from app.db import engine
from sqlalchemy import text
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_migration():
    """마이그레이션 실행"""
    migration_file = project_root / 'sql' / 'migrations' / 'add_market_cap_column.sql'
    
    if not migration_file.exists():
        logger.error(f"마이그레이션 파일을 찾을 수 없습니다: {migration_file}")
        return False
    
    try:
        logger.info("=" * 80)
        logger.info("시가총액 컬럼 추가 마이그레이션 시작")
        logger.info("=" * 80)
        
        with open(migration_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        logger.info("마이그레이션 SQL 실행 중...")
        
        with engine.connect() as conn:
            # 트랜잭션 시작
            trans = conn.begin()
            try:
                # SQL 실행
                conn.execute(text(sql_content))
                trans.commit()
                logger.info("✅ 마이그레이션 완료")
                
                # 확인 쿼리
                result = conn.execute(text("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = 'stocks' AND column_name = 'market_cap'
                """))
                
                row = result.fetchone()
                if row:
                    logger.info(f"✅ market_cap 컬럼 확인:")
                    logger.info(f"   - 컬럼명: {row[0]}")
                    logger.info(f"   - 데이터 타입: {row[1]}")
                    logger.info(f"   - NULL 허용: {row[2]}")
                    
                    # 인덱스 확인
                    idx_result = conn.execute(text("""
                        SELECT indexname
                        FROM pg_indexes
                        WHERE tablename = 'stocks' AND indexname = 'idx_stocks_market_cap'
                    """))
                    
                    if idx_result.fetchone():
                        logger.info("✅ 인덱스 확인: idx_stocks_market_cap")
                    else:
                        logger.warning("⚠️ 인덱스가 생성되지 않았습니다")
                else:
                    logger.error("❌ market_cap 컬럼이 생성되지 않았습니다")
                    return False
                
                return True
                
            except Exception as e:
                trans.rollback()
                logger.error(f"❌ 마이그레이션 실패: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return False
                
    except Exception as e:
        logger.error(f"❌ 마이그레이션 실행 중 오류: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == '__main__':
    success = run_migration()
    sys.exit(0 if success else 1)

