"""
DB 마이그레이션 실행 스크립트
- edges 테이블 개선
- exposure_drivers 제거
"""
import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from app.db import engine
from sqlalchemy import text
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_migration(sql_file_path: str):
    """SQL 마이그레이션 파일 실행"""
    try:
        with open(sql_file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        logger.info(f"마이그레이션 실행: {sql_file_path}")
        
        with engine.connect() as conn:
            # 주석 제거 및 정리
            lines = []
            in_comment_block = False
            for line in sql_content.split('\n'):
                line = line.strip()
                if not line or line.startswith('--'):
                    continue
                if '/*' in line:
                    in_comment_block = True
                if '*/' in line:
                    in_comment_block = False
                    continue
                if not in_comment_block:
                    lines.append(line)
            
            # SQL 문장 재구성
            sql_statements = []
            current_statement = []
            for line in lines:
                current_statement.append(line)
                if line.endswith(';'):
                    stmt = ' '.join(current_statement).rstrip(';').strip()
                    if stmt:
                        sql_statements.append(stmt)
                    current_statement = []
            
            # 남은 문장 처리
            if current_statement:
                stmt = ' '.join(current_statement).strip()
                if stmt:
                    sql_statements.append(stmt)
            
            # 각 문장 실행
            for i, statement in enumerate(sql_statements, 1):
                if not statement:
                    continue
                try:
                    # DO $$ 블록은 전체를 한 번에 실행
                    if 'DO $$' in statement or 'BEGIN' in statement:
                        conn.execute(text(statement))
                    else:
                        conn.execute(text(statement))
                    logger.debug(f"  [{i}/{len(sql_statements)}] 실행 완료")
                except Exception as e:
                    logger.warning(f"  [{i}/{len(sql_statements)}] 실행 중 경고: {e}")
            
            conn.commit()
            logger.info(f"✅ 마이그레이션 완료: {sql_file_path}")
            return True
    
    except Exception as e:
        logger.error(f"❌ 마이그레이션 실패: {sql_file_path} - {e}", exc_info=True)
        return False


def main():
    """메인 실행 함수"""
    migrations_dir = project_root / 'sql' / 'migrations'
    
    # 실행할 마이그레이션 파일 목록
    migration_files = [
        'improve_edges_table.sql',
        'remove_exposure_drivers.sql',
        'add_hierarchical_sectors.sql',
        'add_company_embeddings_table.sql'  # Solar Embedding 벡터 DB
    ]
    
    logger.info("=" * 60)
    logger.info("DB 마이그레이션 시작")
    logger.info("=" * 60)
    
    success_count = 0
    for migration_file in migration_files:
        sql_path = migrations_dir / migration_file
        if not sql_path.exists():
            logger.warning(f"⚠️ 마이그레이션 파일 없음: {sql_path}")
            continue
        
        if run_migration(str(sql_path)):
            success_count += 1
        else:
            logger.error(f"❌ 마이그레이션 실패: {migration_file}")
    
    logger.info("=" * 60)
    logger.info(f"마이그레이션 완료: {success_count}/{len(migration_files)} 성공")
    logger.info("=" * 60)


if __name__ == '__main__':
    main()

