"""
Step 1.4: 테이블 생성 스크립트
SQLAlchemy를 사용하여 모든 모델의 테이블을 생성
"""
import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Windows 환경에서 인코딩 문제 방지
if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from dotenv import load_dotenv
load_dotenv()

from app.db import Base, engine
from app.models import *  # 모든 모델 import
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """모든 테이블 생성"""
    print("=" * 60)
    print("Step 1.4: 테이블 생성")
    print("=" * 60)
    
    try:
        logger.info("데이터베이스 연결 확인 중...")
        with engine.connect() as conn:
            logger.info("데이터베이스 연결 성공")
        
        logger.info("테이블 생성 중...")
        Base.metadata.create_all(bind=engine)
        
        print("\n✅ 테이블 생성 완료!")
        print("\n생성된 테이블 목록:")
        print("-" * 60)
        
        # 생성된 테이블 목록 출력
        table_names = Base.metadata.tables.keys()
        for idx, table_name in enumerate(sorted(table_names), 1):
            print(f"  {idx:2d}. {table_name}")
        
        print("\n" + "=" * 60)
        print("✅ Step 1.4 완료!")
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"테이블 생성 실패: {e}")
        import traceback
        traceback.print_exc()
        print("\n❌ 오류 발생!")
        print("문제 해결 방법:")
        print("1. PostgreSQL이 실행 중인지 확인하세요.")
        print("2. .env 파일의 데이터베이스 연결 정보를 확인하세요.")
        print("3. Docker 컨테이너가 실행 중인지 확인하세요.")
        return False
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

