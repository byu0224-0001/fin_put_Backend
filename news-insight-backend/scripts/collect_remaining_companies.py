"""나머지 19개 기업 데이터 수집 및 섹터 재분류 진행"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

load_dotenv()

# UTF-8 인코딩 설정
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'
else:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
import logging
import subprocess

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def collect_remaining_companies():
    """나머지 19개 기업 데이터 수집 시도"""
    # 이미 수집 완료된 기업
    completed = ['460470', '0009K0', '0015N0']
    
    # 나머지 19개 기업
    remaining = [
        '0015S0', '0054V0', '0068Y0', '0088D0', '0091W0', '0093G0',
        '0096B0', '0096D0', '0098T0', '0105P0', '0120G0', '0126Z0',
        '217590', '261520', '388210', '464490', '466690', '488900', '950250'
    ]
    
    logger.info("=" * 80)
    logger.info("나머지 19개 기업 데이터 수집 시작")
    logger.info("=" * 80)
    
    success_count = 0
    failed_count = 0
    
    for idx, ticker in enumerate(remaining, 1):
        logger.info(f"[{idx}/{len(remaining)}] {ticker} 데이터 수집 시도...")
        
        try:
            # 04_fetch_dart.py 스크립트 실행
            result = subprocess.run(
                [sys.executable, 'scripts/04_fetch_dart.py', '--ticker', ticker, '--year', '2024'],
                cwd=project_root,
                capture_output=True,
                text=True,
                timeout=300  # 5분 타임아웃
            )
            
            if result.returncode == 0:
                # 데이터 수집 성공 여부 확인
                db = SessionLocal()
                try:
                    detail = db.query(CompanyDetail).filter(
                        CompanyDetail.ticker == ticker
                    ).first()
                    
                    if detail:
                        logger.info(f"[{ticker}] 데이터 수집 완료")
                        success_count += 1
                    else:
                        logger.warning(f"[{ticker}] 데이터 수집 실패 (DART 데이터 없음)")
                        failed_count += 1
                finally:
                    db.close()
            else:
                logger.warning(f"[{ticker}] 데이터 수집 실패 (스크립트 오류)")
                failed_count += 1
                
        except subprocess.TimeoutExpired:
            logger.warning(f"[{ticker}] 데이터 수집 타임아웃")
            failed_count += 1
        except Exception as e:
            logger.error(f"[{ticker}] 데이터 수집 중 오류: {e}")
            failed_count += 1
    
    logger.info("=" * 80)
    logger.info(f"데이터 수집 완료: 성공 {success_count}개, 실패 {failed_count}개")
    logger.info("=" * 80)
    
    return success_count, failed_count

def main():
    """메인 실행 함수"""
    logger.info("=" * 80)
    logger.info("나머지 기업 데이터 수집 및 섹터 재분류 진행")
    logger.info("=" * 80)
    
    # 1. 나머지 19개 기업 데이터 수집 시도
    success_count, failed_count = collect_remaining_companies()
    
    # 2. 섹터 재분류 진행
    logger.info("=" * 80)
    logger.info("섹터 재분류 시작")
    logger.info("=" * 80)
    
    try:
        # 섹터 재분류 스크립트 실행
        result = subprocess.run(
            [sys.executable, 'scripts/reclassify_all_sectors_ensemble_optimized.py'],
            cwd=project_root,
            capture_output=False,
            text=True
        )
        
        if result.returncode == 0:
            logger.info("섹터 재분류 완료")
        else:
            logger.error(f"섹터 재분류 실패 (exit code: {result.returncode})")
            
    except Exception as e:
        logger.error(f"섹터 재분류 중 오류: {e}", exc_info=True)
    
    logger.info("=" * 80)
    logger.info("모든 작업 완료")
    logger.info("=" * 80)

if __name__ == '__main__':
    main()

