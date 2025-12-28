"""빠른 데이터 수집 및 섹터 재분류 (병렬 처리)"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
import subprocess
import concurrent.futures

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

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def collect_single_company(ticker):
    """단일 기업 데이터 수집"""
    try:
        result = subprocess.run(
            [sys.executable, 'scripts/04_fetch_dart.py', '--ticker', ticker, '--year', '2024'],
            cwd=project_root,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        if result.returncode == 0:
            # DB에서 확인
            from app.db import SessionLocal
            from app.models.company_detail import CompanyDetail
            
            db = SessionLocal()
            try:
                detail = db.query(CompanyDetail).filter(
                    CompanyDetail.ticker == ticker
                ).first()
                return ticker, detail is not None
            finally:
                db.close()
        return ticker, False
    except Exception as e:
        logger.error(f"[{ticker}] 수집 중 오류: {e}")
        return ticker, False

def main():
    """메인 실행 함수"""
    # 나머지 19개 기업
    remaining = [
        '0015S0', '0054V0', '0068Y0', '0088D0', '0091W0', '0093G0',
        '0096B0', '0096D0', '0098T0', '0105P0', '0120G0', '0126Z0',
        '217590', '261520', '388210', '464490', '466690', '488900', '950250'
    ]
    
    logger.info("=" * 80)
    logger.info("나머지 19개 기업 데이터 수집 시작 (병렬 처리)")
    logger.info("=" * 80)
    
    # 병렬 처리 (최대 3개 동시 실행)
    success_count = 0
    failed_count = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        future_to_ticker = {
            executor.submit(collect_single_company, ticker): ticker 
            for ticker in remaining
        }
        
        for future in concurrent.futures.as_completed(future_to_ticker):
            ticker, success = future.result()
            if success:
                logger.info(f"[{ticker}] 데이터 수집 완료")
                success_count += 1
            else:
                logger.warning(f"[{ticker}] 데이터 수집 실패 (DART 데이터 없음 또는 오류)")
                failed_count += 1
    
    logger.info("=" * 80)
    logger.info(f"데이터 수집 완료: 성공 {success_count}개, 실패 {failed_count}개")
    logger.info("=" * 80)
    
    # 섹터 재분류 진행
    logger.info("=" * 80)
    logger.info("섹터 재분류 시작")
    logger.info("=" * 80)
    
    try:
        result = subprocess.run(
            [sys.executable, 'scripts/reclassify_all_sectors_ensemble_optimized.py'],
            cwd=project_root
        )
        
        if result.returncode == 0:
            logger.info("섹터 재분류 완료")
        else:
            logger.error(f"섹터 재분류 실패 (exit code: {result.returncode})")
    except Exception as e:
        logger.error(f"섹터 재분류 중 오류: {e}", exc_info=True)

if __name__ == '__main__':
    main()

