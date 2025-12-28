# -*- coding: utf-8 -*-
"""
시가총액 데이터 수집 스크립트

pykrx를 사용하여 한국 상장 기업의 시가총액 데이터를 수집하고 stocks 테이블에 저장합니다.

정책:
- market_cap은 '설명용 메타 정보'로만 사용
- 노출도 계산에는 사용하지 않음
- Tie-breaking에만 활용 (순위 변경 없음)
"""

import sys
import os
import codecs
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional
import logging

# 인코딩 설정
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')

# 프로젝트 루트 추가
project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from app.db import SessionLocal
from app.models.stock import Stock
from sqlalchemy import text

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_market_cap_from_pykrx(ticker: str, date: str = None) -> Optional[int]:
    """
    pykrx를 사용하여 특정 종목의 시가총액 조회
    
    Args:
        ticker: 종목 코드 (6자리)
        date: 기준일 (YYYYMMDD 형식, None이면 최근 거래일)
    
    Returns:
        시가총액 (원 단위), 실패 시 None
    """
    try:
        from pykrx import stock
        
        # 날짜가 없으면 최근 거래일 찾기
        if date is None:
            today = datetime.now()
            for days_back in range(7):
                check_date = (today - timedelta(days=days_back)).strftime("%Y%m%d")
                try:
                    # 해당 날짜에 거래가 있었는지 확인 (전체 종목 조회)
                    df = stock.get_market_cap_by_ticker(check_date)
                    if not df.empty:
                        date = check_date
                        break
                except:
                    continue
            
            if date is None:
                logger.warning(f"[{ticker}] 최근 거래일을 찾을 수 없습니다")
                return None
        
        # 시가총액 조회 (전체 종목 조회 후 필터링)
        # pykrx는 전체 종목을 한 번에 조회하는 방식
        df = stock.get_market_cap_by_ticker(date)
        
        if df.empty:
            logger.warning(f"[{ticker}] 시가총액 데이터 없음 (날짜: {date})")
            return None
        
        # 특정 ticker의 시가총액 추출
        if ticker not in df.index:
            logger.warning(f"[{ticker}] 시가총액 데이터 없음 (인덱스에 없음)")
            return None
        
        # 시가총액 추출 (원 단위)
        market_cap = df.loc[ticker, '시가총액']  # pykrx는 '시가총액' 컬럼명 사용
        
        # 문자열이면 숫자로 변환 (예: "500,000,000,000" -> 500000000000)
        if isinstance(market_cap, str):
            market_cap = int(market_cap.replace(',', ''))
        
        return int(market_cap)
        
    except ImportError:
        logger.error("pykrx가 설치되지 않았습니다. pip install pykrx 실행 필요")
        return None
    except Exception as e:
        logger.warning(f"[{ticker}] 시가총액 조회 실패: {e}")
        return None


def get_all_market_caps_from_pykrx(date: str = None) -> Dict[str, int]:
    """
    pykrx를 사용하여 전체 상장 기업의 시가총액 일괄 조회
    
    Args:
        date: 기준일 (YYYYMMDD 형식, None이면 최근 거래일)
    
    Returns:
        {ticker: market_cap} 딕셔너리
    """
    try:
        from pykrx import stock
        
        # 날짜가 없으면 최근 거래일 찾기
        if date is None:
            today = datetime.now()
            for days_back in range(7):
                check_date = (today - timedelta(days=days_back)).strftime("%Y%m%d")
                try:
                    df = stock.get_market_cap_by_ticker(check_date)
                    if not df.empty:
                        date = check_date
                        logger.info(f"기준일: {date}")
                        break
                except:
                    continue
            
            if date is None:
                logger.error("최근 거래일을 찾을 수 없습니다")
                return {}
        
        # 전체 종목 시가총액 조회
        df = stock.get_market_cap_by_ticker(date)
        
        if df.empty:
            logger.error(f"시가총액 데이터 없음 (날짜: {date})")
            return {}
        
        # 딕셔너리로 변환
        market_caps = {}
        for idx, row in df.iterrows():
            ticker = idx  # 인덱스가 ticker
            market_cap = row['시가총액']
            
            # 문자열이면 숫자로 변환
            if isinstance(market_cap, str):
                market_cap = int(market_cap.replace(',', ''))
            
            market_caps[ticker] = int(market_cap)
        
        logger.info(f"시가총액 데이터 수집 완료: {len(market_caps)}개")
        return market_caps
        
    except ImportError:
        logger.error("pykrx가 설치되지 않았습니다. pip install pykrx 실행 필요")
        return {}
    except Exception as e:
        logger.error(f"시가총액 일괄 조회 실패: {e}", exc_info=True)
        return {}


def update_market_caps_batch(db, market_caps: Dict[str, int], batch_size: int = 100):
    """
    시가총액 데이터를 배치로 업데이트
    
    Args:
        db: DB 세션
        market_caps: {ticker: market_cap} 딕셔너리
        batch_size: 배치 크기
    """
    updated_count = 0
    failed_count = 0
    
    tickers = list(market_caps.keys())
    
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        
        try:
            for ticker in batch:
                market_cap = market_caps[ticker]
                
                # UPDATE 쿼리 실행
                result = db.execute(
                    text("""
                        UPDATE stocks 
                        SET market_cap = :market_cap, updated_at = CURRENT_TIMESTAMP
                        WHERE ticker = :ticker
                    """),
                    {'market_cap': market_cap, 'ticker': ticker}
                )
                
                if result.rowcount > 0:
                    updated_count += 1
                else:
                    logger.warning(f"[{ticker}] DB에 존재하지 않는 종목 (시가총액: {market_cap:,}원)")
                    failed_count += 1
            
            db.commit()
            
            if (i + batch_size) % 500 == 0:
                logger.info(f"진행 중... ({i + batch_size}/{len(tickers)})")
                
        except Exception as e:
            logger.error(f"배치 업데이트 실패 (인덱스 {i}~{i+batch_size}): {e}")
            db.rollback()
            failed_count += len(batch)
    
    return updated_count, failed_count


def main():
    """메인 실행 함수"""
    db = SessionLocal()
    
    try:
        logger.info("=" * 80)
        logger.info("시가총액 데이터 수집 시작")
        logger.info("=" * 80)
        
        # 1. 전체 시가총액 데이터 일괄 조회
        logger.info("1. pykrx를 사용하여 전체 상장 기업 시가총액 조회 중...")
        market_caps = get_all_market_caps_from_pykrx()
        
        if not market_caps:
            logger.error("시가총액 데이터 수집 실패")
            return
        
        logger.info(f"수집된 시가총액 데이터: {len(market_caps)}개")
        
        # 샘플 출력
        sample_tickers = list(market_caps.keys())[:5]
        logger.info("샘플 데이터:")
        for ticker in sample_tickers:
            market_cap = market_caps[ticker]
            logger.info(f"  {ticker}: {market_cap:,}원 ({market_cap/1e12:.2f}조원)")
        
        # 2. DB 업데이트
        logger.info("=" * 80)
        logger.info("2. DB 업데이트 중...")
        updated_count, failed_count = update_market_caps_batch(db, market_caps)
        
        # 3. 통계 출력
        logger.info("=" * 80)
        logger.info("시가총액 데이터 수집 완료")
        logger.info(f"  수집된 데이터: {len(market_caps)}개")
        logger.info(f"  업데이트 성공: {updated_count}개")
        logger.info(f"  업데이트 실패: {failed_count}개")
        
        # 4. DB 통계 확인
        result = db.execute(text("""
            SELECT 
                COUNT(*) as total,
                COUNT(market_cap) as with_market_cap,
                COUNT(*) - COUNT(market_cap) as without_market_cap
            FROM stocks
            WHERE country = 'KR'
        """))
        row = result.fetchone()
        
        logger.info("=" * 80)
        logger.info("DB 통계:")
        logger.info(f"  전체 한국 기업: {row[0]}개")
        logger.info(f"  시가총액 있음: {row[1]}개 ({row[1]/row[0]*100:.1f}%)")
        logger.info(f"  시가총액 없음: {row[2]}개 ({row[2]/row[0]*100:.1f}%)")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"시가총액 데이터 수집 실패: {e}", exc_info=True)
        db.rollback()
    finally:
        db.close()


if __name__ == '__main__':
    main()

