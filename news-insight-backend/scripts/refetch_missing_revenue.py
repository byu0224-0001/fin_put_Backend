# -*- coding: utf-8 -*-
"""
매출 비중 데이터가 없는 기업들의 DART 데이터 재수집
- 04_fetch_dart.py의 로직을 활용하여 revenue_by_segment만 재수집
"""
import sys
import os
from pathlib import Path
from datetime import datetime
import time
import argparse

# 프로젝트 루트 설정
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Windows 인코딩 설정
if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from dotenv import load_dotenv
load_dotenv(dotenv_path=project_root / '.env', override=True)

import logging
from app.db import SessionLocal
from app.models.stock import Stock
from app.models.company_detail import CompanyDetail
from app.services.dart_parser import DartParser
from app.services.llm_handler import LLMHandler
from app.services.embedding_filter import select_relevant_chunks

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DART_API_KEY = os.getenv('DART_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
MAX_LLM_CHARS = 50000


def get_missing_revenue_tickers(db):
    """매출 비중이 없는 기업 ticker 조회"""
    all_details = db.query(CompanyDetail, Stock).join(
        Stock, CompanyDetail.ticker == Stock.ticker
    ).all()
    
    missing = []
    for detail, stock in all_details:
        if detail.revenue_by_segment is None or detail.revenue_by_segment == {} or len(detail.revenue_by_segment) == 0:
            missing.append((detail.ticker, stock.stock_name))
    
    return missing


def fetch_and_update_revenue(db, ticker: str, stock_name: str, dart_parser: DartParser, llm_handler: LLMHandler, year: int = 2024):
    """특정 기업의 DART 데이터 재수집 및 revenue_by_segment 업데이트"""
    try:
        # DART API로 섹션 추출 (04_fetch_dart.py와 동일한 방식)
        combined_text = dart_parser.extract_key_sections(ticker, year)
        
        if not combined_text:
            logger.warning(f"[{ticker}] {stock_name}: DART 섹션 추출 실패")
            return False, "NO_REPORT"
        
        logger.info(f"[{ticker}] {stock_name}: DART 섹션 추출 성공 ({len(combined_text)}자)")
        
        # 임베딩 필터로 관련 청크 선택
        try:
            filtered_text = select_relevant_chunks(combined_text, ticker=ticker)
            effective_text = filtered_text if filtered_text and len(filtered_text) > 200 else combined_text
        except Exception as e:
            logger.warning(f"[{ticker}] 임베딩 필터링 실패, 원문 사용: {e}")
            effective_text = combined_text
        
        # 길이 제한
        if len(effective_text) > MAX_LLM_CHARS:
            effective_text = effective_text[:MAX_LLM_CHARS]
        
        # LLM으로 구조화된 데이터 추출 (기존 로직 사용)
        structured_data = llm_handler.extract_structured_data(
            effective_text,
            ticker=ticker,
            company_name=stock_name
        )
        
        if not structured_data:
            logger.warning(f"[{ticker}] {stock_name}: LLM 구조화 실패")
            return False, "LLM_FAIL"
        
        # revenue_by_segment 추출
        revenue_data = structured_data.get('revenue_by_segment', {})
        
        if revenue_data and isinstance(revenue_data, dict) and len(revenue_data) > 0:
            # DB 업데이트
            detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == ticker).first()
            if detail:
                detail.revenue_by_segment = revenue_data
                detail.updated_at = datetime.utcnow()
                db.commit()
                logger.info(f"[{ticker}] {stock_name}: 매출비중 업데이트 완료 - {revenue_data}")
                return True, revenue_data
            else:
                logger.warning(f"[{ticker}] {stock_name}: CompanyDetail 없음")
                return False, "NO_DETAIL"
        else:
            logger.warning(f"[{ticker}] {stock_name}: 매출비중 데이터 없음")
            return False, "NO_REVENUE_DATA"
            
    except Exception as e:
        logger.error(f"[{ticker}] {stock_name}: 오류 - {e}")
        return False, str(e)


def main():
    parser = argparse.ArgumentParser(description='매출 비중 없는 기업 DART 재수집')
    parser.add_argument('--limit', type=int, default=50, help='처리할 기업 수 제한')
    parser.add_argument('--year', type=int, default=2024, help='대상 연도')
    parser.add_argument('--skip', type=int, default=0, help='건너뛸 기업 수')
    args = parser.parse_args()
    
    db = SessionLocal()
    
    # 매출 비중 없는 기업 조회
    missing = get_missing_revenue_tickers(db)
    print(f"\n=== 매출 비중 없는 기업: {len(missing)}개 ===")
    print(f"처리 대상: {args.skip}번째부터 {args.limit}개")
    print()
    
    if not DART_API_KEY:
        print("ERROR: DART_API_KEY가 설정되지 않았습니다.")
        return
    
    if not OPENAI_API_KEY:
        print("ERROR: OPENAI_API_KEY가 설정되지 않았습니다.")
        return
    
    # 파서 및 LLM 핸들러 초기화
    dart_parser = DartParser(DART_API_KEY)
    llm_handler = LLMHandler()
    
    # 처리
    success_count = 0
    fail_count = 0
    results = []
    
    target = missing[args.skip:args.skip + args.limit]
    
    for i, (ticker, stock_name) in enumerate(target):
        print(f"[{i+1}/{len(target)}] {stock_name} ({ticker}) 처리 중...")
        
        success, result = fetch_and_update_revenue(
            db, ticker, stock_name, dart_parser, llm_handler, args.year
        )
        
        if success:
            success_count += 1
            results.append({'ticker': ticker, 'name': stock_name, 'status': 'SUCCESS', 'revenue': result})
        else:
            fail_count += 1
            results.append({'ticker': ticker, 'name': stock_name, 'status': 'FAIL', 'error': result})
        
        # Rate limit 방지
        time.sleep(1)
    
    # 결과 요약
    print()
    print("=" * 60)
    print(f"=== 처리 완료 ===")
    print(f"성공: {success_count}개")
    print(f"실패: {fail_count}개")
    print("=" * 60)
    
    # 실패 목록
    if fail_count > 0:
        print("\n[실패 목록]")
        for r in results:
            if r['status'] == 'FAIL':
                print(f"  - {r['name']} ({r['ticker']}): {r['error']}")
    
    db.close()


if __name__ == '__main__':
    main()

