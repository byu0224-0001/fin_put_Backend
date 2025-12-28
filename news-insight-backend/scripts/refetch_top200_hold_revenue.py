# -*- coding: utf-8 -*-
"""
Top200 HOLD 기업 매출 데이터 재수집 스크립트

Top200 HOLD 기업 중 매출 데이터 없는 기업 대상 DART 재수집
"""
import sys
import os
import json
from datetime import datetime

# Windows 환경에서 UTF-8 인코딩 설정
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'
else:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.services.dart_parser import DartParser
from app.services.llm_handler import LLMHandler
from app.services.embedding_filter import select_relevant_chunks
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DART_API_KEY = os.getenv('DART_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
MAX_LLM_CHARS = 50000

def fetch_and_update_revenue(db, ticker: str, stock_name: str, dart_parser: DartParser, llm_handler: LLMHandler, year: int = 2024):
    """매출 데이터 재수집 (refetch_missing_revenue.py 로직 활용)"""
    try:
        # DART API로 섹션 추출
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
        
        # LLM으로 구조화된 데이터 추출
        structured_data = llm_handler.extract_structured_data(
            effective_text,
            ticker=ticker,
            company_name=stock_name
        )
        
        if not structured_data:
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
                logger.info(f"[{ticker}] {stock_name}: 매출비중 업데이트 완료")
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


def refetch_top200_hold_revenue(dry_run=True, limit=200):
    """Top200 HOLD 기업 매출 데이터 재수집"""
    db = SessionLocal()
    
    if not DART_API_KEY:
        print("\n❌ DART_API_KEY 환경변수가 설정되지 않았습니다.", flush=True)
        return None
    
    if not OPENAI_API_KEY:
        print("\n❌ OPENAI_API_KEY 환경변수가 설정되지 않았습니다.", flush=True)
        return None
    
    dart_parser = DartParser(DART_API_KEY)
    llm_handler = LLMHandler()
    
    try:
        print("=" * 80, flush=True)
        print("Top200 HOLD 기업 매출 데이터 재수집", flush=True)
        print("=" * 80, flush=True)
        
        if dry_run:
            print("\n⚠️  DRY RUN 모드 (실제 수정하지 않음)", flush=True)
        else:
            print("\n✅ 실제 수정 모드", flush=True)
        
        # DRY RUN 결과 파일에서 Top200 HOLD 기업 목록 로드
        report_file = 'reports/reclassify_all_companies_report.json'
        if not os.path.exists(report_file):
            print(f"\n❌ DRY RUN 결과 파일이 없습니다: {report_file}", flush=True)
            print(f"  → python scripts/reclassify_all_companies.py를 먼저 실행하세요.", flush=True)
            return None
        
        with open(report_file, 'r', encoding='utf-8') as f:
            report = json.load(f)
        
        # Top100 HOLD 리스트에서 추출 (시가총액 기준 정렬되어 있음)
        top100_hold = report.get('stats', {}).get('top100_hold', [])
        
        # Top200 추정 (시가총액 1조 이상)
        top200_hold = [h for h in top100_hold if h.get('market_cap', 0) >= 1000000000000]
        
        # 매출 데이터 없는 기업만 필터링
        need_revenue_data = [h for h in top200_hold if not h.get('has_revenue_data', False)]
        
        print(f"\n[대상 기업]", flush=True)
        print(f"  Top200 HOLD 기업: {len(top200_hold)}개", flush=True)
        print(f"  매출 데이터 없는 기업: {len(need_revenue_data)}개", flush=True)
        
        if not need_revenue_data:
            print(f"\n✅ 매출 데이터가 필요한 기업이 없습니다.", flush=True)
            return None
        
        print(f"\n[재수집 대상 기업 목록]", flush=True)
        for i, company in enumerate(need_revenue_data[:20], 1):
            print(f"  {i}. {company['name']} ({company['ticker']}) - {company['hold_reason']}", flush=True)
        if len(need_revenue_data) > 20:
            print(f"  ... 외 {len(need_revenue_data) - 20}개", flush=True)
        
        # 재수집 실행
        success_count = 0
        error_count = 0
        updated_count = 0
        
        for idx, company in enumerate(need_revenue_data[:limit], 1):
            ticker = company['ticker']
            name = company['name']
            
            try:
                stock = db.query(Stock).filter(Stock.ticker == ticker).first()
                if not stock:
                    print(f"  {idx}. ❌ {name} ({ticker}): Stock 데이터 없음", flush=True)
                    error_count += 1
                    continue
                
                print(f"  {idx}. {name} ({ticker}) 재수집 중...", flush=True)
                
                if not dry_run:
                    # DART 데이터 재수집
                    try:
                        success, result = fetch_and_update_revenue(
                            db, ticker, name, dart_parser, llm_handler, year=2024
                        )
                        
                        if success:
                            updated_count += 1
                            print(f"     ✅ 업데이트 완료", flush=True)
                        else:
                            print(f"     ⚠️  {result}", flush=True)
                            error_count += 1
                    except Exception as e:
                        print(f"     ❌ 오류: {e}", flush=True)
                        error_count += 1
                        db.rollback()
                else:
                    print(f"     [DRY RUN] 재수집 예정", flush=True)
                    success_count += 1
                
                if idx % 10 == 0:
                    print(f"  진행: {idx}/{len(need_revenue_data[:limit])} ({idx/len(need_revenue_data[:limit])*100:.1f}%)", flush=True)
                    
            except Exception as e:
                print(f"  {idx}. ❌ {name} ({ticker}): {e}", flush=True)
                error_count += 1
                continue
        
        # 결과 리포트
        print("\n" + "=" * 80, flush=True)
        print("재수집 결과", flush=True)
        print("=" * 80, flush=True)
        
        print(f"\n[처리 통계]", flush=True)
        print(f"  대상 기업: {len(need_revenue_data[:limit])}개", flush=True)
        if dry_run:
            print(f"  재수집 예정: {success_count}개", flush=True)
        else:
            print(f"  성공: {updated_count}개", flush=True)
            print(f"  실패: {error_count}개", flush=True)
        
        # 리포트 저장
        result = {
            'generated_at': datetime.now().isoformat(),
            'dry_run': dry_run,
            'total_target': len(need_revenue_data[:limit]),
            'success_count': updated_count if not dry_run else success_count,
            'error_count': error_count,
            'target_companies': need_revenue_data[:limit]
        }
        
        os.makedirs('reports', exist_ok=True)
        output_file = 'reports/top200_hold_revenue_refetch.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n✅ 결과 저장: {output_file}", flush=True)
        
        if dry_run:
            print(f"\n⚠️  실제 재수집을 원하시면 --apply 플래그를 사용하세요:", flush=True)
            print(f"  python scripts/refetch_top200_hold_revenue.py --apply", flush=True)
        
        print("=" * 80, flush=True)
        
        return result
        
    except Exception as e:
        import traceback
        print(f"\n❌ 오류 발생: {e}", flush=True)
        traceback.print_exc()
        raise
    finally:
        db.close()

def main():
    import sys
    from datetime import datetime
    dry_run = '--apply' not in sys.argv
    limit = 200
    
    # limit 인자 확인
    for arg in sys.argv:
        if arg.startswith('--limit='):
            limit = int(arg.split('=')[1])
    
    refetch_top200_hold_revenue(dry_run=dry_run, limit=limit)

if __name__ == '__main__':
    main()

