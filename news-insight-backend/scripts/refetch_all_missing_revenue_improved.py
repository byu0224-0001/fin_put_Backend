# -*- coding: utf-8 -*-
"""
DB에서 매출 비중이 없는 모든 기업들의 매출 비중 재수집 및 성공률 모니터링 (개선 버전)

개선 사항 (GPT/Gemini 피드백 반영):
1. DRY RUN 결과를 실행 가능한 계획 리포트로 변경
2. 재수집 "성공" 정의를 더 엄격하게
3. Top20 전용 실패 원인 분해 리포트
4. 단계별 성공률 KPI 측정
"""
import sys
import os
import json
import time
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Tuple, Optional

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


def get_missing_revenue_companies(db, limit: Optional[int] = None):
    """매출 비중이 없는 모든 기업 조회"""
    query = db.query(CompanyDetail, Stock).join(
        Stock, CompanyDetail.ticker == Stock.ticker
    ).filter(
        # 매출 비중이 없는 경우: None, {}, 또는 빈 dict
        (
            (CompanyDetail.revenue_by_segment == None) |
            (CompanyDetail.revenue_by_segment == {}) |
            (CompanyDetail.revenue_by_segment == '{}')
        )
    ).order_by(Stock.market_cap.desc().nullslast())
    
    if limit:
        query = query.limit(limit)
    
    results = query.all()
    
    companies = []
    for detail, stock in results:
        # 🆕 CompanyDetail 존재, 티커 유효성 확인
        if not detail or not stock:
            continue
        
        companies.append({
            'ticker': detail.ticker,
            'name': stock.stock_name if stock else detail.ticker,
            'market_cap': stock.market_cap if stock else 0,
            'has_company_detail': True,
            'ticker_valid': bool(detail.ticker and len(detail.ticker) >= 4)
        })
    
    return companies


def validate_revenue_data(revenue_data: Optional[Dict]) -> Tuple[bool, str]:
    """
    🆕 재수집 "성공" 정의를 더 엄격하게
    
    Returns:
        (is_valid, reason)
    """
    if not revenue_data:
        return False, "NO_REVENUE_DATA"
    
    if not isinstance(revenue_data, dict):
        return False, "INVALID_FORMAT"
    
    if len(revenue_data) == 0:
        return False, "EMPTY_DICT"
    
    # 세그먼트/비중이 파싱 가능한지 확인
    valid_segments = 0
    total_pct = 0.0
    
    for segment, pct in revenue_data.items():
        if not segment or not isinstance(segment, str):
            continue
        
        if isinstance(pct, (int, float)) and pct > 0:
            valid_segments += 1
            total_pct += pct
    
    if valid_segments == 0:
        return False, "NO_VALID_SEGMENTS"
    
    # 최소 top1 세그먼트 비중이 존재하는지 확인
    if total_pct < 1.0:  # 최소 1% 이상
        return False, "TOTAL_PCT_TOO_LOW"
    
    return True, "SUCCESS"


def fetch_and_update_revenue(
    db, 
    ticker: str, 
    stock_name: str, 
    dart_parser: DartParser, 
    llm_handler: LLMHandler, 
    year: int = 2024,
    track_steps: bool = True
) -> Tuple[bool, str, Optional[Dict], Dict]:
    """
    매출 데이터 재수집 (단계별 성공률 추적)
    
    Returns:
        (success, error_code, revenue_data, step_tracking)
        error_code: 'SUCCESS', 'NO_REPORT', 'NO_SECTION', 'LLM_FAIL', 'NO_REVENUE_DATA', 'VALIDATION_FAIL', 'NO_DETAIL', 'ERROR'
        step_tracking: {'dart_fetch': bool, 'section_extract': bool, 'llm_extract': bool, 'revenue_extract': bool, 'validation': bool}
    """
    step_tracking = {
        'dart_fetch': False,
        'section_extract': False,
        'llm_extract': False,
        'revenue_extract': False,
        'validation': False
    }
    
    try:
        # Step 1: DART API로 섹션 추출
        combined_text = dart_parser.extract_key_sections(ticker, year)
        step_tracking['dart_fetch'] = True
        
        if not combined_text:
            logger.warning(f"[{ticker}] {stock_name}: DART 섹션 추출 실패")
            return False, "NO_REPORT", None, step_tracking
        
        if len(combined_text) < 200:
            logger.warning(f"[{ticker}] {stock_name}: DART 섹션 추출 성공했지만 내용 부족 ({len(combined_text)}자)")
            return False, "NO_SECTION", None, step_tracking
        
        step_tracking['section_extract'] = True
        logger.info(f"[{ticker}] {stock_name}: DART 섹션 추출 성공 ({len(combined_text)}자)")
        
        # Step 2: 임베딩 필터로 관련 청크 선택
        try:
            filtered_text = select_relevant_chunks(combined_text, ticker=ticker)
            effective_text = filtered_text if filtered_text and len(filtered_text) > 200 else combined_text
        except Exception as e:
            logger.warning(f"[{ticker}] 임베딩 필터링 실패, 원문 사용: {e}")
            effective_text = combined_text
        
        # 길이 제한
        if len(effective_text) > MAX_LLM_CHARS:
            effective_text = effective_text[:MAX_LLM_CHARS]
        
        # Step 3: LLM으로 구조화된 데이터 추출
        structured_data = llm_handler.extract_structured_data(
            effective_text,
            ticker=ticker,
            company_name=stock_name
        )
        
        if not structured_data:
            logger.warning(f"[{ticker}] {stock_name}: LLM 구조화 실패")
            return False, "LLM_FAIL", None, step_tracking
        
        step_tracking['llm_extract'] = True
        
        # Step 4: revenue_by_segment 추출
        revenue_data = structured_data.get('revenue_by_segment', {})
        
        if not revenue_data or not isinstance(revenue_data, dict) or len(revenue_data) == 0:
            logger.warning(f"[{ticker}] {stock_name}: 매출비중 데이터 없음")
            return False, "NO_REVENUE_DATA", None, step_tracking
        
        step_tracking['revenue_extract'] = True
        
        # Step 5: 🆕 엄격한 검증
        is_valid, validation_reason = validate_revenue_data(revenue_data)
        
        if not is_valid:
            logger.warning(f"[{ticker}] {stock_name}: 매출비중 검증 실패 - {validation_reason}")
            return False, f"VALIDATION_FAIL:{validation_reason}", None, step_tracking
        
        step_tracking['validation'] = True
        
        # Step 6: DB 업데이트
        detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == ticker).first()
        if detail:
            detail.revenue_by_segment = revenue_data
            detail.updated_at = datetime.utcnow()
            db.commit()
            logger.info(f"[{ticker}] {stock_name}: 매출비중 업데이트 완료 ({len(revenue_data)}개 세그먼트, 총 {sum(revenue_data.values()):.1f}%)")
            return True, "SUCCESS", revenue_data, step_tracking
        else:
            logger.warning(f"[{ticker}] {stock_name}: CompanyDetail 없음")
            return False, "NO_DETAIL", None, step_tracking
            
    except Exception as e:
        logger.error(f"[{ticker}] {stock_name}: 오류 - {e}")
        import traceback
        traceback.print_exc()
        return False, f"ERROR: {str(e)}", None, step_tracking


def analyze_failure_cause(error_code: str, step_tracking: Dict) -> str:
    """
    🆕 실패 원인 분해 (A/B/C)
    
    Returns:
        'A': DART에서 해당 섹션/표를 못 찾음
        'B': 표/텍스트는 있는데 LLM이 구조화에 실패
        'C': 정규화/매핑에서 실패 (값은 있는데 검증 실패)
        'OTHER': 기타
    """
    if error_code in ['NO_REPORT', 'NO_SECTION']:
        return 'A'  # DART에서 해당 섹션/표를 못 찾음
    elif error_code == 'LLM_FAIL':
        return 'B'  # 표/텍스트는 있는데 LLM이 구조화에 실패
    elif error_code.startswith('VALIDATION_FAIL'):
        return 'C'  # 정규화/매핑에서 실패
    elif error_code == 'NO_REVENUE_DATA':
        # step_tracking을 보고 판단
        if step_tracking.get('llm_extract'):
            return 'B'  # LLM은 성공했지만 revenue_by_segment가 없음
        else:
            return 'A'  # LLM 자체가 실패
    else:
        return 'OTHER'


def refetch_all_missing_revenue(
    dry_run: bool = True,
    limit: Optional[int] = None,
    batch_size: int = 50,
    year: int = 2024
) -> Dict:
    """
    매출 비중이 없는 모든 기업 재수집 및 모니터링 (개선 버전)
    """
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
        print("매출 비중 없는 기업 재수집 및 성공률 모니터링 (개선 버전)", flush=True)
        print("=" * 80, flush=True)
        
        if dry_run:
            print("\n⚠️  DRY RUN 모드 (실제 수정하지 않음)", flush=True)
        else:
            print("\n✅ 실제 수정 모드", flush=True)
        
        # 매출 비중 없는 기업 조회
        print("\n[1단계] 매출 비중 없는 기업 조회 중...", flush=True)
        missing_companies = get_missing_revenue_companies(db, limit=limit)
        
        print(f"\n[대상 기업]", flush=True)
        print(f"  매출 비중 없는 기업: {len(missing_companies)}개", flush=True)
        if limit:
            print(f"  처리 제한: {limit}개", flush=True)
        
        if not missing_companies:
            print("\n✅ 매출 비중이 필요한 기업이 없습니다.", flush=True)
            return None
        
        # 🆕 DRY RUN: 실행 가능한 계획 리포트 생성
        if dry_run:
            print("\n[DRY RUN 계획 리포트]", flush=True)
            
            # CompanyDetail 존재, 티커 유효성 확인
            eligible_count = sum(1 for c in missing_companies if c.get('has_company_detail') and c.get('ticker_valid'))
            will_call_dart_count = eligible_count  # 모든 eligible 기업에 대해 DART 호출 예정
            will_call_llm_count = eligible_count  # DART 성공 시 LLM 호출 예정
            
            # 예상 비용 계산 (대략적)
            estimated_llm_calls = will_call_llm_count
            estimated_cost_usd = estimated_llm_calls * 0.01  # 대략 $0.01 per call
            
            print(f"  Eligible 기업 (CompanyDetail 존재, 티커 유효): {eligible_count}개", flush=True)
            print(f"  예상 DART API 호출: {will_call_dart_count}회", flush=True)
            print(f"  예상 LLM API 호출: {will_call_llm_count}회", flush=True)
            print(f"  예상 비용: 약 ${estimated_cost_usd:.2f}", flush=True)
            
            # 우선순위 표시
            top20 = missing_companies[:20]
            print(f"\n[우선 재수집 대상 (시가총액 상위 20개)]", flush=True)
            for i, company in enumerate(top20, 1):
                market_cap = company.get('market_cap', 0)
                if market_cap >= 1000000000000:
                    market_cap_str = f"{market_cap/1000000000000:.1f}조원"
                else:
                    market_cap_str = f"{market_cap/1000000000:.1f}억원"
                print(f"  {i}. {company['name']} ({company['ticker']}) - {market_cap_str}", flush=True)
            
            # DRY RUN 결과 저장
            plan_report = {
                'generated_at': datetime.now().isoformat(),
                'dry_run': True,
                'year': year,
                'planning': {
                    'total_target': len(missing_companies),
                    'eligible_count': eligible_count,
                    'will_call_dart_count': will_call_dart_count,
                    'will_call_llm_count': will_call_llm_count,
                    'estimated_cost_usd': estimated_cost_usd,
                    'top20_priorities': top20
                }
            }
            
            os.makedirs('reports', exist_ok=True)
            output_file = 'reports/refetch_revenue_plan.json'
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(plan_report, f, ensure_ascii=False, indent=2, default=str)
            
            print(f"\n✅ 계획 리포트 저장: {output_file}", flush=True)
            print(f"\n⚠️  실제 재수집을 원하시면 --apply 플래그를 사용하세요:", flush=True)
            print(f"  python scripts/refetch_all_missing_revenue.py --apply --limit 20", flush=True)
            print("=" * 80, flush=True)
            
            return plan_report
        
        # 실제 재수집 진행
        print("\n[2단계] 매출 비중 재수집 진행 중...", flush=True)
        
        results = []
        success_count = 0
        fail_count = 0
        error_stats = defaultdict(int)
        failure_cause_stats = defaultdict(int)  # A/B/C 분포
        step_success_stats = defaultdict(int)  # 단계별 성공률
        
        # Top20 전용 통계
        top20_results = []
        top20_success_count = 0
        top20_fail_count = 0
        top20_failure_cause_stats = defaultdict(int)
        
        total = len(missing_companies)
        if limit:
            total = min(total, limit)
        
        for idx, company in enumerate(missing_companies[:limit] if limit else missing_companies, 1):
            ticker = company['ticker']
            name = company['name']
            market_cap = company.get('market_cap', 0)
            is_top20 = idx <= 20
            
            print(f"\n[{idx}/{total}] {name} ({ticker}) 재수집 중...", flush=True)
            
            # 실제 재수집
            success, error_code, revenue_data, step_tracking = fetch_and_update_revenue(
                db, ticker, name, dart_parser, llm_handler, year, track_steps=True
            )
            
            # 단계별 성공률 추적
            for step, step_success in step_tracking.items():
                if step_success:
                    step_success_stats[step] += 1
            
            if success:
                success_count += 1
                segment_count = len(revenue_data) if revenue_data else 0
                total_pct = sum(revenue_data.values()) if revenue_data else 0
                
                results.append({
                    'ticker': ticker,
                    'name': name,
                    'market_cap': market_cap,
                    'status': 'SUCCESS',
                    'error_code': error_code,
                    'revenue_data': revenue_data,
                    'segment_count': segment_count,
                    'total_pct': total_pct,
                    'step_tracking': step_tracking
                })
                print(f"  ✅ 성공: {segment_count}개 세그먼트, 총 {total_pct:.1f}%", flush=True)
                
                if is_top20:
                    top20_success_count += 1
                    top20_results.append(results[-1])
            else:
                fail_count += 1
                error_stats[error_code] += 1
                
                # 실패 원인 분해
                failure_cause = analyze_failure_cause(error_code, step_tracking)
                failure_cause_stats[failure_cause] += 1
                
                results.append({
                    'ticker': ticker,
                    'name': name,
                    'market_cap': market_cap,
                    'status': 'FAIL',
                    'error_code': error_code,
                    'revenue_data': None,
                    'step_tracking': step_tracking,
                    'failure_cause': failure_cause
                })
                print(f"  ❌ 실패: {error_code} (원인: {failure_cause})", flush=True)
                
                if is_top20:
                    top20_fail_count += 1
                    top20_failure_cause_stats[failure_cause] += 1
                    top20_results.append(results[-1])
            
            # Rate limit 방지
            if idx < total:
                time.sleep(1)
            
            # 진행률 출력
            if idx % 10 == 0 or idx == total:
                progress = (idx / total * 100) if total > 0 else 0
                print(f"\n  진행: {idx}/{total} ({progress:.1f}%)", flush=True)
        
        # 통계 계산
        total_processed = len(results)
        success_rate = (success_count / total_processed * 100) if total_processed > 0 else 0
        
        # 단계별 성공률 계산
        step_success_rates = {
            step: (count / total_processed * 100) if total_processed > 0 else 0
            for step, count in step_success_stats.items()
        }
        
        # Top20 성공률
        top20_success_rate = (top20_success_count / 20 * 100) if top20_success_count + top20_fail_count > 0 else 0
        
        # 결과 리포트 생성
        report = {
            'generated_at': datetime.now().isoformat(),
            'dry_run': False,
            'year': year,
            'statistics': {
                'total_target': len(missing_companies),
                'total_processed': total_processed,
                'success_count': success_count,
                'fail_count': fail_count,
                'success_rate': success_rate,
                'error_distribution': dict(error_stats),
                'failure_cause_distribution': dict(failure_cause_stats),  # 🆕 A/B/C 분포
                'step_success_rates': step_success_rates  # 🆕 단계별 성공률
            },
            'top20_analysis': {  # 🆕 Top20 전용 분석
                'success_count': top20_success_count,
                'fail_count': top20_fail_count,
                'success_rate': top20_success_rate,
                'failure_cause_distribution': dict(top20_failure_cause_stats),
                'results': top20_results
            },
            'results': results
        }
        
        # 리포트 출력
        print("\n" + "=" * 80, flush=True)
        print("재수집 결과", flush=True)
        print("=" * 80, flush=True)
        
        print(f"\n[처리 통계]", flush=True)
        print(f"  대상 기업: {len(missing_companies)}개", flush=True)
        print(f"  처리 완료: {total_processed}개", flush=True)
        print(f"  성공: {success_count}개", flush=True)
        print(f"  실패: {fail_count}개", flush=True)
        print(f"  성공률: {success_rate:.1f}%", flush=True)
        
        print(f"\n[단계별 성공률]", flush=True)
        for step, rate in step_success_rates.items():
            print(f"  {step}: {rate:.1f}%", flush=True)
        
        if error_stats:
            print(f"\n[실패 원인 분석]", flush=True)
            for error_code, count in sorted(error_stats.items(), key=lambda x: x[1], reverse=True):
                print(f"  {error_code}: {count}개", flush=True)
        
        print(f"\n[실패 원인 분해 (A/B/C)]", flush=True)
        print(f"  A (DART에서 섹션/표 못 찾음): {failure_cause_stats.get('A', 0)}개", flush=True)
        print(f"  B (LLM 구조화 실패): {failure_cause_stats.get('B', 0)}개", flush=True)
        print(f"  C (검증 실패): {failure_cause_stats.get('C', 0)}개", flush=True)
        print(f"  기타: {failure_cause_stats.get('OTHER', 0)}개", flush=True)
        
        print(f"\n[Top20 분석]", flush=True)
        print(f"  성공: {top20_success_count}개", flush=True)
        print(f"  실패: {top20_fail_count}개", flush=True)
        print(f"  성공률: {top20_success_rate:.1f}%", flush=True)
        print(f"  실패 원인 분해:", flush=True)
        for cause, count in top20_failure_cause_stats.items():
            print(f"    {cause}: {count}개", flush=True)
        
        # 리포트 저장
        os.makedirs('reports', exist_ok=True)
        output_file = 'reports/refetch_revenue_monitoring.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n✅ 결과 저장: {output_file}", flush=True)
        print("=" * 80, flush=True)
        
        return report
        
    except Exception as e:
        import traceback
        print(f"\n❌ 오류 발생: {e}", flush=True)
        traceback.print_exc()
        raise
    finally:
        db.close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='매출 비중 없는 기업 재수집 및 모니터링 (개선 버전)')
    parser.add_argument('--apply', action='store_true', help='실제 수정 모드 (기본값: DRY RUN)')
    parser.add_argument('--limit', type=int, default=None, help='처리할 기업 수 제한')
    parser.add_argument('--batch-size', type=int, default=50, help='배치 처리 크기')
    parser.add_argument('--year', type=int, default=2024, help='대상 연도')
    
    args = parser.parse_args()
    
    refetch_all_missing_revenue(
        dry_run=not args.apply,
        limit=args.limit,
        batch_size=args.batch_size,
        year=args.year
    )


if __name__ == '__main__':
    main()

