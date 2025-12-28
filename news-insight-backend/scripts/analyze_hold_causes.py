# -*- coding: utf-8 -*-
"""
HOLD 원인 분석 스크립트 (GPT/Gemini 피드백: HOLD 원인 분석)

Coverage-A 분포, top1 score 분포, margin 분포 등 정책 파라미터 튜닝을 위한 분포 분석
"""
import sys
import os
import json
import statistics
from collections import defaultdict

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
from app.models.investor_sector import InvestorSector
from app.services.sector_classifier import (
    classify_sector_rule_based,
    calculate_revenue_sector_scores,
    normalize_segment_name,
    is_neutral_segment,
    SEGMENT_TO_SECTOR_MAP
)

def analyze_hold_causes(limit=1000, use_dry_run_results=False):
    """HOLD 원인 분석 (DRY RUN 결과 지원)"""
    db = SessionLocal()
    
    try:
        print("=" * 80, flush=True)
        print("HOLD 원인 분석", flush=True)
        print("=" * 80, flush=True)
        
        hold_tickers = []
        hold_reasons = {}
        
        if use_dry_run_results:
            # 🆕 DRY RUN 결과 파일에서 HOLD 기업 목록 로드
            report_file = 'reports/reclassify_all_companies_report.json'
            if os.path.exists(report_file):
                with open(report_file, 'r', encoding='utf-8') as f:
                    report = json.load(f)
                
                # Top100 HOLD 리스트에서 추출
                top100_hold = report.get('stats', {}).get('top100_hold', [])
                hold_tickers = [h['ticker'] for h in top100_hold]
                hold_reasons = {h['ticker']: h['hold_reason'] for h in top100_hold}
                
                print(f"\n[DRY RUN 결과 사용]", flush=True)
                print(f"  Top100 HOLD 기업 {len(hold_tickers)}개 로드", flush=True)
            else:
                print(f"\n❌ DRY RUN 결과 파일이 없습니다: {report_file}", flush=True)
                print(f"  → python scripts/reclassify_all_companies.py를 먼저 실행하세요.", flush=True)
                return None
        else:
            # DB에서 HOLD 기업 조회
            hold_sectors = db.query(InvestorSector).filter(
                InvestorSector.is_primary == True,
                InvestorSector.confidence.like('HOLD%')
            ).limit(limit).all()
            
            hold_tickers = [s.ticker for s in hold_sectors]
            for sector in hold_sectors:
                if sector.confidence and ':' in sector.confidence:
                    hold_reasons[sector.ticker] = sector.confidence.split(':', 1)[1]
                else:
                    hold_reasons[sector.ticker] = 'HOLD'
        
        if not hold_tickers:
            print(f"\n⚠️  HOLD 기업이 없습니다.", flush=True)
            return None
        
        print(f"\nHOLD 기업 {len(hold_tickers)}개 분석 중...", flush=True)
        
        analysis = {
            'total_hold': len(hold_tickers),
            'hold_reason_distribution': defaultdict(int),
            'coverage_distribution': [],
            'top1_score_distribution': [],
            'margin_distribution': [],
            'revenue_data_stats': {
                'has_data': 0,
                'no_data': 0
            },
            'keyword_product_stats': {
                'has_strong_keyword': 0,
                'has_strong_product': 0,
                'has_both': 0,
                'has_neither': 0
            },
            'by_market_cap': {
                'top200': {'count': 0, 'reasons': defaultdict(int)},
                'top500': {'count': 0, 'reasons': defaultdict(int)},
                'others': {'count': 0, 'reasons': defaultdict(int)}
            }
        }
        
        for ticker in hold_tickers:
            stock = db.query(Stock).filter(Stock.ticker == ticker).first()
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == ticker
            ).order_by(CompanyDetail.updated_at.desc()).first()
            
            if not stock or not detail:
                continue
            
            # HOLD 사유 추출
            hold_reason = hold_reasons.get(ticker, 'HOLD_UNKNOWN')
            analysis['hold_reason_distribution'][hold_reason] += 1
            
            # 재분류하여 점수 계산
            revenue_scores, revenue_audit = calculate_revenue_sector_scores(detail.revenue_by_segment)
            coverage = revenue_audit.get('coverage', 0)
            
            # Top1, Top2 score 계산
            top1_score = 0.0
            top2_score = 0.0
            if revenue_scores:
                sorted_scores = sorted(revenue_scores.items(), key=lambda x: x[1], reverse=True)
                if len(sorted_scores) > 0:
                    top1_score = sorted_scores[0][1]
                if len(sorted_scores) > 1:
                    top2_score = sorted_scores[1][1]
            margin = top1_score - top2_score
            
            # 통계 수집
            analysis['coverage_distribution'].append(coverage)
            analysis['top1_score_distribution'].append(top1_score)
            analysis['margin_distribution'].append(margin)
            
            # 매출 데이터 유무
            has_revenue_data = bool(detail.revenue_by_segment and isinstance(detail.revenue_by_segment, dict) and len(detail.revenue_by_segment) > 0)
            if has_revenue_data:
                analysis['revenue_data_stats']['has_data'] += 1
            else:
                analysis['revenue_data_stats']['no_data'] += 1
            
            # 키워드/제품 매칭 확인 (간단히)
            has_keyword = bool(detail.keywords and len(detail.keywords) > 0)
            has_product = bool(detail.products and len(detail.products) > 0)
            
            if has_keyword and has_product:
                analysis['keyword_product_stats']['has_both'] += 1
            elif has_keyword:
                analysis['keyword_product_stats']['has_strong_keyword'] += 1
            elif has_product:
                analysis['keyword_product_stats']['has_strong_product'] += 1
            else:
                analysis['keyword_product_stats']['has_neither'] += 1
            
            # 시가총액별 분류
            market_cap = stock.market_cap or 0
            if market_cap > 0:
                # 시가총액 순위 추정 (간단히)
                if market_cap >= 1000000000000:  # 1조 이상 (대략 Top 200)
                    analysis['by_market_cap']['top200']['count'] += 1
                    analysis['by_market_cap']['top200']['reasons'][hold_reason] += 1
                elif market_cap >= 500000000000:  # 5000억 이상 (대략 Top 500)
                    analysis['by_market_cap']['top500']['count'] += 1
                    analysis['by_market_cap']['top500']['reasons'][hold_reason] += 1
                else:
                    analysis['by_market_cap']['others']['count'] += 1
                    analysis['by_market_cap']['others']['reasons'][hold_reason] += 1
        
        # 통계 계산
        def calc_stats(values):
            if not values:
                return None
            return {
                'count': len(values),
                'mean': statistics.mean(values),
                'median': statistics.median(values),
                'min': min(values),
                'max': max(values),
                'stddev': statistics.stdev(values) if len(values) > 1 else 0
            }
        
        analysis['coverage_stats'] = calc_stats(analysis['coverage_distribution'])
        analysis['top1_score_stats'] = calc_stats(analysis['top1_score_distribution'])
        analysis['margin_stats'] = calc_stats(analysis['margin_distribution'])
        
        # 히스토그램 생성 (간단히)
        def create_histogram(values, bins=10):
            if not values:
                return {}
            min_val = min(values)
            max_val = max(values)
            bin_size = (max_val - min_val) / bins if max_val > min_val else 1
            hist = defaultdict(int)
            for val in values:
                bin_idx = int((val - min_val) / bin_size) if bin_size > 0 else 0
                bin_idx = min(bin_idx, bins - 1)
                hist[bin_idx] += 1
            return dict(hist)
        
        analysis['coverage_histogram'] = create_histogram(analysis['coverage_distribution'])
        analysis['top1_score_histogram'] = create_histogram(analysis['top1_score_distribution'])
        analysis['margin_histogram'] = create_histogram(analysis['margin_distribution'])
        
        # 콘솔 출력
        print("\n" + "=" * 80, flush=True)
        print("HOLD 원인 분석 결과", flush=True)
        print("=" * 80, flush=True)
        
        print(f"\n[HOLD 사유 분포]", flush=True)
        for reason, count in sorted(analysis['hold_reason_distribution'].items(), key=lambda x: x[1], reverse=True):
            pct = (count / analysis['total_hold'] * 100) if analysis['total_hold'] > 0 else 0
            print(f"  {reason}: {count}개 ({pct:.1f}%)", flush=True)
        
        print(f"\n[Coverage-A 분포]", flush=True)
        if analysis['coverage_stats']:
            stats = analysis['coverage_stats']
            print(f"  평균: {stats['mean']:.1f}%", flush=True)
            print(f"  중앙값: {stats['median']:.1f}%", flush=True)
            print(f"  최소: {stats['min']:.1f}%", flush=True)
            print(f"  최대: {stats['max']:.1f}%", flush=True)
            print(f"  표준편차: {stats['stddev']:.1f}%", flush=True)
        
        print(f"\n[Top1 Score 분포]", flush=True)
        if analysis['top1_score_stats']:
            stats = analysis['top1_score_stats']
            print(f"  평균: {stats['mean']:.3f}", flush=True)
            print(f"  중앙값: {stats['median']:.3f}", flush=True)
            print(f"  최소: {stats['min']:.3f}", flush=True)
            print(f"  최대: {stats['max']:.3f}", flush=True)
        
        print(f"\n[Margin (Top1-Top2) 분포]", flush=True)
        if analysis['margin_stats']:
            stats = analysis['margin_stats']
            print(f"  평균: {stats['mean']:.3f}", flush=True)
            print(f"  중앙값: {stats['median']:.3f}", flush=True)
            print(f"  최소: {stats['min']:.3f}", flush=True)
            print(f"  최대: {stats['max']:.3f}", flush=True)
        
        print(f"\n[매출 데이터 유무]", flush=True)
        print(f"  데이터 있음: {analysis['revenue_data_stats']['has_data']}개", flush=True)
        print(f"  데이터 없음: {analysis['revenue_data_stats']['no_data']}개", flush=True)
        
        print(f"\n[키워드/제품 매칭]", flush=True)
        print(f"  둘 다 있음: {analysis['keyword_product_stats']['has_both']}개", flush=True)
        print(f"  키워드만: {analysis['keyword_product_stats']['has_strong_keyword']}개", flush=True)
        print(f"  제품만: {analysis['keyword_product_stats']['has_strong_product']}개", flush=True)
        print(f"  둘 다 없음: {analysis['keyword_product_stats']['has_neither']}개", flush=True)
        
        print(f"\n[시가총액별 HOLD 비율]", flush=True)
        print(f"  Top 200 (1조 이상): {analysis['by_market_cap']['top200']['count']}개", flush=True)
        for reason, count in sorted(analysis['by_market_cap']['top200']['reasons'].items(), key=lambda x: x[1], reverse=True):
            print(f"    {reason}: {count}개", flush=True)
        print(f"  Top 500 (5000억 이상): {analysis['by_market_cap']['top500']['count']}개", flush=True)
        print(f"  기타: {analysis['by_market_cap']['others']['count']}개", flush=True)
        
        # 파일 저장
        os.makedirs('reports', exist_ok=True)
        output_file = 'reports/hold_causes_analysis.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(analysis, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n✅ 분석 결과 저장: {output_file}", flush=True)
        print("=" * 80, flush=True)
        
        return analysis
        
    except Exception as e:
        import traceback
        print(f"\n❌ 오류 발생: {e}", flush=True)
        traceback.print_exc()
        raise
    finally:
        db.close()

if __name__ == '__main__':
    import sys
    limit = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 1000
    use_dry_run = '--dry-run' in sys.argv or '--use-dry-run' in sys.argv
    
    analyze_hold_causes(limit=limit, use_dry_run_results=use_dry_run)

