# -*- coding: utf-8 -*-
"""
Top200 HOLD 중 UNMAPPED_REVENUE_HIGH만 샘플 재수집 (30개)
성공률 및 실패 원인 분포 확인
"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.investor_sector import InvestorSector
from app.models.stock import Stock
from sqlalchemy import text
from scripts.refetch_all_missing_revenue import fetch_and_update_revenue

def refetch_revenue_sample(limit=30, apply=False):
    """Top200 HOLD 중 UNMAPPED_REVENUE_HIGH만 샘플 재수집"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print(f"Top200 HOLD 중 UNMAPPED_REVENUE_HIGH 샘플 재수집 (limit={limit})")
        print("=" * 80)
        
        # Top200 조회
        result = db.execute(text("""
            SELECT s.ticker, s.stock_name, s.market_cap
            FROM stocks s
            WHERE s.market_cap IS NOT NULL
            ORDER BY s.market_cap DESC
            LIMIT 200
        """))
        
        top200_tickers = {row[0]: {'name': row[1], 'market_cap': row[2]} for row in result}
        
        # HOLD_UNMAPPED_REVENUE_HIGH 레코드 조회
        hold_sectors = db.query(InvestorSector).filter(
            InvestorSector.is_primary == True,
            InvestorSector.confidence.like('HOLD:%')
        ).all()
        
        unmapped_tickers = []
        for sector in hold_sectors:
            if sector.ticker not in top200_tickers:
                continue
            
            boosting_log = sector.boosting_log or {}
            classification_meta = boosting_log.get('classification_meta', {})
            hold_reason_code = classification_meta.get('hold_reason_code', '')
            
            if hold_reason_code == 'HOLD_UNMAPPED_REVENUE_HIGH':
                unmapped_tickers.append({
                    'ticker': sector.ticker,
                    'name': top200_tickers[sector.ticker]['name'],
                    'market_cap': top200_tickers[sector.ticker]['market_cap']
                })
        
        # 시가총액 순으로 정렬하여 상위 limit개 선택
        unmapped_tickers.sort(key=lambda x: x['market_cap'] or 0, reverse=True)
        selected_tickers = unmapped_tickers[:limit]
        
        print(f"\n[선택된 티커]")
        print(f"  총 {len(selected_tickers)}개")
        for idx, ticker_info in enumerate(selected_tickers[:10], 1):
            print(f"  {idx}. {ticker_info['name']} ({ticker_info['ticker']}): 시가총액 {ticker_info['market_cap']/1e12:.1f}조")
        if len(selected_tickers) > 10:
            print(f"  ... 외 {len(selected_tickers) - 10}개")
        
        if not apply:
            print(f"\n[DRY RUN 모드]")
            print(f"  실제 재수집을 실행하려면 --apply 옵션을 추가하세요.")
            return
        
        # 재수집 실행
        print(f"\n[재수집 시작]")
        success_count = 0
        fail_count = 0
        fail_reasons = {}
        
        for idx, ticker_info in enumerate(selected_tickers, 1):
            ticker = ticker_info['ticker']
            print(f"\n[{idx}/{len(selected_tickers)}] {ticker_info['name']} ({ticker}) 재수집 중...")
            
            try:
                success = fetch_and_update_revenue(db, ticker, apply=True)
                if success:
                    success_count += 1
                    print(f"  ✅ 성공")
                else:
                    fail_count += 1
                    reason = 'UNKNOWN'
                    fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                    print(f"  ❌ 실패")
            except Exception as e:
                fail_count += 1
                reason = type(e).__name__
                fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                print(f"  ❌ 오류: {e}")
        
        # 결과 리포트
        print(f"\n" + "=" * 80)
        print(f"[재수집 결과]")
        print("=" * 80)
        print(f"  성공: {success_count}개 ({success_count/len(selected_tickers)*100:.1f}%)")
        print(f"  실패: {fail_count}개 ({fail_count/len(selected_tickers)*100:.1f}%)")
        
        if fail_reasons:
            print(f"\n[실패 원인 분포]")
            for reason, count in sorted(fail_reasons.items(), key=lambda x: x[1], reverse=True):
                print(f"  {reason}: {count}개")
        
        # 해석
        print(f"\n[해석]")
        success_rate = success_count / len(selected_tickers) * 100 if selected_tickers else 0
        if success_rate >= 70:
            print(f"  ✅ 성공률 {success_rate:.1f}% - Top200 전체 재수집 권장")
        elif success_rate >= 50:
            print(f"  ⚠️  성공률 {success_rate:.1f}% - Top200 재수집 가능하나 실패 원인 분석 필요")
        else:
            print(f"  ❌ 성공률 {success_rate:.1f}% - 재수집 로직 개선 필요")
        
    finally:
        db.close()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true', help='실제 재수집 실행')
    parser.add_argument('--limit', type=int, default=30, help='샘플 개수 (기본: 30)')
    args = parser.parse_args()
    
    refetch_revenue_sample(limit=args.limit, apply=args.apply)

