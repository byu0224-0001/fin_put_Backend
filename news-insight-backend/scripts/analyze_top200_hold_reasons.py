# -*- coding: utf-8 -*-
"""
Top200 HOLD reason별 분해 리포트
HOLD_UNMAPPED_REVENUE_HIGH vs HOLD_LOW_CONF 구분
"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.investor_sector import InvestorSector
from app.models.stock import Stock
from sqlalchemy import text

def analyze_top200_hold_reasons():
    """Top200 HOLD reason별 분해"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("Top200 HOLD reason별 분해 리포트")
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
        
        # 🆕 P0.5: Top50 조회 (대형주 HOLD 확인용)
        result_top50 = db.execute(text("""
            SELECT s.ticker, s.stock_name, s.market_cap
            FROM stocks s
            WHERE s.market_cap IS NOT NULL
            ORDER BY s.market_cap DESC
            LIMIT 50
        """))
        
        top50_tickers = {row[0]: {'name': row[1], 'market_cap': row[2]} for row in result_top50}
        
        # HOLD 레코드 조회
        hold_sectors = db.query(InvestorSector).filter(
            InvestorSector.is_primary == True,
            InvestorSector.confidence.like('HOLD:%')
        ).all()
        
        # Top200 HOLD 분류
        top200_hold = {
            'HOLD_UNMAPPED_REVENUE_HIGH': [],
            'HOLD_LOW_CONF': [],
            'HOLD_UNKNOWN': []
        }
        
        # 🆕 P0.5: Top50 HOLD 분류 (대형주 HOLD 확인용)
        top50_hold = {
            'HOLD_UNMAPPED_REVENUE_HIGH': [],
            'HOLD_LOW_CONF': [],
            'HOLD_UNKNOWN': []
        }
        
        # 🆕 P0-C: quality_reason별 집계 (JSON_PARSE_FAIL 등)
        quality_reason_stats = {}
        
        for sector in hold_sectors:
            if sector.ticker not in top200_tickers:
                continue
            
            confidence = sector.confidence or ''
            if ':' in confidence:
                hold_reason = confidence.split(':', 1)[1]
            else:
                hold_reason = 'HOLD_UNKNOWN'
            
            boosting_log = sector.boosting_log or {}
            classification_meta = boosting_log.get('classification_meta', {})
            hold_reason_code = classification_meta.get('hold_reason_code', hold_reason)
            quality_reason = classification_meta.get('quality_reason', 'N/A')
            
            # quality_reason 통계
            if quality_reason not in quality_reason_stats:
                quality_reason_stats[quality_reason] = 0
            quality_reason_stats[quality_reason] += 1
            
            info = {
                'ticker': sector.ticker,
                'name': top200_tickers[sector.ticker]['name'],
                'market_cap': top200_tickers[sector.ticker]['market_cap'],
                'hold_reason_code': hold_reason_code,
                'confidence': confidence,
                'primary_sector_source': classification_meta.get('primary_sector_source', 'N/A'),
                'quality_reason': quality_reason  # 🆕 추가
            }
            
            if hold_reason_code == 'HOLD_UNMAPPED_REVENUE_HIGH':
                top200_hold['HOLD_UNMAPPED_REVENUE_HIGH'].append(info)
            elif hold_reason_code == 'HOLD_LOW_CONF':
                top200_hold['HOLD_LOW_CONF'].append(info)
            else:
                top200_hold['HOLD_UNKNOWN'].append(info)
            
            # 🆕 P0.5: Top50 HOLD 분류 (대형주 HOLD 확인용)
            if sector.ticker in top50_tickers:
                if hold_reason_code == 'HOLD_UNMAPPED_REVENUE_HIGH':
                    top50_hold['HOLD_UNMAPPED_REVENUE_HIGH'].append(info)
                elif hold_reason_code == 'HOLD_LOW_CONF':
                    top50_hold['HOLD_LOW_CONF'].append(info)
                else:
                    top50_hold['HOLD_UNKNOWN'].append(info)
        
        # 통계 출력
        total_hold = len(top200_hold['HOLD_UNMAPPED_REVENUE_HIGH']) + len(top200_hold['HOLD_LOW_CONF']) + len(top200_hold['HOLD_UNKNOWN'])
        unmapped_count = len(top200_hold['HOLD_UNMAPPED_REVENUE_HIGH'])
        low_conf_count = len(top200_hold['HOLD_LOW_CONF'])
        unknown_count = len(top200_hold['HOLD_UNKNOWN'])
        
        # 🆕 P0.5: Top50 HOLD 통계 (대형주 HOLD 확인용)
        total_hold_top50 = len(top50_hold['HOLD_UNMAPPED_REVENUE_HIGH']) + len(top50_hold['HOLD_LOW_CONF']) + len(top50_hold['HOLD_UNKNOWN'])
        unmapped_count_top50 = len(top50_hold['HOLD_UNMAPPED_REVENUE_HIGH'])
        low_conf_count_top50 = len(top50_hold['HOLD_LOW_CONF'])
        unknown_count_top50 = len(top50_hold['HOLD_UNKNOWN'])
        
        print(f"\n[Top50 대형주 HOLD 통계] (P0.5)")
        print(f"  총 HOLD: {total_hold_top50}개 ({total_hold_top50/50*100:.1f}%)")
        print(f"  HOLD_UNMAPPED_REVENUE_HIGH: {unmapped_count_top50}개 ({unmapped_count_top50/total_hold_top50*100:.1f}%)" if total_hold_top50 > 0 else "  HOLD_UNMAPPED_REVENUE_HIGH: 0개")
        print(f"  HOLD_LOW_CONF: {low_conf_count_top50}개 ({low_conf_count_top50/total_hold_top50*100:.1f}%)" if total_hold_top50 > 0 else "  HOLD_LOW_CONF: 0개")
        print(f"  HOLD_UNKNOWN: {unknown_count_top50}개 ({unknown_count_top50/total_hold_top50*100:.1f}%)" if total_hold_top50 > 0 else "  HOLD_UNKNOWN: 0개")
        
        if total_hold_top50 > 0:
            print(f"\n  [⚠️  경고] Top50 대형주에 HOLD {total_hold_top50}개 존재")
            print(f"  → 대형주 HOLD는 UX에 치명적이므로 우선 재수집/개선 필요")
            if unmapped_count_top50 > 0:
                print(f"  → Top50 UNMAPPED {unmapped_count_top50}개: 즉시 재수집 큐에 추가 권장")
            if low_conf_count_top50 > 0:
                print(f"  → Top50 LOW_CONF {low_conf_count_top50}개: 분류 로직 개선 필요")
        
        print(f"\n[Top200 HOLD 통계]")
        print(f"  총 HOLD: {total_hold}개 ({total_hold/200*100:.1f}%)")
        print(f"  HOLD_UNMAPPED_REVENUE_HIGH: {unmapped_count}개 ({unmapped_count/total_hold*100:.1f}%)" if total_hold > 0 else "  HOLD_UNMAPPED_REVENUE_HIGH: 0개")
        print(f"  HOLD_LOW_CONF: {low_conf_count}개 ({low_conf_count/total_hold*100:.1f}%)" if total_hold > 0 else "  HOLD_LOW_CONF: 0개")
        print(f"  HOLD_UNKNOWN: {unknown_count}개 ({unknown_count/total_hold*100:.1f}%)" if total_hold > 0 else "  HOLD_UNKNOWN: 0개")
        
        # 🆕 P0-C: quality_reason별 집계
        print(f"\n[Quality Reason별 집계]")
        for reason, count in sorted(quality_reason_stats.items(), key=lambda x: x[1], reverse=True):
            print(f"  {reason}: {count}개")
        
        # 해석
        print(f"\n[해석]")
        if unmapped_count > low_conf_count * 2:
            print(f"  ✅ 재수집으로 해결 가능: HOLD_UNMAPPED_REVENUE_HIGH가 대부분 ({unmapped_count}개)")
            print(f"  → Top200 매출 데이터 재수집 권장")
        elif low_conf_count > unmapped_count * 2:
            print(f"  ⚠️  분류 로직 개선 필요: HOLD_LOW_CONF가 대부분 ({low_conf_count}개)")
            print(f"  → 키워드/제품 신호 강화 or HOLD 조건 완화 필요")
        else:
            print(f"  → 혼합: 재수집 + 분류 로직 개선 병행 권장")
        
        # 🆕 P0-D: LOW_CONF 비중 확인
        if low_conf_count >= 10:
            print(f"\n  [WARN] HOLD_LOW_CONF가 10개 이상 ({low_conf_count}개)")
            print(f"  → 재수집만으로는 목표(≤10%) 도달 어려움")
            print(f"  → Top200 예외 정책(대형주 완화) or 키워드 신호 강화 필요")
        
        # 🆕 P0-C: JSON_PARSE_FAIL 확인
        json_parse_fail_count = quality_reason_stats.get('JSON_PARSE_FAIL', 0)
        if json_parse_fail_count > 0:
            print(f"\n  [WARN] JSON_PARSE_FAIL: {json_parse_fail_count}개")
            print(f"  → 저장 포맷 정리/마이그레이션 필요 (재수집으로는 해결 안 됨)")
        
        # 상위 티커 출력
        if unmapped_count > 0:
            print(f"\n[HOLD_UNMAPPED_REVENUE_HIGH 상위 10개]")
            sorted_unmapped = sorted(top200_hold['HOLD_UNMAPPED_REVENUE_HIGH'], key=lambda x: x['market_cap'] or 0, reverse=True)
            for idx, info in enumerate(sorted_unmapped[:10], 1):
                print(f"  {idx}. {info['name']} ({info['ticker']}): 시가총액 {info['market_cap']/1e12:.1f}조")
        
        if low_conf_count > 0:
            print(f"\n[HOLD_LOW_CONF 상위 10개]")
            sorted_low_conf = sorted(top200_hold['HOLD_LOW_CONF'], key=lambda x: x['market_cap'] or 0, reverse=True)
            for idx, info in enumerate(sorted_low_conf[:10], 1):
                print(f"  {idx}. {info['name']} ({info['ticker']}): 시가총액 {info['market_cap']/1e12:.1f}조")
        
        return {
            'total_hold': total_hold,
            'unmapped_count': unmapped_count,
            'low_conf_count': low_conf_count,
            'unknown_count': unknown_count,
            'top50_hold': {
                'total': total_hold_top50,
                'unmapped': unmapped_count_top50,
                'low_conf': low_conf_count_top50,
                'unknown': unknown_count_top50
            }
        }
        
    finally:
        db.close()

if __name__ == '__main__':
    analyze_top200_hold_reasons()

