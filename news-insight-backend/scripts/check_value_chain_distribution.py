#!/usr/bin/env python3
"""
밸류체인 분류 결과 검증 스크립트

Check A: Anchor 쏠림 현상 확인
Check B: value_chain_detail 활용 여부 확인
Check C: confidence 분포 sanity check
"""

import sys
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db import get_db

def check_value_chain_distribution():
    """밸류체인 분류 결과 검증"""
    db = next(get_db())
    
    try:
        print("=" * 80)
        print("밸류체인 분류 결과 검증")
        print("=" * 80)
        
        # Check A: Anchor 쏠림 현상 확인
        print("\n[Check A] Anchor 쏠림 현상 확인")
        print("-" * 80)
        result = db.execute(text("""
            SELECT 
                value_chain,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM investor_sector
            WHERE is_primary = TRUE
                AND value_chain IS NOT NULL
            GROUP BY value_chain
            ORDER BY COUNT(*) DESC
        """))
        
        total = 0
        vc_distribution = {}
        for row in result:
            vc = row[0] or 'NULL'
            count = row[1]
            pct = row[2]
            vc_distribution[vc] = {'count': count, 'percentage': pct}
            total += count
            print(f"  {vc:<20} {count:>6,}개 ({pct:>5.2f}%)")
        
        print(f"\n  총 레코드 수: {total:,}개")
        
        # 해석
        mid_count = vc_distribution.get('MID_HARD', {}).get('count', 0) + vc_distribution.get('MID_SOFT', {}).get('count', 0)
        mid_pct = (mid_count / total * 100) if total > 0 else 0
        
        upstream_pct = vc_distribution.get('UPSTREAM', {}).get('percentage', 0)
        down_biz_pct = vc_distribution.get('DOWN_BIZ', {}).get('percentage', 0)
        down_service_pct = vc_distribution.get('DOWN_SERVICE', {}).get('percentage', 0)
        down_total_pct = down_biz_pct + down_service_pct
        
        print("\n  해석:")
        if mid_pct > 80:
            print(f"  [WARN] MID_* 비율이 {mid_pct:.1f}%로 매우 높습니다.")
        else:
            print(f"  [OK] MID_* 비율: {mid_pct:.1f}% (정상 범위)")
        
        if upstream_pct >= 5 and down_total_pct >= 5:
            print(f"  [OK] UPSTREAM: {upstream_pct:.1f}%, DOWN_*: {down_total_pct:.1f}% (각각 5% 이상, 건강)")
        elif upstream_pct < 5 or down_total_pct < 5:
            print(f"  [WARN] UPSTREAM: {upstream_pct:.1f}%, DOWN_*: {down_total_pct:.1f}% (일부가 5% 미만)")
        
        # Check B: value_chain_detail 활용 여부 확인
        print("\n[Check B] value_chain_detail 활용 여부 확인")
        print("-" * 80)
        result = db.execute(text("""
            SELECT 
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE value_chain_detail IS NOT NULL) as has_detail,
                COUNT(*) FILTER (WHERE value_chain_detail IS NULL) as no_detail
            FROM investor_sector
            WHERE is_primary = TRUE
                AND value_chain IS NOT NULL
        """))
        
        row = result.fetchone()
        if row:
            total = row[0]
            has_detail = row[1]
            no_detail = row[2]
            detail_pct = (has_detail / total * 100) if total > 0 else 0
            
            print(f"  총 레코드 수: {total:,}개")
            print(f"  value_chain_detail이 있는 레코드: {has_detail:,}개 ({detail_pct:.2f}%)")
            print(f"  value_chain_detail이 없는 레코드: {no_detail:,}개 ({100-detail_pct:.2f}%)")
            
            print("\n  해석:")
            if 10 <= detail_pct <= 30:
                print(f"  [OK] {detail_pct:.1f}% (10~30% 범위, 이상적)")
            elif detail_pct < 10:
                print(f"  [WARN] {detail_pct:.1f}% (10% 미만, anchor가 너무 날카로움)")
            elif detail_pct > 70:
                print(f"  [WARN] {detail_pct:.1f}% (70% 초과, anchor가 너무 애매함)")
            else:
                print(f"  [INFO] {detail_pct:.1f}% (30~70% 범위, 허용 가능)")
        
        # Check C: confidence 분포 sanity check
        print("\n[Check C] confidence 분포 sanity check")
        print("-" * 80)
        result = db.execute(text("""
            SELECT
                MIN(value_chain_confidence) as min_conf,
                AVG(value_chain_confidence) as avg_conf,
                MAX(value_chain_confidence) as max_conf,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value_chain_confidence) as median_conf,
                COUNT(*) as total
            FROM investor_sector
            WHERE is_primary = TRUE
                AND value_chain_confidence IS NOT NULL
        """))
        
        row = result.fetchone()
        if row:
            min_conf = row[0]
            avg_conf = row[1]
            max_conf = row[2]
            median_conf = row[3]
            total = row[4]
            
            print(f"  총 레코드 수: {total:,}개")
            print(f"  최소값: {min_conf:.4f}")
            print(f"  평균값: {avg_conf:.4f}")
            print(f"  중앙값: {median_conf:.4f}")
            print(f"  최대값: {max_conf:.4f}")
            
            print("\n  해석:")
            if 0.05 <= avg_conf <= 0.15:
                print(f"  [OK] 평균 {avg_conf:.4f} (0.05~0.15 범위, 이상적)")
            elif avg_conf < 0.05:
                print(f"  [WARN] 평균 {avg_conf:.4f} (0.05 미만, anchor 문장이 너무 비슷함)")
            elif avg_conf > 0.3:
                print(f"  [WARN] 평균 {avg_conf:.4f} (0.3 초과, anchor가 과도하게 분리됨)")
            else:
                print(f"  [INFO] 평균 {avg_conf:.4f} (0.15~0.3 범위, 허용 가능)")
        
        # 분포 구간별 통계
        print("\n  분포 구간별 통계:")
        print("-" * 80)
        result = db.execute(text("""
            SELECT
                CASE
                    WHEN value_chain_confidence < 0.05 THEN '< 0.05'
                    WHEN value_chain_confidence < 0.1 THEN '0.05 ~ 0.1'
                    WHEN value_chain_confidence < 0.15 THEN '0.1 ~ 0.15'
                    WHEN value_chain_confidence < 0.2 THEN '0.15 ~ 0.2'
                    WHEN value_chain_confidence < 0.3 THEN '0.2 ~ 0.3'
                    ELSE '>= 0.3'
                END as conf_range,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM investor_sector
            WHERE is_primary = TRUE
                AND value_chain_confidence IS NOT NULL
            GROUP BY conf_range
            ORDER BY MIN(value_chain_confidence)
        """))
        
        for row in result:
            conf_range = row[0]
            count = row[1]
            pct = row[2]
            print(f"  {conf_range:<15} {count:>6,}개 ({pct:>5.2f}%)")
        
        # Check 3: confidence 분포 (추가)
        print("\n[Check 3] confidence 분포 (혼합형 비율)")
        print("-" * 80)
        result = db.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE value_chain_confidence < 0.1) as low_conf_count,
                COUNT(*) FILTER (WHERE value_chain_confidence >= 0.1) as high_conf_count,
                COUNT(*) as total
            FROM investor_sector
            WHERE is_primary = TRUE
                AND value_chain_confidence IS NOT NULL
        """))
        
        row = result.fetchone()
        if row:
            low_conf = row[0]
            high_conf = row[1]
            total = row[2]
            low_conf_pct = (low_conf / total * 100) if total > 0 else 0
            
            print(f"  총 레코드 수: {total:,}개")
            print(f"  confidence < 0.1 (혼합형): {low_conf:,}개 ({low_conf_pct:.2f}%)")
            print(f"  confidence >= 0.1 (명확형): {high_conf:,}개 ({100-low_conf_pct:.2f}%)")
            
            print("\n  해석:")
            if 20 <= low_conf_pct <= 30:
                print(f"  [OK] {low_conf_pct:.1f}% (20~30% 범위, 정상 - 혼합 산업 현실 반영)")
            elif low_conf_pct < 20:
                print(f"  [INFO] {low_conf_pct:.1f}% (20% 미만, 대부분 명확한 분류)")
            elif low_conf_pct > 50:
                print(f"  [WARN] {low_conf_pct:.1f}% (50% 초과, Anchor 분리 더 필요)")
            else:
                print(f"  [INFO] {low_conf_pct:.1f}% (30~50% 범위, 허용 가능)")
        
        print("\n" + "=" * 80)
        print("검증 완료")
        print("=" * 80)
        
    except Exception as e:
        print(f"검증 실패: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        raise
    finally:
        db.close()


if __name__ == '__main__':
    check_value_chain_distribution()

