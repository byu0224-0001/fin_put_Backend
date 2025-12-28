#!/usr/bin/env python3
"""
Top-2 저장 및 UI 노출 준비 확인 스크립트
value_chain_detail과 value_chain_confidence 저장 상태 확인
"""
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from app.db import SessionLocal

# UTF-8 인코딩 설정
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

load_dotenv()

def verify_top2_storage():
    """Top-2 저장 상태 확인"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("Top-2 저장 및 UI 노출 준비 확인")
        print("=" * 80)
        
        # 1. 전체 분류 결과 통계
        result = db.execute(text("""
            SELECT 
                COUNT(*) as total,
                COUNT(value_chain) as has_value_chain,
                COUNT(value_chain_detail) as has_detail,
                COUNT(value_chain_confidence) as has_confidence
            FROM investor_sector
            WHERE is_primary = true;
        """))
        
        row = result.fetchone()
        total = row[0]
        has_vc = row[1]
        has_detail = row[2]
        has_conf = row[3]
        
        print(f"\n📊 전체 통계:")
        print(f"  총 기업 수: {total:,}개")
        print(f"  value_chain 저장: {has_vc:,}개 ({has_vc*100/total:.1f}%)")
        print(f"  value_chain_detail 저장: {has_detail:,}개 ({has_detail*100/total:.1f}%)")
        print(f"  value_chain_confidence 저장: {has_conf:,}개 ({has_conf*100/total:.1f}%)")
        
        # 2. Top-2 저장 비율 (confidence < 0.1인 경우)
        result = db.execute(text("""
            SELECT 
                COUNT(*) as total_low_conf,
                COUNT(value_chain_detail) as has_detail_low_conf
            FROM investor_sector
            WHERE is_primary = true
                AND value_chain_confidence < 0.1
                AND value_chain_confidence IS NOT NULL;
        """))
        
        row = result.fetchone()
        low_conf_total = row[0]
        low_conf_with_detail = row[1]
        
        print(f"\n📊 낮은 Confidence (< 0.1) 기업:")
        print(f"  총 {low_conf_total:,}개")
        if low_conf_total > 0:
            detail_pct = (low_conf_with_detail / low_conf_total) * 100
            print(f"  value_chain_detail 저장: {low_conf_with_detail:,}개 ({detail_pct:.1f}%)")
        else:
            print(f"  value_chain_detail 저장: 0개 (0.0%)")
        
        # 3. Top-2 예시 샘플
        result = db.execute(text("""
            SELECT 
                s.ticker,
                s.stock_name,
                is.value_chain,
                is.value_chain_detail,
                is.value_chain_confidence,
                is.sector_l1,
                is.sector_l2
            FROM investor_sector is
            JOIN stocks s ON s.ticker = is.ticker
            WHERE is.is_primary = true
                AND is.value_chain_detail IS NOT NULL
                AND is.value_chain_confidence < 0.15
            ORDER BY is.value_chain_confidence ASC
            LIMIT 10;
        """))
        
        print(f"\n📋 Top-2 저장 예시 (Confidence 낮은 순):")
        print("-" * 100)
        print(f"{'티커':<10} {'회사명':<30} {'Top1':<15} {'Top2':<15} {'Confidence':<12} {'Sector'}")
        print("-" * 100)
        
        samples = []
        for row in result:
            samples.append(row)
            print(f"{row[0]:<10} {row[1][:28]:<30} {row[2] or 'N/A':<15} {row[3] or 'N/A':<15} {row[4] or 0:.4f}      {row[5] or 'N/A'}")
        
        # 4. Gap 분포 확인
        result = db.execute(text("""
            SELECT 
                CASE 
                    WHEN value_chain_confidence < 0.05 THEN '0.00-0.05'
                    WHEN value_chain_confidence < 0.1 THEN '0.05-0.10'
                    WHEN value_chain_confidence < 0.2 THEN '0.10-0.20'
                    WHEN value_chain_confidence < 0.3 THEN '0.20-0.30'
                    ELSE '0.30+'
                END as gap_range,
                COUNT(*) as count,
                COUNT(value_chain_detail) as has_detail_count
            FROM investor_sector
            WHERE is_primary = true
                AND value_chain_confidence IS NOT NULL
            GROUP BY gap_range
            ORDER BY gap_range;
        """))
        
        print(f"\n📊 Confidence Gap 분포:")
        print("-" * 60)
        print(f"{'Gap 범위':<15} {'총 기업':<15} {'Detail 저장':<15} {'비율'}")
        print("-" * 60)
        
        for row in result:
            gap_range = row[0]
            total_count = row[1]
            detail_count = row[2]
            detail_pct = (detail_count / total_count * 100) if total_count > 0 else 0
            print(f"{gap_range:<15} {total_count:>10,}개 {detail_count:>10,}개 {detail_pct:>5.1f}%")
        
        # 5. UI 노출 준비 상태 확인
        print(f"\n✅ UI 노출 준비 상태:")
        if has_detail > 0:
            print(f"  ✅ value_chain_detail 저장됨 ({has_detail:,}개)")
            print(f"     -> '이 기업은 {value_chain} 성격이 강하지만, {value_chain_detail} 특성도 일부 있음' 형태로 UI 노출 가능")
        else:
            print(f"  ⚠️  value_chain_detail이 없습니다")
        
        if has_conf > 0:
            print(f"  ✅ value_chain_confidence 저장됨 ({has_conf:,}개)")
            print(f"     -> Confidence 기반 신뢰도 표시 가능")
        else:
            print(f"  ⚠️  value_chain_confidence가 없습니다")
        
        # 최종 요약
        print("\n" + "=" * 80)
        print("📋 최종 요약")
        print("=" * 80)
        print(f"✅ Top-1 저장: {has_vc:,}개 ({has_vc*100/total:.1f}%)")
        print(f"✅ Top-2 저장: {has_detail:,}개 ({has_detail*100/total:.1f}%)")
        print(f"✅ Confidence 저장: {has_conf:,}개 ({has_conf*100/total:.1f}%)")
        
        if has_detail > 0 and has_conf > 0:
            print("\n✅ UI 노출 준비 완료!")
            print("   - value_chain: 주요 밸류체인")
            print("   - value_chain_detail: 보조 밸류체인 (confidence < 0.1일 때)")
            print("   - value_chain_confidence: 분류 신뢰도")
        else:
            print("\n⚠️  일부 데이터가 누락되었습니다. 재분류를 실행하세요.")
        
    except Exception as e:
        print(f"\n❌ [ERROR] 확인 중 오류: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == '__main__':
    verify_top2_storage()

