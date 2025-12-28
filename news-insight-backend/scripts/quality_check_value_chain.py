#!/usr/bin/env python3
"""
밸류체인 분류 품질 검증 스크립트
- 분류 결과 분포 sanity check
- Confidence 값 분포 확인
- 샘플링 기반 휴먼 검증
- L2 섹터 × 밸류체인 교차 검증
"""
import sys
import os
from pathlib import Path
from typing import Dict, List, Any
import statistics

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

# 밸류체인 5단계
VALUE_CHAIN_STAGES = ['UPSTREAM', 'MID_HARD', 'MID_SOFT', 'DOWN_BIZ', 'DOWN_SERVICE']


def check_distribution(db):
    """1. 분류 결과 분포 sanity check"""
    print("=" * 80)
    print("1️⃣ 분류 결과 분포 Sanity Check")
    print("=" * 80)
    
    result = db.execute(text("""
        SELECT 
            value_chain,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM investor_sector
        WHERE is_primary = true
            AND value_chain IS NOT NULL
        GROUP BY value_chain
        ORDER BY count DESC;
    """))
    
    total = 0
    distribution = {}
    
    print(f"\n{'밸류체인':<20} {'기업 수':<15} {'비율 (%)':<15} {'상태'}")
    print("-" * 80)
    
    for row in result:
        vc = row[0] or 'NULL'
        count = row[1]
        pct = row[2] or 0.0
        total += count
        distribution[vc] = {'count': count, 'percentage': pct}
        
        # 이상 신호 체크
        status = "✅"
        if pct > 70:
            status = "⚠️  과도하게 높음 (>70%)"
        elif pct < 2 and vc in VALUE_CHAIN_STAGES:
            status = "⚠️  비정상적으로 낮음 (<2%)"
        
        print(f"{vc:<20} {count:<15,} {pct:<15.2f} {status}")
    
    print("-" * 80)
    print(f"{'총계':<20} {total:<15,} {'100.00':<15}")
    
    # 경고 체크
    warnings = []
    for vc in VALUE_CHAIN_STAGES:
        if vc in distribution:
            pct = distribution[vc]['percentage']
            if pct > 70:
                warnings.append(f"⚠️  {vc}가 {pct:.1f}%로 과도하게 높습니다 (70% 초과)")
            elif pct < 2:
                warnings.append(f"⚠️  {vc}가 {pct:.1f}%로 비정상적으로 낮습니다 (2% 미만)")
        else:
            warnings.append(f"⚠️  {vc}가 분류 결과에 없습니다")
    
    if warnings:
        print("\n⚠️  경고 사항:")
        for warning in warnings:
            print(f"  {warning}")
    else:
        print("\n✅ 분포가 정상적으로 보입니다")
    
    return distribution, total


def check_confidence_distribution(db):
    """2. Confidence 값 분포 확인"""
    print("\n" + "=" * 80)
    print("2️⃣ Confidence 값 분포 확인")
    print("=" * 80)
    
    result = db.execute(text("""
        SELECT 
            value_chain_confidence,
            COUNT(*) as count
        FROM investor_sector
        WHERE is_primary = true
            AND value_chain IS NOT NULL
            AND value_chain_confidence IS NOT NULL
        GROUP BY value_chain_confidence
        ORDER BY value_chain_confidence;
    """))
    
    confidences = []
    for row in result:
        conf = row[0]
        count = row[1]
        confidences.extend([conf] * count)
    
    if not confidences:
        print("\n❌ Confidence 값이 없습니다!")
        return None
    
    # 통계 계산
    avg_conf = statistics.mean(confidences)
    median_conf = statistics.median(confidences)
    min_conf = min(confidences)
    max_conf = max(confidences)
    
    # < 0.1 비율 계산
    low_conf_count = sum(1 for c in confidences if c < 0.1)
    low_conf_pct = (low_conf_count / len(confidences)) * 100
    
    # 분포 구간별 집계
    bins = {
        '0.0-0.1': sum(1 for c in confidences if 0.0 <= c < 0.1),
        '0.1-0.2': sum(1 for c in confidences if 0.1 <= c < 0.2),
        '0.2-0.3': sum(1 for c in confidences if 0.2 <= c < 0.3),
        '0.3-0.5': sum(1 for c in confidences if 0.3 <= c < 0.5),
        '0.5-1.0': sum(1 for c in confidences if 0.5 <= c <= 1.0),
    }
    
    print(f"\n📊 Confidence 통계:")
    print(f"  평균: {avg_conf:.4f}")
    print(f"  중앙값: {median_conf:.4f}")
    print(f"  최소값: {min_conf:.4f}")
    print(f"  최대값: {max_conf:.4f}")
    print(f"  < 0.1 비율: {low_conf_pct:.2f}% ({low_conf_count}/{len(confidences)}개)")
    
    print(f"\n📊 Confidence 분포:")
    for bin_range, count in bins.items():
        pct = (count / len(confidences)) * 100
        bar = "█" * int(pct / 2)
        print(f"  {bin_range:<10} {count:>6,}개 ({pct:>5.1f}%) {bar}")
    
    # 이상 신호 체크
    warnings = []
    if avg_conf > 0.5:
        warnings.append(f"⚠️  평균 confidence가 {avg_conf:.3f}로 너무 높습니다 (모델이 둔감할 수 있음)")
    elif avg_conf < 0.1:
        warnings.append(f"⚠️  평균 confidence가 {avg_conf:.3f}로 너무 낮습니다 (anchor 텍스트 문제 가능성)")
    
    if low_conf_pct > 50:
        warnings.append(f"⚠️  confidence < 0.1인 기업이 {low_conf_pct:.1f}%로 과도합니다")
    
    if warnings:
        print("\n⚠️  경고 사항:")
        for warning in warnings:
            print(f"  {warning}")
    else:
        print("\n✅ Confidence 분포가 정상적으로 보입니다")
    
    return {
        'mean': avg_conf,
        'median': median_conf,
        'min': min_conf,
        'max': max_conf,
        'low_conf_pct': low_conf_pct,
        'distribution': bins
    }


def sample_for_human_verification(db, n_samples=20):
    """3. 샘플링 기반 휴먼 검증"""
    print("\n" + "=" * 80)
    print("3️⃣ 샘플링 기반 휴먼 검증 (10~20개 샘플)")
    print("=" * 80)
    
    # 반도체 대표주 3~5개
    semi_result = db.execute(text("""
        SELECT 
            s.ticker,
            s.stock_name,
            inv_sector.value_chain,
            inv_sector.value_chain_detail,
            inv_sector.value_chain_confidence,
            inv_sector.sector_l1,
            inv_sector.sector_l2
        FROM investor_sector inv_sector
        JOIN stocks s ON s.ticker = inv_sector.ticker
        WHERE inv_sector.is_primary = true
            AND inv_sector.sector_l1 = 'SEC_SEMI'
            AND inv_sector.value_chain IS NOT NULL
        ORDER BY inv_sector.value_chain_confidence DESC
        LIMIT 5;
    """))
    
    # 제조 + 서비스 경계 기업 3~5개 (confidence 낮은 것)
    boundary_result = db.execute(text("""
        SELECT 
            s.ticker,
            s.stock_name,
            inv_sector.value_chain,
            inv_sector.value_chain_detail,
            inv_sector.value_chain_confidence,
            inv_sector.sector_l1,
            inv_sector.sector_l2
        FROM investor_sector inv_sector
        JOIN stocks s ON s.ticker = inv_sector.ticker
        WHERE inv_sector.is_primary = true
            AND inv_sector.value_chain_confidence < 0.15
            AND inv_sector.value_chain_detail IS NOT NULL
        ORDER BY inv_sector.value_chain_confidence ASC
        LIMIT 5;
    """))
    
    # 플랫폼/콘텐츠 기업 3~5개
    platform_result = db.execute(text("""
        SELECT 
            s.ticker,
            s.stock_name,
            inv_sector.value_chain,
            inv_sector.value_chain_detail,
            inv_sector.value_chain_confidence,
            inv_sector.sector_l1,
            inv_sector.sector_l2
        FROM investor_sector inv_sector
        JOIN stocks s ON s.ticker = inv_sector.ticker
        WHERE inv_sector.is_primary = true
            AND (inv_sector.value_chain = 'MID_SOFT' OR inv_sector.value_chain = 'DOWN_SERVICE')
            AND (inv_sector.sector_l2 LIKE '%플랫폼%' OR inv_sector.sector_l2 LIKE '%콘텐츠%' OR inv_sector.sector_l2 LIKE '%소프트웨어%')
        ORDER BY inv_sector.value_chain_confidence DESC
        LIMIT 5;
    """))
    
    samples = []
    
    print("\n📋 반도체 대표주 샘플:")
    print("-" * 100)
    print(f"{'티커':<10} {'회사명':<30} {'밸류체인':<15} {'Detail':<15} {'Confidence':<12} {'Sector'}")
    print("-" * 100)
    for row in semi_result:
        samples.append(row)
        print(f"{row[0]:<10} {row[1][:28]:<30} {row[2] or 'N/A':<15} {row[3] or 'N/A':<15} {row[4] or 0:.4f}      {row[5] or 'N/A'}")
    
    print("\n📋 제조+서비스 경계 기업 샘플 (confidence 낮음):")
    print("-" * 100)
    print(f"{'티커':<10} {'회사명':<30} {'밸류체인':<15} {'Detail':<15} {'Confidence':<12} {'Sector'}")
    print("-" * 100)
    for row in boundary_result:
        samples.append(row)
        print(f"{row[0]:<10} {row[1][:28]:<30} {row[2] or 'N/A':<15} {row[3] or 'N/A':<15} {row[4] or 0:.4f}      {row[5] or 'N/A'}")
    
    print("\n📋 플랫폼/콘텐츠 기업 샘플:")
    print("-" * 100)
    print(f"{'티커':<10} {'회사명':<30} {'밸류체인':<15} {'Detail':<15} {'Confidence':<12} {'Sector'}")
    print("-" * 100)
    for row in platform_result:
        samples.append(row)
        print(f"{row[0]:<10} {row[1][:28]:<30} {row[2] or 'N/A':<15} {row[3] or 'N/A':<15} {row[4] or 0:.4f}      {row[5] or 'N/A'}")
    
    print(f"\n✅ 총 {len(samples)}개 샘플 추출 완료")
    print("\n💡 휴먼 검증 질문:")
    print("  1. '내가 애널리스트라면 이 분류에 동의하는가?'")
    print("  2. 'IR에서 이 예시를 보여줄 수 있는가?'")
    
    return samples


def check_sector_value_chain_cross_validation(db):
    """4. L2 섹터 × 밸류체인 교차 검증"""
    print("\n" + "=" * 80)
    print("4️⃣ L2 섹터 × 밸류체인 교차 검증")
    print("=" * 80)
    
    # 같은 섹터(L2) 내에서 밸류체인 다양성 확인
    result = db.execute(text("""
        SELECT 
            sector_l2,
            value_chain,
            COUNT(*) as count
        FROM investor_sector
        WHERE is_primary = true
            AND sector_l2 IS NOT NULL
            AND value_chain IS NOT NULL
        GROUP BY sector_l2, value_chain
        HAVING COUNT(*) >= 3
        ORDER BY sector_l2, count DESC;
    """))
    
    sector_vc_map = {}
    for row in result:
        sector_l2 = row[0]
        vc = row[1]
        count = row[2]
        
        if sector_l2 not in sector_vc_map:
            sector_vc_map[sector_l2] = {}
        sector_vc_map[sector_l2][vc] = count
    
    # 각 섹터별 밸류체인 다양성 확인
    print("\n📊 섹터별 밸류체인 분포:")
    print("-" * 100)
    
    warnings = []
    for sector_l2, vc_dist in sorted(sector_vc_map.items()):
        total = sum(vc_dist.values())
        vc_count = len(vc_dist)
        
        # 단일 밸류체인으로 몰린 경우 체크
        max_vc_count = max(vc_dist.values())
        max_vc_pct = (max_vc_count / total) * 100
        
        if max_vc_pct > 90:
            warnings.append(f"⚠️  {sector_l2}: {max_vc_pct:.1f}%가 단일 밸류체인으로 몰림 (독립성 문제 가능)")
        
        print(f"\n{sector_l2} (총 {total}개, {vc_count}개 밸류체인):")
        for vc, count in sorted(vc_dist.items(), key=lambda x: x[1], reverse=True):
            pct = (count / total) * 100
            print(f"  - {vc:<20} {count:>4}개 ({pct:>5.1f}%)")
    
    if warnings:
        print("\n⚠️  경고 사항:")
        for warning in warnings:
            print(f"  {warning}")
    else:
        print("\n✅ 섹터와 밸류체인이 독립적으로 분류되고 있습니다")
    
    return sector_vc_map


def main():
    """메인 함수"""
    db = None
    
    try:
        # DB 연결 테스트
        print("=" * 80)
        print("밸류체인 분류 품질 검증 리포트")
        print("=" * 80)
        print("\n🔌 데이터베이스 연결 확인 중...")
        
        db = SessionLocal()
        # 연결 테스트
        db.execute(text("SELECT 1"))
        print("✅ 데이터베이스 연결 성공\n")
        
        # 1. 분포 확인
        try:
            distribution, total = check_distribution(db)
        except Exception as e:
            print(f"\n❌ [ERROR] 분포 확인 중 오류: {e}")
            import traceback
            traceback.print_exc()
            distribution, total = {}, 0
        
        # 2. Confidence 분포 확인
        try:
            conf_stats = check_confidence_distribution(db)
        except Exception as e:
            print(f"\n❌ [ERROR] Confidence 분포 확인 중 오류: {e}")
            import traceback
            traceback.print_exc()
            conf_stats = None
        
        # 3. 샘플링
        try:
            samples = sample_for_human_verification(db)
            db.rollback()  # 오류 후 트랜잭션 롤백
        except Exception as e:
            print(f"\n❌ [ERROR] 샘플링 중 오류: {e}")
            db.rollback()  # 오류 후 트랜잭션 롤백
            import traceback
            traceback.print_exc()
            samples = []
        
        # 4. 교차 검증
        try:
            sector_vc_map = check_sector_value_chain_cross_validation(db)
        except Exception as e:
            print(f"\n❌ [ERROR] 교차 검증 중 오류: {e}")
            db.rollback()  # 오류 후 트랜잭션 롤백
            import traceback
            traceback.print_exc()
            sector_vc_map = {}
        
        # 최종 요약
        print("\n" + "=" * 80)
        print("📋 최종 요약")
        print("=" * 80)
        if total > 0:
            print(f"✅ 총 분류 기업 수: {total:,}개")
        if samples:
            print(f"✅ 샘플 추출: {len(samples)}개")
        if sector_vc_map:
            print(f"✅ 검증 완료 섹터 수: {len(sector_vc_map)}개")
        
        if conf_stats:
            print(f"✅ 평균 Confidence: {conf_stats['mean']:.4f}")
            print(f"✅ 낮은 Confidence 비율: {conf_stats['low_conf_pct']:.2f}%")
        
        print("\n💡 다음 단계:")
        print("  1. 샘플 결과를 직접 확인하여 품질 검증")
        print("  2. 이상 신호가 있으면 Anchor 텍스트 재점검")
        print("  3. 필요시 재분류 실행")
        
    except Exception as e:
        error_msg = str(e)
        if "connection" in error_msg.lower() or "refused" in error_msg.lower():
            print("\n❌ [ERROR] 데이터베이스 연결 실패")
            print("   PostgreSQL 서버가 실행 중인지 확인하세요.")
            print("   오류 메시지:", error_msg)
        else:
            print(f"\n❌ [ERROR] 품질 검증 중 오류: {e}")
            import traceback
            traceback.print_exc()
    finally:
        if db:
            db.close()


if __name__ == '__main__':
    main()

