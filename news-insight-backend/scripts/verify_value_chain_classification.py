#!/usr/bin/env python3
"""
밸류체인 분류 검증 스크립트

SEC_SEMI 기업 중 value_chain = 'MID_SOFT'인 기업 10개 출력
(ticker, company_name, value_chain_confidence 포함)
"""

import sys
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db import get_db

def print_sample_companies(db, sector_l1: str, value_chain: str, limit: int = 10):
    """샘플 기업 출력 (팹리스 검증 강화)"""
    result = db.execute(text("""
        SELECT 
            i.ticker,
            s.stock_name as company_name,
            i.value_chain,
            i.value_chain_detail,
            i.value_chain_confidence,
            i.sector_l2,
            cd.biz_summary
        FROM investor_sector i
        LEFT JOIN stocks s ON i.ticker = s.ticker
        LEFT JOIN company_detail cd ON i.ticker = cd.ticker
        WHERE i.sector_l1 = :sector_l1
            AND i.is_primary = true
            AND i.value_chain = :value_chain
        ORDER BY i.value_chain_confidence DESC NULLS LAST
        LIMIT :limit
    """), {'sector_l1': sector_l1, 'value_chain': value_chain, 'limit': limit})
    
    print(f"\n[{sector_l1} + {value_chain}] 샘플 {limit}개 (팹리스 검증)")
    print("=" * 80)
    print(f"{'Ticker':<10} {'Company Name':<30} {'Conf':<8} {'Detail':<12} {'검증':<20}")
    print("-" * 80)
    
    count = 0
    correct_count = 0
    
    for row in result:
        ticker = row[0] or 'N/A'
        company_name = row[1] or 'N/A'
        value_chain_detail = row[3] or 'N/A'
        value_chain_confidence = row[4] if row[4] is not None else 'N/A'
        biz_summary = row[6] or ''
        
        if value_chain_confidence != 'N/A':
            confidence_str = f"{value_chain_confidence:.3f}"
        else:
            confidence_str = 'N/A'
        
        # 팹리스 검증 키워드
        biz_text = biz_summary.lower() if biz_summary else ''
        is_fabless = any(keyword in biz_text for keyword in [
            '팹리스', 'fabless', '설계', '개발', 'eda', '아키텍처', 
            '엔지니어링', 'rtl', '검증', '시뮬레이션', 'ip', '칩 설계'
        ])
        is_manufacturing = any(keyword in biz_text for keyword in [
            '양산', '공장', '생산', '제조', '라인', '설비', '수율', '공정', '패키징'
        ])
        is_distribution = any(keyword in biz_text for keyword in [
            '유통', '상사', '도매', '리테일', '판매', '채널'
        ])
        
        # 검증 결과
        if sector_l1 == 'SEC_SEMI' and value_chain == 'MID_SOFT':
            if is_fabless and not is_manufacturing and not is_distribution:
                verification = '[OK] 팹리스'
                correct_count += 1
            elif is_manufacturing:
                verification = '[WARN] 제조사'
            elif is_distribution:
                verification = '[WARN] 유통'
            else:
                verification = '[INFO] 확인 필요'
        else:
            verification = '[OK]'
            correct_count += 1
        
        print(f"{ticker:<10} {company_name[:28]:<30} {confidence_str:<8} {value_chain_detail[:10]:<12} {verification:<20}")
        count += 1
    
    print("-" * 80)
    print(f"총 {count}개 기업, 정확도: {correct_count}/{count} ({correct_count*100//count if count > 0 else 0}%)")
    
    if sector_l1 == 'SEC_SEMI' and value_chain == 'MID_SOFT':
        if correct_count >= count * 0.8:
            print(f"\n[OK] 팹리스 검증 통과 (80% 이상 정확)")
        else:
            print(f"\n[WARN] 팹리스 검증 실패 (80% 미만 정확)")
    
    return count


def verify_value_chain_classification():
    """밸류체인 분류 결과 검증 (P1: 품질 검증)"""
    db = next(get_db())
    
    try:
        print("=" * 80)
        print("밸류체인 분류 품질 검증 (P1)")
        print("=" * 80)
        
        # 1. SEC_SEMI + MID_SOFT 10개
        print_sample_companies(db, 'SEC_SEMI', 'MID_SOFT', 10)
        
        # 2. SEC_SEMI + MID_HARD 10개
        print_sample_companies(db, 'SEC_SEMI', 'MID_HARD', 10)
        
        # 3. SEC_BATTERY + MID_SOFT 10개
        print_sample_companies(db, 'SEC_BATTERY', 'MID_SOFT', 10)
        
        # 통계 출력
        stats_result = db.execute(text("""
            SELECT 
                sector_l1,
                value_chain,
                COUNT(*) as count,
                AVG(value_chain_confidence) as avg_confidence,
                MIN(value_chain_confidence) as min_confidence,
                MAX(value_chain_confidence) as max_confidence
            FROM investor_sector
            WHERE sector_l1 IN ('SEC_SEMI', 'SEC_BATTERY')
                AND is_primary = true
                AND value_chain IS NOT NULL
            GROUP BY sector_l1, value_chain
            ORDER BY sector_l1, count DESC
        """))
        
        print("\n" + "=" * 80)
        print("SEC_SEMI / SEC_BATTERY 기업 밸류체인 분포")
        print("=" * 80)
        print(f"{'Sector':<15} {'Value Chain':<20} {'Count':<10} {'Avg Conf':<12} {'Min':<10} {'Max':<10}")
        print("-" * 80)
        
        for row in stats_result:
            sector = row[0] or 'N/A'
            vc = row[1] or 'N/A'
            count = row[2]
            avg_conf = row[3] if row[3] is not None else 0.0
            min_conf = row[4] if row[4] is not None else 0.0
            max_conf = row[5] if row[5] is not None else 0.0
            
            print(f"{sector:<15} {vc:<20} {count:<10} {avg_conf:.3f}      {min_conf:.3f}      {max_conf:.3f}")
        
        print("=" * 80)
    
    except Exception as e:
        print(f"검증 실패: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        raise
    finally:
        db.close()


if __name__ == '__main__':
    verify_value_chain_classification()

