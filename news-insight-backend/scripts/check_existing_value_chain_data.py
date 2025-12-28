#!/usr/bin/env python3
"""
기존 value_chain 데이터 확인 스크립트

1. 기존 value_chain 값 분포 확인
2. 어떤 기준/모델로 만들어진 건지 추정
3. company_embeddings 임베딩 모델 확인
"""

import sys
import os
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db import get_db

def check_existing_value_chain_data():
    """기존 value_chain 데이터 확인"""
    db = next(get_db())
    
    try:
        print("=" * 80)
        print("기존 value_chain 데이터 확인")
        print("=" * 80)
        
        # 1. 기존 value_chain 값 분포
        print("\n1. 기존 value_chain 값 분포")
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
            ORDER BY count DESC
        """))
        
        total = 0
        for row in result:
            vc = row[0] or 'NULL'
            count = row[1]
            pct = row[2]
            total += count
            print(f"  {vc:<20} {count:>6,}개 ({pct:>5.2f}%)")
        
        print(f"\n  총 레코드 수: {total:,}개")
        
        # 2. 기존 value_chain이 3단계인지 확인
        print("\n2. 기존 value_chain 타입 확인")
        print("-" * 80)
        result = db.execute(text("""
            SELECT DISTINCT value_chain
            FROM investor_sector
            WHERE is_primary = TRUE
                AND value_chain IS NOT NULL
            ORDER BY value_chain
        """))
        
        existing_values = []
        for row in result:
            existing_values.append(row[0])
        
        print(f"  기존 value_chain 값: {', '.join(existing_values)}")
        
        # 3단계인지 확인
        old_3_stage = {'UPSTREAM', 'MIDSTREAM', 'DOWNSTREAM'}
        is_3_stage = set(existing_values).issubset(old_3_stage)
        
        if is_3_stage:
            print(f"  [확인] 기존 3단계 분류입니다 (UPSTREAM, MIDSTREAM, DOWNSTREAM)")
            print(f"  -> 새로운 5단계와 호환되지 않습니다 (MID_HARD, MID_SOFT, DOWN_BIZ, DOWN_SERVICE)")
        else:
            print(f"  [확인] 기존 값이 3단계가 아닙니다")
            print(f"  -> 일부는 이미 5단계일 수 있습니다")
        
        # 3. company_embeddings 임베딩 차원 확인 (샘플로 확인)
        print("\n3. company_embeddings 임베딩 차원 확인")
        print("-" * 80)
        
        # 샘플 10개 가져와서 차원 확인
        result = db.execute(text("""
            SELECT ticker, embedding_vector
            FROM company_embeddings
            WHERE embedding_vector IS NOT NULL
            LIMIT 10
        """))
        
        dims = set()
        sample_count = 0
        for row in result:
            ticker = row[0]
            embedding_vector = row[1]
            
            # pgvector의 vector 타입을 numpy array로 변환
            if hasattr(embedding_vector, 'tolist'):
                embedding_array = np.array(embedding_vector.tolist(), dtype=np.float32)
            elif isinstance(embedding_vector, (list, tuple)):
                embedding_array = np.array(embedding_vector, dtype=np.float32)
            else:
                continue
            
            dim = len(embedding_array)
            dims.add(dim)
            sample_count += 1
            print(f"  [{ticker}] 차원: {dim}")
        
        # 전체 통계
        result = db.execute(text("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT ticker) as unique_companies
            FROM company_embeddings
            WHERE embedding_vector IS NOT NULL
        """))
        
        row = result.fetchone()
        if row:
            total = row[0]
            unique = row[1]
            print(f"\n  전체: {total:,}개 레코드, {unique:,}개 기업")
        
        # Solar Embedding은 4096차원
        import numpy as np
        if 4096 in dims:
            print(f"\n  [확인] 4096차원 임베딩 발견 -> Solar Embedding으로 추정")
            print(f"  -> 기업 벡터 재사용 가능")
        else:
            print(f"\n  [경고] 4096차원 임베딩 없음 (발견된 차원: {dims})")
            print(f"  -> 기업 벡터 재생성 필요 (Solar Embedding 사용)")
        
        # 4. 업데이트 시간 확인 (언제 만들어진 건지)
        print("\n4. value_chain 업데이트 시간 확인")
        print("-" * 80)
        result = db.execute(text("""
            SELECT 
                MIN(updated_at) as min_updated,
                MAX(updated_at) as max_updated,
                COUNT(*) FILTER (WHERE updated_at IS NOT NULL) as has_updated_at
            FROM investor_sector
            WHERE is_primary = TRUE
                AND value_chain IS NOT NULL
        """))
        
        row = result.fetchone()
        if row:
            min_updated = row[0]
            max_updated = row[1]
            has_updated = row[2]
            
            if min_updated and max_updated:
                print(f"  최초 업데이트: {min_updated}")
                print(f"  최근 업데이트: {max_updated}")
            else:
                print(f"  업데이트 시간 정보 없음")
        
        # 5. 판단 및 권장사항
        print("\n" + "=" * 80)
        print("판단 및 권장사항")
        print("=" * 80)
        
        recommendations = []
        
        if is_3_stage:
            recommendations.append("기존 value_chain은 3단계 분류입니다.")
            recommendations.append("새로운 5단계 분류와 호환되지 않으므로 삭제 권장합니다.")
            recommendations.append("삭제 후 새로운 5단계 분류를 실행하세요.")
        else:
            recommendations.append("기존 value_chain이 3단계가 아닙니다.")
            recommendations.append("일부는 이미 5단계일 수 있으므로 확인 필요합니다.")
        
        if 4096 in dims:
            recommendations.append("company_embeddings는 Solar Embedding (4096차원)입니다.")
            recommendations.append("기업 벡터 재사용 가능합니다.")
            recommendations.append("Anchor 5개만 임베딩하여 사용하세요.")
        else:
            recommendations.append("company_embeddings가 Solar Embedding이 아닙니다.")
            recommendations.append("기업 벡터 재생성이 필요합니다.")
            recommendations.append("밸류체인 분류 스크립트에서 재생성 옵션을 켜세요.")
        
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec}")
        
        print("\n" + "=" * 80)
        
    except Exception as e:
        print(f"확인 실패: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        raise
    finally:
        db.close()


if __name__ == '__main__':
    check_existing_value_chain_data()

