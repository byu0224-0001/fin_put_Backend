# -*- coding: utf-8 -*-
"""
DB 구축 상황 간단 확인 스크립트
"""
import sys
sys.path.insert(0, '.')
sys.stdout.reconfigure(encoding='utf-8')

from app.db import SessionLocal
from sqlalchemy import text

def main():
    print("=" * 80)
    print("DB 구축 상황 확인")
    print("=" * 80)
    
    db = SessionLocal()
    
    try:
        # 1. 전체 기업 수집 통계
        print("\n[1] 전체 기업 수집 통계")
        print("-" * 80)
        
        total_stocks = db.execute(text("""
            SELECT COUNT(*) FROM stocks 
            WHERE market IN ('KOSPI', 'KOSDAQ')
        """)).scalar()
        
        collected_companies = db.execute(text("""
            SELECT COUNT(DISTINCT ticker) FROM company_details
        """)).scalar()
        
        print(f"전체 한국 상장 기업: {total_stocks:,}개")
        print(f"수집 완료 기업: {collected_companies:,}개")
        print(f"미수집 기업: {total_stocks - collected_companies:,}개")
        if total_stocks > 0:
            print(f"수집률: {collected_companies / total_stocks * 100:.2f}%")
        
        # 2. 중복 확인
        print("\n[2] 중복 확인")
        print("-" * 80)
        
        duplicate_count = db.execute(text("""
            SELECT COUNT(*) FROM (
                SELECT ticker, COUNT(*) as cnt
                FROM company_details
                GROUP BY ticker
                HAVING COUNT(*) > 1
            ) sub
        """)).scalar()
        
        if duplicate_count > 0:
            print(f"⚠️  중복된 티커: {duplicate_count}개")
            duplicates = db.execute(text("""
                SELECT ticker, COUNT(*) as cnt
                FROM company_details
                GROUP BY ticker
                HAVING COUNT(*) > 1
                ORDER BY cnt DESC
                LIMIT 5
            """)).fetchall()
            for ticker, cnt in duplicates:
                print(f"  - {ticker}: {cnt}개")
        else:
            print("✅ 중복 없음")
        
        # 3. Source별 통계
        print("\n[3] Source별 통계")
        print("-" * 80)
        
        source_stats = db.execute(text("""
            SELECT source, COUNT(DISTINCT ticker) as cnt
            FROM company_details
            GROUP BY source
            ORDER BY cnt DESC
        """)).fetchall()
        
        for source, cnt in source_stats:
            print(f"  {source}: {cnt:,}개")
        
        # 4. 관계(Edge) 통계
        print("\n[4] 관계(Edge) 통계")
        print("-" * 80)
        
        total_edges = db.execute(text("SELECT COUNT(*) FROM edges")).scalar()
        unique_source = db.execute(text("SELECT COUNT(DISTINCT source_id) FROM edges")).scalar()
        unique_target = db.execute(text("SELECT COUNT(DISTINCT target_id) FROM edges")).scalar()
        
        print(f"총 관계(Edge) 수: {total_edges:,}개")
        print(f"고유 Source 기업: {unique_source:,}개")
        print(f"고유 Target 기업: {unique_target:,}개")
        
        if total_edges > 0:
            edge_types = db.execute(text("""
                SELECT relation_type, COUNT(*) as cnt
                FROM edges
                GROUP BY relation_type
                ORDER BY cnt DESC
                LIMIT 5
            """)).fetchall()
            
            print("\n관계 유형별 통계:")
            for rel_type, cnt in edge_types:
                print(f"  {rel_type}: {cnt:,}개")
        
        print("\n" + "=" * 80)
        print("확인 완료")
        print("=" * 80)
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    main()

