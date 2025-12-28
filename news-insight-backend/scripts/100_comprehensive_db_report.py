# -*- coding: utf-8 -*-
"""
DB 구축 상황 종합 리포트
"""
import sys
sys.path.insert(0, '.')
sys.stdout.reconfigure(encoding='utf-8')

from app.db import SessionLocal
from sqlalchemy import text

def main():
    print("\n" + "=" * 80)
    print("📊 DB 구축 상황 종합 리포트")
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
                sources = db.execute(text("""
                    SELECT DISTINCT source FROM company_details WHERE ticker = :ticker
                """), {'ticker': ticker}).fetchall()
                sources_str = ', '.join([s[0] for s in sources])
                print(f"  - {ticker}: {cnt}개 (sources: {sources_str})")
        else:
            print("✅ 중복 없음 - 모든 기업이 고유하게 저장됨")
        
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
            """)).fetchall()
            
            print("\n관계 유형별 통계:")
            for rel_type, cnt in edge_types:
                print(f"  {rel_type}: {cnt:,}개")
        
        # 5. 데이터 품질 확인
        print("\n[5] 데이터 품질 확인")
        print("-" * 80)
        
        total_companies = db.execute(text("SELECT COUNT(*) FROM company_details")).scalar()
        
        empty_biz_summary = db.execute(text("""
            SELECT COUNT(*) FROM company_details 
            WHERE biz_summary IS NULL OR biz_summary = ''
        """)).scalar()
        
        empty_products = db.execute(text("""
            SELECT COUNT(*) FROM company_details 
            WHERE products IS NULL OR products = '[]'::jsonb
        """)).scalar()
        
        if total_companies > 0:
            print(f"biz_summary 비어있음: {empty_biz_summary:,}개 ({empty_biz_summary/total_companies*100:.2f}%)")
            print(f"products 비어있음: {empty_products:,}개 ({empty_products/total_companies*100:.2f}%)")
            
            # 품질 점수 계산
            quality_score = ((total_companies - empty_biz_summary) / total_companies * 0.6 + 
                           (total_companies - empty_products) / total_companies * 0.4) * 100
            print(f"\n데이터 품질 점수: {quality_score:.1f}/100")
        
        print("\n" + "=" * 80)
        print("✅ 리포트 완료")
        print("=" * 80)
        
    except Exception as e:
        print(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()
        import sys
        sys.exit(0)  # 명시적으로 종료

if __name__ == "__main__":
    main()

