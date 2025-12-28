# -*- coding: utf-8 -*-
"""
DB 구축 상황 정밀 확인 스크립트
- 전체 수집 통계
- 중복 확인
- 섹터별 분포
- 관계(Edge) 통계
"""
import sys
sys.path.insert(0, '.')
sys.stdout.reconfigure(encoding='utf-8')

from app.db import SessionLocal
from sqlalchemy import text
from collections import Counter

def main():
    print("=" * 80)
    print("DB 구축 상황 정밀 확인")
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
        print(f"수집률: {collected_companies / total_stocks * 100:.2f}%")
        
        # 2. 중복 확인
        print("\n[2] 중복 확인")
        print("-" * 80)
        
        duplicate_tickers = db.execute(text("""
            SELECT ticker, COUNT(*) as cnt
            FROM company_details
            GROUP BY ticker
            HAVING COUNT(*) > 1
            ORDER BY cnt DESC
        """)).fetchall()
        
        if duplicate_tickers:
            print(f"⚠️  중복된 티커 발견: {len(duplicate_tickers)}개")
            for ticker, cnt in duplicate_tickers[:10]:
                sources = db.execute(text("""
                    SELECT DISTINCT source FROM company_details WHERE ticker = :ticker
                """), {'ticker': ticker}).fetchall()
                sources_str = ', '.join([s[0] for s in sources])
                print(f"  - {ticker}: {cnt}개 (sources: {sources_str})")
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
        
        # 4. 섹터별 분포
        print("\n[4] 섹터별 분포 (상위 10개)")
        print("-" * 80)
        
        sector_stats = db.execute(text("""
            SELECT s.investor_sector_name, COUNT(DISTINCT cd.ticker) as cnt
            FROM company_details cd
            JOIN stocks s ON cd.ticker = s.ticker
            WHERE s.investor_sector_name IS NOT NULL
            GROUP BY s.investor_sector_name
            ORDER BY cnt DESC
            LIMIT 10
        """)).fetchall()
        
        for sector, cnt in sector_stats:
            print(f"  {sector}: {cnt:,}개")
        
        # 5. 관계(Edge) 통계
        print("\n[5] 관계(Edge) 통계")
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
        
        # 6. 최근 수집 기업 (상위 10개)
        print("\n[6] 최근 수집 기업 (상위 10개)")
        print("-" * 80)
        
        # 최근 수집 기업은 id 기준으로 (더 빠름)
        recent_companies = db.execute(text("""
            SELECT cd.ticker, s.stock_name, cd.source
            FROM company_details cd
            JOIN stocks s ON cd.ticker = s.ticker
            ORDER BY cd.id DESC
            LIMIT 10
        """)).fetchall()
        
        for ticker, name, source in recent_companies:
            print(f"  {ticker}: {name} ({source})")
        
        # 7. 데이터 품질 확인
        print("\n[7] 데이터 품질 확인")
        print("-" * 80)
        
        # 비어있는 주요 필드 확인
        empty_biz_summary = db.execute(text("""
            SELECT COUNT(*) FROM company_details 
            WHERE biz_summary IS NULL OR biz_summary = ''
        """)).scalar()
        
        empty_products = db.execute(text("""
            SELECT COUNT(*) FROM company_details 
            WHERE products IS NULL OR products = '[]'::jsonb
        """)).scalar()
        
        total_companies = db.execute(text("SELECT COUNT(*) FROM company_details")).scalar()
        
        if total_companies > 0:
            print(f"biz_summary 비어있음: {empty_biz_summary:,}개 ({empty_biz_summary/total_companies*100:.2f}%)")
            print(f"products 비어있음: {empty_products:,}개 ({empty_products/total_companies*100:.2f}%)")
        else:
            print("수집된 기업이 없습니다.")
        
        # 8. 연도별 수집 통계
        print("\n[8] 연도별 수집 통계")
        print("-" * 80)
        
        # Python에서 연도 추출 (더 안전함)
        all_sources = db.execute(text("""
            SELECT DISTINCT source, COUNT(DISTINCT ticker) as cnt
            FROM company_details
            GROUP BY source
            ORDER BY source DESC
        """)).fetchall()
        
        year_stats = {}
        for source, cnt in all_sources:
            if source and source.startswith('DART_'):
                year = source.replace('DART_', '')
                year_stats[year] = year_stats.get(year, 0) + cnt
            else:
                year_stats['기타'] = year_stats.get('기타', 0) + cnt
        
        for year in sorted(year_stats.keys(), reverse=True):
            print(f"  {year}: {year_stats[year]:,}개")
        
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

