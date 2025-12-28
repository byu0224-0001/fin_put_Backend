"""Check D: rule_score NULL 원인 분석 (Case A vs Case B)"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime

project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

# UTF-8 인코딩 설정
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'
else:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

load_dotenv()

user = quote_plus(os.getenv('POSTGRES_USER', 'postgres'))
pwd = quote_plus(os.getenv('POSTGRES_PASSWORD', 'postgres'))
host = os.getenv('POSTGRES_HOST', 'localhost')
port = os.getenv('POSTGRES_PORT', '5432')
db_name = os.getenv('POSTGRES_DB', 'newsdb')
url = f'postgresql://{user}:{pwd}@{host}:{port}/{db_name}'

engine = create_engine(url)

def main():
    print("=" * 80)
    print("Check D: rule_score NULL 원인 분석")
    print(f"시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    print("목적: rule_score NULL인 기업을 Case A(안전) vs Case B(위험)로 분류")
    print("Case A: 텍스트 충분, Rule 미매칭 (안전)")
    print("Case B: 텍스트 부족, 데이터 결손 (위험)")
    print()
    
    with engine.connect() as conn:
        # 전체 rule_score NULL 개수
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM investor_sector i
            JOIN company_details cd ON i.ticker = cd.ticker
            WHERE i.is_primary = true 
                AND i.rule_score IS NULL
        """))
        total_null = result.fetchone()[0]
        print(f"1. 전체 rule_score NULL 기업 수: {total_null:,}개")
        print()
        
        # Case A vs Case B 분류
        result = conn.execute(text("""
            SELECT
                CASE
                    WHEN LENGTH(COALESCE(cd.biz_summary, '')) < 50 THEN 'DATA_MISSING (Risk)'
                    ELSE 'RULE_MISSING (Safe)'
                END as status,
                COUNT(*) as count
            FROM investor_sector i
            JOIN company_details cd ON i.ticker = cd.ticker
            WHERE i.is_primary = true 
                AND i.rule_score IS NULL
            GROUP BY status
            ORDER BY count DESC
        """))
        
        print("2. Case A vs Case B 분류:")
        print("-" * 80)
        case_dist = {}
        for row in result.fetchall():
            status, count = row
            case_dist[status] = count
            print(f"  {status}: {count:,}개")
        print()
        
        # 상세 분석: Case A (RULE_MISSING)
        safe_count = case_dist.get('RULE_MISSING (Safe)', 0)
        if safe_count > 0:
            result = conn.execute(text("""
                SELECT
                    i.ticker,
                    s.stock_name,
                    LENGTH(COALESCE(cd.biz_summary, '')) as biz_length,
                    CASE WHEN cd.products IS NOT NULL THEN jsonb_array_length(cd.products) ELSE 0 END as products_count,
                    i.embedding_score,
                    i.ensemble_score,
                    i.confidence,
                    LEFT(cd.biz_summary, 100) as biz_preview
                FROM investor_sector i
                JOIN company_details cd ON i.ticker = cd.ticker
                LEFT JOIN stocks s ON i.ticker = s.ticker
                WHERE i.is_primary = true 
                    AND i.rule_score IS NULL
                    AND LENGTH(COALESCE(cd.biz_summary, '')) >= 50
                ORDER BY i.embedding_score DESC NULLS LAST
                LIMIT 20
            """))
            
            print("=" * 80)
            print("3. Case A (RULE_MISSING - Safe) 상세 분석 (상위 20개)")
            print("=" * 80)
            print()
            
            high_embedding_count = 0
            medium_embedding_count = 0
            low_embedding_count = 0
            
            for idx, row in enumerate(result.fetchall(), 1):
                ticker, stock_name, biz_length, products_count, embedding_score, ensemble_score, confidence, biz_preview = row
                
                print(f"[{idx}] {ticker} ({stock_name})")
                print(f"  biz_summary 길이: {biz_length}자")
                print(f"  products 개수: {products_count}개")
                print(f"  embedding_score: {embedding_score:.3f}" if embedding_score else "  embedding_score: None")
                print(f"  ensemble_score: {ensemble_score:.3f}" if ensemble_score else "  ensemble_score: None")
                print(f"  confidence: {confidence}")
                print(f"  biz_preview: {biz_preview[:80] if biz_preview else 'None'}...")
                print()
                
                if embedding_score:
                    if embedding_score >= 0.5:
                        high_embedding_count += 1
                    elif embedding_score >= 0.4:
                        medium_embedding_count += 1
                    else:
                        low_embedding_count += 1
            
            print("=" * 80)
            print("4. Case A 임베딩 점수 분포:")
            print("-" * 80)
            print(f"  높음 (>= 0.5): {high_embedding_count}개")
            print(f"  중간 (0.4-0.5): {medium_embedding_count}개")
            print(f"  낮음 (< 0.4): {low_embedding_count}개")
            print()
        
        # 상세 분석: Case B (DATA_MISSING)
        risk_count = case_dist.get('DATA_MISSING (Risk)', 0)
        if risk_count > 0:
            result = conn.execute(text("""
                SELECT
                    i.ticker,
                    s.stock_name,
                    LENGTH(COALESCE(cd.biz_summary, '')) as biz_length,
                    CASE WHEN cd.products IS NOT NULL THEN jsonb_array_length(cd.products) ELSE 0 END as products_count,
                    i.embedding_score,
                    i.ensemble_score,
                    i.confidence,
                    cd.biz_summary
                FROM investor_sector i
                JOIN company_details cd ON i.ticker = cd.ticker
                LEFT JOIN stocks s ON i.ticker = s.ticker
                WHERE i.is_primary = true 
                    AND i.rule_score IS NULL
                    AND LENGTH(COALESCE(cd.biz_summary, '')) < 50
                ORDER BY biz_length DESC
                LIMIT 20
            """))
            
            print("=" * 80)
            print("5. Case B (DATA_MISSING - Risk) 상세 분석 (상위 20개)")
            print("=" * 80)
            print()
            
            for idx, row in enumerate(result.fetchall(), 1):
                ticker, stock_name, biz_length, products_count, embedding_score, ensemble_score, confidence, biz_summary = row
                
                print(f"[{idx}] {ticker} ({stock_name})")
                print(f"  biz_summary 길이: {biz_length}자")
                print(f"  products 개수: {products_count}개")
                print(f"  embedding_score: {embedding_score:.3f}" if embedding_score else "  embedding_score: None")
                print(f"  ensemble_score: {ensemble_score:.3f}" if ensemble_score else "  ensemble_score: None")
                print(f"  confidence: {confidence}")
                print(f"  biz_summary: {biz_summary if biz_summary else 'NULL'}")
                print()
        
        # 추가 분석: products/keywords 존재 여부
        result = conn.execute(text("""
            SELECT
                CASE
                    WHEN LENGTH(COALESCE(cd.biz_summary, '')) < 50 THEN 'DATA_MISSING'
                    ELSE 'RULE_MISSING'
                END as case_type,
                COUNT(*) FILTER (WHERE cd.products IS NOT NULL AND jsonb_array_length(cd.products) > 0) as has_products,
                COUNT(*) FILTER (WHERE cd.products IS NULL OR jsonb_array_length(cd.products) = 0) as no_products,
                AVG(i.embedding_score) FILTER (WHERE i.embedding_score IS NOT NULL) as avg_embedding,
                AVG(i.ensemble_score) FILTER (WHERE i.ensemble_score IS NOT NULL) as avg_ensemble
            FROM investor_sector i
            JOIN company_details cd ON i.ticker = cd.ticker
            WHERE i.is_primary = true 
                AND i.rule_score IS NULL
            GROUP BY case_type
        """))
        
        print("=" * 80)
        print("6. 추가 분석: products/keywords 존재 여부")
        print("=" * 80)
        print()
        
        for row in result.fetchall():
            case_type, has_products, no_products, avg_embedding, avg_ensemble = row
            total = has_products + no_products
            products_ratio = (has_products / total * 100) if total > 0 else 0
            
            print(f"{case_type}:")
            print(f"  products 있는 기업: {has_products:,}개 ({products_ratio:.1f}%)")
            print(f"  products 없는 기업: {no_products:,}개 ({100 - products_ratio:.1f}%)")
            print(f"  평균 embedding_score: {avg_embedding:.3f}" if avg_embedding else "  평균 embedding_score: None")
            print(f"  평균 ensemble_score: {avg_ensemble:.3f}" if avg_ensemble else "  평균 ensemble_score: None")
            print()
        
        # 최종 결론
        print("=" * 80)
        print("7. 최종 결론 및 권장사항")
        print("=" * 80)
        print()
        
        safe_ratio = (safe_count / total_null * 100) if total_null > 0 else 0
        risk_ratio = (risk_count / total_null * 100) if total_null > 0 else 0
        
        print(f"Case A (RULE_MISSING - Safe): {safe_count:,}개 ({safe_ratio:.1f}%)")
        print(f"  → 밸류체인 분석 가능 (임베딩 벡터 양호)")
        print(f"  → Rule 사전 보강 권장 (구조적 애매형)")
        print()
        
        print(f"Case B (DATA_MISSING - Risk): {risk_count:,}개 ({risk_ratio:.1f}%)")
        print(f"  → 밸류체인 분석 위험 (임베딩 벡터 부실)")
        print(f"  → 데이터 파이프라인 보강 필요")
        print(f"  → 밸류체인에서 UNCERTAIN 노드로 처리")
        print()
        
        if safe_count > 0:
            print("✅ 다음 단계: 구조적 애매형(Case A) 기업 추출 및 Rule 보강")
        if risk_count > 0:
            print("⚠️ 다음 단계: 데이터 파이프라인 보강 (DART/뉴스 재수집)")
        print()

if __name__ == '__main__':
    main()

