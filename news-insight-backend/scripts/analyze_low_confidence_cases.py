"""P2-1: LOW confidence 기업 재점검"""
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
    print("P2-1: LOW confidence 기업 재점검")
    print(f"시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    
    with engine.connect() as conn:
        # LOW confidence 기업 점수 분포
        result = conn.execute(text("""
            SELECT 
                AVG(rule_score) as avg_rule_score,
                AVG(embedding_score) as avg_embedding_score,
                AVG(ensemble_score) as avg_ensemble_score,
                MIN(rule_score) as min_rule_score,
                MIN(embedding_score) as min_embedding_score,
                MIN(ensemble_score) as min_ensemble_score,
                MAX(rule_score) as max_rule_score,
                MAX(embedding_score) as max_embedding_score,
                MAX(ensemble_score) as max_ensemble_score,
                COUNT(*) FILTER (WHERE rule_score IS NULL) as no_rule_score,
                COUNT(*) FILTER (WHERE embedding_score IS NULL) as no_embedding_score,
                COUNT(*) FILTER (WHERE ensemble_score IS NULL) as no_ensemble_score
            FROM investor_sector
            WHERE is_primary = true 
                AND confidence = 'LOW'
        """))
        
        row = result.fetchone()
        avg_rule, avg_embedding, avg_ensemble, min_rule, min_embedding, min_ensemble, max_rule, max_embedding, max_ensemble, no_rule, no_embedding, no_ensemble = row
        
        print("1. LOW confidence 기업 점수 통계:")
        print("-" * 80)
        print(f"  rule_score:")
        print(f"    평균: {avg_rule:.3f}" if avg_rule else "    평균: None")
        print(f"    최소: {min_rule:.3f}" if min_rule else "    최소: None")
        print(f"    최대: {max_rule:.3f}" if max_rule else "    최대: None")
        print(f"    NULL: {no_rule:,}개")
        print()
        print(f"  embedding_score:")
        print(f"    평균: {avg_embedding:.3f}" if avg_embedding else "    평균: None")
        print(f"    최소: {min_embedding:.3f}" if min_embedding else "    최소: None")
        print(f"    최대: {max_embedding:.3f}" if max_embedding else "    최대: None")
        print(f"    NULL: {no_embedding:,}개")
        print()
        print(f"  ensemble_score:")
        print(f"    평균: {avg_ensemble:.3f}" if avg_ensemble else "    평균: None")
        print(f"    최소: {min_ensemble:.3f}" if min_ensemble else "    최소: None")
        print(f"    최대: {max_ensemble:.3f}" if max_ensemble else "    최대: None")
        print(f"    NULL: {no_ensemble:,}개")
        print()
        
        # 점수 구간별 분포
        result = conn.execute(text("""
            SELECT 
                CASE 
                    WHEN ensemble_score < 0.2 THEN '< 0.2'
                    WHEN ensemble_score < 0.3 THEN '0.2-0.3'
                    WHEN ensemble_score < 0.4 THEN '0.3-0.4'
                    WHEN ensemble_score < 0.5 THEN '0.4-0.5'
                    ELSE '>= 0.5'
                END as score_range,
                COUNT(*) as count
            FROM investor_sector
            WHERE is_primary = true 
                AND confidence = 'LOW'
                AND ensemble_score IS NOT NULL
            GROUP BY score_range
            ORDER BY score_range
        """))
        
        print("2. ensemble_score 구간별 분포:")
        print("-" * 80)
        for row in result.fetchall():
            score_range, count = row
            print(f"  {score_range}: {count:,}개")
        print()
        
        # rule_score가 NULL인 경우 분석
        if no_rule > 0:
            result = conn.execute(text("""
                SELECT 
                    AVG(embedding_score) as avg_embedding,
                    AVG(ensemble_score) as avg_ensemble,
                    COUNT(*) as count
                FROM investor_sector
                WHERE is_primary = true 
                    AND confidence = 'LOW'
                    AND rule_score IS NULL
            """))
            
            row = result.fetchone()
            avg_emb, avg_ens, count = row
            print(f"3. rule_score가 NULL인 LOW confidence 기업 ({count:,}개):")
            print("-" * 80)
            print(f"  평균 embedding_score: {avg_emb:.3f}" if avg_emb else "  평균 embedding_score: None")
            print(f"  평균 ensemble_score: {avg_ens:.3f}" if avg_ens else "  평균 ensemble_score: None")
            print()
        
        # 권장사항
        print("=" * 80)
        print("4. 권장사항")
        print("=" * 80)
        
        if avg_ensemble and avg_ensemble < 0.3:
            print("⚠️ 평균 ensemble_score가 0.3 미만")
            print("   → Threshold 조정 고려:")
            print("     - Rule threshold 낮추기")
            print("     - Embedding score cut-off 낮추기")
        elif avg_ensemble and avg_ensemble < 0.4:
            print("✅ 평균 ensemble_score가 0.3~0.4 범위")
            print("   → 현재 Threshold 적절")
        else:
            print("✅ 평균 ensemble_score가 0.4 이상")
            print("   → Threshold 여유 있음")
        
        if no_rule > 0:
            print()
            print(f"⚠️ rule_score가 NULL인 기업 {no_rule:,}개")
            print("   → Rule-based 분류 로직 보완 고려")
        
        print()

if __name__ == '__main__':
    main()

