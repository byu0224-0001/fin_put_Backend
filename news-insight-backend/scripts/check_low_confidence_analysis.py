"""Check 1: LOW confidence 분석 스크립트"""
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
    print("Check 1: LOW confidence 분석")
    print(f"시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    
    with engine.connect() as conn:
        # 전체 Primary 섹터 수
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM investor_sector
            WHERE is_primary = true
        """))
        total_count = result.fetchone()[0]
        print(f"1. 전체 Primary 섹터: {total_count:,}개")
        print()
        
        # Confidence 분포
        result = conn.execute(text("""
            SELECT 
                confidence,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM investor_sector
            WHERE is_primary = true
            GROUP BY confidence
            ORDER BY count DESC
        """))
        
        print("2. Confidence 분포:")
        print("-" * 80)
        confidence_dist = {}
        for row in result.fetchall():
            confidence, count, percentage = row
            confidence_dist[confidence or 'NULL'] = {'count': count, 'percentage': percentage}
            print(f"  {confidence or 'NULL'}: {count:,}개 ({percentage}%)")
        print()
        
        # LOW confidence 비율
        low_count = confidence_dist.get('LOW', {}).get('count', 0)
        low_percentage = confidence_dist.get('LOW', {}).get('percentage', 0)
        
        print("=" * 80)
        print("3. LOW confidence 비율 분석")
        print("=" * 80)
        print(f"LOW confidence 기업 수: {low_count:,}개")
        print(f"LOW confidence 비율: {low_percentage}%")
        print()
        
        # 해석 기준
        if low_percentage <= 20:
            print("✅ 평가: 매우 양호 (~20% 이하)")
        elif low_percentage <= 35:
            print("✅ 평가: 현실적, 허용 가능 (20~35%)")
        elif low_percentage <= 40:
            print("⚠️ 평가: 주의 필요 (35~40%)")
        else:
            print("❌ 평가: '억지 합격' 과다 가능성 (40% 이상)")
        print()
        
        # LOW confidence 기업 reasoning 샘플
        if low_count > 0:
            result = conn.execute(text("""
                SELECT 
                    i.ticker,
                    s.stock_name,
                    i.sector_l1,
                    i.sector_l2,
                    i.confidence,
                    i.classification_reasoning,
                    i.rule_score,
                    i.embedding_score,
                    i.ensemble_score
                FROM investor_sector i
                LEFT JOIN stocks s ON i.ticker = s.ticker
                WHERE i.is_primary = true 
                    AND i.confidence = 'LOW'
                ORDER BY RANDOM()
                LIMIT 5
            """))
            
            print("=" * 80)
            print("4. LOW confidence 기업 reasoning 샘플 (5개)")
            print("=" * 80)
            print()
            
            normal_count = 0
            problem_count = 0
            
            for idx, row in enumerate(result.fetchall(), 1):
                ticker, stock_name, sector_l1, sector_l2, confidence, reasoning, rule_score, embedding_score, ensemble_score = row
                
                print(f"[{idx}] {ticker} ({stock_name})")
                print(f"  섹터: {sector_l1}/{sector_l2}")
                print(f"  confidence: {confidence}")
                print(f"  rule_score: {rule_score}")
                print(f"  embedding_score: {embedding_score}")
                print(f"  ensemble_score: {ensemble_score}")
                print(f"  reasoning: {reasoning[:200] if reasoning else 'None'}...")
                print()
                
                # reasoning 분류
                if reasoning:
                    reasoning_lower = reasoning.lower()
                    if any(keyword in reasoning_lower for keyword in ['정보 부족', '텍스트 없음', '임베딩 실패', '룰 미적용', '데이터 없음']):
                        problem_count += 1
                    else:
                        normal_count += 1
                else:
                    problem_count += 1
            
            print("=" * 80)
            print("5. Reasoning 분류 결과")
            print("=" * 80)
            print(f"정상적 LOW: {normal_count}개")
            print(f"문제적 LOW: {problem_count}개")
            print()
            
            if problem_count > normal_count:
                print("⚠️ 문제적 LOW가 많음 → 데이터/전처리 개선 필요")
            else:
                print("✅ 정상적 LOW 위주 → 합격")
        else:
            print("4. LOW confidence 기업 없음")
            print()
        
        # 최종 판정
        print("=" * 80)
        print("6. 최종 판정")
        print("=" * 80)
        
        if low_percentage <= 35:
            if low_count > 0 and problem_count <= normal_count:
                print("✅ 합격: LOW 비율 ≤ 35% & reasoning 정상")
                print("   → 다음 단계로 진행 가능")
            elif low_count == 0:
                print("✅ 합격: LOW confidence 기업 없음")
                print("   → 다음 단계로 진행 가능")
            else:
                print("△ 조건부 합격: LOW 비율은 양호하나 reasoning 일부 부실")
                print("   → Confidence 정책만 조정 후 진행")
        elif low_percentage <= 40:
            print("△ 조건부 합격: LOW 비율이 높지만 허용 범위")
            print("   → Confidence 정책 조정 권장")
        else:
            print("✖ 보류: LOW 비율이 높고 reasoning 부실 가능성")
            print("   → 분류 로직 보완 필요")
        print()

if __name__ == '__main__':
    main()

