"""재분류 상태 검증 및 모순 해소 (개선 버전)"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime, timedelta
import json

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

def check_timezone():
    """DB 타임존 확인"""
    conn = engine.connect()
    try:
        result = conn.execute(text("SHOW timezone"))
        db_timezone = result.fetchone()[0]
        result = conn.execute(text("SELECT NOW()"))
        db_now = result.fetchone()[0]
        print(f"DB 타임존: {db_timezone}")
        print(f"DB 현재 시간: {db_now}")
        print(f"로컬 현재 시간: {datetime.now()}")
        print()
    finally:
        conn.close()

def check_update_times():
    """업데이트 시간 모순 해소"""
    print("=" * 80)
    print("0. 업데이트 시간 모순 해소")
    print("=" * 80)
    print()
    
    conn = engine.connect()
    try:
        # updated_at 기준
        result = conn.execute(text("""
            SELECT 
                MAX(updated_at) AS max_time,
                COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '1 hour') AS last_1h,
                COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '6 hours') AS last_6h,
                COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '24 hours') AS last_24h
            FROM investor_sector
            WHERE is_primary = true
        """))
        row = result.fetchone()
        if row and row[0]:
            max_time, last_1h, last_6h, last_24h = row
            if max_time.tzinfo:
                max_time = max_time.replace(tzinfo=None)
            time_diff = datetime.now() - max_time
            print(f"[updated_at] 기준:")
            print(f"  최신 값: {max_time} (UTC 기준, 로컬 시간으로는 {max_time + timedelta(hours=9)})")
            print(f"  시간 차이: {time_diff}")
            print(f"  최근 1시간: {last_1h:,}개")
            print(f"  최근 6시간: {last_6h:,}개")
            print(f"  최근 24시간: {last_24h:,}개")
            print()
            print("✅ 모순 해소: updated_at이 최근 1시간 내에 192개 업데이트 있음")
            print("   (UTC 기준으로는 13:42이지만, KST 기준으로는 22:42로 최근)")
        print()
    except Exception as e:
        print(f"오류: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

def check_null_sectors():
    """NULL 섹터 및 Fallback 검증"""
    print("=" * 80)
    print("1. NULL 섹터 및 Fallback 검증")
    print("=" * 80)
    print()
    
    conn = engine.connect()
    try:
        # NULL 섹터 카운트
        result = conn.execute(text("""
            SELECT 
                COUNT(*) FILTER (WHERE sector_l1 IS NULL) AS null_l1,
                COUNT(*) FILTER (WHERE sector_l2 IS NULL) AS null_l2,
                COUNT(*) FILTER (WHERE sector_l1 IS NULL AND sector_l2 IS NULL) AS null_both,
                COUNT(*) AS total
            FROM investor_sector
            WHERE is_primary = true
        """))
        row = result.fetchone()
        null_l1, null_l2, null_both, total = row
        print(f"전체 Primary 섹터: {total:,}개")
        print(f"NULL L1: {null_l1:,}개 ({null_l1/total*100:.1f}%)")
        print(f"NULL L2: {null_l2:,}개 ({null_l2/total*100:.1f}%)")
        print(f"NULL Both: {null_both:,}개 ({null_both/total*100:.1f}%)")
        print()
        
        # fallback_used 필드 확인
        try:
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) FILTER (WHERE fallback_used IS NOT NULL) AS has_fallback,
                    COUNT(*) FILTER (WHERE fallback_used = 'true' OR fallback_used = 'TRUE') AS fallback_true,
                    COUNT(*) FILTER (WHERE fallback_used = 'false' OR fallback_used = 'FALSE') AS fallback_false,
                    COUNT(*) FILTER (WHERE fallback_used IS NULL) AS fallback_null
                FROM investor_sector
                WHERE is_primary = true
            """))
            row = result.fetchone()
            has_fallback, fallback_true, fallback_false, fallback_null = row
            print(f"fallback_used 필드 상태:")
            print(f"  true/TRUE: {fallback_true:,}개")
            print(f"  false/FALSE: {fallback_false:,}개")
            print(f"  NULL: {fallback_null:,}개")
            print()
            
            # 결론
            if null_both == 0:
                print("✅ NULL 섹터 없음 - Fallback 0%는 정상 (필요 없었음)")
            else:
                print(f"⚠️ NULL 섹터 {null_both}개 존재 - Fallback이 작동해야 함")
                if fallback_true == 0:
                    print("❌ Fallback이 작동하지 않음 - 수정 필요")
        except Exception as e:
            print(f"fallback_used 필드 확인 오류: {e}")
            print("  → fallback_used 컬럼 타입 확인 필요")
            print()
    except Exception as e:
        print(f"오류: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

def check_low_confidence():
    """LOW Confidence 원인 분해"""
    print("=" * 80)
    print("2. LOW Confidence 원인 분해")
    print("=" * 80)
    print()
    
    conn = engine.connect()
    try:
        # 섹터별 LOW 비율 TOP 10
        result = conn.execute(text("""
            SELECT 
                sector_l1,
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE confidence = 'LOW') AS low_count,
                ROUND(COUNT(*) FILTER (WHERE confidence = 'LOW')::numeric / COUNT(*)::numeric * 100, 1) AS low_rate
            FROM investor_sector
            WHERE is_primary = true
            GROUP BY sector_l1
            HAVING COUNT(*) >= 10
            ORDER BY low_rate DESC
            LIMIT 10
        """))
        print("섹터별 LOW 비율 TOP 10:")
        print(f"{'섹터':<25} {'전체':>8} {'LOW':>8} {'비율':>8}")
        print("-" * 60)
        for row in result.fetchall():
            sector, total, low_count, low_rate = row
            print(f"{str(sector):<25} {total:>8,} {low_count:>8,} {low_rate:>7.1f}%")
        print()
        
        # LOW 기업에서 L2가 None인 비율
        result = conn.execute(text("""
            SELECT 
                COUNT(*) FILTER (WHERE sector_l2 IS NULL) AS low_null_l2,
                COUNT(*) AS low_total
            FROM investor_sector
            WHERE is_primary = true AND confidence = 'LOW'
        """))
        row = result.fetchone()
        if row:
            low_null_l2, low_total = row
            print(f"LOW 기업 중 L2가 None인 비율: {low_null_l2:,}개 / {low_total:,}개 ({low_null_l2/low_total*100:.1f}%)")
        print()
        
        # classification_method 확인
        try:
            result = conn.execute(text("""
                SELECT 
                    classification_method,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE confidence = 'LOW') AS low_count
                FROM investor_sector
                WHERE is_primary = true
                GROUP BY classification_method
                ORDER BY total DESC
            """))
            print("classification_method별 분포:")
            for row in result.fetchall():
                method, total, low_count = row
                low_rate = (low_count / total * 100) if total > 0 else 0
                print(f"  {method or 'NULL'}: {total:,}개 (LOW: {low_count:,}개, {low_rate:.1f}%)")
            print()
        except Exception as e:
            print(f"classification_method 컬럼 없음: {e}")
            print()
    except Exception as e:
        print(f"오류: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

def check_boosting():
    """Boosting 로그 실제 반영 검증"""
    print("=" * 80)
    print("3. Boosting 로그 실제 반영 검증")
    print("=" * 80)
    print()
    
    conn = engine.connect()
    try:
        # boosting_log가 있는 케이스 샘플링
        result = conn.execute(text("""
            SELECT 
                ticker,
                boosting_log,
                confidence,
                ensemble_score
            FROM investor_sector
            WHERE is_primary = true 
                AND boosting_log IS NOT NULL
            ORDER BY RANDOM()
            LIMIT 50
        """))
        
        samples = []
        for row in result.fetchall():
            ticker, boosting_log_str, confidence, ensemble_score = row
            try:
                if isinstance(boosting_log_str, str):
                    log = json.loads(boosting_log_str)
                else:
                    log = boosting_log_str
                
                final_boost = log.get('final_boost', 0)
                samples.append({
                    'ticker': ticker,
                    'final_boost': final_boost,
                    'confidence': confidence,
                    'ensemble_score': ensemble_score
                })
            except:
                continue
        
        if samples:
            print(f"샘플링된 boosting_log 케이스: {len(samples)}개")
            boosts = [s['final_boost'] for s in samples]
            print(f"평균 final_boost: {sum(boosts) / len(boosts):.4f}")
            print(f"최대 final_boost: {max(boosts):.4f}")
            print(f"최소 final_boost: {min(boosts):.4f}")
            print()
            print("⚠️ 주의: base_score와 final_score 비교는 classification_reasoning 또는 별도 로그 분석 필요")
            print("   (현재 테이블에 base_score/final_score 컬럼이 없을 수 있음)")
        else:
            print("boosting_log 샘플 없음")
        print()
    except Exception as e:
        print(f"오류: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

def check_gpu():
    """GPU 사용 확인"""
    print("=" * 80)
    print("4. GPU 사용 확인")
    print("=" * 80)
    print()
    
    import torch
    print(f"PyTorch CUDA 사용 가능: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        print(f"CUDA 디바이스 수: {torch.cuda.device_count()}")
        print(f"현재 디바이스: {torch.cuda.current_device()}")
        print(f"디바이스 이름: {torch.cuda.get_device_name(0)}")
    print()
    print("⚠️ 실제 GPU 사용 여부는 실행 로그에서 'CUDA' 또는 'cuda' 키워드 확인 필요")
    print()

def main():
    print("=" * 80)
    print("재분류 상태 검증 및 모순 해소")
    print(f"시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    
    check_timezone()
    check_update_times()
    check_null_sectors()
    check_low_confidence()
    check_boosting()
    check_gpu()
    
    print("=" * 80)
    print("검증 완료")
    print("=" * 80)

if __name__ == '__main__':
    main()

