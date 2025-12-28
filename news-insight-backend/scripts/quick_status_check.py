"""재분류 빠른 상태 확인"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from datetime import datetime, timedelta
import psutil

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
    print("재분류 빠른 상태 확인")
    print(f"시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    
    # 프로세스 확인
    print("1. 프로세스 상태:")
    reclassify_proc = None
    monitor_proc = None
    
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] and 'python' in proc.info['name'].lower():
                cmdline = proc.info['cmdline']
                if cmdline:
                    cmdline_str = ' '.join(cmdline)
                    if 'reclassify_all_sectors' in cmdline_str.lower():
                        p = psutil.Process(proc.info['pid'])
                        reclassify_proc = {
                            'pid': proc.info['pid'],
                            'cpu': p.cpu_percent(interval=1.0),
                            'memory': p.memory_info().rss / 1024 / 1024
                        }
                    elif 'auto_monitor' in cmdline_str.lower():
                        monitor_proc = proc.info['pid']
        except:
            continue
    
    if reclassify_proc:
        print(f"  ✅ 재분류: PID {reclassify_proc['pid']}, CPU: {reclassify_proc['cpu']:.1f}%, Memory: {reclassify_proc['memory']:.1f}MB")
    else:
        print("  ❌ 재분류 프로세스 없음")
    
    if monitor_proc:
        print(f"  ✅ 모니터링: PID {monitor_proc}")
    else:
        print("  ⚠️ 모니터링 프로세스 없음")
    
    print()
    
    # DB 업데이트 확인
    print("2. DB 업데이트:")
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '5 minutes') AS last_5m,
                COUNT(*) FILTER (WHERE updated_at > NOW() - INTERVAL '10 minutes') AS last_10m,
                MAX(updated_at) AS max_updated
            FROM investor_sector
            WHERE is_primary = true
        """))
        row = result.fetchone()
        last_5m, last_10m, max_updated = row
        
        print(f"  최근 5분: {last_5m}개")
        print(f"  최근 10분: {last_10m}개")
        
        if max_updated:
            if max_updated.tzinfo:
                max_updated = max_updated.replace(tzinfo=None)
            time_diff = datetime.now() - max_updated
            print(f"  마지막 업데이트: {max_updated} ({time_diff} 전)")
            
            if last_5m > 0:
                print("  ✅ 재분류 진행 중!")
            elif last_10m > 0:
                print("  ⚠️ 재분류 진행 중 (느림)")
            elif time_diff < timedelta(minutes=30):
                print("  ⏳ 초기화 중...")
            else:
                print("  ❌ 재분류 멈춤")
    
    print()
    print("=" * 80)

if __name__ == '__main__':
    main()
