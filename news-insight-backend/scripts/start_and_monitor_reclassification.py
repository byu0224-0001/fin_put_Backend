"""재분류 시작 및 지속 모니터링"""
import os
import sys
import subprocess
import psutil
import time
from pathlib import Path
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

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

from dotenv import load_dotenv
load_dotenv()

user = quote_plus(os.getenv('POSTGRES_USER', 'postgres'))
pwd = quote_plus(os.getenv('POSTGRES_PASSWORD', 'postgres'))
host = os.getenv('POSTGRES_HOST', 'localhost')
port = os.getenv('POSTGRES_PORT', '5432')
db_name = os.getenv('POSTGRES_DB', 'newsdb')
url = f'postgresql://{user}:{pwd}@{host}:{port}/{db_name}'

engine = create_engine(url)

def check_db():
    """DB 연결 확인"""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        print(f"❌ DB 연결 실패: {e}")
        return False

def find_process():
    """재분류 프로세스 찾기"""
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] and 'python' in proc.info['name'].lower():
                cmdline = proc.info['cmdline']
                if cmdline:
                    cmdline_str = ' '.join(cmdline)
                    if 'reclassify_all_sectors' in cmdline_str.lower():
                        p = psutil.Process(proc.info['pid'])
                        return {
                            'pid': proc.info['pid'],
                            'cpu': p.cpu_percent(interval=1.0),
                            'memory': p.memory_info().rss / 1024 / 1024
                        }
        except:
            continue
    return None

def check_updates():
    """DB 업데이트 확인"""
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
        return {
            'last_5m': row[0],
            'last_10m': row[1],
            'max_updated': row[2]
        }

def kill_processes():
    """재분류 프로세스 종료"""
    killed = 0
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] and 'python' in proc.info['name'].lower():
                cmdline = proc.info['cmdline']
                if cmdline:
                    cmdline_str = ' '.join(cmdline)
                    if 'reclassify_all_sectors' in cmdline_str.lower():
                        p = psutil.Process(proc.info['pid'])
                        p.terminate()
                        try:
                            p.wait(timeout=5)
                        except psutil.TimeoutExpired:
                            p.kill()
                        killed += 1
        except:
            continue
    if killed > 0:
        time.sleep(3)
    return killed

def start_reclassification():
    """재분류 시작"""
    script = project_root / "scripts" / "reclassify_all_sectors_ensemble_optimized.py"
    
    env = os.environ.copy()
    env['CUDA_VISIBLE_DEVICES'] = '0'
    env['PYTHONUNBUFFERED'] = '1'
    
    log_dir = project_root / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"reclassification_monitored_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    try:
        with open(log_file, 'w', encoding='utf-8') as f:
            process = subprocess.Popen(
                [sys.executable, str(script)],
                cwd=project_root,
                stdout=f,
                stderr=subprocess.STDOUT,
                env=env
            )
            return process.pid, log_file
    except Exception as e:
        print(f"❌ 시작 실패: {e}")
        return None, None

def main():
    print("=" * 80)
    print("재분류 시작 및 지속 모니터링")
    print("=" * 80)
    print()
    
    # 1. DB 확인
    if not check_db():
        print("❌ DB 연결 실패. Docker와 PostgreSQL을 확인하세요.")
        return
    
    print("✅ DB 연결 성공")
    print()
    
    # 2. 기존 프로세스 정리
    print("기존 프로세스 정리 중...")
    killed = kill_processes()
    print(f"  {killed}개 프로세스 종료")
    time.sleep(2)
    print()
    
    # 3. 재분류 시작
    print("재분류 시작 중...")
    pid, log_file = start_reclassification()
    
    if not pid:
        print("❌ 재분류 시작 실패")
        return
    
    print(f"✅ 재분류 시작됨 (PID: {pid})")
    print(f"   로그 파일: {log_file}")
    print()
    
    # 4. 모니터링 루프
    print("=" * 80)
    print("모니터링 시작 (60초마다 체크)")
    print("=" * 80)
    print()
    
    check_count = 0
    last_update_count = 0
    no_update_count = 0
    
    try:
        while True:
            check_count += 1
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            print(f"\n[{current_time}] 체크 #{check_count}")
            print("-" * 80)
            
            # 프로세스 확인
            proc = find_process()
            if proc:
                print(f"프로세스: PID {proc['pid']}, CPU: {proc['cpu']:.1f}%, Memory: {proc['memory']:.1f}MB")
            else:
                print("프로세스: 없음")
                print("재시작 중...")
                killed = kill_processes()
                time.sleep(2)
                pid, log_file = start_reclassification()
                if pid:
                    print(f"  ✅ 재시작됨 (PID: {pid})")
                    time.sleep(60)  # 초기화 대기
                else:
                    print("  ❌ 재시작 실패")
                continue
            
            # DB 업데이트 확인
            updates = check_updates()
            print(f"DB 업데이트: 최근 5분 {updates['last_5m']}개, 최근 10분 {updates['last_10m']}개")
            
            if updates['max_updated']:
                if updates['max_updated'].tzinfo:
                    updates['max_updated'] = updates['max_updated'].replace(tzinfo=None)
                time_diff = datetime.now() - updates['max_updated']
                print(f"마지막 업데이트: {updates['max_updated']} ({time_diff} 전)")
            
            # 진행 상황 판단
            if updates['last_5m'] > 0:
                print("✅ 재분류 정상 진행 중!")
                last_update_count = updates['last_5m']
                no_update_count = 0
            elif updates['last_10m'] > 0:
                print("⚠️ 재분류 진행 중 (느림)")
                no_update_count = 0
            else:
                no_update_count += 1
                if no_update_count >= 3:
                    print("⚠️ 3회 연속 업데이트 없음 - 프로세스 확인 필요")
                else:
                    print("⏳ 초기화 중 또는 대기 중...")
            
            print("-" * 80)
            print("다음 체크까지 60초 대기...")
            time.sleep(60)
            
    except KeyboardInterrupt:
        print("\n\n모니터링 종료")
    except Exception as e:
        print(f"\n\n오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()

