"""GPU 환경에서 재분류 최종 재시작"""
import os
import sys
import subprocess
import psutil
import time
from pathlib import Path

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

def kill_all_python():
    """모든 Python 프로세스 종료"""
    print("=" * 80)
    print("모든 Python 프로세스 종료")
    print("=" * 80)
    
    killed = 0
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] and 'python' in proc.info['name'].lower():
                cmdline = proc.info['cmdline']
                if cmdline:
                    print(f"  종료: PID {proc.info['pid']}")
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except psutil.TimeoutExpired:
                        proc.kill()
                    killed += 1
        except:
            continue
    
    print(f"✅ {killed}개 프로세스 종료")
    time.sleep(3)
    print()

def start_reclassification():
    """재분류 시작"""
    print("=" * 80)
    print("GPU 환경에서 재분류 시작")
    print("=" * 80)
    
    script = project_root / "scripts" / "reclassify_all_sectors_ensemble_optimized.py"
    
    env = os.environ.copy()
    env['CUDA_VISIBLE_DEVICES'] = '0'
    env['PYTHONUNBUFFERED'] = '1'
    
    process = subprocess.Popen(
        [sys.executable, str(script)],
        cwd=project_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env
    )
    
    print(f"✅ 재분류 시작 (PID: {process.pid})")
    print(f"   GPU 환경: CUDA_VISIBLE_DEVICES=0")
    print()
    print("진행 상황:")
    print("  python scripts/simple_monitor.py")
    return True

def main():
    print("=" * 80)
    print("GPU 환경 재분류 재시작")
    print("=" * 80)
    print()
    
    kill_all_python()
    start_reclassification()
    
    print()
    print("=" * 80)
    print("✅ 완료")
    print("=" * 80)

if __name__ == '__main__':
    main()

