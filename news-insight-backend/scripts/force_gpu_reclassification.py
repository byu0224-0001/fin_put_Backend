"""GPU 환경에서 재분류 강제 재시작"""
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

def kill_all_python_processes():
    """모든 Python 프로세스 종료 (재분류 포함)"""
    print("=" * 80)
    print("모든 Python 프로세스 종료")
    print("=" * 80)
    
    killed_count = 0
    
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] and 'python' in proc.info['name'].lower():
                cmdline = proc.info['cmdline']
                if cmdline:
                    cmdline_str = ' '.join(cmdline)
                    # 재분류 프로세스 우선 종료
                    if 'reclassify' in cmdline_str.lower():
                        print(f"  재분류 프로세스 종료: PID {proc.info['pid']}")
                    else:
                        print(f"  Python 프로세스 종료: PID {proc.info['pid']} - {cmdline_str[:60]}")
                    
                    try:
                        proc.terminate()
                        proc.wait(timeout=5)
                    except psutil.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=2)
                    killed_count += 1
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    
    if killed_count > 0:
        print(f"✅ {killed_count}개 프로세스 종료 완료")
        time.sleep(3)
    else:
        print("⚠️ 종료할 프로세스 없음")
    
    print()
    return killed_count

def check_gpu():
    """GPU 확인"""
    try:
        import torch
        if torch.cuda.is_available():
            gpu_name = torch.cuda.get_device_name(0)
            gpu_memory = torch.cuda.get_device_properties(0).total_memory / 1024**3
            print(f"✅ GPU 사용 가능: {gpu_name} ({gpu_memory:.2f} GB)")
            return True
        else:
            print("⚠️ GPU 사용 불가 (CUDA 없음) - CPU로 진행")
            return False
    except Exception as e:
        print(f"⚠️ GPU 확인 실패: {e} - CPU로 진행")
        return False

def start_reclassification():
    """재분류 시작 (GPU 우선)"""
    print("=" * 80)
    print("재분류 시작 (GPU 우선)")
    print("=" * 80)
    
    script_path = project_root / "scripts" / "reclassify_all_sectors_ensemble_optimized.py"
    
    if not script_path.exists():
        print(f"❌ 스크립트를 찾을 수 없습니다: {script_path}")
        return False
    
    try:
        # GPU 환경 변수 설정
        env = os.environ.copy()
        env['CUDA_VISIBLE_DEVICES'] = '0'
        env['PYTHONUNBUFFERED'] = '1'
        
        # 백그라운드에서 실행
        process = subprocess.Popen(
            [sys.executable, str(script_path)],
            cwd=project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env
        )
        
        print(f"✅ 재분류 프로세스 시작됨 (PID: {process.pid})")
        print(f"   GPU 환경 변수: CUDA_VISIBLE_DEVICES=0")
        print()
        print("진행 상황 확인:")
        print("  python scripts/simple_monitor.py")
        
        return True
    except Exception as e:
        print(f"❌ 재분류 시작 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """메인 실행 함수"""
    print("=" * 80)
    print("GPU 환경에서 재분류 강제 재시작")
    print("=" * 80)
    print()
    
    # 1. GPU 확인
    check_gpu()
    print()
    
    # 2. 모든 Python 프로세스 종료
    kill_all_python_processes()
    
    # 3. 재시작
    if start_reclassification():
        print()
        print("=" * 80)
        print("✅ 재시작 완료")
        print("=" * 80)
        print()
        print("몇 분 후 다음 명령어로 상태를 확인하세요:")
        print("  python scripts/simple_monitor.py")
    else:
        print()
        print("=" * 80)
        print("❌ 재시작 실패")
        print("=" * 80)

if __name__ == '__main__':
    main()

