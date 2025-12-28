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

def verify_gpu():
    """GPU 사용 가능 여부 확인"""
    print("=" * 80)
    print("GPU 사용 가능 여부 확인")
    print("=" * 80)
    
    try:
        import torch
        if torch.cuda.is_available():
            gpu_name = torch.cuda.get_device_name(0)
            gpu_memory = torch.cuda.get_device_properties(0).total_memory / 1024**3
            print(f"✅ GPU 사용 가능: {gpu_name}")
            print(f"   GPU 메모리: {gpu_memory:.2f} GB")
            print(f"   CUDA 버전: {torch.version.cuda}")
            print(f"   PyTorch 버전: {torch.__version__}")
            return True
        else:
            print("❌ GPU 사용 불가 (CUDA 없음)")
            print(f"   PyTorch 버전: {torch.__version__}")
            return False
    except Exception as e:
        print(f"❌ GPU 확인 실패: {e}")
        return False

def kill_all_python():
    """모든 Python 프로세스 종료"""
    print()
    print("=" * 80)
    print("Python 프로세스 종료")
    print("=" * 80)
    
    killed = 0
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if proc.info['name'] and 'python' in proc.info['name'].lower():
                cmdline = proc.info['cmdline']
                if cmdline:
                    cmdline_str = ' '.join(cmdline)
                    if 'reclassify' in cmdline_str.lower() or 'python' in cmdline_str.lower():
                        print(f"  종료: PID {proc.info['pid']}")
                        p = psutil.Process(proc.info['pid'])
                        p.terminate()
                        try:
                            p.wait(timeout=5)
                        except psutil.TimeoutExpired:
                            p.kill()
                        killed += 1
        except:
            continue
    
    print(f"✅ {killed}개 프로세스 종료")
    time.sleep(3)
    print()

def start_gpu_reclassification():
    """GPU 환경에서 재분류 시작"""
    print("=" * 80)
    print("GPU 환경에서 재분류 시작")
    print("=" * 80)
    
    script = project_root / "scripts" / "reclassify_all_sectors_ensemble_optimized.py"
    
    if not script.exists():
        print(f"❌ 스크립트를 찾을 수 없습니다: {script}")
        return False
    
    # GPU 환경 변수 설정
    env = os.environ.copy()
    env['CUDA_VISIBLE_DEVICES'] = '0'
    env['PYTHONUNBUFFERED'] = '1'
    
    try:
        process = subprocess.Popen(
            [sys.executable, str(script)],
            cwd=project_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env=env
        )
        
        print(f"✅ 재분류 프로세스 시작됨 (PID: {process.pid})")
        print(f"   GPU 환경: CUDA_VISIBLE_DEVICES=0")
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
    print("=" * 80)
    print("GPU 환경 재분류 재시작")
    print("=" * 80)
    print()
    
    # 1. GPU 확인
    if not verify_gpu():
        print()
        print("⚠️ GPU를 사용할 수 없습니다.")
        print("   PyTorch CUDA 버전이 설치되지 않았을 수 있습니다.")
        print("   다음 명령어로 설치하세요:")
        print("   pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu127")
        return
    
    print()
    
    # 2. 프로세스 종료
    kill_all_python()
    
    # 3. 재시작
    if start_gpu_reclassification():
        print()
        print("=" * 80)
        print("✅ GPU 환경에서 재분류 재시작 완료")
        print("=" * 80)
        print()
        print("몇 분 후 다음 명령어로 상태를 확인하세요:")
        print("  python scripts/simple_monitor.py")
    else:
        print()
        print("=" * 80)
        print("❌ 재시작 실패")
        print("=" * 80)
    
    print()

if __name__ == '__main__':
    main()

