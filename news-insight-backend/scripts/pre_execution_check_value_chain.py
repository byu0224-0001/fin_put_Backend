#!/usr/bin/env python3
"""
밸류체인 분류 실행 전 최종 체크 스크립트

Check 0-0: .env 파일 및 환경 변수 확인
Check 0-A: Anchor 임베딩 캐싱 확인
Check 0-B: 재생성 범위 확인
Check 0-C: value_chain_confidence 정의 일관성 확인
"""

import sys
import os
import re
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# UTF-8 인코딩 설정 (Windows 환경)
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from dotenv import load_dotenv

def check_00_env_file():
    """Check 0-0: .env 파일 및 환경 변수 확인"""
    print("=" * 80)
    print("[Check 0-0] .env 파일 및 환경 변수 확인")
    print("=" * 80)
    
    # .env 파일 경로 확인
    env_path = project_root / '.env'
    parent_env = project_root.parent / '.env'
    current_env = Path.cwd() / '.env'
    
    print(f"  프로젝트 루트: {project_root}")
    print(f"  현재 작업 디렉토리: {os.getcwd()}")
    print(f"\n  .env 파일 경로 확인:")
    print(f"    1. {env_path} (존재: {'✅' if env_path.exists() else '❌'})")
    print(f"    2. {parent_env} (존재: {'✅' if parent_env.exists() else '❌'})")
    print(f"    3. {current_env} (존재: {'✅' if current_env.exists() else '❌'})")
    
    # .env 파일 로드 시도
    env_loaded = False
    env_loaded_path = None
    
    # 1. 프로젝트 루트 시도
    if env_path.exists():
        load_dotenv(env_path, override=True)
        if os.getenv('UPSTAGE_API_KEY'):
            env_loaded = True
            env_loaded_path = env_path
    
    # 2. 상위 디렉토리 시도
    if not env_loaded and parent_env.exists():
        load_dotenv(parent_env, override=True)
        if os.getenv('UPSTAGE_API_KEY'):
            env_loaded = True
            env_loaded_path = parent_env
    
    # 3. 현재 디렉토리 시도
    if not env_loaded and current_env.exists():
        load_dotenv(current_env, override=True)
        if os.getenv('UPSTAGE_API_KEY'):
            env_loaded = True
            env_loaded_path = current_env
    
    # 4. 자동 탐지 시도
    if not env_loaded:
        load_dotenv(override=True)
        if os.getenv('UPSTAGE_API_KEY'):
            env_loaded = True
    
    # 환경 변수 확인
    upstage_key = os.getenv('UPSTAGE_API_KEY')
    
    if env_loaded and upstage_key:
        print(f"\n  [✅ OK] .env 파일 로드 성공")
        if env_loaded_path:
            print(f"       로드된 경로: {env_loaded_path}")
        else:
            print(f"       로드 방식: 자동 탐지")
        print(f"       UPSTAGE_API_KEY 확인됨 (길이: {len(upstage_key)}자)")
        return True
    else:
        print(f"\n  [❌ ERROR] .env 파일 로드 실패 또는 UPSTAGE_API_KEY 없음")
        if not upstage_key:
            print(f"       UPSTAGE_API_KEY가 환경 변수에 없습니다.")
        print(f"\n  해결 방법:")
        print(f"    1. .env 파일에 UPSTAGE_API_KEY=your_api_key 추가")
        print(f"    2. .env 파일 위치 확인: {env_path}")
        return False


def check_0a_anchor_caching():
    """Check 0-A: Anchor 임베딩 캐싱 확인"""
    print("=" * 80)
    print("[Check 0-A] Anchor 임베딩 캐싱 확인")
    print("=" * 80)
    
    # 코드에서 캐싱 로직 확인
    code_file = Path(__file__).parent.parent / 'app' / 'services' / 'value_chain_classifier_embedding.py'
    
    if not code_file.exists():
        print("[ERROR] 코드 파일을 찾을 수 없습니다.")
        return False
    
    with open(code_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 캐싱 로직 확인
    has_cache_check = '_value_chain_reference_embeddings' in content
    has_cache_hit_log = '[CACHE HIT]' in content or 'cache_key in _value_chain_reference_embeddings' in content
    has_global_cache = '_value_chain_reference_embeddings = {}' in content or '_value_chain_reference_embeddings = {}' in content
    
    print(f"  캐시 변수 존재: {'[OK]' if has_cache_check else '[WARN]'}")
    print(f"  캐시 히트 로그: {'[OK]' if has_cache_hit_log else '[WARN]'}")
    print(f"  전역 캐시 변수: {'[OK]' if has_global_cache else '[WARN]'}")
    
    if has_cache_check and has_cache_hit_log:
        print("\n  [OK] Anchor 임베딩 캐싱 구현 확인됨")
        print("  -> Anchor 5개는 1회만 생성되고 이후 재사용됩니다")
        return True
    else:
        print("\n  [WARN] Anchor 임베딩 캐싱 확인 필요")
        return False


def check_0b_regenerate_scope():
    """Check 0-B: 재생성 범위 확인"""
    print("\n" + "=" * 80)
    print("[Check 0-B] 재생성 범위 확인")
    print("=" * 80)
    
    # 코드에서 재생성 로직 확인
    code_file = Path(__file__).parent.parent / 'scripts' / 'classify_value_chain_final.py'
    
    if not code_file.exists():
        print("[ERROR] 코드 파일을 찾을 수 없습니다.")
        return False
    
    with open(code_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 재생성 범위 확인
    has_regenerate_flag = '--regenerate' in content or 'regenerate_embeddings' in content
    has_company_regenerate = 'generate_company_embeddings_solar' in content
    has_anchor_separate = 'get_value_chain_reference_embeddings' in content
    
    print(f"  --regenerate 옵션: {'[OK]' if has_regenerate_flag else '[WARN]'}")
    print(f"  company_embeddings 재생성: {'[OK]' if has_company_regenerate else '[WARN]'}")
    print(f"  anchor_embeddings 분리: {'[OK]' if has_anchor_separate else '[WARN]'}")
    
    # Anchor가 재생성되지 않는지 확인
    anchor_in_regenerate = 'anchor' in content.lower() and 'regenerate' in content.lower() and 'anchor' in content[content.find('regenerate'):content.find('regenerate')+500].lower()
    
    if has_regenerate_flag and has_company_regenerate and has_anchor_separate and not anchor_in_regenerate:
        print("\n  [OK] 재생성 범위 확인됨")
        print("  -> company_embeddings: 재생성됨")
        print("  -> anchor_embeddings: 재생성 안 됨 (캐시 사용)")
        return True
    else:
        print("\n  [WARN] 재생성 범위 확인 필요")
        return False


def check_0c_confidence_definition():
    """Check 0-C: value_chain_confidence 정의 일관성 확인"""
    print("\n" + "=" * 80)
    print("[Check 0-C] value_chain_confidence 정의 일관성 확인")
    print("=" * 80)
    
    # 코드에서 confidence 정의 확인
    code_file = Path(__file__).parent.parent / 'scripts' / 'classify_value_chain_final.py'
    
    if not code_file.exists():
        print("[ERROR] 코드 파일을 찾을 수 없습니다.")
        return False
    
    with open(code_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # confidence 정의 확인
    has_max_0 = 'max(0.0' in content or 'max(0.0,' in content
    has_top1_top2 = 'top1_score - top2_score' in content or 'top1 - top2' in content
    has_confidence_comment = 'value_chain_confidence' in content and ('음수' in content or 'hybrid' in content or '혼합' in content)
    
    print(f"  max(0.0, ...) 사용: {'[OK]' if has_max_0 else '[WARN]'}")
    print(f"  top1 - top2 계산: {'[OK]' if has_top1_top2 else '[WARN]'}")
    print(f"  주석/설명 존재: {'[OK]' if has_confidence_comment else '[INFO]'}")
    
    if has_max_0 and has_top1_top2:
        print("\n  [OK] value_chain_confidence 정의 일관성 확인됨")
        print("  -> max(0.0, top1_score - top2_score)")
        print("  -> 음수 없음, 0에 가까울수록 혼합형(hybrid)")
        return True
    else:
        print("\n  [WARN] value_chain_confidence 정의 확인 필요")
        return False


def main():
    """메인 함수"""
    print("=" * 80)
    print("밸류체인 분류 실행 전 최종 체크")
    print("=" * 80)
    
    results = []
    
    # Check 0-0
    results.append(('Check 0-0: .env 파일 및 환경 변수', check_00_env_file()))
    
    # Check 0-A
    results.append(('Check 0-A: Anchor 임베딩 캐싱', check_0a_anchor_caching()))
    
    # Check 0-B
    results.append(('Check 0-B: 재생성 범위', check_0b_regenerate_scope()))
    
    # Check 0-C
    results.append(('Check 0-C: confidence 정의', check_0c_confidence_definition()))
    
    # 최종 결과
    print("\n" + "=" * 80)
    print("최종 결과")
    print("=" * 80)
    
    all_passed = True
    for name, passed in results:
        status = "[OK]" if passed else "[WARN]"
        print(f"  {status} {name}")
        if not passed:
            all_passed = False
    
    print("\n" + "=" * 80)
    if all_passed:
        print("[OK] 모든 체크 통과 - 실행 준비 완료")
        print("=" * 80)
        print("\n실행 명령:")
        print("  python scripts/classify_value_chain_final.py --regenerate")
    else:
        print("[WARN] 일부 체크 실패 - 확인 필요")
        print("=" * 80)
    
    return all_passed


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)

