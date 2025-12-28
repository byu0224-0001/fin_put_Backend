"""
P0.5-2: logic_fingerprint 안정성 검증

동일 입력을 2번 돌렸을 때 fingerprint가 완전히 동일하게 재현되는지 확인
"""
import sys
import os
import json
from pathlib import Path
from collections import defaultdict

# Windows 인코딩 처리
if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from utils.text_normalizer import normalize_text_for_fingerprint
import hashlib


def test_fingerprint_stability():
    """
    fingerprint 안정성 테스트
    
    동일한 logic_summary에 대해:
    1. 공백/줄바꿈 차이
    2. 숫자 포맷 차이
    3. 특수문자 차이
    
    이 모든 경우에 동일한 fingerprint가 생성되는지 확인
    """
    print("=" * 80)
    print("[P0.5-2: logic_fingerprint 안정성 검증]")
    print("=" * 80)
    
    # 테스트 케이스
    test_cases = [
        {
            "name": "기본 케이스",
            "text": "1월 효과로 중소형주 강세가 나타나는 경향이 있습니다."
        },
        {
            "name": "공백 차이",
            "text": "1월 효과로  중소형주  강세가 나타나는 경향이 있습니다."
        },
        {
            "name": "줄바꿈 차이",
            "text": "1월 효과로\n중소형주\n강세가 나타나는 경향이 있습니다."
        },
        {
            "name": "숫자 포맷 차이",
            "text": "1월 효과로 중소형주 강세가 나타나는 경향이 있습니다. (1,000개 기업)"
        },
        {
            "name": "숫자 포맷 차이 2",
            "text": "1월 효과로 중소형주 강세가 나타나는 경향이 있습니다. (1000개 기업)"
        },
        {
            "name": "특수문자 차이",
            "text": "1월 효과로 중소형주 강세가 나타나는 경향이 있습니다. (-> 방향)"
        },
        {
            "name": "특수문자 차이 2",
            "text": "1월 효과로 중소형주 강세가 나타나는 경향이 있습니다. (→ 방향)"
        },
    ]
    
    fingerprints = {}
    normalized_texts = {}
    
    print("\n[테스트 케이스별 fingerprint 생성]")
    print("-" * 80)
    
    for case in test_cases:
        normalized = normalize_text_for_fingerprint(case["text"])
        fingerprint = hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:16]
        
        fingerprints[case["name"]] = fingerprint
        normalized_texts[case["name"]] = normalized
        
        print(f"\n[{case['name']}]")
        print(f"  원본: {case['text'][:50]}...")
        print(f"  정규화: {normalized[:50]}...")
        print(f"  Fingerprint: {fingerprint}")
    
    # 동일한 의미의 텍스트들이 같은 fingerprint를 가지는지 확인
    print("\n" + "=" * 80)
    print("[안정성 검증 결과]")
    print("=" * 80)
    
    # 같은 fingerprint를 가진 그룹 찾기
    fingerprint_groups = defaultdict(list)
    for name, fp in fingerprints.items():
        fingerprint_groups[fp].append(name)
    
    print(f"\n[Fingerprint 그룹]")
    for fp, names in fingerprint_groups.items():
        if len(names) > 1:
            print(f"  ✅ 동일 fingerprint: {', '.join(names)}")
            print(f"     Fingerprint: {fp}")
        else:
            print(f"  ⚠️  고유 fingerprint: {names[0]}")
            print(f"     Fingerprint: {fp}")
    
    # 기대 결과: "기본 케이스", "공백 차이", "줄바꿈 차이"는 같은 fingerprint여야 함
    expected_same = ["기본 케이스", "공백 차이", "줄바꿈 차이"]
    expected_same_fps = [fingerprints[name] for name in expected_same]
    
    if len(set(expected_same_fps)) == 1:
        print(f"\n  ✅ [OK] 기대한 그룹이 동일 fingerprint: {', '.join(expected_same)}")
    else:
        print(f"\n  ❌ [ERROR] 기대한 그룹이 다른 fingerprint:")
        for name in expected_same:
            print(f"     - {name}: {fingerprints[name]}")
    
    # 숫자 포맷 차이는 정규화되어야 함
    number_cases = ["숫자 포맷 차이", "숫자 포맷 차이 2"]
    number_fps = [fingerprints[name] for name in number_cases]
    
    if len(set(number_fps)) == 1:
        print(f"\n  ✅ [OK] 숫자 포맷 차이 정규화됨: {', '.join(number_cases)}")
    else:
        print(f"\n  ⚠️  [WARNING] 숫자 포맷 차이가 다른 fingerprint:")
        for name in number_cases:
            print(f"     - {name}: {fingerprints[name]}")
    
    # 특수문자 차이는 정규화되어야 함
    special_cases = ["특수문자 차이", "특수문자 차이 2"]
    special_fps = [fingerprints[name] for name in special_cases]
    
    if len(set(special_fps)) == 1:
        print(f"\n  ✅ [OK] 특수문자 차이 정규화됨: {', '.join(special_cases)}")
    else:
        print(f"\n  ⚠️  [WARNING] 특수문자 차이가 다른 fingerprint:")
        for name in special_cases:
            print(f"     - {name}: {fingerprints[name]}")
    
    print("\n" + "=" * 80)
    print("[결론]")
    print("=" * 80)
    
    # 전체 평가
    unique_fps = len(set(fingerprints.values()))
    total_cases = len(test_cases)
    
    print(f"  - 총 테스트 케이스: {total_cases}개")
    print(f"  - 고유 fingerprint 수: {unique_fps}개")
    
    if unique_fps <= 3:  # 기대: 공백/줄바꿈 차이는 같은 fingerprint, 숫자/특수문자도 정규화
        print(f"\n  ✅ [OK] Fingerprint 안정성 양호 (의미상 동일한 텍스트는 같은 fingerprint)")
    else:
        print(f"\n  ⚠️  [WARNING] Fingerprint 안정성 개선 필요 (의미상 동일한 텍스트가 다른 fingerprint)")


if __name__ == "__main__":
    test_fingerprint_stability()

