# -*- coding: utf-8 -*-
"""
정규화 디버깅 스크립트
"""
import sys
sys.path.insert(0, '.')

from app.services.sector_classifier import normalize_segment_name, SEGMENT_TO_SECTOR_MAP

def debug_normalize():
    """정규화 디버깅"""
    
    test_segments = [
        'PX',
        'B-C',
        '석유',
        '화학',
        '제품',
        '기 타',
        '석유제품',
        '정제유'
    ]
    
    print("=" * 80)
    print("정규화 및 매핑 디버깅")
    print("=" * 80)
    
    for segment in test_segments:
        print(f"\n[세그먼트: {segment}]")
        
        # 정규화
        normalized = normalize_segment_name(segment)
        print(f"  정규화 결과: '{normalized}'")
        print(f"  정규화 길이: {len(normalized)}")
        print(f"  정규화 타입: {type(normalized)}")
        
        # 매핑 시도 (정규화된 버전)
        matched_sector = None
        matched_keyword = None
        
        print(f"  매핑 시도 (정규화된 버전):")
        for keyword, sector in SEGMENT_TO_SECTOR_MAP.items():
            if keyword in normalized:
                matched_sector = sector
                matched_keyword = keyword
                print(f"    [매칭] '{keyword}' in '{normalized}' → {sector}")
                break
        
        if matched_sector:
            print(f"  [OK] 매핑 성공: {matched_sector} (키워드: {matched_keyword})")
        else:
            # 원본으로 재시도
            segment_lower = segment.lower()
            print(f"  매핑 시도 (원본 소문자: '{segment_lower}'):")
            for keyword, sector in SEGMENT_TO_SECTOR_MAP.items():
                if keyword in segment_lower:
                    matched_sector = sector
                    matched_keyword = keyword
                    print(f"    [매칭] '{keyword}' in '{segment_lower}' → {sector}")
                    break
            
            if matched_sector:
                print(f"  [OK] 매핑 성공 (원본): {matched_sector} (키워드: {matched_keyword})")
            else:
                print(f"  [X] 매핑 실패")
                # 관련 키워드 찾기
                print(f"  관련 키워드 검색:")
                for keyword in ['석유', '정유', '정제', '화학', 'PX', 'B-C']:
                    if keyword in SEGMENT_TO_SECTOR_MAP:
                        print(f"    - '{keyword}' → {SEGMENT_TO_SECTOR_MAP[keyword]}")
                        if keyword in normalized:
                            print(f"      → '{keyword}' in '{normalized}' = True")
                        elif keyword in segment_lower:
                            print(f"      → '{keyword}' in '{segment_lower}' = True")

if __name__ == '__main__':
    debug_normalize()

