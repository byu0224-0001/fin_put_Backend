# -*- coding: utf-8 -*-
"""
세그먼트 매핑 디버깅 스크립트
"""
import sys
sys.path.insert(0, '.')

from app.services.sector_classifier import SEGMENT_TO_SECTOR_MAP, normalize_segment_name

def debug_segment_mapping():
    """세그먼트 매핑 디버깅"""
    
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
    print("세그먼트 매핑 디버깅")
    print("=" * 80)
    
    for segment in test_segments:
        print(f"\n[세그먼트: {segment}]")
        
        # 정규화
        normalized = normalize_segment_name(segment)
        print(f"  정규화 결과: '{normalized}'")
        
        # 매핑 시도 (정규화된 버전)
        matched_sector = None
        matched_keyword = None
        
        for keyword, sector in SEGMENT_TO_SECTOR_MAP.items():
            if keyword in normalized:
                matched_sector = sector
                matched_keyword = keyword
                break
        
        if matched_sector:
            print(f"  [OK] 매핑 성공: {matched_sector} (키워드: {matched_keyword})")
        else:
            # 원본으로 재시도
            segment_lower = segment.lower()
            for keyword, sector in SEGMENT_TO_SECTOR_MAP.items():
                if keyword in segment_lower:
                    matched_sector = sector
                    matched_keyword = keyword
                    break
            
            if matched_sector:
                print(f"  [OK] 매핑 성공 (원본): {matched_sector} (키워드: {matched_keyword})")
            else:
                print(f"  [X] 매핑 실패")
                # 관련 키워드 찾기
                related_keywords = []
                for keyword in SEGMENT_TO_SECTOR_MAP.keys():
                    if keyword in normalized or keyword in segment_lower:
                        related_keywords.append(keyword)
                if related_keywords:
                    print(f"    관련 키워드: {related_keywords}")

if __name__ == '__main__':
    debug_segment_mapping()

