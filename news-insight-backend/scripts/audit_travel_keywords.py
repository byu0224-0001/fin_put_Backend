# -*- coding: utf-8 -*-
"""
SEC_TRAVEL 키워드 전수 조사 스크립트 (P0 피드백 반영)
- SEC_TRAVEL로 매핑되는 모든 키워드 출력
- 키워드 길이 분포 확인
- 1~2자 키워드 존재 여부 확인
"""
import sys
sys.path.insert(0, '.')

from app.services.sector_classifier import SEGMENT_TO_SECTOR_MAP

def audit_travel_keywords():
    """
    SEC_TRAVEL 관련 키워드 전수 조사
    """
    print("=" * 80)
    print("SEC_TRAVEL 키워드 전수 조사")
    print("=" * 80)
    
    # SEC_TRAVEL로 매핑되는 모든 키워드 수집
    travel_keywords = []
    for keyword, sector in SEGMENT_TO_SECTOR_MAP.items():
        if sector == 'SEC_TRAVEL':
            travel_keywords.append(keyword)
    
    print(f"\n[SEC_TRAVEL 키워드 목록]")
    print(f"  총 키워드 수: {len(travel_keywords)}개")
    
    # 길이별 분류
    length_groups = {}
    for keyword in travel_keywords:
        length = len(keyword)
        if length not in length_groups:
            length_groups[length] = []
        length_groups[length].append(keyword)
    
    print(f"\n[키워드 길이 분포]")
    for length in sorted(length_groups.keys()):
        keywords = length_groups[length]
        print(f"  {length}자: {len(keywords)}개")
        for kw in sorted(keywords):
            print(f"    - '{kw}'")
    
    # 1~2자 키워드 확인 (피드백: len<3이면 contains 금지)
    short_keywords = [kw for kw in travel_keywords if len(kw) < 3]
    if short_keywords:
        print(f"\n[경고] [경고] 1~2자 키워드 발견 (contains 매칭 시 오탐 위험):")
        for kw in short_keywords:
            print(f"  - '{kw}' ({len(kw)}자)")
        print(f"\n  → 이 키워드들은 exact match만 허용되어야 함")
    else:
        print(f"\n[OK] 1~2자 키워드 없음 (안전)")
    
    # SEC_ENT도 함께 확인
    print(f"\n" + "=" * 80)
    print("SEC_ENT 키워드 전수 조사")
    print("=" * 80)
    
    ent_keywords = []
    for keyword, sector in SEGMENT_TO_SECTOR_MAP.items():
        if sector == 'SEC_ENT':
            ent_keywords.append(keyword)
    
    print(f"\n[SEC_ENT 키워드 목록]")
    print(f"  총 키워드 수: {len(ent_keywords)}개")
    
    # 길이별 분류
    ent_length_groups = {}
    for keyword in ent_keywords:
        length = len(keyword)
        if length not in ent_length_groups:
            ent_length_groups[length] = []
        ent_length_groups[length].append(keyword)
    
    print(f"\n[키워드 길이 분포]")
    for length in sorted(ent_length_groups.keys()):
        keywords = ent_length_groups[length]
        print(f"  {length}자: {len(keywords)}개")
        for kw in sorted(keywords):
            print(f"    - '{kw}'")
    
    # 1~2자 키워드 확인
    ent_short_keywords = [kw for kw in ent_keywords if len(kw) < 3]
    if ent_short_keywords:
        print(f"\n[경고] [경고] 1~2자 키워드 발견 (contains 매칭 시 오탐 위험):")
        for kw in ent_short_keywords:
            print(f"  - '{kw}' ({len(kw)}자)")
        print(f"\n  → 이 키워드들은 exact match만 허용되어야 함")
    else:
        print(f"\n[OK] 1~2자 키워드 없음 (안전)")
    
    # 전체 통계
    print(f"\n" + "=" * 80)
    print("전체 통계")
    print("=" * 80)
    
    all_keywords = list(SEGMENT_TO_SECTOR_MAP.keys())
    all_short = [kw for kw in all_keywords if len(kw) < 3]
    
    print(f"\n전체 키워드 수: {len(all_keywords)}개")
    print(f"1~2자 키워드 수: {len(all_short)}개")
    
    if all_short:
        print(f"\n1~2자 키워드 목록:")
        for kw in sorted(all_short):
            sector = SEGMENT_TO_SECTOR_MAP[kw]
            print(f"  - '{kw}' ({len(kw)}자) → {sector}")


if __name__ == '__main__':
    audit_travel_keywords()

