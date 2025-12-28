# -*- coding: utf-8 -*-
"""
현재 섹터 분류 구조를 한눈에 보기 쉽게 출력
"""
import sys
sys.path.insert(0, '.')
sys.stdout.reconfigure(encoding='utf-8')

from app.services.sector_classifier import SECTOR_KEYWORDS

def main():
    print("=" * 100)
    print("현재 섹터 분류 구조 전체")
    print("=" * 100)
    print(f"\n총 {len(SECTOR_KEYWORDS)}개 주요 섹터\n")
    
    for idx, (sector_code, sector_info) in enumerate(sorted(SECTOR_KEYWORDS.items()), 1):
        print(f"[{idx}] {sector_code}")
        print("-" * 100)
        print(f"키워드: {', '.join(sector_info['keywords'][:10])}{' ...' if len(sector_info['keywords']) > 10 else ''}")
        print(f"제품: {', '.join(sector_info['products'][:5])}{' ...' if len(sector_info['products']) > 5 else ''}")
        print(f"\n세부 섹터 ({len(sector_info['sub_sectors'])}개):")
        for sub_code, sub_keywords in sector_info['sub_sectors'].items():
            print(f"  - {sub_code}: {', '.join(sub_keywords[:5])}{' ...' if len(sub_keywords) > 5 else ''}")
        print()
    
    print("=" * 100)
    print("누락 기업 섹터 매핑 제안")
    print("=" * 100)
    
    missing_mapping = {
        '002200 한국수출포장': 'SEC_MATERIALS / PAPER (신규 섹터)',
        '003830 대한화섬': 'SEC_MATERIALS / CHEMICAL_FIBER 또는 SEC_CHEMICAL 확장',
        '010470 오리콤': 'SEC_ELECTRONICS 확장 (통신장비 키워드)',
        '014160 대영포장': 'SEC_MATERIALS / PAPER (신규 섹터)',
        '016590 신대양제지': 'SEC_MATERIALS / PAPER (신규 섹터)',
        '017650 대림제지': 'SEC_MATERIALS / PAPER (신규 섹터)',
        '026040 제이에스티나': 'SEC_DISCRETIONARY / JEWELRY (신규 세부 섹터)',
        '037230 한국팩키지': 'SEC_MATERIALS / PACKAGING (신규 섹터)',
        '068930 디지털대성': 'SEC_IT (키워드 있음, 로직 점검 필요)',
        '102370 케이옥션': 'SEC_SERVICES / AUCTION (신규 섹터)',
        '307870 비투엔': 'SEC_IT (키워드 있음, 로직 점검 필요)'
    }
    
    print("\n누락 기업 11개 섹터 매핑 제안:\n")
    for company, suggestion in missing_mapping.items():
        print(f"  {company:30} → {suggestion}")
    
    print("\n" + "=" * 100)

if __name__ == "__main__":
    main()

