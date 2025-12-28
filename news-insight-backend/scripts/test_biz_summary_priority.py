# -*- coding: utf-8 -*-
"""
"사업의 개요" 우선 포함 로직 테스트
"""
import sys
sys.path.insert(0, '.')

from app.services.sector_classifier_ensemble import _extract_biz_summary_with_priority

def test_biz_summary_priority():
    """"사업의 개요" 우선 포함 테스트"""
    
    # 테스트 케이스 1: 지주회사 (일동홀딩스 예시)
    test_case_1 = """
일동홀딩스는 2016년 6월 24일 주주총회 승인을 받아 지주회사 체제로 전환하였습니다.
2016년 8월 1일(분할일) 인적분할 및 물적분할을 실시하여 현재는 투자, 브랜드 로열티, 경영컨설팅 수수료, 임대수익 등의 지주사업을 영위하고 있습니다.

당사의 지주사업 부문 영업수익은 총 139.5억원이며 전년동기 대비 5.4% 성장하였습니다.

주요 사업부문:
- 의약품 제조 및 판매: 88.41%
- 미용성형 의료기기 제조 판매 및 소프트웨어 개발: 4.03%
- 건강기능성 식품: 3.42%

회사는 지주회사로서 자회사들의 경영을 지원하고 있습니다.
"""
    
    result_1 = _extract_biz_summary_with_priority(test_case_1, max_chars=500)
    print("=" * 80)
    print("[테스트 1] 지주회사 (일동홀딩스)")
    print("=" * 80)
    print(f"원본 길이: {len(test_case_1)}자")
    print(f"압축 길이: {len(result_1)}자")
    print(f"\n압축 결과:\n{result_1}")
    print("\n✅ '지주회사' 키워드 포함 여부:", "지주회사" in result_1)
    print("✅ '의약품' 키워드 포함 여부:", "의약품" in result_1)
    
    # 테스트 케이스 2: 일반 제조업 (삼성물산 예시)
    test_case_2 = """
삼성물산은 건설, 상사, 리조트 등 다양한 사업을 영위하고 있습니다.

주요 사업부문:
- 건설부문: 44.3%
- 상사부문: 30.9%
- 리조트부문: 15.2%
- 기타: 9.6%

건설부문에서는 주택, 상업시설, 인프라 건설을 담당하고 있습니다.
상사부문에서는 무역, 유통 사업을 영위하고 있습니다.
리조트부문에서는 골프장, 호텔 운영을 담당하고 있습니다.

회사는 지속적인 성장을 위해 신사업 발굴에 노력하고 있습니다.
"""
    
    result_2 = _extract_biz_summary_with_priority(test_case_2, max_chars=500)
    print("\n" + "=" * 80)
    print("[테스트 2] 일반 제조업 (삼성물산)")
    print("=" * 80)
    print(f"원본 길이: {len(test_case_2)}자")
    print(f"압축 길이: {len(result_2)}자")
    print(f"\n압축 결과:\n{result_2}")
    print("\n✅ '건설부문' 키워드 포함 여부:", "건설부문" in result_2 or "건설" in result_2)
    print("✅ '매출 비중' 관련 포함 여부:", any(kw in result_2 for kw in ["44.3", "30.9", "부문"]))
    
    # 테스트 케이스 3: "사업의 개요" 키워드가 없는 경우
    test_case_3 = """
회사는 전자제품 제조 및 판매를 주요 사업으로 영위하고 있습니다.
주요 제품으로는 스마트폰, 태블릿, 노트북 등이 있습니다.
고객사는 Apple, Samsung, LG 등 글로벌 기업들입니다.
원재료는 반도체, 디스플레이, 배터리 등을 사용합니다.
"""
    
    result_3 = _extract_biz_summary_with_priority(test_case_3, max_chars=500)
    print("\n" + "=" * 80)
    print("[테스트 3] 우선 키워드 없는 경우 (Fallback)")
    print("=" * 80)
    print(f"원본 길이: {len(test_case_3)}자")
    print(f"압축 길이: {len(result_3)}자")
    print(f"\n압축 결과:\n{result_3}")
    print("\n✅ 첫 문단 포함 여부:", "전자제품" in result_3)
    
    print("\n" + "=" * 80)
    print("✅ 모든 테스트 완료!")
    print("=" * 80)


if __name__ == '__main__':
    test_biz_summary_priority()

