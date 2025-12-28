"""
Driver 추출 로직 단위 테스트

의심스러운 _extract_unknown_economic_terms 함수를 직접 테스트
"""
import sys
from pathlib import Path
import logging

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from extractors.driver_normalizer import _extract_unknown_economic_terms, _normalize_candidate_term

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_driver_extraction():
    """Driver 추출 단위 테스트"""
    print("=" * 80)
    print("[Driver 추출 단위 테스트]")
    print("=" * 80)
    
    test_cases = [
        {
            "name": "D램 가격 상승",
            "text": "D램 가격이 상승하면서 반도체 업종에 긍정적 영향을 미칠 것으로 예상됩니다."
        },
        {
            "name": "금리 인하",
            "text": "금리 인하 정책이 발표되면서 시장에 유동성이 증가할 것으로 전망됩니다."
        },
        {
            "name": "AI 반도체 수요 급증",
            "text": "AI 반도체 수요가 급증하면서 HBM, NPU 등 신기술에 대한 관심이 높아지고 있습니다."
        },
        {
            "name": "정책/규제",
            "text": "Clarity 법안 통과 시 가상자산 시장에 미칠 영향이 주목받고 있습니다. SEC 및 CFTC 관할권이 명확해질 것으로 예상됩니다."
        },
        {
            "name": "산업 신조어",
            "text": "EUV 공정 기술과 OPEC+ 감산 결정이 에너지 시장에 영향을 미칠 것으로 보입니다."
        },
        {
            "name": "복합 경제 변수",
            "text": "물가 상승률(CPI)이 3%를 넘어서면서 금리 인상 압력이 커지고 있습니다. 환율 변동성도 증가하고 있어 수출 기업에 영향을 줄 것으로 예상됩니다."
        },
        {
            "name": "짧은 텍스트",
            "text": "금리 상승"
        },
        {
            "name": "긴 텍스트",
            "text": """
            최근 글로벌 반도체 시장에서 D램 가격이 상승세를 보이고 있습니다. 
            이는 AI 반도체 수요 급증과 HBM 메모리 기술 발전에 따른 것으로 분석됩니다.
            또한 OPEC+의 감산 결정으로 인해 원유 가격이 상승하면서 에너지 비용이 증가하고 있습니다.
            이러한 상황에서 중앙은행의 금리 정책이 시장에 미치는 영향이 주목받고 있습니다.
            특히 CPI 물가 상승률이 3%를 넘어서면서 금리 인상 압력이 커지고 있습니다.
            """
        }
    ]
    
    print("\n[테스트 케이스별 결과]\n")
    
    total_extracted = 0
    total_cases = len(test_cases)
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"[{i}/{total_cases}] {test_case['name']}")
        print(f"  입력 텍스트: {test_case['text'][:100]}...")
        
        # 추출 실행
        extracted_terms = _extract_unknown_economic_terms(test_case['text'])
        
        print(f"  추출된 후보: {len(extracted_terms)}개")
        if extracted_terms:
            total_extracted += 1
            for j, term in enumerate(extracted_terms[:5], 1):  # 최대 5개만 출력
                normalized = _normalize_candidate_term(term)
                print(f"    {j}. '{term}' (정규화: '{normalized}')")
            if len(extracted_terms) > 5:
                print(f"    ... 외 {len(extracted_terms) - 5}개")
        else:
            print("    [FAIL] 후보 추출 실패")
        
        print()
    
    print("=" * 80)
    print(f"[최종 결과]")
    print(f"  총 테스트 케이스: {total_cases}개")
    print(f"  후보 추출 성공: {total_extracted}개 ({total_extracted/total_cases*100:.1f}%)")
    print(f"  후보 추출 실패: {total_cases - total_extracted}개")
    print("=" * 80)
    
    if total_extracted == 0:
        print("\n[WARNING] 모든 테스트 케이스에서 후보 추출 실패")
        print("  -> _extract_unknown_economic_terms 함수의 패턴이 너무 보수적일 가능성")
        print("  -> economic_keywords 패턴 확장 필요")
    elif total_extracted < total_cases * 0.5:
        print("\n[WARNING] 후보 추출 성공률이 50% 미만")
        print("  -> 패턴 확장 또는 입력 텍스트 품질 개선 필요")
    else:
        print("\n[SUCCESS] 후보 추출 로직이 정상 작동 중")


if __name__ == "__main__":
    test_driver_extraction()

