"""
P0 개선 사항 통합 테스트

테스트 항목:
1. kiwipiepy 문장 분리
2. 메타데이터 제거
3. insight_extractor 프롬프트
4. 문장 단위 트렁케이션
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')


def test_kiwipiepy():
    """kiwipiepy 문장 분리 테스트"""
    print("=" * 60)
    print("[1] kiwipiepy 문장 분리 테스트")
    print("=" * 60)
    
    from app.utils.sentence_split import split_sentences
    
    test_text = "AI 데이터센터 수요가 급증하고 있다. 반도체 업종의 실적 개선이 기대된다. HBM 가격은 전분기 대비 15% 상승했다."
    
    sentences = split_sentences(test_text)
    print(f"입력: {test_text}")
    print(f"문장 수: {len(sentences)}")
    for i, s in enumerate(sentences, 1):
        print(f"  {i}. {s}")
    
    return len(sentences) == 3


def test_metadata_cleaning():
    """메타데이터 제거 테스트"""
    print("\n" + "=" * 60)
    print("[2] 메타데이터 제거 테스트")
    print("=" * 60)
    
    from extractors.insight_extractor import clean_metadata, detect_metadata_leak
    
    # 메타데이터가 포함된 텍스트
    dirty_text = """
▶Analyst 김철수 analyst@hanwha.com
02-1234-5678
AI 반도체 시장이 급성장하고 있다.
Compliance Notice
본 자료는 투자 판단의 참고자료입니다.
"""
    
    cleaned = clean_metadata(dirty_text)
    
    print(f"원본 길이: {len(dirty_text)}자")
    print(f"정제 후 길이: {len(cleaned)}자")
    print(f"정제 결과: {cleaned[:100]}...")
    
    # 오염 감지 테스트
    leak1 = detect_metadata_leak("2025.12.24 Analyst@test.com")
    leak2 = detect_metadata_leak("반도체 업종 실적 개선 전망")
    
    print(f"\n오염 감지 테스트:")
    print(f"  '2025.12.24 Analyst@test.com' → 오염: {leak1}")
    print(f"  '반도체 업종 실적 개선 전망' → 오염: {leak2}")
    
    return leak1 == True and leak2 == False


def test_truncate_to_sentence():
    """문장 단위 트렁케이션 테스트"""
    print("\n" + "=" * 60)
    print("[3] 문장 단위 트렁케이션 테스트")
    print("=" * 60)
    
    from extractors.industry_insight_router import truncate_to_sentence, MAX_LOGIC_SUMMARY_LEN
    
    long_text = "AI 반도체 시장이 급성장하고 있다. 데이터센터 수요 증가로 HBM 가격이 상승했다. 2025년에도 성장세가 지속될 전망이다. 주요 기업들의 실적 개선이 기대된다."
    
    truncated = truncate_to_sentence(long_text, MAX_LOGIC_SUMMARY_LEN)
    
    print(f"최대 길이 설정: {MAX_LOGIC_SUMMARY_LEN}자")
    print(f"원본: {long_text}")
    print(f"원본 길이: {len(long_text)}자")
    print(f"잘린 결과: {truncated}")
    print(f"잘린 길이: {len(truncated)}자")
    
    # 260자 하드코딩 확인
    is_not_260 = len(truncated) != 260
    is_within_limit = len(truncated) <= MAX_LOGIC_SUMMARY_LEN
    
    print(f"\n검증:")
    print(f"  260자 아님: {is_not_260}")
    print(f"  최대 길이 이내: {is_within_limit}")
    
    return is_not_260 and is_within_limit


def test_insight_extraction_sample():
    """insight_extractor 샘플 테스트 (LLM 호출)"""
    print("\n" + "=" * 60)
    print("[4] insight_extractor 샘플 테스트 (LLM 호출)")
    print("=" * 60)
    
    from extractors.insight_extractor import extract_insights
    
    sample_text = """
유통 (Positive)
11월주요유통업체매출동향–왜대형마트는 부진했을까?

11월 Review - 엇갈린 백화점과 대형마트
백화점과 대형마트 모두 호조를 보인 10월과는 달리 11월의 지표는 엇갈리는 모습이다.
백화점은 전월과 유사하게 +12% YoY 성장한 반면, 대형마트의 매출은 -9% YoY 감소했다.

당사는 전년 대비 늦은 추석에 따라 늦어진 대형마트 매출 휴지기의 영향과 함께 
외부 환경의 영향이 아직까지도 대형마트 업황에 부정적으로 작용했을 것으로 추정한다.

소비쿠폰과 상생페이백 정책이 대표적이겠다. 소비쿠폰의 사용 기한은 11월까지였기 때문에 
잔여 소비쿠폰이 11월에 집중된 것으로 판단된다.
"""
    
    print(f"샘플 텍스트 길이: {len(sample_text)}자")
    print("LLM 호출 중...")
    
    try:
        result = extract_insights(sample_text, report_title="유통업체 매출 동향")
        
        print(f"\n추출 결과:")
        print(f"  analyst_logic: {result.get('analyst_logic', '')[:150]}...")
        print(f"  key_sentence: {result.get('key_sentence', '')}")
        print(f"  confidence: {result.get('extraction_confidence', 'N/A')}")
        print(f"  meta_leak: {result.get('meta_leak_detected', False)}")
        
        # 메타데이터 오염 여부 확인
        no_leak = result.get('meta_leak_detected', False) == False
        has_logic = len(result.get('analyst_logic', '')) > 20
        
        print(f"\n검증:")
        print(f"  메타데이터 오염 없음: {no_leak}")
        print(f"  논리 추출됨: {has_logic}")
        
        return no_leak and has_logic
        
    except Exception as e:
        print(f"오류: {e}")
        return False


def main():
    print("=" * 60)
    print("P0 개선 사항 통합 테스트")
    print("=" * 60)
    
    results = {}
    
    # 1. kiwipiepy 테스트
    results["kiwipiepy"] = test_kiwipiepy()
    
    # 2. 메타데이터 제거 테스트
    results["metadata_cleaning"] = test_metadata_cleaning()
    
    # 3. 트렁케이션 테스트
    results["truncation"] = test_truncate_to_sentence()
    
    # 4. insight_extractor 테스트 (LLM 호출 포함)
    print("\n[4번 테스트는 LLM 호출이 필요합니다. 스킵하려면 Ctrl+C]")
    try:
        results["insight_extraction"] = test_insight_extraction_sample()
    except KeyboardInterrupt:
        print("스킵됨")
        results["insight_extraction"] = None
    
    # 결과 요약
    print("\n" + "=" * 60)
    print("[테스트 결과 요약]")
    print("=" * 60)
    
    for name, passed in results.items():
        if passed is None:
            status = "⏭️ 스킵"
        elif passed:
            status = "✅ 통과"
        else:
            status = "❌ 실패"
        print(f"  {name}: {status}")
    
    # 전체 통과 여부
    all_passed = all(v == True for v in results.values() if v is not None)
    print(f"\n전체 결과: {'✅ 모든 테스트 통과' if all_passed else '⚠️ 일부 테스트 실패'}")


if __name__ == "__main__":
    main()

