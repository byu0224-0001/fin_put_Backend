"""
GPT-5-mini 실제 호출 안정성 체크 (Phase 2.0 P0)

목적: gpt-5-mini 모델이 실제로 안정적으로 작동하는지 확인

체크 항목:
1. 모델 접근 권한/리전/프로젝트 설정
2. 응답 포맷 변화 (JSON-only 강제 시)
3. 속도/비용/레이트리밋
4. JSON 파싱 실패율

DoD: 리포트 3개 연속으로 ticker_matcher + driver_normalizer 실제 호출 성공

⚠️ 실패 시 행동 정의:
- LLM 호출 실패 → 해당 리포트 skip + unmatched_logs 기록
- JSON 파싱 실패 → 해당 리포트 skip + unmatched_logs 기록
- 전체 파이프라인 중단하지 않음 (다른 리포트는 계속 처리)
- 실패한 리포트는 나중에 재처리 가능하도록 로그 보존
"""
import sys
from pathlib import Path
from typing import Dict, Any, List
import json
import time
import logging

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from extractors.ticker_matcher import extract_company_name_with_llm
from extractors.driver_normalizer import normalize_driver_with_llm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_gpt5_mini_ticker_extraction():
    """Ticker Matcher의 LLM 호출 테스트"""
    test_cases = [
        {
            "title": "삼성전자 투자포인트",
            "text_head": "삼성전자는 반도체 및 IT 기기 제조 기업입니다."
        },
        {
            "title": "S-Oil 분석 리포트",
            "text_head": "S-Oil은 정유 및 석유화학 사업을 영위합니다."
        },
        {
            "title": "SK하이닉스 전망",
            "text_head": "SK하이닉스는 메모리 반도체 제조 기업입니다."
        }
    ]
    
    results = []
    
    for i, test_case in enumerate(test_cases, 1):
        logger.info(f"[테스트 {i}/3] Ticker Matcher LLM 호출: {test_case['title']}")
        
        try:
            start_time = time.time()
            company_name = extract_company_name_with_llm(
                title=test_case["title"],
                text_head=test_case["text_head"]
            )
            elapsed_time = time.time() - start_time
            
            results.append({
                "test_case": test_case["title"],
                "success": company_name is not None,
                "result": company_name,
                "elapsed_time": elapsed_time,
                "error": None
            })
            
            logger.info(f"✅ 성공: {company_name} (소요 시간: {elapsed_time:.2f}초)")
            
        except Exception as e:
            results.append({
                "test_case": test_case["title"],
                "success": False,
                "result": None,
                "elapsed_time": 0,
                "error": str(e)
            })
            logger.error(f"❌ 실패: {e}")
        
        # Rate limit 방지 (1초 대기)
        if i < len(test_cases):
            time.sleep(1)
    
    return results


def test_gpt5_mini_driver_normalization():
    """Driver Normalizer의 LLM 호출 테스트"""
    test_cases = [
        "정제마진 회복이 핵심",
        "크랙스프레드 개선 전망",
        "D램 가격 상승"
    ]
    
    results = []
    
    for i, test_text in enumerate(test_cases, 1):
        logger.info(f"[테스트 {i}/3] Driver Normalizer LLM 호출: {test_text}")
        
        try:
            start_time = time.time()
            candidates = normalize_driver_with_llm(test_text)
            elapsed_time = time.time() - start_time
            
            # JSON 파싱 성공 여부 확인
            json_valid = isinstance(candidates, list) and len(candidates) > 0
            
            results.append({
                "test_case": test_text,
                "success": json_valid,
                "result": candidates,
                "elapsed_time": elapsed_time,
                "error": None
            })
            
            logger.info(f"✅ 성공: {len(candidates)}개 후보 (소요 시간: {elapsed_time:.2f}초)")
            
        except Exception as e:
            results.append({
                "test_case": test_text,
                "success": False,
                "result": None,
                "elapsed_time": 0,
                "error": str(e)
            })
            logger.error(f"❌ 실패: {e}")
        
        # Rate limit 방지 (1초 대기)
        if i < len(test_cases):
            time.sleep(1)
    
    return results


def run_stability_test():
    """전체 안정성 테스트 실행"""
    print("=" * 60)
    print("GPT-5-mini 실제 호출 안정성 체크")
    print("=" * 60)
    
    # 테스트 1: Ticker Matcher
    print("\n[테스트 1] Ticker Matcher LLM 호출 (3회 연속)")
    ticker_results = test_gpt5_mini_ticker_extraction()
    
    # 테스트 2: Driver Normalizer
    print("\n[테스트 2] Driver Normalizer LLM 호출 (3회 연속)")
    driver_results = test_gpt5_mini_driver_normalization()
    
    # 결과 요약
    print("\n" + "=" * 60)
    print("테스트 결과 요약")
    print("=" * 60)
    
    ticker_success = sum(1 for r in ticker_results if r["success"])
    driver_success = sum(1 for r in driver_results if r["success"])
    
    print(f"\nTicker Matcher: {ticker_success}/3 성공")
    for r in ticker_results:
        status = "✅" if r["success"] else "❌"
        print(f"  {status} {r['test_case']}: {r.get('result', 'N/A')} ({r['elapsed_time']:.2f}초)")
        if r.get("error"):
            print(f"    오류: {r['error']}")
    
    print(f"\nDriver Normalizer: {driver_success}/3 성공")
    for r in driver_results:
        status = "✅" if r["success"] else "❌"
        print(f"  {status} {r['test_case']}: {len(r.get('result', []))}개 후보 ({r['elapsed_time']:.2f}초)")
        if r.get("error"):
            print(f"    오류: {r['error']}")
    
    # JSON 파싱 실패율 계산
    total_tests = len(ticker_results) + len(driver_results)
    total_success = ticker_success + driver_success
    parse_failure_rate = (total_tests - total_success) / total_tests if total_tests > 0 else 0
    
    print(f"\n전체 성공률: {total_success}/{total_tests} ({100*(1-parse_failure_rate):.1f}%)")
    print(f"JSON 파싱 실패율: {parse_failure_rate*100:.1f}%")
    
    # DoD 확인
    if ticker_success >= 3 and driver_success >= 3 and parse_failure_rate < 0.1:
        print("\n✅ DoD 통과: 모든 테스트 성공, 파싱 실패율 < 10%")
        return True
    else:
        print("\n❌ DoD 실패: 일부 테스트 실패 또는 파싱 실패율 >= 10%")
        return False


if __name__ == "__main__":
    success = run_stability_test()
    sys.exit(0 if success else 1)

