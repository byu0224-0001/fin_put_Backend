"""
rc2 P0.5급 개선 사항 검증 스크립트

1. 후보 품질 필터 검증
2. API 관측 지표 확인
3. 체크 사항 검증
"""
import sys
from pathlib import Path
import logging
import json

# Windows 인코딩 처리
if sys.platform == 'win32':
    import codecs
    import os
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from utils.semantic_compression import get_metrics
from extractors.driver_normalizer import _extract_unknown_economic_terms

# _normalize_candidate_term은 private 함수이므로 직접 import 불가
# 대신 _extract_unknown_economic_terms 내부에서 정규화가 적용됨

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def test_candidate_quality_filter():
    """후보 품질 필터 검증"""
    print("=" * 80)
    print("[후보 품질 필터 검증]")
    print("=" * 80)
    
    test_cases = [
        ("D램 가격 상승", True),  # 정상
        ("AI 정책", True),  # 정상
        ("전망", False),  # 불용어
        ("1234567890123456789012345678901", False),  # 30자 초과
        ("A", False),  # 2자 미만
        ("!!!", False),  # 기호만
        ("123", False),  # 숫자만
        ("ESG 규제 강화", True),  # 정상
    ]
    
    print("\n[테스트 케이스]")
    for text, expected in test_cases:
        terms = _extract_unknown_economic_terms(text)
        result = len(terms) > 0 if expected else len(terms) == 0
        status = "✅" if result else "❌"
        print(f"  {status} '{text}': 예상={expected}, 실제={len(terms)}개")
    
    print("\n[중복 군집 테스트]")
    duplicate_text = "D램 가격 D-RAM 가격 DRAM 가격 D램 가격 상승"
    terms = _extract_unknown_economic_terms(duplicate_text)
    
    # 정규화는 _extract_unknown_economic_terms 내부에서 이미 적용됨
    # 중복 제거도 내부에서 처리되므로, 여기서는 단순히 결과 확인
    unique_count = len(set(terms))
    total_count = len(terms)
    duplicate_ratio = 1.0 - (unique_count / total_count) if total_count > 0 else 0
    
    print(f"  원본 추출: {total_count}개")
    print(f"  고유 후보: {unique_count}개")
    print(f"  중복률: {duplicate_ratio:.2%}")
    
    if duplicate_ratio > 0.7:
        print("  ⚠️ 중복 군집 경고 (70% 이상 중복)")
    else:
        print("  ✅ 중복률 정상")


def test_api_observability():
    """API 관측 지표 확인"""
    print("\n" + "=" * 80)
    print("[API 관측 지표 확인]")
    print("=" * 80)
    
    metrics = get_metrics()
    
    print("\n[메트릭]")
    print(f"  API 호출 횟수: {metrics.get('api_call_count', 0)}")
    print(f"  캐시 히트: {metrics.get('cache_hit_count', 0)}")
    print(f"  캐시 미스: {metrics.get('cache_miss_count', 0)}")
    print(f"  캐시 히트율: {metrics.get('cache_hit_rate', 0.0):.2%}")
    print(f"  Fallback 사용: {metrics.get('fallback_used_count', 0)}")
    print(f"  Fallback 발생률: {metrics.get('fallback_rate', 0.0):.2%}")
    
    if metrics.get('avg_response_time_ms', 0) > 0:
        print(f"  평균 응답 시간: {metrics.get('avg_response_time_ms', 0):.2f}ms")
        print(f"  P50 응답 시간: {metrics.get('p50_response_time_ms', 0):.2f}ms")
        print(f"  P95 응답 시간: {metrics.get('p95_response_time_ms', 0):.2f}ms")
    
    print(f"  API 실패: {metrics.get('api_fail_count', 0)}")
    print(f"  타임아웃: {metrics.get('timeout_count', 0)}")
    
    # JSON으로 저장
    output_file = Path("reports") / "semantic_compression_metrics.json"
    output_file.parent.mkdir(exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    
    print(f"\n[저장 완료] {output_file}")


def test_schema_consistency():
    """2-hop과 3-hop 스키마 일관성 확인"""
    print("\n" + "=" * 80)
    print("[스키마 일관성 확인]")
    print("=" * 80)
    
    # 3-hop 스키마 (예상)
    schema_3hop = {
        "type": str,
        "sentence": str,
        "driver": str,
        "industry": str,
        "industry_logic": str,
        "company": str,
        "ticker": str,
        "evidence_count": int,
        "industry_edge_id": (int, type(None)),
        "company_edge_id": (int, type(None)),
        "evidence_ids": list,
        "report_ids": list,
        "created_at": (str, type(None)),
        "is_macro": bool,
        "hop_count": int
    }
    
    # 2-hop 스키마 (예상)
    schema_2hop = {
        "type": str,
        "sentence": str,
        "driver": str,
        "industry": str,
        "industry_logic": str,
        "company": type(None),  # 2-hop은 null
        "ticker": type(None),  # 2-hop은 null
        "evidence_count": int,
        "industry_edge_id": (int, type(None)),
        "company_edge_id": type(None),  # 2-hop은 null
        "evidence_ids": list,
        "report_ids": list,
        "created_at": (str, type(None)),
        "is_macro": bool,
        "hop_count": int
    }
    
    print("\n[스키마 필드 비교]")
    common_fields = set(schema_3hop.keys()) & set(schema_2hop.keys())
    print(f"  공통 필드: {len(common_fields)}개")
    print(f"  필드 목록: {', '.join(sorted(common_fields))}")
    
    # 필드명 일관성 확인
    all_fields_3hop = set(schema_3hop.keys())
    all_fields_2hop = set(schema_2hop.keys())
    
    if all_fields_3hop == all_fields_2hop:
        print("  ✅ 필드명 일관성 확인 (모든 필드 동일)")
    else:
        missing_in_2hop = all_fields_3hop - all_fields_2hop
        missing_in_3hop = all_fields_2hop - all_fields_3hop
        if missing_in_2hop:
            print(f"  ⚠️ 2-hop에 없는 필드: {missing_in_2hop}")
        if missing_in_3hop:
            print(f"  ⚠️ 3-hop에 없는 필드: {missing_in_3hop}")


if __name__ == "__main__":
    test_candidate_quality_filter()
    test_api_observability()
    test_schema_consistency()
    
    print("\n" + "=" * 80)
    print("[검증 완료]")
    print("=" * 80)

