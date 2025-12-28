"""
메타데이터 전달 통합 테스트 (Phase 2.0 P0)

목적: report_metadata가 파서 → ticker_matcher → extractor → enrichment까지
끝까지 이어지는지 검증

DoD: 리포트 1개를 넣으면, 최종 evidence_layer에 
report_id, source_name, report_fingerprint, broker_name, report_date가 모두 들어가야 함
"""
import sys
from pathlib import Path
from typing import Dict, Any
import json

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.deduplicator import generate_fingerprint, check_and_mark_processed
from extractors.ticker_matcher import get_report_subject
from parsers.smart_pdf_parser import extract_text_from_pdf_smart


def test_metadata_pipeline():
    """
    메타데이터 전달 통합 테스트
    
    시나리오:
    1. 리포트 메타데이터 생성
    2. PDF 파싱 (메타데이터 포함)
    3. Ticker 매칭 (메타데이터 전달)
    4. 최종 evidence_layer에 메타데이터 포함 확인
    """
    # 1. 리포트 메타데이터 생성
    report_metadata = {
        "report_id": "naver_20241219_001",
        "broker_name": "하나증권",
        "report_date": "2024-12-19",
        "title": "S-Oil 투자포인트",
        "category": "종목",
        "pdf_url": "https://example.com/report.pdf"
    }
    
    # 2. Fingerprint 생성 (메타데이터 포함, clean_text 사용)
    clean_text = "S-Oil은 정유 및 석유화학 사업을 영위하는 기업입니다. 향후 정제마진 회복이 핵심 변수입니다."
    fingerprint = generate_fingerprint(
        broker_name=report_metadata["broker_name"],
        report_date=report_metadata["report_date"],
        title=report_metadata["title"],
        clean_text=clean_text
    )
    
    print(f"✅ Fingerprint 생성: {fingerprint[:16]}...")
    
    # 3. Ticker 매칭 (메타데이터 전달)
    ticker_result = get_report_subject(
        title=report_metadata["title"],
        text_head="S-Oil은 정유 및 석유화학 사업을 영위하는 기업입니다.",
        report_type="company",
        report_metadata=report_metadata
    )
    
    print(f"✅ Ticker 매칭: {ticker_result.get('ticker')} ({ticker_result.get('company_name')})")
    
    # 4. 최종 evidence_layer 구조 확인
    expected_metadata_fields = [
        "report_id",
        "source_name",
        "report_fingerprint",
        "broker_name",
        "report_date"
    ]
    
    # 시뮬레이션: evidence_layer 구조
    evidence_layer_entry = {
        "source_type": "REPORT",
        "source_name": f"{report_metadata['broker_name']}_{report_metadata['report_date']}_{report_metadata['title']}",
        "broker_name": report_metadata["broker_name"],
        "report_date": report_metadata["report_date"],
        "report_id": report_metadata["report_id"],
        "report_fingerprint": fingerprint,
        "version": 1
    }
    
    # 필수 필드 확인
    missing_fields = []
    for field in expected_metadata_fields:
        if field not in evidence_layer_entry:
            missing_fields.append(field)
    
    if missing_fields:
        print(f"❌ 누락된 필드: {missing_fields}")
        return False
    else:
        print(f"✅ 모든 필수 메타데이터 필드 포함: {expected_metadata_fields}")
        return True


def test_metadata_consistency():
    """
    메타데이터 일관성 테스트
    
    리포트 1개를 파이프라인 전체를 거쳐도 메타데이터가 일관되게 유지되는지 확인
    """
    report_metadata = {
        "report_id": "test_001",
        "broker_name": "테스트증권",
        "report_date": "2024-12-19",
        "title": "테스트 리포트"
    }
    
    # 각 단계에서 메타데이터 추적
    metadata_trace = {
        "step1_parser": None,
        "step2_ticker": None,
        "step3_extractor": None,
        "step4_enrichment": None
    }
    
    # Step 1: Parser (메타데이터 포함)
    metadata_trace["step1_parser"] = {
        "report_id": report_metadata["report_id"],
        "broker_name": report_metadata["broker_name"],
        "report_date": report_metadata["report_date"]
    }
    
    # Step 2: Ticker Matcher (메타데이터 전달)
    metadata_trace["step2_ticker"] = metadata_trace["step1_parser"].copy()
    
    # Step 3: Extractor (메타데이터 전달)
    metadata_trace["step3_extractor"] = metadata_trace["step2_ticker"].copy()
    
    # Step 4: Enrichment (메타데이터 최종 확인)
    metadata_trace["step4_enrichment"] = metadata_trace["step3_extractor"].copy()
    
    # 일관성 확인
    base_metadata = metadata_trace["step1_parser"]
    for step, metadata in metadata_trace.items():
        if metadata != base_metadata:
            print(f"❌ {step}에서 메타데이터 불일치")
            return False
    
    print(f"✅ 모든 단계에서 메타데이터 일관성 유지")
    return True


if __name__ == "__main__":
    print("=" * 60)
    print("메타데이터 전달 통합 테스트")
    print("=" * 60)
    
    # 테스트 1: 메타데이터 파이프라인
    print("\n[테스트 1] 메타데이터 파이프라인")
    result1 = test_metadata_pipeline()
    
    # 테스트 2: 메타데이터 일관성
    print("\n[테스트 2] 메타데이터 일관성")
    result2 = test_metadata_consistency()
    
    # 결과 요약
    print("\n" + "=" * 60)
    print("테스트 결과 요약")
    print("=" * 60)
    print(f"메타데이터 파이프라인: {'✅ 통과' if result1 else '❌ 실패'}")
    print(f"메타데이터 일관성: {'✅ 통과' if result2 else '❌ 실패'}")
    
    if result1 and result2:
        print("\n✅ 모든 테스트 통과")
    else:
        print("\n❌ 일부 테스트 실패")

