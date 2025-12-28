"""
Driver 후보 추출 (원문 기반, 추출 실패 시에도 후보 생성)

⭐ 개선: driver 추출 성공/실패와 무관하게 원문에서 후보 추출
"""
import sys
from pathlib import Path
import logging
import re
from typing import List, Dict, Optional, Any

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

from app.db import SessionLocal
from app.models.driver_candidate import DriverCandidate
from extractors.driver_normalizer import _extract_unknown_economic_terms, _register_driver_candidates
from datetime import datetime, timedelta
from sqlalchemy import and_

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def extract_candidates_from_original_text(
    original_text: str,
    report_metadata: Dict[str, Any],
    use_top_paragraphs: int = 3
) -> List[str]:
    """
    원문 텍스트에서 driver 후보 추출
    
    ⭐ 개선: 요약본이 아닌 원문 chunk 사용
    
    Args:
        original_text: 원문 텍스트 (전체 또는 상위 문단)
        report_metadata: 리포트 메타데이터
        use_top_paragraphs: 사용할 상위 문단 수 (기본 3개)
    
    Returns:
        추출된 후보 리스트
    """
    # 원문을 문단 단위로 분리
    paragraphs = original_text.split('\n\n')
    
    # 상위 N개 문단만 사용 (요약본이 아닌 원문 표현 보존)
    top_paragraphs = paragraphs[:use_top_paragraphs]
    original_chunk = '\n\n'.join(top_paragraphs)
    
    # unknown term 추출 (이제 튜플 반환)
    result = _extract_unknown_economic_terms(original_chunk)
    if isinstance(result, tuple):
        unknown_terms, extraction_metrics = result
    else:
        # 레거시 호환성
        unknown_terms = result
        extraction_metrics = {}
    
    logger.info(f"원문에서 추출된 후보: {len(unknown_terms)}개")
    
    return unknown_terms


def register_candidates_from_failed_extraction(
    original_text: str,
    report_metadata: Dict[str, Any],
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Driver 추출 실패 시에도 후보 등록
    
    ⭐ 개선: 추출 실패 경로에서도 후보 생성
    ⭐ P0-3: 원인 3분류 계측 (INPUT/EXTRACTOR/PERSIST)
    
    Args:
        original_text: 원문 텍스트
        report_metadata: 리포트 메타데이터
    
    Returns:
        {
            "registered_count": int,
            "metrics": {
                "original_chunk_len": int,
                "candidate_terms_extracted_count": int,
                "candidate_rows_inserted_count": int,
                "candidate_insert_skipped_cooldown_count": int
            }
        }
    """
    metrics = {
        "original_chunk_len": 0,
        "candidate_terms_extracted_count": 0,
        "candidate_rows_inserted_count": 0,
        "candidate_insert_skipped_cooldown_count": 0
    }
    
    # 원문 chunk 길이 확인 (INPUT 문제 감지)
    original_chunk = extract_candidates_from_original_text(
        original_text=original_text,
        report_metadata=report_metadata,
        use_top_paragraphs=3
    )
    
    # 원문 chunk 길이 계산
    paragraphs = original_text.split('\n\n')
    top_paragraphs = paragraphs[:3]
    original_chunk_text = '\n\n'.join(top_paragraphs)
    metrics["original_chunk_len"] = len(original_chunk_text)
    
    # EXTRACTOR 문제 감지: 후보 추출 (이제 튜플 반환)
    extraction_result = _extract_unknown_economic_terms(original_chunk_text)
    if isinstance(extraction_result, tuple):
        unknown_terms, extraction_metrics = extraction_result
        metrics.update(extraction_metrics)  # known/unknown 분해 메트릭 포함
    else:
        # 레거시 호환성
        unknown_terms = extraction_result
        extraction_metrics = {}
    
    metrics["candidate_terms_extracted_count"] = len(unknown_terms)
    
    if unknown_terms:
        # PERSIST 문제 감지: 후보 등록
        try:
            from extractors.driver_normalizer import _register_driver_candidates
            
            # ⭐ P0-D: dry_run 모드 체크
            if dry_run:
                # Dry-run: DB 저장 없이 메트릭만 계산
                logger.info(f"[Dry-run] 후보 추출 완료: {len(unknown_terms)}개 (DB 저장 안 함)")
                metrics["candidate_rows_inserted_count"] = 0
                metrics["candidate_insert_skipped_cooldown_count"] = 0
                metrics["candidate_insert_skipped_unique_conflict_count"] = 0
                # Dry-run에서도 등록 가능한 후보 수는 계산
                metrics["candidate_rows_inserted_count"] = len(unknown_terms)  # 실제로는 저장 안 함
            else:
                # 등록 전 카운트
                from app.db import SessionLocal
                from app.models.driver_candidate import DriverCandidate
                db = SessionLocal()
                try:
                    before_count = db.query(DriverCandidate).count()
                    
                    # 후보 등록 (extraction_metrics 전달)
                    register_result = _register_driver_candidates(
                        unknown_terms=unknown_terms,
                        report_text=original_chunk_text,
                        report_metadata=report_metadata,
                        extraction_metrics=extraction_metrics
                    )
                    
                    # 등록 후 카운트
                    after_count = db.query(DriverCandidate).count()
                    metrics["candidate_rows_inserted_count"] = after_count - before_count
                    
                    # ⭐ P0-추가 4: 스킵 사유 분해
                    if register_result:
                        metrics["candidate_insert_skipped_cooldown_count"] = register_result.get("skipped_cooldown_count", 0)
                        metrics["candidate_insert_skipped_unique_conflict_count"] = register_result.get("skipped_unique_conflict_count", 0)
                    else:
                        # 추정
                        metrics["candidate_insert_skipped_cooldown_count"] = len(unknown_terms) - metrics["candidate_rows_inserted_count"]
                finally:
                    db.close()
        except Exception as e:
            logger.error(f"후보 등록 실패 (PERSIST 문제): {e}")
            metrics["candidate_rows_inserted_count"] = 0
    
    return {
        "registered_count": metrics["candidate_rows_inserted_count"],
        "metrics": metrics
    }


if __name__ == "__main__":
    # 테스트 코드
    test_text = """
    최근 D램 가격이 상승하고 있으며, AI 정책이 시장에 영향을 미치고 있습니다.
    ESG 규제 강화로 인한 비용 증가 우려가 있습니다.
    정제마진 회복이 핵심이며, 크랙스프레드 개선 전망이 나왔습니다.
    """
    
    test_metadata = {
        "report_id": "test_001",
        "title": "테스트 리포트",
        "broker_name": "테스트 증권"
    }
    
    candidates = extract_candidates_from_original_text(
        original_text=test_text,
        report_metadata=test_metadata
    )
    
    print(f"추출된 후보: {candidates}")

