"""
Fingerprint Deduplicator (Phase 2.0 P0)

목적: 같은 리포트를 두 번 처리해서 Evidence가 중복 쌓이는 것 방지
(idempotent 보장)

핵심 로직:
- hash(Broker + Date + Title + Text[:300]) 값을 생성
- 이미 처리된 해시면 Skip
"""
import sys
from pathlib import Path
from typing import Dict, Optional, Set
import hashlib
import json
import logging
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 처리된 리포트 해시 저장 파일
PROCESSED_REPORTS_FILE = project_root / "data" / "processed_reports.json"
PROCESSED_REPORTS_FILE.parent.mkdir(parents=True, exist_ok=True)


def generate_fingerprint(
    broker_name: str,
    report_date: str,
    title: str,
    clean_text: str = "",
    max_text_length: int = 500
) -> str:
    """
    리포트 Fingerprint 생성 (개선: 컷오프/클린업 완료된 본문 기준)
    
    Args:
        broker_name: 증권사명
        report_date: 리포트 발행일 (YYYY-MM-DD)
        title: 리포트 제목
        clean_text: 컷오프/클린업 완료된 본문 (compliance/disclaimer 제거된 텍스트)
        max_text_length: 본문 최대 길이
    
    Returns:
        SHA256 해시값 (fingerprint)
    """
    # 정규화: 공백 제거, 소문자 변환
    normalized_title = title.strip().lower()
    normalized_broker = broker_name.strip().lower()
    normalized_date = report_date.strip()
    
    # 본문 정규화 (컷오프/클린업 완료된 텍스트만 사용)
    if clean_text:
        # 본문의 마지막 "의미 문단" 찾기 (마지막 3개 문단 중 가장 긴 텍스트)
        paragraphs = [p.strip() for p in clean_text.split('\n\n') if p.strip() and len(p.strip()) > 20]
        
        if paragraphs:
            # 앞부분 (최대 300자)
            text_head = paragraphs[0][:300] if len(paragraphs) > 0 else ""
            
            # 마지막 의미 문단 (compliance/disclaimer 제외)
            # 마지막 3개 문단 중 가장 긴 것 선택
            last_paragraphs = paragraphs[-3:] if len(paragraphs) >= 3 else paragraphs
            if last_paragraphs:
                # 가장 긴 문단 선택
                meaningful_tail = max(last_paragraphs, key=len)
                # 너무 짧으면(20자 미만) 제외
                if len(meaningful_tail) >= 20:
                    text_tail = meaningful_tail[:200]
                else:
                    text_tail = ""
            else:
                text_tail = ""
            
            normalized_text = f"{text_head}|{text_tail}".strip().lower()
        else:
            # 문단이 없으면 전체 텍스트 사용 (최대 길이 제한)
            normalized_text = clean_text[:max_text_length].strip().lower()
    else:
        normalized_text = ""
    
    # Fingerprint 생성용 문자열
    fingerprint_string = f"{normalized_broker}|{normalized_date}|{normalized_title}|{normalized_text}"
    
    # SHA256 해시
    fingerprint = hashlib.sha256(fingerprint_string.encode('utf-8')).hexdigest()
    
    return fingerprint


def load_processed_reports() -> Set[str]:
    """
    처리된 리포트 해시 목록 로드
    
    Returns:
        처리된 fingerprint 집합
    """
    if not PROCESSED_REPORTS_FILE.exists():
        return set()
    
    try:
        with open(PROCESSED_REPORTS_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return set(data.get("processed_fingerprints", []))
    except Exception as e:
        logger.warning(f"처리된 리포트 목록 로드 실패: {e}")
        return set()


def save_processed_report(fingerprint: str, report_info: Optional[Dict] = None):
    """
    처리된 리포트 해시 저장
    
    Args:
        fingerprint: 리포트 fingerprint
        report_info: 리포트 메타데이터 (선택, 로깅용)
    """
    processed = load_processed_reports()
    processed.add(fingerprint)
    
    data = {
        "processed_fingerprints": list(processed),
        "last_updated": datetime.now().isoformat()
    }
    
    # 리포트 정보도 저장 (디버깅용)
    if report_info:
        if "report_details" not in data:
            data["report_details"] = []
        data["report_details"].append({
            "fingerprint": fingerprint,
            "broker_name": report_info.get("broker_name"),
            "report_date": report_info.get("report_date"),
            "title": report_info.get("title", "")[:100],
            "processed_at": datetime.now().isoformat()
        })
        # 최근 1000개만 유지
        data["report_details"] = data["report_details"][-1000:]
    
    try:
        with open(PROCESSED_REPORTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"처리된 리포트 목록 저장 실패: {e}")


def is_processed(fingerprint: str) -> bool:
    """
    리포트가 이미 처리되었는지 확인
    
    Args:
        fingerprint: 리포트 fingerprint
    
    Returns:
        이미 처리되었으면 True
    """
    processed = load_processed_reports()
    return fingerprint in processed


def check_and_mark_processed(
    broker_name: str,
    report_date: str,
    title: str,
    text_head: str = "",
    report_info: Optional[Dict] = None
) -> tuple[bool, str]:
    """
    리포트 중복 체크 및 처리 마킹
    
    Args:
        broker_name: 증권사명
        report_date: 리포트 발행일
        title: 리포트 제목
        text_head: 리포트 본문 앞부분
        report_info: 리포트 메타데이터
    
    Returns:
        (is_processed, fingerprint)
        - is_processed: 이미 처리되었으면 True
        - fingerprint: 리포트 fingerprint
    """
    fingerprint = generate_fingerprint(broker_name, report_date, title, text_head, text_tail)
    
    if is_processed(fingerprint):
        logger.info(f"중복 리포트 감지 (스킵): {title[:50]}... (fingerprint: {fingerprint[:16]}...)")
        return True, fingerprint
    
    # 처리 마킹
    save_processed_report(fingerprint, report_info)
    logger.info(f"새 리포트 등록: {title[:50]}... (fingerprint: {fingerprint[:16]}...)")
    return False, fingerprint


def clear_processed_reports(older_than_days: Optional[int] = None):
    """
    처리된 리포트 목록 초기화 (선택적)
    
    Args:
        older_than_days: N일 이전 리포트만 삭제 (None이면 전체 삭제)
    """
    if older_than_days is None:
        # 전체 삭제
        if PROCESSED_REPORTS_FILE.exists():
            PROCESSED_REPORTS_FILE.unlink()
            logger.info("처리된 리포트 목록 전체 삭제")
    else:
        # 날짜 기반 삭제는 추후 구현
        logger.warning("날짜 기반 삭제는 아직 구현되지 않았습니다.")


if __name__ == "__main__":
    # 테스트 코드
    test_report = {
        "broker_name": "하나증권",
        "report_date": "2024-12-19",
        "title": "S-Oil 투자포인트",
        "text_head": "S-Oil은 정유 및 석유화학 사업을 영위하는 기업입니다."
    }
    
    is_dup, fp = check_and_mark_processed(**test_report)
    print(f"중복 여부: {is_dup}")
    print(f"Fingerprint: {fp}")
    
    # 같은 리포트 다시 체크
    is_dup2, fp2 = check_and_mark_processed(**test_report)
    print(f"재체크 중복 여부: {is_dup2}")
    print(f"Fingerprint 동일 여부: {fp == fp2}")

