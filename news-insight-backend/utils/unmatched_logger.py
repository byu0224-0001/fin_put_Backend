"""
Unmatched Logger (Phase 2.0 P0+)

목적: Ticker 매칭 실패 케이스를 로깅하여 별칭 사전 업데이트에 활용

로그 형식:
{
    "timestamp": "...",
    "report_title": "...",
    "report_broker": "...",
    "report_date": "...",
    "failure_reason": "TICKER_NOT_FOUND",
    "extracted_company_name": "...",
    "debug_info": {...},
    "suggested_aliases": [...]
}
"""
import sys
from pathlib import Path
from typing import Dict, Optional, Any
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

# 매칭 실패 로그 파일
UNMATCHED_LOGS_FILE = project_root / "data" / "unmatched_logs.json"
UNMATCHED_LOGS_FILE.parent.mkdir(parents=True, exist_ok=True)


def load_unmatched_logs() -> list:
    """매칭 실패 로그 목록 로드"""
    if not UNMATCHED_LOGS_FILE.exists():
        return []
    
    try:
        with open(UNMATCHED_LOGS_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"매칭 실패 로그 로드 실패: {e}")
        return []


def save_unmatched_log(log_entry: Dict[str, Any]):
    """
    매칭 실패 로그 저장
    
    Args:
        log_entry: 로그 엔트리
    """
    logs = load_unmatched_logs()
    
    # 타임스탬프 추가
    log_entry["timestamp"] = datetime.now().isoformat()
    
    logs.append(log_entry)
    
    # 최근 1000개만 유지
    logs = logs[-1000:]
    
    try:
        with open(UNMATCHED_LOGS_FILE, 'w', encoding='utf-8') as f:
            json.dump(logs, f, ensure_ascii=False, indent=2)
        logger.debug(f"매칭 실패 로그 저장: {log_entry.get('report_title', 'Unknown')[:50]}")
    except Exception as e:
        logger.error(f"매칭 실패 로그 저장 실패: {e}")


def log_ticker_match_failure(
    report_title: str,
    report_broker: str,
    report_date: str,
    failure_reason: str,
    extracted_company_name: Optional[str] = None,
    debug_info: Optional[Dict] = None,
    suggested_aliases: Optional[list] = None
):
    """
    Ticker 매칭 실패 로그 기록
    
    Args:
        report_title: 리포트 제목
        report_broker: 증권사명
        report_date: 리포트 발행일
        failure_reason: 실패 이유 (TICKER_NOT_FOUND, FUZZY_VALIDATION_FAILED 등)
        extracted_company_name: 추출된 기업명 (LLM 결과 등)
        debug_info: 디버그 정보 (ticker_matcher의 debug 필드)
        suggested_aliases: 제안된 별칭 (수동 검토용)
    """
    log_entry = {
        "report_title": report_title,
        "report_broker": report_broker,
        "report_date": report_date,
        "failure_reason": failure_reason,
        "extracted_company_name": extracted_company_name,
        "debug_info": debug_info or {},
        "suggested_aliases": suggested_aliases or []
    }
    
    save_unmatched_log(log_entry)
    logger.info(f"매칭 실패 로그 기록: {report_title[:50]}... ({failure_reason})")


def get_unmatched_summary() -> Dict[str, Any]:
    """
    매칭 실패 로그 요약 통계
    
    Returns:
        {
            "total_failures": 100,
            "by_reason": {"TICKER_NOT_FOUND": 50, ...},
            "top_extracted_names": [{"name": "...", "count": 10}, ...],
            "suggested_aliases": [...]
        }
    """
    logs = load_unmatched_logs()
    
    if not logs:
        return {
            "total_failures": 0,
            "by_reason": {},
            "top_extracted_names": [],
            "suggested_aliases": []
        }
    
    # 실패 이유별 집계
    by_reason = {}
    extracted_names = {}
    
    for log in logs:
        reason = log.get("failure_reason", "UNKNOWN")
        by_reason[reason] = by_reason.get(reason, 0) + 1
        
        name = log.get("extracted_company_name")
        if name:
            extracted_names[name] = extracted_names.get(name, 0) + 1
    
    # 상위 추출된 기업명
    top_extracted_names = sorted(
        [{"name": k, "count": v} for k, v in extracted_names.items()],
        key=lambda x: x["count"],
        reverse=True
    )[:20]
    
    return {
        "total_failures": len(logs),
        "by_reason": by_reason,
        "top_extracted_names": top_extracted_names,
        "suggested_aliases": list(set(
            alias for log in logs 
            for alias in log.get("suggested_aliases", [])
        ))
    }


if __name__ == "__main__":
    # 테스트 코드
    log_ticker_match_failure(
        report_title="테스트 리포트",
        report_broker="테스트증권",
        report_date="2024-12-19",
        failure_reason="TICKER_NOT_FOUND",
        extracted_company_name="하이닉스",
        debug_info={"step1_rule": "NOT_FOUND", "step2_fuzzy": "NOT_FOUND"},
        suggested_aliases=["하이닉스 → SK하이닉스"]
    )
    
    summary = get_unmatched_summary()
    print(f"매칭 실패 요약: {summary}")

