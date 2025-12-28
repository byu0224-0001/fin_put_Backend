"""
Rollup Manager (Phase 2.0 P0+)

목적: Edge당 evidence_layer가 50개 초과 시 rollup으로 합침

정책:
- Edge당 evidence_layer 최대 50개 유지
- 오래된 evidence_layer를 rollup 요약으로 합침
- source_report_ids 포함하여 provenance 유지
"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Edge당 evidence_layer 최대 개수
MAX_EVIDENCE_LAYER_COUNT = 50


def create_rollup(
    evidence_list: List[Dict[str, Any]],
    rollup_period: Optional[str] = None
) -> Dict[str, Any]:
    """
    Evidence Layer Rollup 생성
    
    Args:
        evidence_list: Rollup할 evidence_layer 리스트
        rollup_period: Rollup 기간 (예: "2024-01~2024-06")
    
    Returns:
        Rollup 객체
    """
    if not evidence_list:
        return None
    
    # source_report_ids 추출
    source_report_ids = []
    alignment_distribution = {
        "ALIGNED": 0,
        "PARTIAL": 0,
        "CONFLICT": 0,
        "UNKNOWN": 0
    }
    temporal_hints = {}
    
    summary_logics = []
    
    for evidence in evidence_list:
        # report_id 수집
        report_id = evidence.get("report_id")
        if report_id:
            source_report_ids.append(report_id)
        
        # alignment 분포 집계
        alignment = evidence.get("alignment", "UNKNOWN")
        if alignment in alignment_distribution:
            alignment_distribution[alignment] += 1
        
        # temporal_hint 분포 집계
        temporal_hint = evidence.get("temporal_hint", "MID_TERM")
        temporal_hints[temporal_hint] = temporal_hints.get(temporal_hint, 0) + 1
        
        # analyst_logic 수집 (요약용)
        analyst_logic = evidence.get("analyst_logic", "")
        if analyst_logic:
            summary_logics.append(analyst_logic[:100])  # 최대 100자
    
    # 가장 많은 temporal_hint 선택
    most_common_temporal = max(temporal_hints.items(), key=lambda x: x[1])[0] if temporal_hints else "MID_TERM"
    
    # Rollup 요약 생성
    if summary_logics:
        # 간단한 요약 (실제로는 LLM으로 더 정교하게 가능)
        summary_logic = f"이 기간 동안 {len(evidence_list)}개 리포트에서 유사한 논리가 반복적으로 언급됨"
    else:
        summary_logic = f"이 기간 동안 {len(evidence_list)}개 리포트에서 관련 인사이트가 언급됨"
    
    # Rollup 기간 자동 계산
    if not rollup_period:
        dates = [e.get("report_date", "") for e in evidence_list if e.get("report_date")]
        if dates:
            min_date = min(dates)
            max_date = max(dates)
            rollup_period = f"{min_date}~{max_date}"
        else:
            rollup_period = "UNKNOWN"
    
    rollup = {
        "source_type": "REPORT_ROLLUP",
        "rollup_period": rollup_period,
        "rollup_count": len(evidence_list),
        "summary_logic": summary_logic,
        "alignment_distribution": alignment_distribution,
        "temporal_hint": most_common_temporal,
        "source_report_ids": source_report_ids,  # ⭐ 필수: 근거 추적/감사용
        "created_at": datetime.now().isoformat()
    }
    
    logger.info(f"Rollup 생성: {len(evidence_list)}개 evidence → 1개 rollup (period: {rollup_period})")
    
    return rollup


def manage_evidence_layer_size(
    current_evidence: List[Dict[str, Any]],
    new_evidence: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Evidence Layer 크기 관리 (50개 초과 시 rollup)
    
    ⚠️ 트리거 타이밍: append 직후 즉시 실행
    - Edge Enrichment에서 evidence_layer에 append할 때마다 호출
    - batch/주기적 실행이 아닌, 실시간 크기 관리
    - "왜 120개 쌓였지?" 같은 DB 사고 방지
    
    Args:
        current_evidence: 현재 evidence_layer 리스트
        new_evidence: 새로 추가할 evidence
    
    Returns:
        관리된 evidence_layer 리스트
    """
    if not current_evidence:
        current_evidence = []
    
    # 새 evidence 추가
    updated_evidence = current_evidence + [new_evidence]
    
    # 최대 개수 체크 (append 직후 즉시 체크)
    if len(updated_evidence) <= MAX_EVIDENCE_LAYER_COUNT:
        return updated_evidence
    
    # 50개 초과 시 rollup 필요 (즉시 실행)
    logger.info(f"Evidence layer가 {len(updated_evidence)}개로 초과. Rollup 즉시 실행")
    
    # 최신 50개는 유지
    latest_evidence = updated_evidence[-MAX_EVIDENCE_LAYER_COUNT:]
    
    # 오래된 것들 rollup
    old_evidence = updated_evidence[:-MAX_EVIDENCE_LAYER_COUNT]
    
    if old_evidence:
        rollup = create_rollup(old_evidence)
        if rollup:
            # Rollup을 맨 앞에 추가
            latest_evidence = [rollup] + latest_evidence
    
    return latest_evidence


if __name__ == "__main__":
    # 테스트 코드
    test_evidence = [
        {
            "report_id": f"test_{i:03d}",
            "report_date": "2024-01-15",
            "alignment": "ALIGNED",
            "temporal_hint": "MID_TERM",
            "analyst_logic": f"테스트 논리 {i}"
        }
        for i in range(60)
    ]
    
    # 크기 관리 테스트
    managed = manage_evidence_layer_size([], test_evidence[0])
    for evidence in test_evidence[1:]:
        managed = manage_evidence_layer_size(managed, evidence)
    
    print(f"최종 evidence_layer 개수: {len(managed)}")
    print(f"Rollup 포함 여부: {any(e.get('source_type') == 'REPORT_ROLLUP' for e in managed)}")

