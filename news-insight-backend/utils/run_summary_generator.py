"""
Run Summary Generator (P0+)

E2E 실행 리포트 생성 (20개 리포트 기준)
"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import json

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_run_summary(
    results: List[Dict[str, Any]],
    metrics: Optional[Any] = None
) -> Dict[str, Any]:
    """
    E2E 실행 리포트 생성
    
    Args:
        results: 처리 결과 리스트
        metrics: PipelineMetrics 객체 (선택)
    
    Returns:
        실행 리포트 딕셔너리
    """
    total_reports = len(results)
    success_count = sum(1 for r in results if r.get("edge_enriched"))
    
    # 단계별 성공률
    ticker_matched = sum(1 for r in results if r.get("ticker"))
    driver_matched = sum(1 for r in results if r.get("driver_code"))
    quality_pass = sum(1 for r in results if r.get("quality_gate_status") == "PASS")
    insights_extracted = sum(1 for r in results if r.get("insights_extracted"))
    
    # HOLD 사유별 카운트
    hold_reasons = {}
    for r in results:
        if r.get("quality_gate_status") == "HOLD":
            hold_metadata = r.get("hold_metadata", {})
            reason = hold_metadata.get("hold_reason", "UNKNOWN")
            hold_reasons[reason] = hold_reasons.get(reason, 0) + 1
    
    # CONFLICT 비율
    conflict_count = sum(1 for r in results if r.get("alignment") == "CONFLICT")
    conflict_ratio = (conflict_count / success_count * 100) if success_count > 0 else 0
    
    # ⭐ P0-3: dedupe 메트릭 (과도 병합 감지용)
    dedupe_skipped = sum(1 for r in results if r.get("dedupe_skipped") or r.get("industry_dedupe_skipped"))
    industry_edge_enriched = sum(1 for r in results if r.get("industry_edge_enriched"))
    company_edge_enriched = sum(1 for r in results if r.get("company_edge_enriched"))
    
    # save_result별 카운트
    save_results = {}
    for r in results:
        sr = r.get("save_result") or r.get("industry_save_result")
        if sr:
            save_results[sr] = save_results.get(sr, 0) + 1
    
    # 카테고리별 dedupe 분포
    dedupe_by_category = {}
    for r in results:
        if r.get("dedupe_skipped") or r.get("industry_dedupe_skipped"):
            cat = r.get("category", "UNKNOWN")
            dedupe_by_category[cat] = dedupe_by_category.get(cat, 0) + 1
    
    # ⭐ P0-H: APPENDED/NOOP/EVICTED 구분 (enum 기반 - 견고함)
    dedupe_appended = 0
    dedupe_noop = 0
    dedupe_evicted = 0
    evicted_edge_ids = set()  # cap_hit_edge_count 계산용
    
    for r in results:
        sr = r.get("save_result") or r.get("industry_save_result")
        if sr == "SKIPPED_DEDUPE_APPENDED":
            dedupe_appended += 1
        elif sr == "SKIPPED_DEDUPE_NOOP":
            dedupe_noop += 1
        elif sr == "SKIPPED_DEDUPE_EVICTED":
            dedupe_evicted += 1
            # evict 발생한 edge 추적 (edge_id 또는 logic_fingerprint)
            edge_id = r.get("edge_id") or r.get("logic_fingerprint")
            if edge_id:
                evicted_edge_ids.add(edge_id)
    
    # cap_hit_edge_count: 실제로 cap이 발생한 고유 edge 개수
    cap_hit_edge_count = len(evicted_edge_ids)
    
    # 평균 토큰/리포트
    avg_tokens = 0
    if metrics:
        llm_usage = metrics.metrics.get("llm_usage", {})
        total_tokens = llm_usage.get("total_tokens", 0)
        total_calls = sum(llm_usage.get("calls_by_stage", {}).values())
        avg_tokens = (total_tokens / total_calls) if total_calls > 0 else 0
    
    summary = {
        "run_timestamp": datetime.utcnow().isoformat() + "Z",
        "total_reports": total_reports,
        "end_to_end_success": success_count,
        "end_to_end_rate": round((success_count / total_reports * 100) if total_reports > 0 else 0, 2),
        "stage_success_rates": {
            "ticker_matching": round((ticker_matched / total_reports * 100) if total_reports > 0 else 0, 2),
            "driver_normalization": round((driver_matched / ticker_matched * 100) if ticker_matched > 0 else 0, 2),
            "quality_gate_pass": round((quality_pass / driver_matched * 100) if driver_matched > 0 else 0, 2),
            "insight_extraction": round((insights_extracted / quality_pass * 100) if quality_pass > 0 else 0, 2),
            "edge_enrichment": round((success_count / insights_extracted * 100) if insights_extracted > 0 else 0, 2)
        },
        "hold_reasons_top5": dict(sorted(hold_reasons.items(), key=lambda x: x[1], reverse=True)[:5]),
        "conflict_ratio": round(conflict_ratio, 2),
        "avg_tokens_per_report": round(avg_tokens, 0),
        "llm_usage": metrics.metrics.get("llm_usage", {}) if metrics else {},
        "idempotency": metrics.metrics.get("idempotency", {}) if metrics else {},
        # ⭐ P0-3: dedupe 메트릭 (Evidence 누적 품질 지표)
        # ═══════════════════════════════════════════════════════════════════
        # [단위 정의] - 팀 혼동 방지용
        #   - industry_edge_enriched: "리포트 단위" - industry_edge 저장에 성공한 리포트 수
        #   - dedupe_appended: "리포트 단위" - 기존 edge에 source 추가된 리포트 수
        #   - cap_hit_edge_count: "edge 단위" - 이번 런에서 evict 발생한 unique edge 수
        # ═══════════════════════════════════════════════════════════════════
        "dedupe_metrics": {
            "total_dedupe_skipped": dedupe_skipped,
            # [리포트 단위] industry_edge 저장에 성공한 리포트 수
            "industry_edge_enriched": industry_edge_enriched,
            # [리포트 단위] company_edge 저장에 성공한 리포트 수
            "company_edge_enriched": company_edge_enriched,
            "dedupe_skip_rate": round((dedupe_skipped / industry_edge_enriched * 100) if industry_edge_enriched > 0 else 0, 2),
            "save_results_distribution": save_results,
            "dedupe_by_category": dedupe_by_category,
            # ⭐ P0-H: APPENDED/NOOP/EVICTED 구분 (enum 기반)
            # [리포트 단위] 기존 edge에 source가 추가된 리포트 수
            "dedupe_appended": dedupe_appended,
            # [리포트 단위] 이미 존재하여 변화 없는 리포트 수
            "dedupe_noop": dedupe_noop,
            # [리포트 단위] 오래된 source 제거 후 추가된 리포트 수
            "dedupe_evicted": dedupe_evicted,
            # ⭐ P0-Final: cap_hit_edge_count
            # [edge 단위] 이번 런에서 evict가 발생한 unique edge 수
            # (≠ "cap이 가득 찬 edge 수" - 그건 DB 조회로 별도 확인)
            # 운영 판단: 이 숫자가 높으면 SOURCES_CAP(50) 확대 검토 필요
            "cap_hit_edge_count": cap_hit_edge_count,
            # ⭐ (A) Evict 경고 - 노이즈 방지 (최소 표본 10개 이상일 때만)
            "evict_ratio": round((dedupe_evicted / dedupe_appended), 3) if dedupe_appended > 0 else None,
            "evict_warning": (dedupe_appended >= 10 and dedupe_evicted / dedupe_appended > 0.2),
            # ⭐ P0-Final: append_per_saved_rate
            # 분모: industry_edge_enriched (저장 성공한 "리포트 수")
            # 분자: dedupe_appended (기존 edge에 source 추가된 "리포트 수")
            # 의미: "저장된 리포트 중 합의를 강화한 비율 (높을수록 논리 재확인 多)"
            "append_per_saved_rate": round((dedupe_appended / industry_edge_enriched * 100) if industry_edge_enriched > 0 else 0, 2),
            # ⭐ P1-Final: sources>=3 게이트 (데모팩 추출 기준)
            # [edge 단위] DB에서 sources 배열 길이 >= 3인 edge 개수
            # 게이트: 10개 이상 OR 10% 이상 → 데모팩 추출 가능
            "sources_gte3_edge_count": 0,  # 실제 값은 DB 조회 필요, placeholder
            "sources_gte3_gate_note": "DB 조회 필요: verify_p0h_performance.py --days-back 30"
        }
    }
    
    return summary


def save_run_summary(
    summary: Dict[str, Any],
    output_path: Optional[Path] = None
) -> Path:
    """
    실행 리포트 저장
    
    Args:
        summary: 실행 리포트 딕셔너리
        output_path: 출력 경로 (None이면 기본 경로)
    
    Returns:
        저장된 파일 경로
    """
    if output_path is None:
        output_path = project_root / "reports" / f"run_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    
    logger.info(f"실행 리포트 저장: {output_path}")
    return output_path

