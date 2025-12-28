"""
E2E 테스트 리포트 생성 스크립트

20개 리포트 기준 Quality Gate 캘리브레이션 리포트 생성
"""
import sys
import os
from pathlib import Path
from dotenv import load_dotenv
import json
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

load_dotenv(project_root / '.env')

import logging
from utils.pipeline_metrics import PipelineMetrics

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_user_output_sentence(
    company_name: str,
    driver_code: str,
    target_sector_code: str,
    logic_summary: str,
    evidence_sentence: str,
    relation_type: str,
    report_type: str = "COMPANY"  # ⭐ P0-final-3: 리포트 타입 추가 (MACRO vs COMPANY)
) -> str:
    """
    ⭐ P0-α5: 유저 아웃풋 문장 생성 (템플릿 기반)
    
    형식: "(산업 드라이버) → (섹터 노출) → (기업 노출 + evidence_layer)"
    
    ⭐ P0-final-3: 리포트 타입에 따라 톤 구분
    - MACRO: "가능성/환경" 톤 (예: "시장에서는 ... 경향이 있으며, 해당 기업은 이 구간에서 상대적 관심을 받을 수 있습니다")
    - COMPANY: "직접 노출" 톤 (예: "이 기업은 ... 영향을 받습니다")
    
    Args:
        company_name: 기업명
        driver_code: 드라이버 코드
        target_sector_code: 타겟 섹터 코드
        logic_summary: 산업 로직 요약
        evidence_sentence: 증거 문장
        relation_type: 관계 타입
        report_type: 리포트 타입 (MACRO, COMPANY, INDUSTRY)
    
    Returns:
        유저에게 보여줄 문장
    """
    # 드라이버 코드를 읽기 쉬운 이름으로 변환
    driver_names = {
        "OIL_PRICE_WTI": "유가",
        "EXCHANGE_RATE": "환율",
        "INTEREST_RATE": "금리",
        "AI_TECH": "AI 기술",
        "SEMICONDUCTOR": "반도체",
        "BATTERY": "배터리",
        "EV": "전기차",
        "HYDROGEN": "수소",
        "BIO": "바이오",
        "MACRO_EFFECT": "시장 효과",
        "MACRO_STRATEGY": "시장 전략",
        "STYLE_FACTOR": "스타일 팩터",
        "STYLE_ROTATION": "스타일 로테이션",
        "FLOW_FACTOR": "자금 흐름",
        "FLOW_FUND": "펀드 흐름",
        "FLOW_ETF": "ETF 흐름",
        "MACRO_WEEKLY": "주간 시장 동향",
        "MACRO_DAILY": "일일 시장 동향",
        "MACRO_MARKET": "시장 전반"
    }
    driver_name = driver_names.get(driver_code, driver_code)
    
    # 섹터 코드를 읽기 쉬운 이름으로 변환
    sector_names = {
        "SEC_SEMI": "반도체",
        "SEC_CHEM": "화학",
        "SEC_BIO": "바이오",
        "SEC_IT": "IT",
        "SEC_AUTO": "자동차",
        "SEC_BATTERY": "배터리",
        "SEC_GAME": "게임",
        "MARKET": "시장 전반"
    }
    sector_name = sector_names.get(target_sector_code, target_sector_code)
    
    # ⭐ P0-final-3: 리포트 타입에 따라 톤 구분
    if report_type == "MACRO":
        # MACRO 리포트: "가능성/환경" 톤
        # ❌ 금지: "이 종목은 1월 효과 수혜주입니다"
        # ✅ 권장: "시장에서는 1월 효과로 중소형주 강세가 나타나는 경향이 있으며, 해당 기업은 이 구간에서 상대적 관심을 받을 수 있습니다"
        if logic_summary:
            logic_short = logic_summary[:100] if len(logic_summary) > 100 else logic_summary
            sentence = f"최근 시장에서는 {logic_short} 경향이 나타나고 있습니다. "
        else:
            sentence = f"최근 시장에서는 {driver_name} 관련 변화가 관찰되고 있습니다. "
        
        if target_sector_code and target_sector_code != "MARKET":
            sentence += f"이러한 시장 환경 속에서 {sector_name} 업종은 상대적 관심을 받을 수 있으며, "
        
        if company_name and evidence_sentence:
            sentence += f"{company_name}은(는) 해당 구간에서 상대적 관심을 받을 수 있습니다."
        elif company_name:
            sentence += f"{company_name}은(는) 이러한 시장 환경의 영향을 받을 수 있습니다."
        else:
            sentence += "해당 구간에서 상대적 관심을 받을 수 있습니다."
    else:
        # COMPANY/INDUSTRY 리포트: "직접 노출" 톤
        if evidence_sentence:
            # 증거 문장이 있으면 그것을 활용
            sentence = f"{company_name}은(는) {sector_name} 섹터에 속하며, {driver_name} 변화에 영향을 받습니다. "
            sentence += f"산업 전반적으로는 '{logic_summary[:100] if logic_summary else '관련 동향'}' 추세가 있으며, "
            sentence += f"해당 기업의 경우 '{evidence_sentence[:150]}' 상황입니다."
        else:
            # 증거 문장이 없으면 기본 템플릿
            sentence = f"{company_name}은(는) {sector_name} 섹터에 속하며, {driver_name} 변화에 영향을 받습니다. "
            if logic_summary:
                sentence += f"산업 전반적으로는 '{logic_summary[:100]}' 추세가 있습니다."
            else:
                sentence += f"산업 전반적인 동향을 주시할 필요가 있습니다."
    
    return sentence

def generate_e2e_report(metrics_file: Path):
    """E2E 테스트 리포트 생성"""
    logger.info("=" * 80)
    logger.info("E2E 테스트 리포트 생성")
    logger.info("=" * 80)
    
    # Metrics 파일 로드
    if not metrics_file.exists():
        logger.error(f"Metrics 파일이 없습니다: {metrics_file}")
        return
    
    with open(metrics_file, 'r', encoding='utf-8') as f:
        metrics_data = json.load(f)
    
    # 리포트 생성
    report = {
        "timestamp": datetime.now().isoformat(),
        "test_type": "E2E_20_REPORTS",
        "summary": {
            "total_reports_processed": metrics_data.get("overall", {}).get("total_processed", 0),
            "end_to_end_success": metrics_data.get("overall", {}).get("end_to_end_success", 0),
            "end_to_end_success_rate": 0.0,
            "skipped_already_processed": metrics_data.get("overall", {}).get("skipped_already_processed", 0),
            "skipped_daily_cap": metrics_data.get("overall", {}).get("skipped_daily_cap", 0)
        },
        "stage_success_rates": {},
        "hold_reasons_top5": {},
        "conflict_ratio": 0.0,
        "avg_tokens_per_report": 0.0,
        "quality_gate_metrics": {},
        "recommendations": []
    }
    
    # 성공률 계산
    overall = metrics_data.get("overall", {})
    total = overall.get("total_processed", 0)
    if total > 0:
        report["summary"]["end_to_end_success_rate"] = (overall.get("end_to_end_success", 0) / total) * 100
    
    # Stage별 성공률
    stages = ["collection", "parsing", "ticker_matching", "driver_normalization", 
              "quality_gate", "insight_extraction", "edge_enrichment"]
    
    for stage in stages:
        stage_data = metrics_data.get("by_stage", {}).get(stage, {})
        stage_total = stage_data.get("total", 0)
        if stage_total > 0:
            success = stage_data.get("success", 0)
            report["stage_success_rates"][stage] = {
                "success_rate": (success / stage_total) * 100,
                "total": stage_total,
                "success": success,
                "failed": stage_data.get("failed", 0)
            }
    
    # HOLD 사유 Top 5 (quality_gate + idempotency)
    quality_gate_data = metrics_data.get("by_stage", {}).get("quality_gate", {})
    qg_hold_counts = quality_gate_data.get("hold_counts_by_reason", {})
    
    idempotency_data = metrics_data.get("by_stage", {}).get("idempotency", {})
    idem_hold_counts = idempotency_data.get("hold_counts_by_reason", {})
    
    # 통합
    all_hold_counts = {}
    for reason, count in qg_hold_counts.items():
        all_hold_counts[reason] = all_hold_counts.get(reason, 0) + count
    for reason, count in idem_hold_counts.items():
        all_hold_counts[reason] = all_hold_counts.get(reason, 0) + count
    
    sorted_holds = sorted(all_hold_counts.items(), key=lambda x: x[1], reverse=True)
    report["hold_reasons_top5"] = dict(sorted_holds[:5])
    
    # CONFLICT 비율
    enrichment_data = metrics_data.get("by_stage", {}).get("edge_enrichment", {})
    enrichment_total = enrichment_data.get("total", 0)
    if enrichment_total > 0:
        conflict_count = enrichment_data.get("conflict", 0)
        report["conflict_ratio"] = (conflict_count / enrichment_total) * 100
    
    # 평균 토큰/리포트
    total_tokens = 0
    for stage in ["ticker_matching", "driver_normalization", "insight_extraction"]:
        stage_data = metrics_data.get("by_stage", {}).get(stage, {})
        total_tokens += stage_data.get("total_tokens", 0)
    
    if total > 0:
        report["avg_tokens_per_report"] = total_tokens / total
    
    # Quality Gate 메트릭
    report["quality_gate_metrics"] = {
        "pass": quality_gate_data.get("pass", 0),
        "hold": quality_gate_data.get("hold", 0),
        "drop": quality_gate_data.get("drop", 0),
        "pass_rate": 0.0,
        "hold_rate": 0.0,
        "drop_rate": 0.0
    }
    
    qg_total = quality_gate_data.get("total", 0)
    if qg_total > 0:
        report["quality_gate_metrics"]["pass_rate"] = (quality_gate_data.get("pass", 0) / qg_total) * 100
        report["quality_gate_metrics"]["hold_rate"] = (quality_gate_data.get("hold", 0) / qg_total) * 100
        report["quality_gate_metrics"]["drop_rate"] = (quality_gate_data.get("drop", 0) / qg_total) * 100
    
    # 추천 사항
    if report["quality_gate_metrics"]["hold_rate"] > 30:
        report["recommendations"].append("HOLD 비율이 30%를 초과합니다. Quality Gate 임계값 조정을 고려하세요.")
    
    if report["conflict_ratio"] > 10:
        report["recommendations"].append("CONFLICT 비율이 10%를 초과합니다. Alignment 판정 로직 개선을 고려하세요.")
    
    if report["avg_tokens_per_report"] > 5000:
        report["recommendations"].append("평균 토큰 수가 5000을 초과합니다. 프롬프트 최적화를 고려하세요.")
    
    # ⭐ P0-α4: 라우팅 감사 데이터 로드 및 추가
    reports_dir = project_root / "reports"
    routing_audit_files = sorted(
        list(reports_dir.glob("routing_audit_*.json")),
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    if routing_audit_files:
        with open(routing_audit_files[0], 'r', encoding='utf-8') as f:
            routing_audit_data = json.load(f)
        report["routing_audit"] = {
            "total_reports": routing_audit_data.get("total_reports", 0),
            "company_reports_sample": routing_audit_data.get("company_reports_sample", [])[:10],
            "industry_reports_sample": routing_audit_data.get("industry_reports_sample", [])[:10]
        }
    else:
        report["routing_audit"] = {
            "note": "라우팅 감사 데이터가 없습니다."
        }
    
    # ⭐ P0-α5: 유저 아웃풋 생성 가능성 검증 (DB 쿼리)
    try:
        from sqlalchemy.orm import Session
        from app.db import SessionLocal
        from sqlalchemy import text
        
        db = SessionLocal()
        try:
            # 조건 A: company_edges에서 driver 기반 evidence_layer 1개 이상
            query_a = text("""
                SELECT COUNT(*) as count
                FROM edges e
                WHERE e.properties->'evidence_layer' IS NOT NULL
                AND jsonb_array_length(e.properties->'evidence_layer') > 0
                LIMIT 1
            """)
            result_a = db.execute(query_a).fetchone()
            condition_a_met = result_a[0] > 0 if result_a else False
            
            # 조건 B: industry_edges에서 sector 기반 logic_summary 1개 이상
            query_b = text("""
                SELECT COUNT(*) as count
                FROM industry_edges ie
                WHERE ie.logic_summary IS NOT NULL
                AND ie.logic_summary != ''
                LIMIT 1
            """)
            result_b = db.execute(query_b).fetchone()
            condition_b_met = result_b[0] > 0 if result_b else False
            
            # 조건 C: (A)와 (B)가 같은 sector에서 연결 (샘플 3개)
            query_c = text("""
                SELECT 
                    e.ticker,
                    e.driver_code,
                    ie.target_sector_code,
                    ie.logic_summary,
                    e.properties->'evidence_layer'->0->>'source_type' as evidence_source,
                    e.properties->'evidence_layer'->0->>'key_sentence' as evidence_sentence,
                    ie.relation_type,
                    cd.company_name
                FROM edges e
                JOIN industry_edges ie ON 1=1
                JOIN investor_sector isec ON isec.ticker = e.ticker
                LEFT JOIN company_details cd ON cd.ticker = e.ticker
                WHERE isec.major_sector = ie.target_sector_code
                AND e.properties->'evidence_layer' IS NOT NULL
                AND jsonb_array_length(e.properties->'evidence_layer') > 0
                AND ie.logic_summary IS NOT NULL
                AND ie.logic_summary != ''
                LIMIT 3
            """)
            result_c = db.execute(query_c).fetchall()
            condition_c_samples = []
            for row in result_c:
                ticker = row[0]
                driver_code = row[1]
                target_sector_code = row[2]
                logic_summary = row[3][:200] if row[3] else ""
                evidence_source = row[4]
                evidence_sentence = row[5] if row[5] else ""
                relation_type = row[6] if row[6] else ""
                company_name = row[7] if row[7] else f"기업({ticker})"
                
                # ⭐ 실제 유저 문장 생성 (템플릿 기반)
                # 리포트 타입 확인 (MACRO 리포트는 "가능성/환경" 톤 사용)
                report_type = "COMPANY"  # 기본값
                if target_sector_code == "MARKET":
                    report_type = "MACRO"
                elif target_sector_code and target_sector_code.startswith("SEC_"):
                    report_type = "INDUSTRY"
                
                user_sentence = generate_user_output_sentence(
                    company_name=company_name,
                    driver_code=driver_code,
                    target_sector_code=target_sector_code,
                    logic_summary=logic_summary,
                    evidence_sentence=evidence_sentence,
                    relation_type=relation_type,
                    report_type=report_type  # ⭐ 리포트 타입 전달
                )
                
                condition_c_samples.append({
                    "ticker": ticker,
                    "company_name": company_name,
                    "driver_code": driver_code,
                    "target_sector_code": target_sector_code,
                    "logic_summary": logic_summary,
                    "evidence_source": evidence_source,
                    "evidence_sentence": evidence_sentence,
                    "user_output_sentence": user_sentence  # ⭐ 추가: 실제 유저 문장
                })
            condition_c_met = len(condition_c_samples) > 0
            
            report["user_output_verification"] = {
                "condition_a": {
                    "description": "company_edges에서 driver 기반 evidence_layer 1개 이상",
                    "met": condition_a_met,
                    "count": result_a[0] if result_a else 0
                },
                "condition_b": {
                    "description": "industry_edges에서 sector 기반 logic_summary 1개 이상",
                    "met": condition_b_met,
                    "count": result_b[0] if result_b else 0
                },
                "condition_c": {
                    "description": "(A)와 (B)가 같은 sector에서 연결",
                    "met": condition_c_met,
                    "sample_count": len(condition_c_samples),
                    "samples": condition_c_samples
                },
                "all_conditions_met": condition_a_met and condition_b_met and condition_c_met
            }
        finally:
            db.close()
    except Exception as e:
        logger.warning(f"유저 아웃풋 검증 쿼리 실행 실패: {e}")
        report["user_output_verification"] = {
            "error": str(e)
        }
    
    # 리포트 저장
    output_file = project_root / "reports" / f"e2e_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    logger.info(f"\nE2E 테스트 리포트 저장: {output_file}")
    logger.info(f"\n요약:")
    logger.info(f"  - 총 처리 리포트: {report['summary']['total_reports_processed']}개")
    logger.info(f"  - E2E 성공률: {report['summary']['end_to_end_success_rate']:.2f}%")
    logger.info(f"  - HOLD 비율: {report['quality_gate_metrics']['hold_rate']:.2f}%")
    logger.info(f"  - CONFLICT 비율: {report['conflict_ratio']:.2f}%")
    logger.info(f"  - 평균 토큰/리포트: {report['avg_tokens_per_report']:.0f}")
    
    if report["hold_reasons_top5"]:
        logger.info(f"\n  HOLD 사유 Top 5:")
        for reason, count in list(report["hold_reasons_top5"].items())[:5]:
            logger.info(f"    - {reason}: {count}개")
    
    if report["recommendations"]:
        logger.info(f"\n  추천 사항:")
        for rec in report["recommendations"]:
            logger.info(f"    - {rec}")
    
    # ⭐ P0-α4: 라우팅 감사 출력
    if "routing_audit" in report and "company_reports_sample" in report["routing_audit"]:
        logger.info(f"\n  라우팅 감사 (샘플 20개):")
        logger.info(f"    - COMPANY 리포트 샘플: {len(report['routing_audit']['company_reports_sample'])}개")
        logger.info(f"    - INDUSTRY/MACRO 리포트 샘플: {len(report['routing_audit']['industry_reports_sample'])}개")
        logger.info(f"    상세 내용은 리포트 파일의 'routing_audit' 섹션을 참고하세요.")
    
    # ⭐ P0-α5: 유저 아웃풋 검증 출력
    if "user_output_verification" in report:
        verification = report["user_output_verification"]
        if "error" not in verification:
            logger.info(f"\n  유저 아웃풋 생성 가능성 검증:")
            logger.info(f"    - 조건 A (company_edges evidence_layer): {'✓' if verification['condition_a']['met'] else '✗'}")
            logger.info(f"    - 조건 B (industry_edges logic_summary): {'✓' if verification['condition_b']['met'] else '✗'}")
            logger.info(f"    - 조건 C (sector 연결): {'✓' if verification['condition_c']['met'] else '✗'}")
            logger.info(f"    - 전체 조건 충족: {'✓' if verification.get('all_conditions_met', False) else '✗'}")
            if verification.get("condition_c", {}).get("samples"):
                logger.info(f"    - 샘플 연결 경로: {len(verification['condition_c']['samples'])}개")
                for i, sample in enumerate(verification['condition_c']['samples'][:3], 1):
                    logger.info(f"      {i}. Driver: {sample.get('driver_code')} → Sector: {sample.get('target_sector_code')} → Ticker: {sample.get('ticker')}")
                    # ⭐ 유저 문장 출력
                    if sample.get('user_output_sentence'):
                        logger.info(f"         문장: {sample['user_output_sentence'][:150]}...")
    
    return output_file

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="E2E 테스트 리포트 생성")
    parser.add_argument("--metrics", type=str, help="Pipeline metrics JSON 파일 경로")
    args = parser.parse_args()
    
    if args.metrics:
        metrics_file = Path(args.metrics)
    else:
        # 최신 metrics 파일 찾기
        reports_dir = project_root / "reports"
        metrics_files = sorted(
            list(reports_dir.glob("pipeline_metrics_*.json")),
            key=lambda p: p.stat().st_mtime,
            reverse=True
        )
        if not metrics_files:
            logger.error("Pipeline metrics 파일이 없습니다.")
            return
        metrics_file = metrics_files[0]
        logger.info(f"최신 metrics 파일 사용: {metrics_file}")
    
    generate_e2e_report(metrics_file)

if __name__ == "__main__":
    main()

