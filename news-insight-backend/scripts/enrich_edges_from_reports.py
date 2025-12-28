"""
Edge Enrichment 통합 파이프라인 (Phase 2.0)

프로세스:
1. 파싱된 리포트 로드
2. Ticker 매칭
3. Driver 정규화
4. Quality Gate (LLM 직전) ⭐
5. Insight 추출
6. Edge Enrichment

⚠️ 중요: Quality Gate는 LLM 진입 직전에 호출
- Ticker/Driver 매칭 후
- Insight 추출 전
"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
import json
import logging
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from sqlalchemy.orm import Session
from app.db import SessionLocal

# Import extractors
from extractors.ticker_matcher import match_ticker
from extractors.driver_normalizer import normalize_driver
from extractors.quality_gate import evaluate_quality, check_market_cap_weight
from extractors.insight_extractor import extract_insights
from extractors.edge_enricher import enrich_edge_with_evidence
from extractors.kirs_logic_extractor import route_kirs_report
from extractors.industry_insight_router import save_industry_insight, enrich_company_edge_with_kirs, SaveResult
from app.services.llm_handler import LLMHandler
from utils.pipeline_metrics import PipelineMetrics
from utils.report_status_manager import ReportStatusManager
from utils.run_summary_generator import generate_run_summary, save_run_summary
from utils.text_normalizer import normalize_date_for_report_id

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def process_single_report(
    db: Session,
    report: Dict[str, Any],
    llm_handler: Optional[LLMHandler] = None,
    metrics: Optional[PipelineMetrics] = None,
    status_manager: Optional[ReportStatusManager] = None,
    daily_enrich_count: int = 0,
    daily_cap: int = 50
) -> Dict[str, Any]:
    """
    단일 리포트 처리 (트랜잭션 범위: 리포트 1개 단위)
    
    Args:
        db: DB 세션
        report: 파싱된 리포트 데이터
        llm_handler: LLM 핸들러 (재사용)
        metrics: 파이프라인 지표 (선택)
        status_manager: 리포트 상태 관리자 (선택)
        daily_enrich_count: 오늘 처리한 Enrichment 수
        daily_cap: 일일 최대 Enrichment 수
    
    Returns:
        처리 결과
    """
    result = {
        "report_id": report.get("report_id", ""),
        "title": report.get("title", "")[:50],
        "ticker": None,
        "driver_code": None,
        "quality_gate_status": None,
        "insights_extracted": False,
        "edge_enriched": False,
        "error": None,
        "error_type": None
    }
    
    # 리포트 상태 관리자 초기화
    if status_manager is None:
        status_manager = ReportStatusManager()
    
    # P0+1: Report-level Idempotency 체크 (LLM 호출 전)
    # ⭐ P0-D: force_candidate_extraction 모드 체크
    # ⭐ P0-1: rebuild_industry_insights 모드 체크
    force_candidate_extraction = False
    write_candidates = False
    rebuild_industry_insights = False
    if hasattr(process_single_report, '_force_candidate_extraction'):
        force_candidate_extraction = process_single_report._force_candidate_extraction
    if hasattr(process_single_report, '_write_candidates'):
        write_candidates = process_single_report._write_candidates
    if hasattr(process_single_report, '_rebuild_industry_insights'):
        rebuild_industry_insights = process_single_report._rebuild_industry_insights
    
    # ⭐ P0-1: rebuild 모드 체크 (ENRICHED 스킵과 무관하게 logic_summary만 재계산)
    if rebuild_industry_insights:
        try:
            from extractors.industry_insight_router import find_industry_edges_for_rebuild, rebuild_industry_insight_logic_summary
            
            # 리포트 메타데이터 구성
            report_metadata = {
                "report_id": report.get("report_id", ""),
                "broker_name": report.get("broker_name", ""),
                "report_date": report.get("date", ""),
                "title": report.get("title", ""),
                "source": report.get("source", "naver")
            }
            
            # Rebuild 대상 조회 (fallback 로직 포함)
            # ⭐ P0-추가2: 구조화된 반환값 처리
            rebuild_result = find_industry_edges_for_rebuild(db, report, report_metadata)
            
            existing_edges = rebuild_result.get("edges", [])
            matched_by = rebuild_result.get("matched_by")
            skipped_reason = rebuild_result.get("skipped_reason")
            candidate_count = rebuild_result.get("candidate_count", 0)
            
            # ⭐ P0-추가2: 스킵 이유 메트릭 기록
            if skipped_reason:
                if metrics:
                    metrics.record_rebuild_skip(skipped_reason, candidate_count)
                
                result["rebuild_skipped"] = True
                result["rebuild_skip_reason"] = skipped_reason
                result["rebuild_candidate_count"] = candidate_count
                result["rebuild_matched_by"] = matched_by
                logger.info(f"Rebuild 스킵: {skipped_reason} (candidate_count={candidate_count})")
                return result
            
            if existing_edges:
                # industry_logic 재구성 (기존 데이터 기반 또는 새로 추출)
                # 우선 기존 edge의 데이터를 사용하되, 필요시 새로 추출
                rebuild_count = 0
                
                for edge in existing_edges:
                    # industry_logic 재구성
                    industry_logic = {
                        "logic_summary": edge.logic_summary or "",  # 원본 (압축 전)
                        "key_sentence": edge.key_sentence or "",
                        "target_sector_code": edge.target_sector_code,
                        "target_drivers": [edge.source_driver_code] if edge.source_driver_code else [],
                        "target_type": edge.target_type or "SECTOR",
                        "conditions": edge.conditions or {}
                    }
                    
                    # 재빌드 실행
                    if rebuild_industry_insight_logic_summary(
                        db=db,
                        industry_edge=edge,
                        industry_logic=industry_logic
                    ):
                        rebuild_count += 1
                
                result["rebuild_count"] = rebuild_count
                result["rebuild_total"] = len(existing_edges)
                result["rebuild_matched_by"] = matched_by
                logger.info(f"Industry Insight 재빌드 완료: {rebuild_count}/{len(existing_edges)}개 (matched_by={matched_by})")
                return result
            else:
                logger.warning(f"Rebuild 대상 없음: report_id={report.get('report_id', 'N/A')}")
                result["error"] = "Rebuild 대상 없음"
                result["error_type"] = "REBUILD_NO_TARGET"
                return result
                
        except Exception as e:
            logger.error(f"Rebuild 실패: {e}", exc_info=True)
            result["error"] = str(e)
            result["error_type"] = "REBUILD_ERROR"
            return result
    
    if status_manager.should_skip_llm(report) and not force_candidate_extraction:
        logger.info(f"리포트 이미 Enriched 상태, LLM 호출 스킵: {report.get('title', 'N/A')[:50]}...")
        if metrics:
            metrics.record_idempotency_skip()
        result["error"] = "이미 처리된 리포트 (ENRICHED)"
        result["error_type"] = "ALREADY_PROCESSED"
        return result
    
    # ⭐ P0-D: force_candidate_extraction 모드일 때는 driver 후보 추출 강제 실행
    if force_candidate_extraction and status_manager.should_skip_llm(report):
        logger.info(f"[Force Mode] 이미 처리된 리포트에서 driver 후보 추출 강제 실행: {report.get('title', 'N/A')[:50]}...")
        try:
            from scripts.extract_driver_candidates_from_original_text import register_candidates_from_failed_extraction
            original_text = report.get("full_text", "") or report.get("summary", "")
            if original_text:
                candidate_result = register_candidates_from_failed_extraction(
                    original_text=original_text,
                    report_metadata={
                        "report_id": report.get("report_id", ""),
                        "title": report.get("title", ""),
                        "broker_name": report.get("broker_name", ""),
                        "report_date": report.get("date", "")
                    },
                    dry_run=not write_candidates  # ⭐ dry-run 모드
                )
                # 메트릭 기록
                if metrics and candidate_result:
                    metrics.record_driver_candidate_extraction(
                        success=(candidate_result.get("registered_count", 0) > 0),
                        original_chunk_len=candidate_result.get("metrics", {}).get("original_chunk_len", 0),
                        terms_extracted_total=candidate_result.get("metrics", {}).get("terms_extracted_total", 0),
                        terms_known_count=candidate_result.get("metrics", {}).get("terms_known_count", 0),
                        terms_unknown_count=candidate_result.get("metrics", {}).get("terms_unknown_count", 0),
                        inserted_count=candidate_result.get("metrics", {}).get("candidate_rows_inserted_count", 0),
                        skipped_count=candidate_result.get("metrics", {}).get("candidate_insert_skipped_cooldown_count", 0),
                        skipped_unique_conflict=candidate_result.get("metrics", {}).get("skipped_unique_conflict_count", 0)
                    )
                if not write_candidates:
                    logger.info(f"  [Dry-run] 후보 추출 완료 (DB 저장 안 함): {candidate_result.get('registered_count', 0)}개")
                else:
                    logger.info(f"  [Write] 후보 추출 및 저장 완료: {candidate_result.get('registered_count', 0)}개")
        except Exception as e:
            logger.warning(f"Force candidate extraction 실패: {e}")
        
        # force 모드에서는 여기서 종료 (LLM 호출 안 함)
        result["error"] = "Force candidate extraction 완료 (LLM 호출 스킵)"
        result["error_type"] = "FORCE_CANDIDATE_EXTRACTION"
        return result
    
    # P0+3: LLM 호출은 트랜잭션 밖에서 수행
    # 트랜잭션은 DB write만 짧게 감싼다
    
    try:
        # 상태를 PARSED_OK로 설정 (처리 시작, 트랜잭션 밖)
        status_manager.set_status(report, "PARSED_OK")
        
        # ⭐ P0-1: 리포트 타입 기반 라우팅 (KIRS 감지 전에)
        source = report.get("source", "")
        report_type = report.get("report_type", "")
        category = report.get("category", "")
        
        # 리포트 타입 결정 (Naver 리포트의 경우)
        is_naver_report = source == "네이버" or "naver" in source.lower()
        is_kirs_report = source == "한국IR협의회" or "kirs" in source.lower()
        
        # ⭐ P0-α4: 라우팅 감사 데이터 (로컬 변수로 선언)
        audit_entry = None
        
        # ⭐ P0-1: Naver 리포트의 경우 report_type 확인 및 라우팅
        if is_naver_report:
            from extractors.naver_report_router import route_naver_report, determine_report_type
            
            # ⭐ 개선: route_naver_report()를 먼저 호출하여 report_type과 route_confidence 확보
            route_result = route_naver_report(report)
            route_type = route_result.get("route_type", "UNKNOWN")
            route_confidence = route_result.get("route_confidence", 0.0)
            
            # route_result에서 추출한 정보를 report에 반영
            report["report_type"] = route_type
            report["route_confidence"] = route_confidence
            report["route_evidence"] = route_result.get("route_evidence", [])
            report["safety_latch_applied"] = route_result.get("safety_latch_applied", False)
            report["original_type"] = route_result.get("original_type", "UNKNOWN")
            
            # ⭐ P0-α4: 라우팅 감사 데이터 기록
            audit_entry = {
                "report_id": report.get("report_id", ""),
                "title": report.get("title", "")[:50],
                "original_category": report.get("category", ""),
                "determined_report_type": route_type,
                "route_confidence": route_confidence,
                "route_confidence_breakdown": route_result.get("route_confidence_breakdown", {}),
                "route_evidence": route_result.get("route_evidence", []),
                "safety_latch_applied": route_result.get("safety_latch_applied", False),
                "original_type": route_result.get("original_type", "UNKNOWN"),
                "final_routing": "UNKNOWN",  # 최종 라우팅은 아래에서 결정
                "hold_reason": None
            }
            
            # ⭐ MIXED 리포트 처리 (섹션 분리 필요)
            if route_type == "MIXED":
                logger.info(f"Naver MIXED 리포트 감지: {report.get('title', 'N/A')[:50]}... (기업/산업 섹션 분리 필요)")
                audit_entry["final_routing"] = "MIXED_PROCESSING"
                return process_mixed_report(
                    db=db,
                    report=report,
                    metrics=metrics,
                    status_manager=status_manager,
                    daily_enrich_count=daily_enrich_count,
                    daily_cap=daily_cap,
                    routing_audit_entry=audit_entry
                )
            
            # 산업/매크로 리포트는 industry 라우팅 (ticker 매칭 시도하지 않음)
            if route_type in ["INDUSTRY", "MACRO"]:
                logger.info(f"Naver 산업/매크로 리포트 감지: {report.get('title', 'N/A')[:50]}... (type: {route_type}, confidence: {route_confidence:.2f})")
                # ⭐ P0-final-2: MACRO 리포트는 driver 후보를 오염시키지 않음을 명확히 기록
                if route_type == "MACRO":
                    logger.debug(
                        f"MACRO 리포트 처리: driver_candidates 생성 스킵 "
                        f"(오직 industry_edges로만 라우팅, driver_normalizer 호출 안 함)"
                    )
                audit_entry["final_routing"] = "INDUSTRY_EDGES"
                if metrics:
                    metrics.record_ticker_matching(False, skipped=True)  # ticker 매칭 스킵 기록
                return process_naver_industry_report(
                    db=db,
                    report=report,
                    metrics=metrics,
                    status_manager=status_manager,
                    daily_enrich_count=daily_enrich_count,
                    daily_cap=daily_cap,
                    route_result=route_result,  # route_result 전달
                    routing_audit_entry=audit_entry
                )
        
        # KIRS 리포트 감지 및 라우팅
        if is_kirs_report and (report.get("checkpoint") or report.get("sections")):
            # KIRS 리포트 전용 처리
            logger.info(f"KIRS 리포트 감지: {report.get('title', 'N/A')[:50]}...")
            return process_kirs_report(
                db=db,
                report=report,
                metrics=metrics,
                status_manager=status_manager,
                daily_enrich_count=daily_enrich_count,
                daily_cap=daily_cap
            )
        
        # 일반 리포트 처리 (기존 로직) - COMPANY 리포트만
        # 1. Ticker 매칭
        logger.info(f"리포트 처리 시작: {report.get('title', 'N/A')[:50]}...")
        
        # ⭐ 개선: 종목분석 리스트에서 추출한 stock_name을 최우선 활용
        # 1순위: 리스트에서 추출한 종목명 (종목분석 리포트)
        # 2순위: 상세 페이지에서 추출한 종목명
        stock_name = report.get("stock_name")  # 리스트 또는 상세 페이지에서 추출한 종목명
        ticker_result = None
        
        # ⭐ 종목분석 리포트: 리스트에서 추출한 종목명을 1순위로 사용
        if stock_name and report.get("category") == "종목분석":
            # 종목명으로 직접 ticker 조회 시도
            from sqlalchemy import text
            # ⭐ SQL 파라미터 바인딩 수정: PostgreSQL은 %s 대신 :name 사용
            ticker_query = text("""
                SELECT ticker, stock_name 
                FROM stocks 
                WHERE stock_name = :name OR stock_name LIKE :name_pattern
            """)
            ticker_rows = db.execute(ticker_query, {
                "name": stock_name,
                "name_pattern": f"%{stock_name}%"
            }).fetchall()
            
            # ⭐ P0-β1: 2단계 확인
            if len(ticker_rows) == 1:
                # 결과가 1개면 확정값 (confidence = 1.0)
                ticker_row = ticker_rows[0]
                ticker_result = {
                    "ticker": ticker_row[0],
                    "company_name": ticker_row[1],
                    "confidence": 1.0,  # 검증된 확정값
                    "method": "HTML_STOCK_NAME_TAG"
                }
                logger.info(f"상세 페이지 종목명 태그에서 ticker 확정: {stock_name} → {ticker_row[0]}")
            elif len(ticker_rows) > 1:
                # 결과가 2개 이상이면 애매함 → HOLD
                result["error"] = f"Ticker 애매함: {stock_name}에 대해 {len(ticker_rows)}개 후보 발견"
                result["error_type"] = "HOLD_TICKER_AMBIGUOUS"
                result["hold_metadata"] = {
                    "hold_reason": "HOLD_TICKER_AMBIGUOUS",
                    "stock_name": stock_name,
                    "candidate_count": len(ticker_rows),
                    "retry_eligible": True,
                    "retry_after": "ticker_disambiguation_v2"
                }
                logger.warning(f"Ticker 애매함: {stock_name} → {len(ticker_rows)}개 후보 (HOLD_TICKER_AMBIGUOUS)")
                status_manager.set_status(report, "HOLD")
                if metrics:
                    metrics.record_hold_reason("HOLD_TICKER_AMBIGUOUS")
                return result
            else:
                # 결과가 0개면 찾을 수 없음 → HOLD
                result["error"] = f"Ticker 찾을 수 없음: {stock_name}"
                result["error_type"] = "HOLD_TICKER_NOT_FOUND"
                result["hold_metadata"] = {
                    "hold_reason": "HOLD_TICKER_NOT_FOUND",
                    "stock_name": stock_name,
                    "retry_eligible": True,
                    "retry_after": "ticker_mapping_v2"
                }
                logger.warning(f"Ticker 찾을 수 없음: {stock_name} (HOLD_TICKER_NOT_FOUND)")
                status_manager.set_status(report, "HOLD")
                if metrics:
                    metrics.record_hold_reason("HOLD_TICKER_NOT_FOUND")
                return result
        
        # 상세 페이지에서 추출하지 못한 경우 기존 로직 사용
        if not ticker_result:
            # ⭐ 종목분석 리포트: stock_name을 파라미터로 전달
            ticker_result = match_ticker(
                title=report.get("title", ""),
                text_head=report.get("full_text", "")[:1000],  # 앞부분만 사용
                db=db,
                stock_name=stock_name  # ⭐ 종목분석 리스트에서 추출한 종목명 전달
            )
        
        if not ticker_result or not ticker_result.get("ticker"):
            result["error"] = "Ticker 매칭 실패"
            result["error_type"] = "TICKER_MATCHING_FAILED"
            match_category = ticker_result.get("match_category", "NOT_FOUND") if ticker_result else "NOT_FOUND"
            logger.warning(f"Ticker 매칭 실패: {report.get('title', 'N/A')[:50]}... (category: {match_category})")
            if metrics:
                metrics.record_ticker_matching(False, match_category=match_category)
            # 실패 시 상태 업데이트 (트랜잭션 밖)
            status_manager.set_status(report, "MATCHED_HOLD")
            return result
        
        ticker = ticker_result["ticker"]
        result["ticker"] = ticker
        match_category = ticker_result.get("match_category", "MATCHED_IN_UNIVERSE")  # 기본값
        logger.info(f"Ticker 매칭 성공: {ticker} (method: {ticker_result.get('method', 'UNKNOWN')}, category: {match_category})")
        if metrics:
            metrics.record_ticker_matching(True, ticker_result.get("method"), match_category=match_category)
        
        # 2. Driver 정규화
        report_text = report.get("full_text", "")
        if not report_text:
            report_text = " ".join([p.get("text", "") for p in report.get("paragraphs", [])])
        
        # 리포트 제목과 본문 조합
        combined_text = f"{report.get('title', '')} {report_text[:1000]}"
        
        driver_result = normalize_driver(
            report_text=combined_text,
            use_llm=True,  # LLM 사용
            db=db,
            report_metadata={  # ⭐ 후보 등록용 메타데이터
                "report_id": report.get("report_id", ""),
                "title": report.get("title", ""),
                "broker_name": report.get("broker_name", ""),
                "report_date": report.get("date", "")
            }
        )
        
        # ⭐ 개선: 원문 텍스트 보존 (후보 추출용)
        original_text = report.get("full_text", "") or report.get("parsed_content", "") or report_text
        
        if not driver_result or not driver_result.get("driver_code"):
            result["error"] = "Driver 정규화 실패"
            result["error_type"] = "DRIVER_NORMALIZATION_FAILED"
            logger.warning(f"Driver 정규화 실패")
            if metrics:
                metrics.record_driver_normalization(False)
            
            # ⭐ 개선: Driver 추출 실패 시에도 원문에서 후보 추출
            # ⭐ P0-3: 원인 3분류 계측
            try:
                from scripts.extract_driver_candidates_from_original_text import register_candidates_from_failed_extraction
                candidate_result = register_candidates_from_failed_extraction(
                    original_text=original_text,
                    report_metadata={
                        "report_id": report.get("report_id", ""),
                        "title": report.get("title", ""),
                        "broker_name": report.get("broker_name", ""),
                        "report_date": report.get("date", "")
                    }
                )
                
                registered_count = candidate_result.get("registered_count", 0)
                candidate_metrics = candidate_result.get("metrics", {})
                
                if registered_count > 0:
                    logger.info(f"Driver 추출 실패 시 원문에서 후보 {registered_count}개 등록")
                else:
                    # 원인 분석 로깅
                    logger.warning(
                        f"후보 등록 실패 - "
                        f"원문길이: {candidate_metrics.get('original_chunk_len', 0)}, "
                        f"추출: {candidate_metrics.get('candidate_terms_extracted_count', 0)}, "
                        f"저장: {candidate_metrics.get('candidate_rows_inserted_count', 0)}, "
                        f"스킵: {candidate_metrics.get('candidate_insert_skipped_cooldown_count', 0)}"
                    )
                
                # 메트릭 기록
                if metrics:
                    metrics.record_driver_candidate_extraction(
                        success=(registered_count > 0),
                        original_chunk_len=candidate_metrics.get('original_chunk_len', 0),
                        extracted_count=candidate_metrics.get('candidate_terms_extracted_count', 0),
                        inserted_count=candidate_metrics.get('candidate_rows_inserted_count', 0),
                        skipped_count=candidate_metrics.get('candidate_insert_skipped_cooldown_count', 0),
                        skipped_unique_conflict=candidate_metrics.get('candidate_insert_skipped_unique_conflict_count', 0),
                        terms_known_count=candidate_metrics.get('terms_known_count', 0),
                        terms_unknown_count=candidate_metrics.get('terms_unknown_count', 0)
                    )
            except Exception as e:
                logger.warning(f"원문 후보 추출 실패 (계속 진행): {e}")
            
            return result
        
        driver_code = driver_result["driver_code"]
        result["driver_code"] = driver_code
        logger.info(f"Driver 정규화 성공: {driver_code} (method: {driver_result.get('method', 'UNKNOWN')})")
        if metrics:
            metrics.record_driver_normalization(
                True, 
                driver_result.get("method"),
                single_candidate=(len(driver_result.get("all_candidates", [])) == 1)
            )
        
        # 3. Quality Gate (LLM 직전) ⭐
        quality_result = evaluate_quality(
            parsed_report=report,
            ticker=ticker,
            driver_code=driver_code
        )
        
        quality_status = quality_result["status"]
        result["quality_gate_status"] = quality_status
        
        # ⭐ P0+: Market Cap Weight 체크 (선택적)
        if ticker and quality_status == "PASS":
            from extractors.quality_gate import check_market_cap_weight
            market_cap_info = check_market_cap_weight(ticker=ticker, db=db)
            
            if market_cap_info.get("strict_mode", False):
                # 시가총액 상위 20% 기업은 더 엄격한 기준 적용
                # 예: 한글 문자 수 200자 이상, meaningful_paragraphs 3개 이상
                scores = quality_result.get("scores", {})
                korean_chars = scores.get("korean_char_count", 0)
                meaningful_paras = scores.get("meaningful_paragraphs", 0)
                
                if korean_chars < 200 or meaningful_paras < 3:
                    quality_status = "HOLD"
                    quality_result["status"] = "HOLD"
                    quality_result["reason"] = f"시가총액 상위 기업 엄격 기준 미달 (한글: {korean_chars}자, 문단: {meaningful_paras}개)"
                    quality_result["hold_metadata"] = {
                        "hold_reason": "MARKET_CAP_STRICT_MODE",
                        "retry_eligible": True,
                        "retry_after": "parser_v2"
                    }
                    logger.info(f"시가총액 상위 기업 엄격 기준 적용: ticker={ticker}, market_cap={market_cap_info.get('market_cap')}")
        
        result["quality_gate_status"] = quality_status
        
        if metrics:
            metrics.record_quality_gate(quality_status)
            if quality_result.get("hold_metadata"):
                metrics.record_hold_reason(quality_result["hold_metadata"].get("hold_reason", "UNKNOWN"))
        
        # DROP: 즉시 종료
        if quality_status == "DROP":
            result["error"] = f"Quality Gate DROP: {quality_result['reason']}"
            result["error_type"] = "QUALITY_GATE_DROP"
            logger.warning(f"Quality Gate DROP: {quality_result['reason']}")
            return result
        
        # HOLD: 저장 후 종료 (재처리 대상)
        if quality_status == "HOLD":
            result["error"] = f"Quality Gate HOLD: {quality_result['reason']}"
            result["error_type"] = "QUALITY_GATE_HOLD"
            result["hold_metadata"] = quality_result.get("hold_metadata")
            logger.info(f"Quality Gate HOLD: {quality_result['reason']}")
            
            # HOLD 사유 기록
            if metrics and quality_result.get("hold_metadata"):
                hold_reason = quality_result["hold_metadata"].get("hold_reason", "UNKNOWN")
                metrics.record_hold_reason(hold_reason)
            
            # 상태 업데이트 (HOLD는 실패가 아님, 트랜잭션 밖)
            status_manager.set_status(report, "PARSED_HOLD")
            
            return result
        
        # PASS: 계속 진행
        
        # 4. Daily Cap 체크
        if daily_enrich_count >= daily_cap:
            result["error"] = f"Daily Auto-Enrich Cap 도달 ({daily_cap}건)"
            result["error_type"] = "DAILY_CAP_REACHED"
            result["quality_gate_status"] = "HOLD_AUTO_LIMIT"
            logger.warning(f"Daily Cap 도달: {daily_enrich_count}/{daily_cap}")
            return result
        
        # 5. Market Cap Weight 체크 (선택적)
        market_cap_info = check_market_cap_weight(ticker, db)
        if market_cap_info.get("strict_mode"):
            logger.info(f"시가총액 상위 20% 기업: 엄격 모드 적용 ({ticker})")
        
        # 6. Insight 추출 (LLM) - 트랜잭션 밖에서 수행 ⭐
        if llm_handler is None:
            llm_handler = LLMHandler()
        
        # LLM 호출 (DB 연결 없이)
        insights = extract_insights(
            report_text=report_text,
            report_title=report.get("title", ""),
            driver_code=driver_code,
            llm_handler=llm_handler
        )
        
        # 토큰 사용량 기록
        if metrics and insights.get("_usage"):
            usage = insights.get("_usage", {})
            metrics.record_llm_usage(
                prompt_tokens=usage.get("prompt_tokens", 0),
                completion_tokens=usage.get("completion_tokens", 0),
                stage="insight_extractor"
            )
        
        if not insights.get("analyst_logic"):
            result["error"] = "Insight 추출 실패"
            result["error_type"] = "INSIGHT_EXTRACTION_FAILED"
            logger.warning(f"Insight 추출 실패")
            if metrics:
                metrics.record_insight_extraction(False)
            # 실패 시 상태 업데이트 (트랜잭션 밖)
            status_manager.set_status(report, "FAILED_RETRYABLE")
            return result
        
        result["insights_extracted"] = True
        confidence = insights.get("extraction_confidence", "LOW")
        logger.info(f"Insight 추출 성공 (confidence: {confidence})")
        if metrics:
            metrics.record_insight_extraction(True, confidence)
        
        # 7. Edge Enrichment (트랜잭션 시작 - DB write만)
        report_metadata = {
            "title": report.get("title", ""),
            "broker_name": report.get("broker_name", ""),
            "report_date": report.get("date", ""),
            "report_id": report.get("report_id", f"naver_{report.get('date', '')}_{hash(report.get('title', ''))}"),
            "text_head": report_text[:500],
            "pdf_url": report.get("pdf_url"),
            "url": report.get("url")
        }
        
        # 트랜잭션 시작 (DB write만)
        try:
            success, alignment, failure_reason = enrich_edge_with_evidence(
                db=db,
                ticker=ticker,
                driver_code=driver_code,
                insights=insights,
                report_metadata=report_metadata
            )
            
            if not success:
                result["error"] = "Edge Enrichment 실패"
                result["error_type"] = "EDGE_ENRICHMENT_FAILED"
                result["failure_reason"] = failure_reason  # ⭐ 실패 원인 추가
                logger.warning(f"Edge Enrichment 실패 (reason: {failure_reason})")
                if metrics:
                    metrics.record_edge_enrichment(False, failure_reason=failure_reason)
                # 실패 시 롤백
                db.rollback()
                status_manager.set_status(report, "FAILED_RETRYABLE")
                return result
            
            result["edge_enriched"] = True
            result["alignment"] = alignment
            logger.info(f"Edge Enrichment 성공 (alignment: {alignment})")
            if metrics:
                metrics.record_edge_enrichment(True, alignment, failure_reason=None)
            
            # 성공 시 커밋 (짧게)
            db.commit()
            
            # 커밋 성공 후 상태 업데이트 (트랜잭션 밖)
            status_manager.set_status(report, "ENRICHED")
            
        except Exception as e:
            # 트랜잭션 내부 에러
            logger.error(f"Edge Enrichment 트랜잭션 오류: {e}", exc_info=True)
            db.rollback()
            status_manager.set_status(report, "FAILED_RETRYABLE")
            result["error"] = f"트랜잭션 오류: {str(e)}"
            result["error_type"] = "TRANSACTION_ERROR"
            return result
        
        return result
        
    except Exception as e:
        logger.error(f"리포트 처리 중 오류: {e}", exc_info=True)
        result["error"] = str(e)
        result["error_type"] = type(e).__name__
        
        # 실패 시 상태 업데이트 및 롤백
        status_manager.set_status(report, "FAILED")
        db.rollback()
        
        return result


def process_mixed_report(
    db: Session,
    report: Dict[str, Any],
    metrics: Optional[PipelineMetrics] = None,
    status_manager: Optional[ReportStatusManager] = None,
    daily_enrich_count: int = 0,
    daily_cap: int = 50
) -> Dict[str, Any]:
    """
    ⭐ MIXED 리포트 처리 (기업 섹션 + 산업 섹션 분리)
    
    정책:
    - 기업 섹션: edges로만
    - 산업 섹션: industry_edges로만
    - 섹션 분리 실패하면: HOLD_MIXED_UNSPLIT (저장만, KG 반영 금지)
    
    Args:
        db: DB 세션
        report: 파싱된 Naver MIXED 리포트 데이터
        metrics: 파이프라인 지표
        status_manager: 리포트 상태 관리자
        daily_enrich_count: 오늘 처리한 Enrichment 수
        daily_cap: 일일 최대 Enrichment 수
    
    Returns:
        처리 결과
    """
    result = {
        "report_id": report.get("report_id", ""),
        "title": report.get("title", "")[:50],
        "ticker": None,
        "route_type": "MIXED",
        "company_edge_enriched": False,
        "industry_edge_enriched": False,
        "section_split_success": False,
        "error": None,
        "error_type": None
    }
    
    try:
        # 섹션 분리 시도
        full_text = report.get("full_text", "")
        text_length = len(full_text)
        
        if text_length < 500:
            # 텍스트가 너무 짧으면 섹션 분리 불가
            result["error"] = "MIXED 리포트 섹션 분리 실패: 텍스트가 너무 짧음"
            result["error_type"] = "HOLD_MIXED_UNSPLIT"
            result["hold_metadata"] = {
                "hold_reason": "HOLD_MIXED_UNSPLIT",
                "retry_eligible": True,
                "retry_after": "section_split_v2",
                "reason": "텍스트가 너무 짧아 섹션 분리 불가"
            }
            logger.warning(f"MIXED 리포트 섹션 분리 실패: {report.get('title', 'N/A')[:50]}...")
            status_manager.set_status(report, "HOLD_MIXED_UNSPLIT", result["hold_metadata"])
            if metrics:
                metrics.record_hold_reason("HOLD_MIXED_UNSPLIT")
            return result
        
        # 간단한 섹션 분리: 앞부분(기업), 뒷부분(산업)
        # 실제로는 LLM이나 더 정교한 로직이 필요하지만, 일단 기본 구조만
        split_point = text_length // 2
        company_section = full_text[:split_point]
        industry_section = full_text[split_point:]
        
        # ⭐ split_method 기록 (routing_audit용)
        result["split_method"] = "heuristic"  # 현재는 휴리스틱 방식
        
        # 기업 섹션 처리 (ticker 매칭 시도)
        from extractors.ticker_matcher import match_ticker
        stock_name_from_report = report.get("stock_name")
        ticker_result = match_ticker(
            title=report.get("title", ""),
            text_head=company_section[:1000],
            db=db,
            stock_name=stock_name_from_report  # ⭐ 종목분석 리스트에서 추출한 종목명 전달
        )
        
        company_processed = False
        if ticker_result and ticker_result.get("ticker"):
            logger.info(f"MIXED 리포트 기업 섹션 처리: ticker={ticker_result['ticker']}")
            # 실제로는 더 정교한 처리가 필요하지만, 일단 표시만
            company_processed = True
            result["ticker"] = ticker_result["ticker"]
        
        # 산업 섹션 처리
        industry_report = {**report, "full_text": industry_section, "report_type": "INDUSTRY"}
        industry_result = process_naver_industry_report(
            db=db,
            report=industry_report,
            metrics=metrics,
            status_manager=status_manager,
            daily_enrich_count=daily_enrich_count,
            daily_cap=daily_cap
        )
        
        # 결과 통합
        result["section_split_success"] = company_processed or industry_result.get("industry_edge_enriched", False)
        result["company_edge_enriched"] = company_processed
        result["industry_edge_enriched"] = industry_result.get("industry_edge_enriched", False)
        
        if not result["section_split_success"]:
            # 섹션 분리 실패
            result["error"] = "MIXED 리포트 섹션 분리 실패: 기업/산업 섹션 모두 처리 실패"
            result["error_type"] = "HOLD_MIXED_UNSPLIT"
            result["hold_metadata"] = {
                "hold_reason": "HOLD_MIXED_UNSPLIT",
                "retry_eligible": True,
                "retry_after": "section_split_v2",
                "reason": "기업/산업 섹션 모두 처리 실패"
            }
            status_manager.set_status(report, "HOLD_MIXED_UNSPLIT", result["hold_metadata"])
            if metrics:
                metrics.record_hold_reason("HOLD_MIXED_UNSPLIT")
        else:
            # 섹션 분리 성공
            status_manager.set_status(report, "ENRICHED")
            if metrics:
                if company_processed:
                    metrics.record_edge_enrichment(True, "COMPANY")
                if industry_result.get("industry_edge_enriched"):
                    metrics.record_edge_enrichment(True, "INDUSTRY")
        
        return result
    
    except Exception as e:
        logger.error(f"MIXED 리포트 처리 중 오류: {e}", exc_info=True)
        result["error"] = str(e)
        result["error_type"] = type(e).__name__
        result["hold_metadata"] = {
            "hold_reason": "HOLD_MIXED_UNSPLIT",
            "retry_eligible": True,
            "retry_after": "section_split_v2",
            "reason": f"처리 중 오류: {str(e)}"
        }
        status_manager.set_status(report, "HOLD_MIXED_UNSPLIT", result["hold_metadata"])
        if metrics:
            metrics.record_hold_reason("HOLD_MIXED_UNSPLIT")
        return result


def process_naver_industry_report(
    db: Session,
    report: Dict[str, Any],
    metrics: Optional[PipelineMetrics] = None,
    status_manager: Optional[ReportStatusManager] = None,
    daily_enrich_count: int = 0,
    daily_cap: int = 50,
    route_result: Optional[Dict[str, Any]] = None,  # ⭐ route_naver_report() 결과 (중복 호출 방지)
    routing_audit_entry: Optional[Dict[str, Any]] = None  # ⭐ P0-α4: 라우팅 감사 데이터
) -> Dict[str, Any]:
    """
    네이버 산업/매크로 리포트 전용 처리 (industry_edges로 라우팅)
    
    Args:
        db: DB 세션
        report: 파싱된 Naver 리포트 데이터
        metrics: 파이프라인 지표
        status_manager: 리포트 상태 관리자
        daily_enrich_count: 오늘 처리한 Enrichment 수
        daily_cap: 일일 최대 Enrichment 수
    
    Returns:
        처리 결과
    """
    result = {
        "report_id": report.get("report_id", ""),
        "title": report.get("title", "")[:50],
        "ticker": None,
        "route_type": report.get("report_type", "INDUSTRY"),
        "company_edge_enriched": False,
        "industry_edge_enriched": False,
        "error": None,
        "error_type": None
    }
    
    report_metadata = {
        "title": report.get("title", ""),
        "broker_name": report.get("broker_name", "네이버"),
        "report_date": report.get("date", ""),
        "report_id": report.get("report_id", ""),
        "text_head": report.get("full_text", "")[:500] if report.get("full_text") else "",
        "pdf_url": report.get("pdf_url"),
        "url": report.get("url"),
        "parser_version": report.get("parser_version", "v1.0"),
        "cleaning_profile": report.get("cleaning_profile", "naver_default")
    }
    
    try:
        # ⭐ P0-α2: route_confidence 게이트 (company 리포트와 동일한 "입국심사")
        # ⭐ route_result가 없으면 route_naver_report() 호출 (중복 호출 방지)
        if not route_result:
            from extractors.naver_report_router import route_naver_report
            route_result = route_naver_report(report)
        
        route_confidence = route_result.get("route_confidence", 0.0)
        route_type = route_result.get("route_type", "INDUSTRY")
        
        # route_confidence threshold (환경변수 또는 기본값)
        import os
        confidence_threshold = float(os.getenv("ROUTE_CONFIDENCE_THRESHOLD", "0.6"))
        confidence_pass_threshold = float(os.getenv("ROUTE_CONFIDENCE_PASS_THRESHOLD", "0.7"))
        
        result["route_confidence"] = route_confidence
        result["route_evidence"] = route_result.get("route_evidence", [])
        result["safety_latch_applied"] = route_result.get("safety_latch_applied", False)
        result["original_type"] = route_result.get("original_type", "UNKNOWN")
        
        # route_confidence 게이트 체크
        if route_confidence < confidence_threshold:
            result["error"] = f"Route Confidence HOLD (LOW): {route_confidence:.2f} < {confidence_threshold}"
            result["error_type"] = "HOLD_ROUTE_LOW"
            result["hold_metadata"] = {
                "hold_reason": "HOLD_ROUTE_LOW",
                "route_confidence": route_confidence,
                "retry_eligible": True,
                "retry_after": "route_logic_v2"
            }
            logger.warning(f"Route Confidence HOLD (LOW): {report.get('title', 'N/A')[:50]}... (confidence: {route_confidence:.2f})")
            status_manager.set_status(report, "HOLD_ROUTE_LOW", result["hold_metadata"])
            if metrics:
                metrics.record_hold_reason("HOLD_ROUTE_LOW")
                metrics.record_route_confidence_bucket(route_confidence)
            return result
        
        if confidence_threshold <= route_confidence < confidence_pass_threshold:
            result["error"] = f"Route Confidence HOLD (MED): {route_confidence:.2f} (상위 후보, 관측 필요)"
            result["error_type"] = "HOLD_ROUTE_MED"
            result["hold_metadata"] = {
                "hold_reason": "HOLD_ROUTE_MED",
                "route_confidence": route_confidence,
                "retry_eligible": True,
                "retry_after": "threshold_calibration"
            }
            logger.info(f"Route Confidence HOLD (MED): {report.get('title', 'N/A')[:50]}... (confidence: {route_confidence:.2f})")
            status_manager.set_status(report, "HOLD_ROUTE_MED", result["hold_metadata"])
            if metrics:
                metrics.record_hold_reason("HOLD_ROUTE_MED")
                metrics.record_route_confidence_bucket(route_confidence)
            return result
        
        # route_confidence >= confidence_pass_threshold: PASS 후보 (다음 게이트로 진행)
        
        # ⭐ P0-α2: Quality Gate 통과 여부 확인 (LLM 호출 전)
        quality_result = evaluate_quality(
            parsed_report=report,
            ticker=None,  # 산업 리포트는 ticker 없음
            driver_code=None
        )
        result["quality_gate_status"] = quality_result["status"]
        
        if quality_result["status"] == "HOLD":
            result["error"] = f"Quality Gate HOLD: {quality_result['reason']}"
            result["error_type"] = "QUALITY_GATE_HOLD"
            result["hold_metadata"] = quality_result["hold_metadata"]
            logger.warning(f"Quality Gate HOLD (Naver Industry): {report.get('title', 'N/A')[:50]}... - {quality_result['reason']}")
            status_manager.set_status(report, "PARSED_HOLD", quality_result["hold_metadata"])
            if metrics:
                metrics.record_quality_gate(False, quality_result["hold_metadata"].get("hold_reason"))
            return result
        
        if quality_result["status"] == "DROP":
            result["error"] = f"Quality Gate DROP: {quality_result['reason']}"
            result["error_type"] = "QUALITY_GATE_DROP"
            logger.error(f"Quality Gate DROP (Naver Industry): {report.get('title', 'N/A')[:50]}... - {quality_result['reason']}")
            status_manager.set_status(report, "FAILED_FATAL")
            if metrics:
                metrics.record_quality_gate(False, "QUALITY_GATE_DROP")
            return result
        
        if metrics:
            metrics.record_quality_gate(True)
            metrics.record_route_confidence_bucket(route_confidence)
        
        # ⭐ P0: route_result의 hold_reason 확인 (Sanity Check 실패 등)
        hold_reason = route_result.get("hold_reason") if route_result else None
        if hold_reason:
            result["error"] = f"라우팅 단계에서 HOLD: {hold_reason}"
            result["error_type"] = "ROUTING_HOLD"
            result["hold_reason"] = hold_reason
            result["hold_metadata"] = {
                "hold_reason": hold_reason,
                "retry_eligible": True,
                "retry_after": "routing_v2"
            }
            # ⭐ routing_audit_entry에도 hold_reason 반영
            if routing_audit_entry is not None:
                routing_audit_entry["hold_reason"] = hold_reason
            logger.warning(f"라우팅 단계에서 HOLD: {report.get('title', 'N/A')[:50]}... - {hold_reason}")
            if status_manager:
                status_manager.set_status(report, "HOLD")
            if metrics:
                metrics.record_hold_reason(hold_reason)
            return result
        
        # P1-1: 산업/시장 리포트에서 driver_candidates 추출 및 sector_code 매핑
        # Naver 리포트 라우팅을 통해 industry_logic 추출
        # ⭐ P0-final-2: MACRO 리포트는 driver_normalizer를 호출하지 않음 (driver 후보 오염 방지)
        industry_logic = route_result.get("industry_logic")
        
        # MACRO 리포트인 경우 로그 기록
        if route_type == "MACRO":
            logger.debug(
                f"MACRO 리포트 처리: driver_normalizer 호출 스킵 "
                f"(오직 industry_edges로만 라우팅, driver_candidates 생성 안 함)"
            )
        
        if not industry_logic:
            result["error"] = "산업 인사이트 추출 실패 (industry_logic 없음)"
            result["error_type"] = "INDUSTRY_INSIGHT_EXTRACTION_FAILED"
            logger.warning(f"Naver 산업/매크로 리포트 Insight 추출 실패: {report.get('title', 'N/A')[:50]}...")
            status_manager.set_status(report, "EXTRACTED_HOLD", {
                "hold_reason": "INDUSTRY_INSIGHT_EXTRACTION_FAILED",
                "retry_eligible": True,
                "retry_after": "llm_logic_v2"
            })
            if metrics:
                metrics.record_insight_extraction(False)
            return result
        
        # ⭐ 중요: MACRO 리포트(투자정보)는 sector_code를 MARKET or L1 only로 제한
        # 이유: 투자정보 리포트는 시장 전반/전략 리포트로, L2/L3 섹터 추론 금지
        # 예: "1월 효과", "스타일 로테이션" → MARKET
        # 금지: "2차 전지", "로봇", "반도체 공정" 같은 L2/L3
        if route_type == "MACRO" and report.get("category") == "투자정보":
            # 투자정보 리포트는 route_result에서 이미 MARKET으로 설정됨
            target_sector_code = route_result.get("target_sector_code", "MARKET")
            logger.info(f"투자정보 리포트(MACRO): target_sector_code = {target_sector_code} (MARKET, L2/L3 추론 금지)")
        else:
            # 산업 리포트는 기존 로직 유지
            # ⭐ [1순위] HTML 태그에서 추출한 target_sector_code 우선 사용
            target_sector_code = route_result.get("target_sector_code")
            if not target_sector_code:
                # [2순위] industry_logic에서 추출
                target_sector_code = industry_logic.get("target_sector_code")
        
        if not industry_logic.get("target_drivers") or not target_sector_code:
            result["error"] = "산업 인사이트 추출 실패 (드라이버/섹터 코드 없음)"
            result["error_type"] = "INDUSTRY_INSIGHT_EXTRACTION_FAILED"
            logger.warning(f"Naver 산업/매크로 리포트 Insight 추출 실패: {report.get('title', 'N/A')[:50]}...")
            status_manager.set_status(report, "EXTRACTED_HOLD", {
                "hold_reason": "INDUSTRY_INSIGHT_EXTRACTION_FAILED",
                "retry_eligible": True,
                "retry_after": "llm_logic_v2"
            })
            if metrics:
                metrics.record_insight_extraction(False)
            return result
        
        # target_sector_code를 industry_logic에 반영
        industry_logic["target_sector_code"] = target_sector_code
        
        result["insights_extracted"] = True
        if metrics:
            metrics.record_insight_extraction(True)
        
        # Industry Edge 저장
        # ⭐ 중요: MACRO 리포트(투자정보)는 이미 target_sector_code가 설정되어 있음 (MARKET)
        # ⭐ P0+: 섹터 코드 매핑 실패 시 fallback 정책
        # MACRO 리포트의 경우 route_result의 target_sector_code를 우선 사용
        if route_type == "MACRO" and report.get("category") == "투자정보":
            # 투자정보 리포트는 route_result에서 MARKET으로 설정됨
            target_sector_code = route_result.get("target_sector_code", "MARKET")
            logger.info(f"투자정보 리포트(MACRO): target_sector_code = {target_sector_code} (route_result에서 가져옴)")
        else:
            # 산업 리포트는 industry_logic에서 가져옴
            target_sector_code = industry_logic.get("target_sector_code")
        
        if not target_sector_code:
            result["error"] = "섹터 코드 매핑 실패 (target_sector_code 없음)"
            result["error_type"] = "SECTOR_MAPPING_FAILED"
            logger.warning(f"섹터 코드 매핑 실패, HOLD_SECTOR_MAPPING으로 저장")
            status_manager.set_status(report, "HOLD_SECTOR_MAPPING", {
                "hold_reason": "SECTOR_MAPPING_FAILED",
                "retry_eligible": True,
                "retry_after": "sector_mapping_v2",
                "retry_trigger": "sector_mapping_updated"
            })
            if metrics:
                metrics.record_hold_reason("SECTOR_MAPPING_FAILED")
            return result
        
        # ⭐ 섹션 단위 fingerprint 가져오기 (Naver 리포트는 섹션이 명확하지 않으므로 전체 텍스트 해시 사용)
        import hashlib
        section_fingerprint = hashlib.sha256(report.get("full_text", "").encode()).hexdigest()
        
        logger.info(f"Naver Industry Insight 저장 준비: {target_sector_code} (READY_TO_ENRICH)")
        
        # ⭐ report_id 강제 생성 (broker_reports 테이블과 연결)
        report_id = report.get("report_id")
        if not report_id:
            # report_id가 없으면 생성 (v0.1-rc1 규칙 사용)
            from utils.report_id_generator import generate_report_id
            
            report_id = generate_report_id(
                broker_name=report.get("broker_name", ""),
                title=report.get("title", ""),
                date=report.get("date", ""),
                url=report.get("url", "")
            )
            logger.warning(f"report_id가 없어서 생성: {report_id[:16]}...")
        
        # ⭐ NULL report_id 방어 가드
        if not report_id or report_id.strip() == "":
            result["error"] = "report_id 생성 실패"
            result["error_type"] = "MISSING_REPORT_ID"
            result["hold_reason"] = "MISSING_REPORT_ID"
            logger.error(f"report_id가 비어있어서 저장 불가: {report.get('title', 'N/A')[:50]}")
            status_manager.set_status(report, "HOLD", {
                "hold_reason": "MISSING_REPORT_ID",
                "retry_eligible": True,
                "retry_after": "report_id_generation_v2"
            })
            if metrics:
                metrics.record_hold_reason("MISSING_REPORT_ID")
            return result
        
        # broker_reports에 upsert (report_id가 존재하도록 보장)
        try:
            from app.models.broker_report import BrokerReport
            broker_report = db.query(BrokerReport).filter(
                BrokerReport.report_id == report_id
            ).first()
            
            if not broker_report:
                # broker_reports에 없으면 생성
                # 날짜 정규화 (YYYY-MM-DD 형식으로 변환)
                raw_date = report.get("date", "")
                normalized_date = normalize_date_for_report_id(raw_date)
                # "NA"는 None으로 변환 (PostgreSQL DATE 타입에 맞춤)
                report_date = None if normalized_date == "NA" else normalized_date
                
                broker_report = BrokerReport(
                    report_id=report_id,
                    report_title=report.get("title", ""),  # ⭐ 필드명 수정
                    broker_name=report.get("broker_name", ""),
                    report_date=report_date,  # ⭐ 정규화된 날짜 사용
                    source="naver",
                    report_url=report.get("url", ""),  # ⭐ 필드명 수정
                    processing_status="ENRICHED"
                )
                db.add(broker_report)
                db.commit()
                logger.info(f"broker_reports에 리포트 생성: {report_id[:16]}...")
        except Exception as e:
            logger.warning(f"broker_reports upsert 실패 (계속 진행): {e}")
            db.rollback()
        
        save_result, skip_reason = save_industry_insight(
            db=db,
            report_id=report_id,  # ⭐ 강제 생성된 report_id 사용
            industry_logic=industry_logic,
            report_metadata=report_metadata,
            section_fingerprint=section_fingerprint
        )
        
        # ⭐ 결과 타입별 처리 (성공/스킵/실패 구분) - 세분화된 enum 지원
        is_success = save_result in (SaveResult.CREATED, SaveResult.UPDATED)
        is_dedupe = save_result in (
            SaveResult.SKIPPED_DEDUPE, SaveResult.SKIPPED_DB_CONFLICT,
            SaveResult.SKIPPED_DEDUPE_APPENDED, SaveResult.SKIPPED_DEDUPE_EVICTED, SaveResult.SKIPPED_DEDUPE_NOOP
        )
        
        result["save_result"] = save_result.value
        result["skip_reason"] = skip_reason
        
        if is_success:
            result["industry_edge_enriched"] = True
            logger.info(f"Naver Industry Insight 저장 성공 ({save_result.value})")
            if metrics:
                metrics.record_edge_enrichment(True, "INDUSTRY")
            status_manager.set_status(report, "ENRICHED")
        elif is_dedupe:
            # 중복 스킵도 성공으로 처리 (Evidence 누적됨)
            result["industry_edge_enriched"] = True
            result["dedupe_skipped"] = True
            logger.info(f"Naver Industry Insight 중복 스킵 ({save_result.value}): {skip_reason}")
            if metrics:
                metrics.record_edge_enrichment(True, "INDUSTRY")
            status_manager.set_status(report, "ENRICHED")
        else:
            result["error"] = f"Naver Industry Insight 저장 실패: {skip_reason}"
            result["error_type"] = "INDUSTRY_EDGE_ENRICHMENT_FAILED"
            status_manager.set_status(report, "FAILED_RETRYABLE")
            if metrics:
                metrics.record_edge_enrichment(False, "INDUSTRY")
        
        return result
    
    except Exception as e:
        logger.error(f"Naver 산업/매크로 리포트 처리 중 오류: {e}", exc_info=True)
        result["error"] = str(e)
        result["error_type"] = type(e).__name__
        status_manager.set_status(report, "FAILED_RETRYABLE")
        return result


def process_kirs_report(
    db: Session,
    report: Dict[str, Any],
    metrics: Optional[PipelineMetrics] = None,
    status_manager: Optional[ReportStatusManager] = None,
    daily_enrich_count: int = 0,
    daily_cap: int = 50
) -> Dict[str, Any]:
    """
    KIRS 리포트 전용 처리 (힌트 데이터로 변환)
    
    Args:
        db: DB 세션
        report: 파싱된 KIRS 리포트 데이터
        metrics: 파이프라인 지표
        status_manager: 리포트 상태 관리자
        daily_enrich_count: 오늘 처리한 Enrichment 수
        daily_cap: 일일 최대 Enrichment 수
    
    Returns:
        처리 결과
    """
    result = {
        "report_id": report.get("report_id", ""),
        "title": report.get("title", "")[:50],
        "ticker": report.get("ticker"),
        "route_type": None,
        "company_edge_enriched": False,
        "industry_edge_enriched": False,
        "error": None,
        "error_type": None
    }
    
    try:
        # KIRS 리포트 라우팅
        route_result = route_kirs_report(report)
        result["route_type"] = route_result["route_type"]
        
        logger.info(f"KIRS 리포트 라우팅: {route_result['route_type']}")
        
        # 리포트 메타데이터 준비
        report_metadata = {
            "title": report.get("title", ""),
            "broker_name": report.get("source", "한국IR협의회"),
            "report_date": report.get("date", ""),
            "report_id": report.get("report_id", ""),
            "text_head": report.get("full_text", "")[:500] if report.get("full_text") else "",
            "pdf_url": report.get("pdf_url"),
            "url": report.get("url"),
            "parser_version": "v1.0",
            "cleaning_profile": "kirs_smallcap"
        }
        
        # 트랜잭션 시작 (DB write만)
        try:
            # ⭐ P0-β: route_confidence 게이트 체크
            # ⚠️ 중요: 0.6은 고정값이 아니라 관측용 마커
            # 테스트 후 분포를 보고 캘리브레이션 필요
            route_confidence = route_result.get("route_confidence", 0.0)
            
            # 임계값 설정 (환경변수 또는 기본값, 향후 캘리브레이션 후 조정)
            import os
            CONFIDENCE_THRESHOLD = float(os.getenv("ROUTE_CONFIDENCE_THRESHOLD", "0.6"))
            CONFIDENCE_PASS_THRESHOLD = float(os.getenv("ROUTE_CONFIDENCE_PASS_THRESHOLD", "0.7"))
            
            # ⭐ 관측용: confidence 분포 기록
            result["route_confidence"] = route_confidence
            result["route_evidence"] = route_result.get("route_evidence", [])
            
            # 분포 관측을 위한 로깅
            confidence_bucket = "UNKNOWN"
            if route_confidence < 0.5:
                confidence_bucket = "0.0-0.5"
            elif route_confidence < 0.6:
                confidence_bucket = "0.5-0.6"
            elif route_confidence < 0.7:
                confidence_bucket = "0.6-0.7"
            elif route_confidence < 0.8:
                confidence_bucket = "0.7-0.8"
            else:
                confidence_bucket = "0.8-1.0"
            
            result["route_confidence_bucket"] = confidence_bucket
            logger.info(f"라우팅 신뢰도: {route_confidence:.2f} (bucket: {confidence_bucket}, evidence: {len(result['route_evidence'])})")
            
            # ⭐ 게이트 정책 (관측용, 고정 아님)
            # - < CONFIDENCE_THRESHOLD (기본 0.6): HOLD_ROUTE_LOW (자동 반영 차단)
            # - 0.6 ~ route_type별 threshold: HOLD_ROUTE_MED (상위 후보, 관측 필요)
            # - >= route_type별 threshold: READY_TO_ENRICH (후속 게이트 통과 시에만 write)
            
            # ⭐ route_type별 threshold 분리
            route_type = route_result.get("route_type", "UNKNOWN")
            if route_type in ["COMPANY", "BOTH"]:
                # 기업 edges는 오탐 비용이 큼 (티커 잘못 매핑 → KG 오염)
                ROUTE_TYPE_THRESHOLD = float(os.getenv("ROUTE_CONFIDENCE_COMPANY_THRESHOLD", "0.75"))
            elif route_type == "INDUSTRY":
                # 산업 edges는 상대적으로 덜 치명적 (섹터 단 힌트)
                ROUTE_TYPE_THRESHOLD = float(os.getenv("ROUTE_CONFIDENCE_INDUSTRY_THRESHOLD", "0.70"))
            else:
                ROUTE_TYPE_THRESHOLD = CONFIDENCE_PASS_THRESHOLD
            
            result["route_type"] = route_type
            result["route_type_threshold"] = ROUTE_TYPE_THRESHOLD
            
            # ⭐ 점수 분해 필드 저장
            confidence_breakdown = route_result.get("route_confidence_breakdown", {})
            result["route_confidence_breakdown"] = confidence_breakdown
            
            if route_confidence < CONFIDENCE_THRESHOLD:
                # < 0.6: HOLD_ROUTE_LOW
                result["error"] = f"라우팅 신뢰도 부족 (confidence: {route_confidence:.2f} < {CONFIDENCE_THRESHOLD})"
                result["error_type"] = "ROUTE_CONFIDENCE_LOW"
                logger.warning(f"라우팅 신뢰도 부족으로 KG 반영 차단: {route_confidence:.2f} (bucket: {confidence_bucket}, route_type: {route_type})")
                status_manager.set_status(report, "HOLD_ROUTE_LOW", {
                    "hold_reason": "ROUTE_CONFIDENCE_LOW",
                    "route_confidence": route_confidence,
                    "route_confidence_bucket": confidence_bucket,
                    "route_type": route_type,
                    "route_type_threshold": ROUTE_TYPE_THRESHOLD,
                    "route_confidence_breakdown": confidence_breakdown,
                    "retry_eligible": True,
                    "retry_after": "route_logic_v2_or_calibration",
                    "retry_trigger": "route_logic_update"
                })
                if metrics:
                    metrics.record_hold_reason("ROUTE_CONFIDENCE_LOW")
                return result
            elif route_confidence < ROUTE_TYPE_THRESHOLD:
                # 0.6 ~ route_type별 threshold: HOLD_ROUTE_MED (상위 후보)
                result["warning"] = f"라우팅 신뢰도 중간 (confidence: {route_confidence:.2f} < {ROUTE_TYPE_THRESHOLD}, route_type: {route_type})"
                result["route_confidence_status"] = "HOLD_ROUTE_MED"
                logger.info(f"라우팅 신뢰도 중간 구간: {route_confidence:.2f} (bucket: {confidence_bucket}, route_type: {route_type}, threshold: {ROUTE_TYPE_THRESHOLD})")
                # HOLD_ROUTE_MED 처리 정책
                status_manager.set_status(report, "HOLD_ROUTE_MED", {
                    "hold_reason": "ROUTE_CONFIDENCE_MED",
                    "route_confidence": route_confidence,
                    "route_confidence_bucket": confidence_bucket,
                    "route_type": route_type,
                    "route_type_threshold": ROUTE_TYPE_THRESHOLD,
                    "route_confidence_breakdown": confidence_breakdown,
                    "retry_eligible": True,
                    "retry_after": "sector_mapping_update_or_route_logic_v2",
                    "retry_trigger": ["sector_mapping_updated", "route_logic_v2", "threshold_calibration"]
                })
                result["needs_review"] = True
                # ⚠️ 중요: HOLD_ROUTE_MED는 처리하되, DB write는 하지 않음 (관측용)
                # 계속 진행하되, 실제 write는 Quality Gate + dedupe + ticker/sector mapping 통과 후에만
            else:
                # >= route_type별 threshold: READY_TO_ENRICH (후속 게이트 통과 시에만 write)
                result["route_confidence_status"] = "READY_TO_ENRICH"
                logger.info(f"라우팅 신뢰도 충분: {route_confidence:.2f} >= {ROUTE_TYPE_THRESHOLD} (route_type: {route_type}, READY_TO_ENRICH)")
                # ⚠️ 중요: READY_TO_ENRICH는 "반영 큐" 상태, 실제 write는 Quality Gate + dedupe + ticker/sector mapping 통과 후에만
                result["ready_to_enrich"] = True
                # 상태는 아직 저장하지 않음 (후속 게이트 통과 후 ENRICHED로 변경)
            
            # 1. 기업 섹션 처리 (티커가 있는 경우)
            # ⭐ P0+ 보강: 산업 섹션에서 기업 언급이 나와도 자동 company-edge 생성 금지
            # 기업 섹션만 처리 (명시적 기업 분석 섹션)
            if route_result["company_logic"] and report.get("ticker"):
                ticker = report.get("ticker")
                company_logic = route_result["company_logic"]
                
                # ⭐ 섹션 단위 fingerprint 가져오기
                section_fingerprints = route_result.get("section_fingerprints", {})
                company_section_fp = section_fingerprints.get("company")
                
                # 드라이버 코드 추출 (target_drivers에서 첫 번째)
                driver_code = None
                if company_logic.get("target_drivers"):
                    driver_code = company_logic["target_drivers"][0]
                
                if driver_code:
                    logger.info(f"기업 Edge Enrichment: ticker={ticker}, driver={driver_code} (confidence: {route_result.get('route_confidence', 0):.2f})")
                    
                    success, alignment = enrich_company_edge_with_kirs(
                        db=db,
                        ticker=ticker,
                        driver_code=driver_code,
                        company_logic=company_logic,
                        report_metadata=report_metadata,
                        section_fingerprint=company_section_fp  # 섹션 단위 dedup
                    )
                    
                    if success:
                        result["company_edge_enriched"] = True
                        result["alignment"] = alignment
                        logger.info(f"기업 Edge Enrichment 성공 (alignment: {alignment})")
                        if metrics:
                            metrics.record_edge_enrichment(True, alignment)
                    else:
                        logger.warning(f"기업 Edge Enrichment 실패")
                else:
                    logger.warning(f"드라이버 코드 없음, 기업 Edge Enrichment 스킵")
            
            # 2. 산업 섹션 처리 (티커 없어도 가능)
            # ⭐ P0+ 보강: 산업 섹션에서 기업 언급이 나와도 company-edge 생성하지 않음
            # ⚠️ READY_TO_ENRICH 상태이고, sector mapping + dedupe 통과 후에만 write
            if route_result["industry_logic"]:
                industry_logic = route_result["industry_logic"]
                
                # ⭐ P0+: 섹터 코드 매핑 실패 시 fallback 정책
                target_sector_code = industry_logic.get("target_sector_code")
                if not target_sector_code:
                    result["error"] = "섹터 코드 매핑 실패 (target_sector_code 없음)"
                    result["error_type"] = "SECTOR_MAPPING_FAILED"
                    logger.warning(f"섹터 코드 매핑 실패, HOLD_SECTOR_MAPPING으로 저장")
                    status_manager.set_status(report, "HOLD_SECTOR_MAPPING", {
                        "hold_reason": "SECTOR_MAPPING_FAILED",
                        "retry_eligible": True,
                        "retry_after": "sector_mapping_v2",
                        "retry_trigger": "sector_mapping_updated"
                    })
                    if metrics:
                        metrics.record_hold_reason("SECTOR_MAPPING_FAILED")
                    # ⭐ unmapped로 별도 저장 (향후 수동 매핑 가능)
                    # TODO: industry_insight_unmapped 테이블 생성 또는 broker_reports.key_points에 저장
                    return result
                
                # ⭐ READY_TO_ENRICH 상태 확인
                if result.get("route_confidence_status") != "READY_TO_ENRICH":
                    logger.info(f"산업 섹션 발견했으나 route_confidence 미달, HOLD 상태 유지 (status: {result.get('route_confidence_status')})")
                    # HOLD_ROUTE_MED 상태는 이미 설정됨
                else:
                    # READY_TO_ENRICH 상태: 후속 게이트 통과 후 write
                    # ⭐ 섹션 단위 fingerprint 가져오기
                    section_fingerprints = route_result.get("section_fingerprints", {})
                    industry_section_fp = section_fingerprints.get("industry")
                    
                    logger.info(f"Industry Insight 저장 준비: {target_sector_code} (confidence: {route_result.get('route_confidence', 0):.2f}, READY_TO_ENRICH)")
                    
                    # ⚠️ 실제 write는 Quality Gate + dedupe + sector mapping 통과 후에만
                    # ⭐ report_id 강제 생성 (broker_reports 테이블과 연결)
                    report_id = report.get("report_id")
                    if not report_id:
                        # report_id가 없으면 생성 (v0.1-rc1 규칙 사용)
                        from utils.report_id_generator import generate_report_id
                        
                        report_id = generate_report_id(
                            broker_name=report.get("broker_name", ""),
                            title=report.get("title", ""),
                            date=report.get("date", ""),
                            url=report.get("url", "")
                        )
                        logger.warning(f"report_id가 없어서 생성: {report_id[:16]}...")
                    
                    # ⭐ NULL report_id 방어 가드
                    if not report_id or report_id.strip() == "":
                        logger.error(f"report_id가 비어있어서 저장 불가 (industry 섹션 스킵)")
                        # industry 섹션만 스킵하고 company 섹션은 계속 처리
                    else:
                        # broker_reports에 upsert (report_id가 존재하도록 보장)
                        try:
                            from app.models.broker_report import BrokerReport
                            broker_report = db.query(BrokerReport).filter(
                                BrokerReport.report_id == report_id
                            ).first()
                            
                            if not broker_report:
                                # broker_reports에 없으면 생성
                                # 날짜 정규화 (YYYY-MM-DD 형식으로 변환)
                                raw_date = report.get("date", "")
                                normalized_date = normalize_date_for_report_id(raw_date)
                                # "NA"는 None으로 변환 (PostgreSQL DATE 타입에 맞춤)
                                report_date = None if normalized_date == "NA" else normalized_date
                                
                                broker_report = BrokerReport(
                                    report_id=report_id,
                                    report_title=report.get("title", ""),  # ⭐ 필드명 수정
                                    broker_name=report.get("broker_name", ""),
                                    report_date=report_date,  # ⭐ 정규화된 날짜 사용
                                    source="naver",
                                    report_url=report.get("url", ""),  # ⭐ 필드명 수정
                                    processing_status="ENRICHED"
                                )
                                db.add(broker_report)
                                db.commit()
                                logger.info(f"broker_reports에 리포트 생성: {report_id[:16]}...")
                        except Exception as e:
                            logger.warning(f"broker_reports upsert 실패 (계속 진행): {e}")
                            db.rollback()
                        
                        save_result2, skip_reason2 = save_industry_insight(
                            db=db,
                            report_id=report_id,  # ⭐ 강제 생성된 report_id 사용
                            industry_logic=industry_logic,
                            report_metadata=report_metadata,
                            section_fingerprint=industry_section_fp  # 섹션 단위 dedup
                        )
                        
                        # ⭐ 결과 타입별 처리 (성공/스킵/실패 구분) - 세분화된 enum 지원
                        is_success2 = save_result2 in (SaveResult.CREATED, SaveResult.UPDATED)
                        is_dedupe2 = save_result2 in (
                            SaveResult.SKIPPED_DEDUPE, SaveResult.SKIPPED_DB_CONFLICT,
                            SaveResult.SKIPPED_DEDUPE_APPENDED, SaveResult.SKIPPED_DEDUPE_EVICTED, SaveResult.SKIPPED_DEDUPE_NOOP
                        )
                        
                        if is_success2 or is_dedupe2:
                            result["industry_edge_enriched"] = True
                            result["industry_save_result"] = save_result2.value
                            if is_dedupe2:
                                result["industry_dedupe_skipped"] = True
                            logger.info(f"Industry Insight 저장 ({save_result2.value})")
                            if metrics:
                                # Industry Insight 저장도 지표에 기록
                                metrics.record_edge_enrichment(True, "INDUSTRY")
                        else:
                            logger.warning(f"Industry Insight 저장 실패: {skip_reason2}")
            
            # ⚠️ 중요: READY_TO_ENRICH 상태여도 실제 DB write는 Quality Gate + dedupe + ticker/sector mapping 통과 후에만
            # 성공 시 커밋 (READY_TO_ENRICH 상태이고, 모든 후속 게이트 통과한 경우만)
            if result.get("route_confidence_status") == "READY_TO_ENRICH" and (result["company_edge_enriched"] or result["industry_edge_enriched"]):
                db.commit()
                status_manager.set_status(report, "ENRICHED")
                logger.info(f"KIRS 리포트 처리 완료: {result['route_type']} (READY_TO_ENRICH → ENRICHED)")
            elif result.get("route_confidence_status") == "HOLD_ROUTE_MED":
                # HOLD_ROUTE_MED는 DB write 없이 상태만 저장
                db.rollback()  # 트랜잭션 롤백 (write 없음)
                logger.info(f"KIRS 리포트 HOLD_ROUTE_MED 상태 유지: {result['route_type']} (관측 필요)")
                # 상태는 이미 HOLD_ROUTE_MED로 설정됨
            else:
                db.rollback()
                result["error"] = "KIRS 리포트 처리 실패 (기업/산업 섹션 모두 실패 또는 route_confidence 미달)"
                result["error_type"] = "KIRS_PROCESSING_FAILED"
                status_manager.set_status(report, "FAILED_RETRYABLE")
        
        except Exception as e:
            logger.error(f"KIRS 리포트 트랜잭션 오류: {e}", exc_info=True)
            db.rollback()
            status_manager.set_status(report, "FAILED_RETRYABLE")
            result["error"] = f"트랜잭션 오류: {str(e)}"
            result["error_type"] = "TRANSACTION_ERROR"
        
        return result
    
    except Exception as e:
        logger.error(f"KIRS 리포트 처리 중 오류: {e}", exc_info=True)
        result["error"] = str(e)
        result["error_type"] = type(e).__name__
        status_manager.set_status(report, "FAILED_RETRYABLE")
        return result


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="리포트 기반 Edge Enrichment")
    parser.add_argument("--input", type=str, help="파싱된 리포트 JSON 파일 경로")
    parser.add_argument("--limit", type=int, default=10, help="처리할 리포트 수 제한")
    parser.add_argument("--daily-cap", type=int, default=50, help="일일 최대 Enrichment 수 (기본: 50)")
    parser.add_argument('--force-candidate-extraction', action='store_true',
                        help='이미 ENRICHED 리포트도 driver 후보 추출 강제 실행 (dry-run 모드)')
    parser.add_argument('--write-candidates', action='store_true',
                        help='--force-candidate-extraction과 함께 사용 시 실제 DB에 후보 저장')
    parser.add_argument('--rebuild-industry-insights', action='store_true',
                        help='ENRICHED 상태라도 industry_insights.logic_summary만 재계산/업데이트')
    args = parser.parse_args()
    
    # ⭐ P0-D: force 모드 설정 (전역 변수로 전달)
    # ⭐ P0-1: rebuild 모드 설정
    process_single_report._force_candidate_extraction = args.force_candidate_extraction
    process_single_report._write_candidates = args.write_candidates
    process_single_report._rebuild_industry_insights = args.rebuild_industry_insights
    
    logger.info("=" * 80)
    logger.info("Edge Enrichment 파이프라인 시작")
    logger.info("=" * 80)
    logger.info(f"일일 최대 Enrichment 수: {args.daily_cap}건")
    
    # PipelineMetrics 초기화
    metrics = PipelineMetrics()
    
    # 입력 파일 로드
    if args.input:
        # ⭐ P0-α3: 입력 파일 검증 (glob 패턴 지원)
        input_path = Path(args.input)
        
        # Glob 패턴인 경우
        if '*' in str(input_path) or '?' in str(input_path):
            import glob
            matched_files = list(Path(input_path.parent if input_path.parent != Path('.') else project_root / "reports").glob(input_path.name))
            if not matched_files:
                logger.error(f"입력 glob 패턴에 매칭되는 파일이 없습니다: {args.input}")
                logger.error("표준 패턴: reports/parsed_{source}_reports_{timestamp}.json")
                return
            input_file = max(matched_files, key=lambda p: p.stat().st_mtime)
            logger.info(f"Glob 패턴 매칭: {len(matched_files)}개 파일 발견, 최신 파일 사용: {input_file}")
        else:
            input_file = input_path
            if not input_file.exists():
                logger.error(f"입력 파일이 존재하지 않습니다: {input_file}")
                return
    else:
        # 최신 파싱 리포트 파일 찾기
        reports_dir = project_root / "reports"
        # ⭐ P0-α3: 표준화된 파일 패턴 검색
        # 수집: reports/{source}_reports_{ts}.json
        # 파싱: reports/parsed_{source}_reports_{ts}.json
        json_files = []
        # Naver 리포트 패턴
        json_files.extend(list(reports_dir.glob("parsed_naver_reports_*.json")))
        # KIRS 리포트 패턴
        json_files.extend(list(reports_dir.glob("parsed_kirs_reports_*.json")))
        # 기존 패턴 (하위 호환성)
        json_files.extend(list(reports_dir.glob("parsed_reports_*.json")))
        
        if not json_files:
            logger.error(
                "파싱된 리포트 파일이 없습니다. "
                "표준 패턴: reports/parsed_{source}_reports_{timestamp}.json "
                "(예: reports/parsed_naver_reports_20251220_120000.json)"
            )
            return
        
        input_file = max(json_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"최신 리포트 파일 사용: {input_file}")
    
    # ⭐ P0-α3: 파일 검증 (존재 여부, 크기)
    if not input_file.exists():
        logger.error(f"입력 파일이 존재하지 않습니다: {input_file}")
        return
    
    if input_file.stat().st_size == 0:
        logger.error(f"입력 파일이 비어있습니다: {input_file}")
        return
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reports = json.load(f)
    
    # ⭐ P0-α3: 리포트 리스트 검증
    if not isinstance(reports, list):
        logger.error(f"입력 파일이 리포트 리스트 형식이 아닙니다: {input_file}")
        return
    
    if len(reports) == 0:
        logger.error(f"입력 파일에 리포트가 없습니다: {input_file}")
        return
    
    logger.info(f"로드된 리포트 수: {len(reports)}개")
    
    # 파싱 성공한 리포트만 필터링
    valid_reports = [r for r in reports if r.get("total_paragraphs", 0) > 0]
    logger.info(f"유효한 리포트 수: {len(valid_reports)}개")
    
    # 수집 단계 기록
    metrics.record_collection(len(reports), len(valid_reports), len(reports) - len(valid_reports))
    
    if args.limit > 0:
        valid_reports = valid_reports[:args.limit]
        logger.info(f"제한 적용: {len(valid_reports)}개 처리")
    
    # LLM 핸들러 초기화 (재사용)
    llm_handler = LLMHandler()
    
    # 리포트 상태 관리자 초기화
    status_manager = ReportStatusManager()
    
    # 리포트 처리
    db = SessionLocal()
    results = []
    failed_reports = []
    hold_reports = []
    daily_enrich_count = 0
    
    try:
        for i, report in enumerate(valid_reports, 1):
            logger.info(f"\n[{i}/{len(valid_reports)}] 리포트 처리 중...")
            
            result = process_single_report(
                db=db,
                report=report,
                llm_handler=llm_handler,
                metrics=metrics,
                status_manager=status_manager,
                daily_enrich_count=daily_enrich_count,
                daily_cap=args.daily_cap
            )
            
            # ⭐ P0-α4: 라우팅 감사 데이터 수집
            routing_audit_data = {
                "report_id": report.get("report_id", ""),
                "title": report.get("title", "")[:100],
                "original_category": report.get("category", ""),
                "determined_report_type": result.get("route_type", report.get("report_type", "UNKNOWN")),
                "route_confidence": result.get("route_confidence", 0.0),
                "route_confidence_breakdown": result.get("route_confidence_breakdown", {}),
                "route_evidence": result.get("route_evidence", []),
                "safety_latch_applied": result.get("safety_latch_applied", False),
                "original_type": result.get("original_type", "UNKNOWN"),
                "final_routing": "EDGES" if result.get("edge_enriched") else ("INDUSTRY_EDGES" if result.get("industry_edge_enriched") else "HOLD"),
                "hold_reason": result.get("hold_reason") or (result.get("error_type") if result.get("error") else None)  # ⭐ route_result의 hold_reason 우선
            }
            result["routing_audit"] = routing_audit_data
            
            results.append(result)
            
            # Daily Cap 체크
            if result.get("edge_enriched"):
                daily_enrich_count += 1
            
            # 실패 리포트 수집
            if result.get("error"):
                if result.get("error_type") == "QUALITY_GATE_HOLD":
                    hold_reports.append({
                        "report_id": result.get("report_id"),
                        "title": result.get("title"),
                        "ticker": result.get("ticker"),
                        "driver_code": result.get("driver_code"),
                        "error": result.get("error"),
                        "hold_metadata": result.get("hold_metadata")
                    })
                else:
                    failed_reports.append({
                        "report_id": result.get("report_id"),
                        "title": result.get("title"),
                        "error": result.get("error"),
                        "error_type": result.get("error_type")
                    })
            
            # 진행 상황 출력
            if i % 5 == 0:
                success_count = sum(1 for r in results if r.get("edge_enriched"))
                logger.info(f"진행 상황: {i}/{len(valid_reports)} ({success_count}개 성공, {daily_enrich_count}/{args.daily_cap} Enrichment)")
    
    finally:
        db.close()
    
    # 결과 통계
    success_count = sum(1 for r in results if r.get("edge_enriched"))
    ticker_matched = sum(1 for r in results if r.get("ticker"))
    driver_matched = sum(1 for r in results if r.get("driver_code"))
    quality_pass = sum(1 for r in results if r.get("quality_gate_status") == "PASS")
    quality_hold = sum(1 for r in results if r.get("quality_gate_status") == "HOLD")
    insights_extracted = sum(1 for r in results if r.get("insights_extracted"))
    
    logger.info("\n" + "=" * 80)
    logger.info("처리 완료")
    logger.info("=" * 80)
    logger.info(f"총 리포트 수: {len(results)}")
    logger.info(f"Ticker 매칭 성공: {ticker_matched}개")
    logger.info(f"Driver 매칭 성공: {driver_matched}개")
    logger.info(f"Quality Gate PASS: {quality_pass}개")
    logger.info(f"Quality Gate HOLD: {quality_hold}개")
    logger.info(f"Insight 추출 성공: {insights_extracted}개")
    logger.info(f"Edge Enrichment 성공: {success_count}개")
    logger.info(f"일일 Enrichment: {daily_enrich_count}/{args.daily_cap}건")
    
    # 토큰/비용 정보 출력
    llm_usage = metrics.metrics.get("llm_usage", {})
    logger.info(f"\nLLM 사용량:")
    logger.info(f"  - 총 토큰: {llm_usage.get('total_tokens', 0):,}개")
    logger.info(f"  - 프롬프트: {llm_usage.get('total_prompt_tokens', 0):,}개")
    logger.info(f"  - 완성: {llm_usage.get('total_completion_tokens', 0):,}개")
    logger.info(f"  - 예상 비용: ${llm_usage.get('estimated_cost_usd', 0.0):.4f}")
    logger.info(f"  - 단계별 호출:")
    for stage, count in llm_usage.get("calls_by_stage", {}).items():
        logger.info(f"    - {stage}: {count}회")
    
    # Idempotency 정보 출력
    idempotency = metrics.metrics.get("idempotency", {})
    logger.info(f"\nIdempotency:")
    logger.info(f"  - 스킵된 리포트: {idempotency.get('skipped_already_processed', 0)}개")
    if idempotency.get("hold_counts_by_reason"):
        logger.info(f"  - HOLD 사유별:")
        for reason, count in idempotency["hold_counts_by_reason"].items():
            logger.info(f"    - {reason}: {count}개")
    
    # 실패 리포트 저장
    if failed_reports:
        failed_file = project_root / "reports" / f"failed_reports_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(failed_file, 'w', encoding='utf-8') as f:
            json.dump(failed_reports, f, ensure_ascii=False, indent=2)
        logger.warning(f"실패 리포트 {len(failed_reports)}개 저장: {failed_file}")
    
    # HOLD 리포트 저장 (재처리 대상)
    if hold_reports:
        hold_file = project_root / "reports" / f"hold_reports_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(hold_file, 'w', encoding='utf-8') as f:
            json.dump(hold_reports, f, ensure_ascii=False, indent=2)
        logger.info(f"HOLD 리포트 {len(hold_reports)}개 저장 (재처리 대상): {hold_file}")
    
    # 결과 저장
    output_file = project_root / "reports" / f"enrichment_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    logger.info(f"결과 저장: {output_file}")
    
    # ⭐ P0-α4: 라우팅 감사 데이터 저장
    routing_audit_data = [r.get("routing_audit", {}) for r in results if r.get("routing_audit")]
    if routing_audit_data:
        # COMPANY로 분류된 상위 10개
        company_reports = sorted(
            [r for r in routing_audit_data if r.get("determined_report_type") == "COMPANY"],
            key=lambda x: x.get("route_confidence", 0.0),
            reverse=True
        )[:10]
        
        # INDUSTRY/MACRO로 분류된 상위 10개
        industry_reports = sorted(
            [r for r in routing_audit_data if r.get("determined_report_type") in ["INDUSTRY", "MACRO"]],
            key=lambda x: x.get("route_confidence", 0.0),
            reverse=True
        )[:10]
        
        # ⭐ top_missing_features 집계 (0.6~0.7 구간에서 어떤 feature가 부족한지)
        from collections import Counter
        missing_features_counter = Counter()
        confidence_06_07_reports = [
            r for r in routing_audit_data 
            if 0.6 <= r.get("route_confidence", 0.0) < 0.7
        ]
        
        for report in confidence_06_07_reports:
            breakdown = report.get("route_confidence_breakdown", {})
            missing_features = breakdown.get("missing_features", [])
            for missing in missing_features:
                feature_name = missing.get("feature", "unknown")
                missing_features_counter[feature_name] += 1
        
        top_missing_features = dict(missing_features_counter.most_common(5))
        
        routing_audit_file = project_root / "reports" / f"routing_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(routing_audit_file, 'w', encoding='utf-8') as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "total_reports": len(routing_audit_data),
                "company_reports_sample": company_reports,
                "industry_reports_sample": industry_reports,
                "confidence_06_07_count": len(confidence_06_07_reports),
                "top_missing_features": top_missing_features,  # ⭐ 추가
                "all_routing_data": routing_audit_data
            }, f, ensure_ascii=False, indent=2)
        logger.info(f"라우팅 감사 데이터 저장: {routing_audit_file}")
        if top_missing_features:
            logger.info(f"  - 0.6~0.7 구간 부족한 feature Top 5: {top_missing_features}")
    
    # 파이프라인 지표 리포트 생성 및 저장
    metrics_file = metrics.save_report()
    
    # 지표 요약 출력
    report = metrics.generate_report()
    logger.info("\n" + "=" * 80)
    logger.info("파이프라인 지표 요약")
    logger.info("=" * 80)
    logger.info(f"End-to-End 성공률: {report['summary']['end_to_end_rate']:.2f}%")
    logger.info(f"단계별 성공률:")
    for stage, rate in report['stage_success_rates'].items():
        logger.info(f"  - {stage}: {rate:.2f}%")
    
    if report['bottlenecks']:
        logger.warning(f"병목 지점: {[b['stage'] for b in report['bottlenecks']]}")
    
    logger.info(f"상세 지표 리포트: {metrics_file}")


if __name__ == "__main__":
    main()

