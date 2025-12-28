"""
Pipeline Metrics (Phase 2.0 P0)

목적: 파이프라인 지표 측정 및 리포트 생성
이건 "있으면 좋은 것"이 아니라 "없으면 서비스 못 굴림"
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


class PipelineMetrics:
    """파이프라인 지표 측정 및 리포트 생성"""
    
    def __init__(self):
        self.metrics = {
            "collection": {
                "total": 0,
                "success": 0,
                "failed": 0
            },
            "parsing": {
                "total": 0,
                "success": 0,
                "low_quality": 0,
                "avg_text_length": 0,
                "avg_paragraphs": 0,
                "total_text_length": 0,
                "total_paragraphs": 0
            },
            "ticker_matching": {
                "total": 0,
                "success": 0,
                "skipped": 0,  # ⭐ P0-1: 산업/매크로 리포트 스킵
                "skipped_industry": 0,  # ⭐ P0-1: 산업/매크로 리포트 스킵
                "rule_based": 0,
                "fuzzy": 0,
                "llm": 0,
                "failed": 0,
                # ⭐ P0.7: 4분류 메트릭 추가
                "matched_in_universe": 0,  # DB에 기업 상세 정보 있음
                "matched_out_of_universe": 0,  # ticker는 찾았지만 DB에 상세 정보 없음
                "ambiguous_name": 0,  # 동명이인/복수 후보
                "not_found": 0  # 진짜 못 찾음
            },
            "driver_normalization": {
                "total": 0,
                "success": 0,
                "synonym": 0,
                "llm": 0,
                "single_candidate": 0,
                "failed": 0
            },
            "quality_gate": {
                "total": 0,
                "pass": 0,
                "hold": 0,
                "drop": 0,
                "hold_counts_by_reason": {}
            },
            "insight_extraction": {
                "total": 0,
                "success": 0,
                "high_confidence": 0,
                "med_confidence": 0,
                "low_confidence": 0,
                "failed": 0
            },
            "edge_enrichment": {
                "total": 0,
                "success": 0,
                "aligned": 0,
                "partial": 0,
                "conflict": 0,
                "failed": 0,
                # ⭐ DRIVEN_BY 실패 원인 세분화
                "no_driver_extracted": 0,
                "driver_pending_approval": 0,
                "driver_not_in_universe": 0,
                "blocked_by_gate": 0
            },
            "llm_usage": {
                "total_prompt_tokens": 0,
                "total_completion_tokens": 0,
                "total_tokens": 0,
                "estimated_cost_usd": 0.0,
                "calls_by_stage": {
                    "ticker_matcher": 0,
                    "driver_normalizer": 0,
                    "insight_extractor": 0
                }
            },
            "idempotency": {
                "skipped_already_processed": 0,
                "hold_counts_by_reason": {}
            },
            "rebuild_skips": {  # ⭐ P0-추가2: Rebuild 스킵 메트릭
                "total_skips": 0,
                "by_reason": {},
                "total_candidates": 0
            },
            "route_confidence": {
                "buckets": {
                    "0.0-0.5": 0,
                    "0.5-0.6": 0,
                    "0.6-0.7": 0,
                    "0.7-0.8": 0,
                    "0.8-1.0": 0
                }
            },
            "detail_metadata": {  # ⭐ P0-β3: 상세 페이지 메타데이터 추출 성공률
                "total_attempts": 0,
                "fetch_success": 0,
                "extraction_success": 0,
                "fields_extracted": {
                    "stock_name": 0,
                    "industry_category": 0,
                    "target_price": 0,
                    "investment_opinion": 0,
                    "analyst_name": 0
                },
                "fields_missing": {
                    "stock_name": 0,
                    "industry_category": 0,
                    "target_price": 0,
                    "investment_opinion": 0,
                    "analyst_name": 0
                }
            },
            "driver_candidate_extraction": {  # ⭐ P0-3: 후보 생성 원인 3분류 계측
                "total_attempts": 0,
                "input_empty_or_short": 0,  # INPUT 문제
                "terms_extracted_total": 0,  # EXTRACTOR 문제 (0이면 문제)
                "terms_known_count": 0,  # ⭐ P0-추가 3: known count
                "terms_unknown_count": 0,  # ⭐ P0-추가 3: unknown count
                "rows_inserted": 0,  # PERSIST 문제 (추출 > 0, 저장 = 0)
                "skipped_cooldown": 0,
                "skipped_unique_conflict": 0,  # ⭐ P0-추가 4: unique 충돌
                "skipped_max_per_report": 0,
                "quality_filtered": 0  # ⭐ P0.5-1: 후보 품질 필터
            }
        }
    
    def record_collection(self, total: int, success: int, failed: int):
        """수집 단계 기록"""
        self.metrics["collection"]["total"] += total
        self.metrics["collection"]["success"] += success
        self.metrics["collection"]["failed"] += failed
    
    def record_parsing(self, success: bool, text_length: int = 0, paragraphs: int = 0, quality: str = "UNKNOWN"):
        """파싱 단계 기록"""
        self.metrics["parsing"]["total"] += 1
        if success:
            self.metrics["parsing"]["success"] += 1
            self.metrics["parsing"]["total_text_length"] += text_length
            self.metrics["parsing"]["total_paragraphs"] += paragraphs
            if quality == "LOW":
                self.metrics["parsing"]["low_quality"] += 1
        
        # 평균 계산
        if self.metrics["parsing"]["success"] > 0:
            self.metrics["parsing"]["avg_text_length"] = (
                self.metrics["parsing"]["total_text_length"] / 
                self.metrics["parsing"]["success"]
            )
            self.metrics["parsing"]["avg_paragraphs"] = (
                self.metrics["parsing"]["total_paragraphs"] / 
                self.metrics["parsing"]["success"]
            )
    
    def record_ticker_matching(
        self, 
        success: bool, 
        method: Optional[str] = None, 
        skipped: bool = False,
        match_category: Optional[str] = None  # ⭐ P0.7: 4분류 추가
    ):
        """Ticker 매칭 단계 기록"""
        self.metrics["ticker_matching"]["total"] += 1
        if skipped:
            self.metrics["ticker_matching"]["skipped"] += 1
        elif success:
            self.metrics["ticker_matching"]["success"] += 1
            if method:
                if method == "RULE":
                    self.metrics["ticker_matching"]["rule_based"] += 1
                elif method == "FUZZY":
                    self.metrics["ticker_matching"]["fuzzy"] += 1
                elif method == "LLM":
                    self.metrics["ticker_matching"]["llm"] += 1
            
            # ⭐ P0.7: 4분류 메트릭 기록
            if match_category:
                if match_category == "MATCHED_IN_UNIVERSE":
                    self.metrics["ticker_matching"]["matched_in_universe"] += 1
                elif match_category == "MATCHED_OUT_OF_UNIVERSE":
                    self.metrics["ticker_matching"]["matched_out_of_universe"] += 1
                elif match_category == "AMBIGUOUS_NAME":
                    self.metrics["ticker_matching"]["ambiguous_name"] += 1
        else:
            self.metrics["ticker_matching"]["failed"] += 1
            # ⭐ 실패 시에도 match_category 확인
            if match_category == "NOT_FOUND":
                self.metrics["ticker_matching"]["not_found"] += 1
            elif match_category == "AMBIGUOUS_NAME":
                self.metrics["ticker_matching"]["ambiguous_name"] += 1
    
    def record_driver_normalization(self, success: bool, method: Optional[str] = None, single_candidate: bool = False):
        """Driver 정규화 단계 기록"""
        self.metrics["driver_normalization"]["total"] += 1
        if success:
            self.metrics["driver_normalization"]["success"] += 1
            if method:
                if method == "SYNONYM":
                    self.metrics["driver_normalization"]["synonym"] += 1
                elif method == "LLM":
                    self.metrics["driver_normalization"]["llm"] += 1
            if single_candidate:
                self.metrics["driver_normalization"]["single_candidate"] += 1
        else:
            self.metrics["driver_normalization"]["failed"] += 1
    
    def record_quality_gate(self, status: str, hold_reason: Optional[str] = None):
        """Quality Gate 기록"""
        self.metrics["quality_gate"]["total"] += 1
        if status == "PASS":
            self.metrics["quality_gate"]["pass"] += 1
        elif status == "HOLD":
            self.metrics["quality_gate"]["hold"] += 1
            if hold_reason:
                if hold_reason not in self.metrics["quality_gate"]["hold_counts_by_reason"]:
                    self.metrics["quality_gate"]["hold_counts_by_reason"][hold_reason] = 0
                self.metrics["quality_gate"]["hold_counts_by_reason"][hold_reason] += 1
        elif status == "DROP":
            self.metrics["quality_gate"]["drop"] += 1
    
    def record_insight_extraction(self, success: bool, confidence: Optional[str] = None):
        """Insight 추출 단계 기록"""
        self.metrics["insight_extraction"]["total"] += 1
        if success:
            self.metrics["insight_extraction"]["success"] += 1
            if confidence:
                if confidence == "HIGH":
                    self.metrics["insight_extraction"]["high_confidence"] += 1
                elif confidence == "MED":
                    self.metrics["insight_extraction"]["med_confidence"] += 1
                elif confidence == "LOW":
                    self.metrics["insight_extraction"]["low_confidence"] += 1
        else:
            self.metrics["insight_extraction"]["failed"] += 1
    
    def record_edge_enrichment(self, success: bool, alignment: Optional[str] = None):
        """Edge Enrichment 단계 기록"""
        self.metrics["edge_enrichment"]["total"] += 1
        if success:
            self.metrics["edge_enrichment"]["success"] += 1
            if alignment:
                if alignment == "ALIGNED":
                    self.metrics["edge_enrichment"]["aligned"] += 1
                elif alignment == "PARTIAL":
                    self.metrics["edge_enrichment"]["partial"] += 1
                elif alignment == "CONFLICT":
                    self.metrics["edge_enrichment"]["conflict"] += 1
        else:
            self.metrics["edge_enrichment"]["failed"] += 1
    
    def record_llm_usage(
        self,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        stage: Optional[str] = None
    ):
        """
        LLM 토큰 사용량 기록
        
        Args:
            prompt_tokens: 프롬프트 토큰 수
            completion_tokens: 완성 토큰 수
            stage: 단계 (ticker_matcher, driver_normalizer, insight_extractor)
        """
        self.metrics["llm_usage"]["total_prompt_tokens"] += prompt_tokens
        self.metrics["llm_usage"]["total_completion_tokens"] += completion_tokens
        self.metrics["llm_usage"]["total_tokens"] += (prompt_tokens + completion_tokens)
        
        # 단계별 호출 횟수
        if stage:
            if stage not in self.metrics["llm_usage"]["calls_by_stage"]:
                self.metrics["llm_usage"]["calls_by_stage"][stage] = 0
            self.metrics["llm_usage"]["calls_by_stage"][stage] += 1
        
        # 비용 추정 (gpt-5-mini 기준: $0.15/1M input, $0.60/1M output)
        input_cost = (prompt_tokens / 1_000_000) * 0.15
        output_cost = (completion_tokens / 1_000_000) * 0.60
        self.metrics["llm_usage"]["estimated_cost_usd"] += (input_cost + output_cost)
    
    def record_idempotency_skip(self):
        """이미 처리된 리포트 스킵 기록"""
        self.metrics["idempotency"]["skipped_already_processed"] += 1
    
    def record_hold_reason(self, reason: str):
        """HOLD 사유 기록"""
        if reason not in self.metrics["idempotency"]["hold_counts_by_reason"]:
            self.metrics["idempotency"]["hold_counts_by_reason"][reason] = 0
        self.metrics["idempotency"]["hold_counts_by_reason"][reason] += 1
    
    def record_rebuild_skip(self, reason: str, candidate_count: int = 0):
        """
        Rebuild 스킵 메트릭 기록
        
        ⭐ P0-추가2: Rebuild 스킵 이유 및 후보 수 기록
        
        Args:
            reason: 스킵 사유 (MULTIPLE_CANDIDATES, DATE_TOO_FAR, SOURCE_MISMATCH, LOW_SIMILARITY, NO_MATCH_FOUND)
            candidate_count: 매칭 후보 수
        """
        self.metrics["rebuild_skips"]["total_skips"] += 1
        
        if reason not in self.metrics["rebuild_skips"]["by_reason"]:
            self.metrics["rebuild_skips"]["by_reason"][reason] = {
                "count": 0,
                "total_candidates": 0
            }
        
        self.metrics["rebuild_skips"]["by_reason"][reason]["count"] += 1
        self.metrics["rebuild_skips"]["by_reason"][reason]["total_candidates"] += candidate_count
        self.metrics["rebuild_skips"]["total_candidates"] += candidate_count
    
    def record_detail_metadata(self, fetch_success: bool, extraction_success: bool, fields: Dict[str, Any]):
        """
        ⭐ P0-β3: 상세 페이지 메타데이터 추출 성공률 기록
        
        Args:
            fetch_success: HTML 다운로드 성공 여부
            extraction_success: 메타데이터 추출 성공 여부
            fields: 추출된 필드 딕셔너리 (stock_name, industry_category 등)
        """
        self.metrics["detail_metadata"]["total_attempts"] += 1
        if fetch_success:
            self.metrics["detail_metadata"]["fetch_success"] += 1
        if extraction_success:
            self.metrics["detail_metadata"]["extraction_success"] += 1
        
        # 필드별 추출 성공/실패 기록
        field_names = ["stock_name", "industry_category", "target_price", "investment_opinion", "analyst_name"]
        for field_name in field_names:
            if fields.get(field_name):
                self.metrics["detail_metadata"]["fields_extracted"][field_name] += 1
            else:
                self.metrics["detail_metadata"]["fields_missing"][field_name] += 1
    
    def record_driver_candidate_extraction(
        self,
        success: bool = False,
        original_chunk_len: int = 0,
        extracted_count: int = 0,
        inserted_count: int = 0,
        skipped_count: int = 0,
        skipped_unique_conflict: int = 0,  # ⭐ P0-추가 4
        quality_filtered: int = 0,
        terms_known_count: int = 0,  # ⭐ P0-추가 3
        terms_unknown_count: int = 0  # ⭐ P0-추가 3
    ):
        """
        ⭐ P0-3: Driver 후보 추출 메트릭 기록 (3분류: INPUT/EXTRACTOR/PERSIST)
        ⭐ P0-추가 3: known/unknown 분해 계측
        ⭐ P0-추가 4: unique 충돌 카운트
        
        Args:
            success: 후보 등록 성공 여부
            original_chunk_len: 원문 chunk 길이
            extracted_count: 추출된 후보 개수
            inserted_count: DB에 저장된 후보 개수
            skipped_count: 쿨다운으로 스킵된 개수
            skipped_unique_conflict: unique 충돌로 스킵된 개수
            quality_filtered: 품질 필터로 제외된 개수
            terms_known_count: 이미 DB에 있는 driver 개수
            terms_unknown_count: 새로운 unknown driver 개수
        """
        self.metrics["driver_candidate_extraction"]["total_attempts"] += 1
        
        # INPUT 문제: 원문이 너무 짧음
        if original_chunk_len < 100:
            self.metrics["driver_candidate_extraction"]["input_empty_or_short"] += 1
        
        # EXTRACTOR 문제: 추출된 후보가 0개
        self.metrics["driver_candidate_extraction"]["terms_extracted_total"] += extracted_count
        
        # ⭐ P0-추가 3: known/unknown 분해
        self.metrics["driver_candidate_extraction"]["terms_known_count"] += terms_known_count
        self.metrics["driver_candidate_extraction"]["terms_unknown_count"] += terms_unknown_count
        
        # PERSIST 문제: 추출은 됐는데 저장이 안 됨
        self.metrics["driver_candidate_extraction"]["rows_inserted"] += inserted_count
        
        # 스킵 카운트
        self.metrics["driver_candidate_extraction"]["skipped_cooldown"] += skipped_count
        self.metrics["driver_candidate_extraction"]["skipped_unique_conflict"] += skipped_unique_conflict
        
        # 품질 필터 카운트
        self.metrics["driver_candidate_extraction"]["quality_filtered"] += quality_filtered
    
    def record_route_confidence_bucket(self, confidence: float):
        """
        ⭐ P0-α: route_confidence 분포 기록 (캘리브레이션용)
        
        Args:
            confidence: route_confidence 값 (0.0~1.0)
        """
        if confidence < 0.5:
            bucket = "0.0-0.5"
        elif confidence < 0.6:
            bucket = "0.5-0.6"
        elif confidence < 0.7:
            bucket = "0.6-0.7"
        elif confidence < 0.8:
            bucket = "0.7-0.8"
        else:
            bucket = "0.8-1.0"
        
        self.metrics["route_confidence"]["buckets"][bucket] = (
            self.metrics["route_confidence"]["buckets"].get(bucket, 0) + 1
        )
    
    def _identify_bottlenecks(self) -> List[Dict[str, Any]]:
        """병목 지점 식별"""
        bottlenecks = []
        
        # 각 단계별 성공률 계산
        stages = [
            ("collection", "collection"),
            ("parsing", "parsing"),
            ("ticker_matching", "ticker_matching"),
            ("driver_normalization", "driver_normalization"),
            ("quality_gate", "quality_gate"),
            ("insight_extraction", "insight_extraction"),
            ("edge_enrichment", "edge_enrichment")
        ]
        
        for stage_name, stage_key in stages:
            stage_metrics = self.metrics[stage_key]
            total = stage_metrics.get("total", 0)
            
            if total == 0:
                continue
            
            if stage_name == "collection":
                success = stage_metrics.get("success", 0)
            elif stage_name == "quality_gate":
                success = stage_metrics.get("pass", 0)
            else:
                success = stage_metrics.get("success", 0)
            
            success_rate = (success / total * 100) if total > 0 else 0
            
            if success_rate < 80:  # 80% 미만이면 병목
                bottlenecks.append({
                    "stage": stage_name,
                    "success_rate": round(success_rate, 2),
                    "total": total,
                    "success": success
                })
        
        return sorted(bottlenecks, key=lambda x: x["success_rate"])
    
    def generate_report(self) -> Dict[str, Any]:
        """최종 리포트 생성"""
        collection_total = self.metrics["collection"]["total"]
        edge_success = self.metrics["edge_enrichment"]["success"]
        
        end_to_end_rate = (
            (edge_success / collection_total * 100) 
            if collection_total > 0 else 0
        )
        
        return {
            "summary": {
                "total_reports": collection_total,
                "end_to_end_success": edge_success,
                "end_to_end_rate": round(end_to_end_rate, 2),
                "timestamp": datetime.utcnow().isoformat() + "Z"
            },
            "by_stage": self.metrics,
            "bottlenecks": self._identify_bottlenecks(),
            "stage_success_rates": {
                "collection": round(
                    (self.metrics["collection"]["success"] / max(collection_total, 1)) * 100, 2
                ),
                "parsing": round(
                    (self.metrics["parsing"]["success"] / max(self.metrics["parsing"]["total"], 1)) * 100, 2
                ),
                "ticker_matching": round(
                    (self.metrics["ticker_matching"]["success"] / max(self.metrics["ticker_matching"]["total"], 1)) * 100, 2
                ),
                "driver_normalization": round(
                    (self.metrics["driver_normalization"]["success"] / max(self.metrics["driver_normalization"]["total"], 1)) * 100, 2
                ),
                "quality_gate_pass": round(
                    (self.metrics["quality_gate"]["pass"] / max(self.metrics["quality_gate"]["total"], 1)) * 100, 2
                ),
                "insight_extraction": round(
                    (self.metrics["insight_extraction"]["success"] / max(self.metrics["insight_extraction"]["total"], 1)) * 100, 2
                ),
                "edge_enrichment": round(
                    (self.metrics["edge_enrichment"]["success"] / max(self.metrics["edge_enrichment"]["total"], 1)) * 100, 2
                )
            }
        }
    
    def save_report(self, output_path: Optional[Path] = None):
        """리포트 저장"""
        if output_path is None:
            output_path = project_root / "reports" / f"pipeline_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        report = self.generate_report()
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        logger.info(f"파이프라인 지표 리포트 저장: {output_path}")
        return output_path

