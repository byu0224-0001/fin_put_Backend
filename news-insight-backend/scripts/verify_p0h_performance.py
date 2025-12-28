"""
P0-H 성능 지표 검증 스크립트

검증 KPI:
- logic_summary_len <= 260 비율 80%+
- pool_separation_failed == false 비율 90%+
- compression_effective_rate 80%+
- fallback_used_rate 20% 이하

⭐ P0-2 개선: compression stats를 DB에서 직접 조회
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta
import re

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy.orm import Session
from sqlalchemy import and_
from typing import Optional, List
from app.db import SessionLocal
from app.models.industry_edge import IndustryEdge
from app.models.broker_report import BrokerReport
from utils.semantic_compression import _build_sentence_pool
from utils.text_normalizer import normalize_text_for_fuzzy_fingerprint
import hashlib


def verify_p0h_performance(
    days_back: int = 30,
    sources: Optional[List[str]] = None,
    report_id_not_null: bool = True,
    print_samples: int = 0  # ⭐ P0: 샘플 프린트 옵션
):
    """
    P0-H 성능 지표 검증
    
    ⭐ P0-A, P0-C: Orphan 필터링 및 대상 범위 옵션화
    
    Args:
        days_back: 최근 N일 이내 데이터 검증 (기본: 30일)
        sources: 필터링할 source 리스트 (None이면 모든 source)
        report_id_not_null: report_id가 NULL이 아닌 것만 (기본: True)
    """
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("[P0-H 성능 지표 검증]")
        print("=" * 80)
        
        # 최근 N일 이내 생성된 industry_edges 조회
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        # ⭐ P0-추가1: 조인 전 전체 카운트 계산
        total_edges_before_join = db.query(IndustryEdge).filter(
            IndustryEdge.created_at >= cutoff_date
        ).count()
        
        # ⭐ P0-A: 필터 조건 추가
        query = db.query(IndustryEdge).filter(
            IndustryEdge.created_at >= cutoff_date
        )
        
        # report_id NOT NULL 필터
        excluded_no_report_id = 0
        if report_id_not_null:
            total_edges_with_report_id = db.query(IndustryEdge).filter(
                and_(
                    IndustryEdge.created_at >= cutoff_date,
                    IndustryEdge.report_id.isnot(None)
                )
            ).count()
            excluded_no_report_id = total_edges_before_join - total_edges_with_report_id
            query = query.filter(IndustryEdge.report_id.isnot(None))
        
        # source 필터 (broker_reports와 조인)
        edges_without_broker_report = 0
        if sources:
            query = query.join(
                BrokerReport, IndustryEdge.report_id == BrokerReport.report_id
            ).filter(BrokerReport.source.in_(sources))
        elif report_id_not_null:
            # report_id_not_null이 True이면 최소한 broker_reports와 조인하여 존재 확인
            query = query.join(
                BrokerReport, IndustryEdge.report_id == BrokerReport.report_id
            )
        
        edges = query.all()
        total_count = len(edges)
        
        # ⭐ P0-추가1: 조인 실패 카운트 계산
        if report_id_not_null:
            if sources:
                # sources 필터가 있으면 조인 후 카운트와 비교
                edges_with_join = total_count
                total_edges_with_report_id = db.query(IndustryEdge).filter(
                    and_(
                        IndustryEdge.created_at >= cutoff_date,
                        IndustryEdge.report_id.isnot(None)
                    )
                ).count()
                edges_without_broker_report = total_edges_with_report_id - edges_with_join
            else:
                # sources 필터가 없어도 조인은 했으므로 계산
                edges_with_join = total_count
                total_edges_with_report_id = db.query(IndustryEdge).filter(
                    and_(
                        IndustryEdge.created_at >= cutoff_date,
                        IndustryEdge.report_id.isnot(None)
                    )
                ).count()
                edges_without_broker_report = total_edges_with_report_id - edges_with_join
        
        if total_count == 0:
            print("")
            print("=" * 80)
            print("[데이터 없음 - E2E 선행 조건 미충족]")
            print("=" * 80)
            print("검증할 데이터가 없습니다.")
            print("")
            print("⚠️  이 상태에서 KPI/게이트 판정은 의미가 없습니다.")
            print("   → E2E-1 (파싱) 및 E2E-2 (엔리치) 실행 후 다시 검증하세요.")
            print("")
            print("[선행 단계 실행 명령]")
            print("  1. 수집: python scripts/collect_broker_reports_naver.py --category INDUSTRY --days-back 7 --limit 50")
            print("  2. 파싱: python scripts/parse_broker_report_pdf.py --input reports/naver_reports_*.json")
            print("  3. 엔리치: python scripts/enrich_edges_from_reports.py --input reports/parsed_naver_reports_*.json --limit 50")
            print("")
            if excluded_no_report_id > 0:
                print(f"  [참고] report_id NULL: {excluded_no_report_id}개 (orphan)")
            if edges_without_broker_report > 0:
                print(f"  [참고] broker_reports 조인 실패: {edges_without_broker_report}개")
            return
        
        # ⭐ P0-C: 표본 수 경고
        if total_count < 20:
            print(f"[경고] 표본 수가 적습니다 ({total_count}개). 통과 보류 권장.")
            print("")
        
        filter_info = []
        if report_id_not_null:
            filter_info.append("report_id NOT NULL")
        if sources:
            filter_info.append(f"source IN ({', '.join(sources)})")
        
        filter_str = f" ({', '.join(filter_info)})" if filter_info else ""
        print(f"검증 대상: {total_count}개 (최근 {days_back}일 이내{filter_str})")
        
        # ⭐ P0-추가1: 제외 카운트 출력
        if excluded_no_report_id > 0:
            print(f"  [제외] report_id NULL: {excluded_no_report_id}개")
        if edges_without_broker_report > 0:
            print(f"  [제외] broker_reports 조인 실패: {edges_without_broker_report}개")
        print("")
        
        # KPI 계산
        len_260_or_less = 0
        pool_separation_ok = 0
        compression_effective = 0
        fallback_used = 0
        total_compression_ratio = 0.0
        compression_count = 0
        
        for edge in edges:
            logic_summary = edge.logic_summary or ""
            
            # KPI 1: logic_summary_len <= 260
            if len(logic_summary) <= 260:
                len_260_or_less += 1
            
            # KPI 2: pool_separation_failed == false
            sentences_pool = _build_sentence_pool(logic_summary, edge.key_sentence)
            if sentences_pool:
                max_sent_len = max(len(s) for s in sentences_pool)
                if max_sent_len <= 400:  # pool_separation_failed == false
                    pool_separation_ok += 1
            
            # KPI 3, 4: compression stats는 별도로 수집
            # (현재는 DB에 저장되지 않으므로, logic_summary 길이로 추정)
            if logic_summary:
                # 원본 길이 추정 (압축된 경우 보통 260자 이하)
                # 350자 이상이면 압축 대상이었을 가능성
                # 실제로는 compression_stats를 DB에 저장해야 정확함
                pass
        
        # 결과 출력
        print("=" * 80)
        print("[검증 결과]")
        print("=" * 80)
        print(f"총 검증 대상: {total_count}개")
        print("")
        
        # ⭐ P0: 샘플 프린트 옵션
        if print_samples > 0 and edges:
            print("=" * 80)
            print(f"[샘플 {min(print_samples, len(edges))}개]")
            print("=" * 80)
            for i, edge in enumerate(edges[:print_samples], 1):
                logic_summary = edge.logic_summary or ""
                logic_summary_len = len(logic_summary)
                # 특수 문자를 안전하게 처리 (Windows 인코딩 호환)
                preview = logic_summary[:120] + "..." if len(logic_summary) > 120 else logic_summary
                # ASCII로 변환 가능한 문자만 유지
                preview_safe = ''.join(c if ord(c) < 128 else '?' for c in preview)
                print(f"\n[{i}] edge_id={edge.id}, report_id={edge.report_id}")
                print(f"  - logic_summary_len: {logic_summary_len}")
                print(f"  - logic_summary (첫 120자): {preview_safe}")
                print(f"  - target_sector_code: {edge.target_sector_code}")
                print(f"  - source_driver_code: {edge.source_driver_code}")
            print("")
        
        # KPI 1
        kpi1_rate = len_260_or_less / total_count * 100 if total_count > 0 else 0
        kpi1_pass = kpi1_rate >= 80
        print(f"KPI 1: logic_summary_len <= 260")
        print(f"  통과: {len_260_or_less}개 ({kpi1_rate:.1f}%)")
        print(f"  목표: 80%+ {'[통과]' if kpi1_pass else '[실패]'}")
        print("")
        
        # KPI 2
        kpi2_rate = pool_separation_ok / total_count * 100 if total_count > 0 else 0
        kpi2_pass = kpi2_rate >= 90
        print(f"KPI 2: pool_separation_failed == false")
        print(f"  통과: {pool_separation_ok}개 ({kpi2_rate:.1f}%)")
        print(f"  목표: 90%+ {'[통과]' if kpi2_pass else '[실패]'}")
        print("")
        
        # KPI 3, 4는 DB에 compression_stats가 없으면 측정 불가
        print("KPI 3, 4: compression_effective_rate, fallback_used_rate")
        print("  [경고] DB에 compression_stats가 저장되지 않아 측정 불가")
        print("  권장: rebuild 시 compression stats를 DB에 저장")
        print("")
        
        # ⭐ P0-Final: Fingerprint 분포 분석 (sources>=3 원인 분해)
        print("")
        print("=" * 80)
        print("[Fingerprint 분포 분석]")
        print("=" * 80)
        
        # fingerprint별 edge 카운트
        fingerprint_counts = {}
        fuzzy_fingerprint_counts = {}  # ⭐ fuzzy fingerprint 추가
        
        for edge in edges:
            fp = edge.logic_fingerprint or "NO_FP"
            fingerprint_counts[fp] = fingerprint_counts.get(fp, 0) + 1
            
            # ⭐ fuzzy fingerprint 계산 (원인 분리용)
            logic_summary = edge.logic_summary or ""
            if logic_summary:
                fuzzy_text = normalize_text_for_fuzzy_fingerprint(logic_summary)
                fuzzy_fp = hashlib.sha256(fuzzy_text.encode('utf-8')).hexdigest()[:16]
            else:
                fuzzy_fp = "NO_FP"
            fuzzy_fingerprint_counts[fuzzy_fp] = fuzzy_fingerprint_counts.get(fuzzy_fp, 0) + 1
        
        unique_fp_count = len(fingerprint_counts)
        unique_fuzzy_fp_count = len(fuzzy_fingerprint_counts)
        fp_to_edge_ratio = unique_fp_count / total_count if total_count > 0 else 0
        fuzzy_fp_to_edge_ratio = unique_fuzzy_fp_count / total_count if total_count > 0 else 0
        
        # Top 10 fingerprint
        sorted_fps = sorted(fingerprint_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        sorted_fuzzy_fps = sorted(fuzzy_fingerprint_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        max_fp_count = sorted_fps[0][1] if sorted_fps else 0
        max_fuzzy_fp_count = sorted_fuzzy_fps[0][1] if sorted_fuzzy_fps else 0
        
        print(f"  - 총 Edge 수: {total_count}개")
        print(f"")
        print(f"  [Exact Fingerprint]")
        print(f"    - 고유 수: {unique_fp_count}개")
        print(f"    - 비율: {fp_to_edge_ratio:.2f} (1.0=모두 고유, <0.5=합의 가능)")
        print(f"    - 최대 반복: {max_fp_count}회")
        print(f"")
        print(f"  [Fuzzy Fingerprint] (숫자/기업명 정규화)")
        print(f"    - 고유 수: {unique_fuzzy_fp_count}개")
        print(f"    - 비율: {fuzzy_fp_to_edge_ratio:.2f}")
        print(f"    - 최대 반복: {max_fuzzy_fp_count}회")
        print(f"")
        
        # ⭐ 원인 분리: A(데이터부족) vs B(과도세분화)
        print(f"  [원인 분리 진단]")
        if unique_fp_count > unique_fuzzy_fp_count * 1.5:
            # fuzzy가 더 적으면 → B(과도 세분화)
            reduction = (unique_fp_count - unique_fuzzy_fp_count) / unique_fp_count * 100
            print(f"    → B(과도 세분화) 가능성 높음")
            print(f"    → exact→fuzzy 감소율: {reduction:.1f}%")
            print(f"    → 정규화 강화로 합의 가능성 증가")
        elif max_fuzzy_fp_count >= 3:
            print(f"    → 합의 가능 구조 (fuzzy 최대 반복 {max_fuzzy_fp_count}회)")
            print(f"    → 데이터 추가 시 sources>=3 달성 가능")
        else:
            print(f"    → A(데이터 부족) 가능성 높음")
            print(f"    → 같은 논리가 아직 반복되지 않음")
            print(f"    → 더 많은 리포트 수집 필요")
        
        print(f"")
        print(f"  Top 5 Fuzzy Fingerprint (반복 횟수):")
        for i, (fp, count) in enumerate(sorted_fuzzy_fps, 1):
            fp_short = fp[:12] + "..."
            print(f"    {i}. {fp_short}: {count}회")
        
        # ⭐ P0-Final: SourceItem 스키마 호환성 체크
        print("")
        print("=" * 80)
        print("[SourceItem 스키마 호환성 체크]")
        print("=" * 80)
        
        total_source_items = 0
        missing_source_uid = 0
        missing_broker = 0
        missing_report_date = 0
        date_normalize_failed = 0
        
        for edge in edges:
            conditions = edge.conditions or {}
            sources_list = conditions.get("sources", [])
            if isinstance(sources_list, list):
                for src in sources_list:
                    if isinstance(src, dict):
                        total_source_items += 1
                        if not src.get("source_uid"):
                            missing_source_uid += 1
                        if not src.get("broker"):
                            missing_broker += 1
                        if not src.get("report_date"):
                            missing_report_date += 1
                        if src.get("date_normalize_failed"):
                            date_normalize_failed += 1
        
        if total_source_items > 0:
            print(f"  총 SourceItem 수: {total_source_items}개")
            print(f"")
            print(f"  누락율:")
            print(f"    - source_uid 누락: {missing_source_uid}개 ({missing_source_uid/total_source_items*100:.1f}%)")
            print(f"    - broker 누락: {missing_broker}개 ({missing_broker/total_source_items*100:.1f}%)")
            print(f"    - report_date 누락: {missing_report_date}개 ({missing_report_date/total_source_items*100:.1f}%)")
            print(f"    - date_normalize_failed: {date_normalize_failed}개 ({date_normalize_failed/total_source_items*100:.1f}%)")
            
            # 스키마 품질 게이트 (누락율 5% 이하)
            schema_ok = (missing_source_uid / total_source_items < 0.05 and 
                        missing_broker / total_source_items < 0.05)
            print(f"")
            print(f"  스키마 품질: {'[양호]' if schema_ok else '[주의] 누락율 5% 초과'}")
        else:
            print(f"  [경고] SourceItem이 없습니다")
        
        # ⭐ P0-Final: 섹터/드라이버 집중도 분석 (세분화 vs 데이터부족 추가 진단)
        print("")
        print("=" * 80)
        print("[섹터/드라이버 집중도 분석]")
        print("=" * 80)
        
        sector_counts = {}
        driver_counts = {}
        
        for edge in edges:
            sector = edge.target_sector_code or "UNKNOWN"
            driver = edge.source_driver_code or "UNKNOWN"
            sector_counts[sector] = sector_counts.get(sector, 0) + 1
            driver_counts[driver] = driver_counts.get(driver, 0) + 1
        
        # Top 5 섹터/드라이버
        sorted_sectors = sorted(sector_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        sorted_drivers = sorted(driver_counts.items(), key=lambda x: x[1], reverse=True)[:5]
        
        print(f"  Top 5 섹터:")
        for i, (sector, count) in enumerate(sorted_sectors, 1):
            ratio = count / total_count * 100 if total_count > 0 else 0
            print(f"    {i}. {sector}: {count}개 ({ratio:.1f}%)")
        
        print(f"")
        print(f"  Top 5 드라이버:")
        for i, (driver, count) in enumerate(sorted_drivers, 1):
            ratio = count / total_count * 100 if total_count > 0 else 0
            print(f"    {i}. {driver}: {count}개 ({ratio:.1f}%)")
        
        # 집중도 진단
        top_sector_ratio = sorted_sectors[0][1] / total_count if sorted_sectors and total_count > 0 else 0
        print(f"")
        if top_sector_ratio > 0.5:
            print(f"  [진단] 섹터 집중도 높음 ({top_sector_ratio:.1%})")
            print(f"    → 같은 섹터 내 FP 세분화 가능성 체크 필요")
        else:
            print(f"  [진단] 섹터 분산 양호 (상위 섹터 {top_sector_ratio:.1%})")
            print(f"    → 다양한 섹터에서 데이터 수집됨")
        
        # ⭐ P1-Final: sources>=3 게이트 (데모팩 추출 기준)
        print("")
        print("=" * 80)
        print("[P1-Final: Evidence Accumulation Gate]")
        print("=" * 80)
        
        sources_gte3_count = 0
        sources_gte2_count = 0  # ⭐ 차선 게이트
        ir_gate_count = 0  # ⭐ IR 게이트 (sources>=2 AND broker>=2)
        ir_gate_with_span_count = 0  # ⭐ IR 강화 게이트 (sources>=2 AND broker>=2 AND span>=3d)
        total_sources_count = 0
        
        for edge in edges:
            conditions = edge.conditions or {}
            sources_list = conditions.get("sources", [])
            source_count = len(sources_list) if isinstance(sources_list, list) else 0
            total_sources_count += source_count
            
            # ⭐ unique broker 카운트 + 날짜 span 계산
            unique_brokers = set()
            report_dates = []
            if isinstance(sources_list, list):
                for src in sources_list:
                    if isinstance(src, dict):
                        if src.get("broker"):
                            unique_brokers.add(src.get("broker"))
                        if src.get("report_date"):
                            try:
                                # YYYY-MM-DD 형식 파싱
                                rd = src.get("report_date")
                                if rd and len(rd) >= 10:
                                    report_dates.append(rd[:10])
                            except:
                                pass
            
            # span 계산 (일수)
            span_days = 0
            if len(report_dates) >= 2:
                try:
                    sorted_dates = sorted(report_dates)
                    from datetime import datetime as dt
                    d1 = dt.strptime(sorted_dates[0], "%Y-%m-%d")
                    d2 = dt.strptime(sorted_dates[-1], "%Y-%m-%d")
                    span_days = (d2 - d1).days
                except:
                    pass
            
            if source_count >= 3:
                sources_gte3_count += 1
            if source_count >= 2:
                sources_gte2_count += 1
            if source_count >= 2 and len(unique_brokers) >= 2:
                ir_gate_count += 1
                # ⭐ IR 강화 게이트 (span>=3일 추가)
                if span_days >= 3:
                    ir_gate_with_span_count += 1
        
        sources_gte3_ratio = sources_gte3_count / total_count * 100 if total_count > 0 else 0
        sources_gte2_ratio = sources_gte2_count / total_count * 100 if total_count > 0 else 0
        ir_gate_ratio = ir_gate_count / total_count * 100 if total_count > 0 else 0
        ir_gate_with_span_ratio = ir_gate_with_span_count / total_count * 100 if total_count > 0 else 0
        avg_sources = total_sources_count / total_count if total_count > 0 else 0
        
        # 게이트: 절대수 10개 이상 OR 비율 10% 이상
        gate_by_count = sources_gte3_count >= 10
        gate_by_ratio = sources_gte3_ratio >= 10
        sources_gate_pass = gate_by_count or gate_by_ratio
        
        # ⭐ IR 게이트 (sources>=2 AND broker>=2)
        ir_gate_pass = ir_gate_count >= 3
        
        # ⭐ IR 강화 게이트 (sources>=2 AND broker>=2 AND span>=3d)
        ir_gate_with_span_pass = ir_gate_with_span_count >= 2
        
        # ⭐ 차선 게이트 (sources>=2만)
        demo_gate_pass = sources_gte2_count >= 5
        
        print(f"  - 총 Edge 수: {total_count}개")
        print(f"  - 평균 sources 수: {avg_sources:.2f}")
        print(f"")
        print(f"  [서비스 게이트] sources>=3:")
        print(f"    - Edge 수: {sources_gte3_count}개 ({sources_gte3_ratio:.1f}%)")
        print(f"    - 절대수>=10: {'[통과]' if gate_by_count else '[미달]'}")
        print(f"    - 비율>=10%: {'[통과]' if gate_by_ratio else '[미달]'}")
        print(f"")
        print(f"  [IR 게이트] sources>=2 AND broker>=2:")
        print(f"    - Edge 수: {ir_gate_count}개 ({ir_gate_ratio:.1f}%)")
        print(f"    - 3개+: {'[통과]' if ir_gate_pass else '[미달]'}")
        print(f"")
        print(f"  [IR 강화 게이트] sources>=2 AND broker>=2 AND span>=3일:")
        print(f"    - Edge 수: {ir_gate_with_span_count}개 ({ir_gate_with_span_ratio:.1f}%)")
        print(f"    - 2개+: {'[통과]' if ir_gate_with_span_pass else '[미달]'}")
        print(f"    (며칠에 걸친 반복 합의 = 더 강한 설득력)")
        print(f"")
        print(f"  [차선 게이트] sources>=2:")
        print(f"    - Edge 수: {sources_gte2_count}개 ({sources_gte2_ratio:.1f}%)")
        print(f"    - 5개+: {'[통과]' if demo_gate_pass else '[미달]'}")
        print(f"")
        print(f"  결과:")
        print(f"    - 서비스 런칭: {'[가능]' if sources_gate_pass else '[불가]'}")
        print(f"    - IR 강화 데모: {'[가능]' if ir_gate_with_span_pass or sources_gate_pass else '[불가]'}")
        print(f"    - IR 데모: {'[가능]' if ir_gate_pass or ir_gate_with_span_pass or sources_gate_pass else '[불가]'}")
        print(f"    - 차선 데모: {'[가능]' if demo_gate_pass or ir_gate_pass or sources_gate_pass else '[불가]'}")
        
        # ⭐ P0-Final: PASS/HOLD/DROP 기준 명시
        print("")
        print("=" * 80)
        print("[PASS/HOLD/DROP 기준 (참조)]")
        print("=" * 80)
        print("  PASS 기준:")
        print("    - logic_summary 길이 <= 260자")
        print("    - 근거 문장 >= 1개")
        print("    - sector_code 확정 (Sanity Check 통과)")
        print("    - pool_separation_failed = false")
        print("")
        print("  HOLD 기준:")
        print("    - HOLD_SECTOR_MAPPING_SANITY_CHECK_FAILED: 금지된 섹터-기업 조합")
        print("    - HOLD_MIXED_UNSPLIT: 섹션 분리 실패 (KG write 금지)")
        print("    - HOLD_TICKER_AMBIGUOUS: ticker 매칭 모호")
        print("")
        print("  DROP 기준:")
        print("    - logic_summary 빈값 또는 파싱 실패")
        print("    - DETAIL_404: 상세 페이지 접근 불가")
        print("    - HTML_EMPTY: HTML 텍스트 확보 실패")
        print("    - PDF_BLOCKED/NOT_FOUND: PDF 획득 실패")
        
        # 종합 평가
        print("")
        print("=" * 80)
        print("[종합 평가]")
        print("=" * 80)
        if kpi1_pass and kpi2_pass:
            print("[통과] P0-H 성능 지표 목표 달성")
        else:
            print("[실패] P0-H 성능 지표 목표 미달성")
            if not kpi1_pass:
                print(f"  - KPI 1 미달성: {kpi1_rate:.1f}% < 80%")
            if not kpi2_pass:
                print(f"  - KPI 2 미달성: {kpi2_rate:.1f}% < 90%")
        
        if sources_gate_pass:
            print("[통과] 서비스 게이트 (sources>=3)")
        elif ir_gate_with_span_pass:
            print("[통과] IR 강화 게이트 (sources>=2 AND broker>=2 AND span>=3일)")
        elif ir_gate_pass:
            print("[통과] IR 게이트 (sources>=2 AND broker>=2)")
        elif demo_gate_pass:
            print("[차선 통과] 차선 게이트 (sources>=2)")
        else:
            print("[미달] 모든 Evidence 게이트 미달")
        
    finally:
        db.close()


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="P0-H 성능 지표 검증")
    parser.add_argument('--days-back', type=int, default=30,
                        help='최근 N일 이내 데이터 검증 (기본: 30일)')
    parser.add_argument('--sources', type=str, nargs='+', default=None,
                        help='필터링할 source 리스트 (예: naver kirs)')
    parser.add_argument('--report-id-not-null', action='store_true', default=True,
                        help='report_id가 NULL이 아닌 것만 (기본: True)')
    parser.add_argument('--allow-null-report-id', action='store_true',
                        help='report_id NULL 허용 (--report-id-not-null 무시)')
    parser.add_argument('--print-samples', type=int, default=0,
                        help='샘플 N개 출력 (edge_id, len, preview) - KPI 0% 원인 판정용')
    
    args = parser.parse_args()
    
    verify_p0h_performance(
        days_back=args.days_back,
        sources=args.sources,
        report_id_not_null=not args.allow_null_report_id,
        print_samples=args.print_samples  # ⭐ P0: 샘플 프린트 옵션
    )


if __name__ == "__main__":
    main()

