"""
3-hop 유저 문장 생성 스크립트

Driver → Industry → Company 연결을 자연스러운 문장으로 생성
"""
import sys
import os
from pathlib import Path
from typing import List, Dict, Optional

# Windows 인코딩 처리
if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from app.db import SessionLocal
from app.models.industry_edge import IndustryEdge
from app.models.broker_report import BrokerReport
from app.models.edge import Edge
from app.models.investor_sector import InvestorSector
from app.models.stock import Stock
from sqlalchemy import and_
from sqlalchemy.orm import Session
import re
import logging
from datetime import datetime
import json

logger = logging.getLogger(__name__)


def generate_3hop_sentences(max_samples: int = 10) -> List[Dict[str, any]]:
    """
    3-hop 유저 문장 생성
    
    Driver → Industry → Company 연결을 자연스러운 문장으로 생성
    
    Args:
        max_samples: 최대 샘플 수
    
    Returns:
        생성된 문장 리스트
    """
    print("=" * 80)
    print("[3-hop 유저 문장 생성]")
    print("=" * 80)
    
    db = SessionLocal()
    sentences = []
    
    try:
        # 1. Industry Edges 조회 (활성화된 것만)
        # ⭐ P0-추가1: 조인 전 전체 카운트 계산
        total_edges_before = db.query(IndustryEdge).filter(
            IndustryEdge.is_active == "TRUE"
        ).count()
        
        # ⭐ P0-A: Orphan 필터링 - report_id NOT NULL이고 broker_reports와 연결된 것만
        industry_edges = db.query(IndustryEdge).join(
            BrokerReport, IndustryEdge.report_id == BrokerReport.report_id
        ).filter(
            and_(
                IndustryEdge.is_active == "TRUE",
                IndustryEdge.report_id.isnot(None),
                BrokerReport.source.in_(['naver', 'kirs'])  # 증권사 리포트만
            )
        ).limit(50).all()
        
        # ⭐ P0-추가1: 조인 실패 제외 카운트 계산
        excluded_no_broker_report = total_edges_before - len(industry_edges)
        
        print(f"\n[Industry Edges] {len(industry_edges)}개 발견 (orphan 제외)")
        if excluded_no_broker_report > 0:
            print(f"  [제외] broker_reports 조인 실패: {excluded_no_broker_report}개")
        print("")
        
        if not industry_edges:
            print("  ⚠️  Industry Edges가 없습니다. 먼저 enrichment를 실행하세요.")
            return []
        
        # ⭐ P0-5: MARKET 2-hop 출력 비율 관리
        # COMPANY 3-hop 최소 6개, MARKET 2-hop 최대 4개
        company_3hop_count = 0
        market_2hop_count = 0
        max_company_3hop = max(6, int(max_samples * 0.6))  # 최소 6개, 최대 60%
        max_market_2hop = min(4, int(max_samples * 0.4))  # 최대 4개, 최대 40%
        
        # 2. 각 Industry Edge에 대해 Company 연결 찾기
        for ie in industry_edges[:max_samples * 2]:  # 더 많은 후보에서 선택
            # ⭐ MARKET 처리 정책: 2-hop만 생성 (비율 제한)
            if ie.target_sector_code == "MARKET":
                if market_2hop_count >= max_market_2hop:
                    continue  # MARKET 2-hop 비율 초과 시 스킵
                
                # MARKET은 2-hop (MACRO → Industry)로만 보여주기
                sentence = _generate_2hop_sentence(ie, db)
                if sentence:
                    sentences.append(sentence)
                    market_2hop_count += 1
                    if len(sentences) >= max_samples:
                        break
                continue
            else:
                # 같은 섹터의 기업 찾기
                companies = db.query(InvestorSector).filter(
                    and_(
                        InvestorSector.sector_l1 == ie.target_sector_code,
                        InvestorSector.is_primary == True
                    )
                ).limit(3).all()
                
                if not companies:
                    # L1 매칭 실패 시 major_sector로 시도
                    companies = db.query(InvestorSector).filter(
                        and_(
                            InvestorSector.major_sector == ie.target_sector_code,
                            InvestorSector.is_primary == True
                        )
                    ).limit(3).all()
            
            if not companies:
                continue
            
            # 각 기업에 대해 Edge 찾기
            for company in companies:
                # Company Edge 찾기 (같은 driver 또는 유사한 driver)
                company_edges = db.query(Edge).filter(
                    Edge.source_id == company.ticker,
                    Edge.relation_type == "DRIVEN_BY"
                ).limit(1).all()
                
                if not company_edges:
                    continue
                
                # 문장 생성
                sentence = _generate_sentence(ie, company, company_edges[0], db)
                if sentence:
                    sentences.append(sentence)
                    if len(sentences) >= max_samples:
                        break
            
            if len(sentences) >= max_samples:
                break
        
        # 3. 결과 출력
        print(f"\n[생성된 문장] {len(sentences)}개\n")
        print("=" * 80)
        
        for i, sent in enumerate(sentences, 1):
            print(f"\n[{i}] {sent['type']}")
            print(f"    {sent['sentence']}")
            print(f"    - Driver: {sent['driver']}")
            print(f"    - Industry: {sent['industry']}")
            print(f"    - Company: {sent['company']} ({sent['ticker']})")
            if sent.get('evidence_count'):
                print(f"    - Evidence: {sent['evidence_count']}개")
        
        # 4. 파일 저장
        output_file = Path("reports") / f"3hop_sentences_{Path(__file__).stem}_{len(sentences)}samples.json"
        output_file.parent.mkdir(exist_ok=True)
        
        import json
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(sentences, f, ensure_ascii=False, indent=2)
        
        print(f"\n[저장 완료] {output_file}")
        
        # ⭐ Semantic Compression 메트릭 출력
        try:
            from utils.semantic_compression import get_metrics
            compression_metrics = get_metrics()
            if "compression_summary" in compression_metrics:
                summary = compression_metrics["compression_summary"]
                print(f"\n[Semantic Compression 메트릭]")
                print(f"  총 압축 횟수: {summary.get('total_compressions', 0)}")
                print(f"  압축 방법: {summary.get('compression_methods', {})}")
                print(f"  평균 ratio: {summary.get('avg_ratio', 0.0):.3f}")
                print(f"  압축 효과적: {summary.get('compression_effective_count', 0)}개 ({summary.get('compression_effective_rate', 0.0):.1%})")
                print(f"  게이트 통과: {summary.get('gate_passed_count', 0)}개 ({summary.get('gate_passed_rate', 0.0):.1%})")
        except Exception as e:
            logger.warning(f"Semantic Compression 메트릭 조회 실패: {e}")
        
        # ⭐ P0-A: 진단 로그 저장
        try:
            diagnostics_list = []
            if hasattr(_generate_2hop_sentence, '_diagnostics_list'):
                diagnostics_list.extend(_generate_2hop_sentence._diagnostics_list)
            if hasattr(_generate_sentence, '_diagnostics_list'):
                diagnostics_list.extend(_generate_sentence._diagnostics_list)
            
            if diagnostics_list:
                diagnostics_file = Path("reports") / f"compression_diagnostics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                diagnostics_file.parent.mkdir(exist_ok=True)
                with open(diagnostics_file, 'w', encoding='utf-8') as f:
                    json.dump(diagnostics_list, f, ensure_ascii=False, indent=2)
                print(f"\n[진단 로그 저장] {diagnostics_file} ({len(diagnostics_list)}개)")
        except Exception as e:
            logger.warning(f"진단 로그 저장 실패: {e}")
        
        return sentences
        
    except Exception as e:
        print(f"\n  ❌ [ERROR] 생성 실패: {e}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        db.close()


def _generate_2hop_sentence(
    industry_edge: IndustryEdge,
    db: Session
) -> Optional[Dict[str, any]]:
    """
    2-hop 문장 생성 (MARKET용)
    
    MACRO → Industry 연결만 표현
    
    Args:
        industry_edge: Industry Edge
        db: DB 세션
    
    Returns:
        문장 데이터 또는 None
    """
    # Industry Logic 처리 (Semantic Compression 사용)
    industry_logic = industry_edge.logic_summary or industry_edge.key_sentence or ""
    
    if not industry_logic:
        return None
    
    # ⭐ P0-A: 진단 로그 (문장 생성 직전)
    input_preview = industry_logic[:200] if industry_logic else ""
    input_has_newline = '\n' in (industry_logic or "")
    input_has_bullets = bool(re.search(r'[•\-\*○▶→]', industry_logic or ""))
    logic_summary_len = len(industry_edge.logic_summary) if industry_edge.logic_summary else 0
    key_sentence_len = len(industry_edge.key_sentence) if industry_edge.key_sentence else 0
    
    # 줄바꿈 제거 및 길이 제한
    industry_logic = industry_logic.replace('\n', ' ', 1).replace('\r', ' ', 1).strip()  # 첫 줄바꿈만 제거 (진단용)
    industry_logic_clean = industry_logic.replace('\n', ' ').replace('\r', ' ').strip()  # 전체 정리
    industry_logic_clean = _remove_duplicate_phrases(industry_logic_clean)
    
    # sentence_pool 통계 (압축 전)
    from utils.semantic_compression import _build_sentence_pool
    sentences_pool = _build_sentence_pool(industry_logic_clean, industry_edge.key_sentence)
    num_sentences = len(sentences_pool)
    if sentences_pool:
        avg_sentence_len = sum(len(s) for s in sentences_pool) / len(sentences_pool)
        max_sentence_len = max(len(s) for s in sentences_pool)
    else:
        avg_sentence_len = 0
        max_sentence_len = 0
    
    # 진단 메트릭 기록
    import json
    diagnostics = {
        "input_preview": input_preview,
        "input_has_newline": input_has_newline,
        "input_has_bullets": input_has_bullets,
        "num_sentences": num_sentences,
        "avg_sentence_len": avg_sentence_len,
        "max_sentence_len": max_sentence_len,
        "logic_summary_len": logic_summary_len,
        "key_sentence_len": key_sentence_len,
        "logic_summary_is_long": logic_summary_len > 500,
        "key_sentence_is_long": key_sentence_len > 200,
        "pool_separation_failed": max_sentence_len > 400  # pool 분리 실패 신호
    }
    
    # 진단 로그 저장 (첫 번째 호출 시만)
    if not hasattr(_generate_2hop_sentence, '_diagnostics_saved'):
        _generate_2hop_sentence._diagnostics_saved = True
        diagnostics_file = Path("reports") / f"compression_diagnostics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        diagnostics_file.parent.mkdir(exist_ok=True)
        with open(diagnostics_file, 'w', encoding='utf-8') as f:
            json.dump([diagnostics], f, ensure_ascii=False, indent=2)
        logger.info(f"진단 로그 저장: {diagnostics_file}")
    
    # Semantic Compression 사용
    try:
        from utils.semantic_compression import compress_semantically
        industry_logic = compress_semantically(
            industry_logic=industry_logic_clean,
            key_sentence=industry_edge.key_sentence,
            logic_summary=industry_edge.logic_summary,
            logic_fingerprint=industry_edge.logic_fingerprint,
            max_sentences=2,
            max_length=220
        )
    except Exception as e:
        logger.warning(f"Semantic Compression 실패, rule-based fallback: {e}")
        if len(industry_logic) > 150:
            sentences = industry_logic.split('.')
            industry_logic = '. '.join(sentences[:2]).strip()
            if not industry_logic.endswith('.'):ㅋㅋ 6
                industry_logic += '.'
    
    # 2-hop 문장 생성
    sentence = f"시장에서는 {industry_logic} 이러한 시장 환경 속에서 {industry_edge.target_sector_code} 업종은 관련 인사이트가 언급되고 있습니다."
    
    # Evidence 개수 및 근거 링크 추출
    evidence_count = 0
    evidence_ids = []
    report_ids = []
    
    if hasattr(industry_edge, 'report_id') and industry_edge.report_id:
        report_ids.append(str(industry_edge.report_id))
    
    # ⭐ Check 2: 2-hop과 3-hop 스키마 일관성 유지 (모든 필드 동일, 값만 다름)
    return {
        "type": "가능성 톤",
        "sentence": sentence,
        "driver": industry_edge.source_driver_code or "UNKNOWN",
        "industry": industry_edge.target_sector_code,   
        "industry_logic": industry_logic,
        "company": None,  # ⭐ 2-hop은 company 정보 없음
        "ticker": None,  # ⭐ 2-hop은 ticker 정보 없음
        "evidence_count": evidence_count,
        "industry_edge_id": industry_edge.id if hasattr(industry_edge, 'id') else None,
        "company_edge_id": None,  # ⭐ 2-hop은 company_edge_id 없음
        "evidence_ids": evidence_ids,
        "report_ids": report_ids,
        "created_at": industry_edge.created_at.isoformat() if industry_edge.created_at else None,
        "is_macro": True,  # ⭐ 2-hop 플래그
        "hop_count": 2  # ⭐ hop 수 명시
    }


def _generate_sentence(
    industry_edge: IndustryEdge,
    company_sector: InvestorSector,
    company_edge: Edge,
    db: Session
) -> Optional[Dict[str, any]]:
    """
    개별 문장 생성
    
    Args:
        industry_edge: Industry Edge
        company_sector: Company Sector 정보
        company_edge: Company Edge
    
    Returns:
        생성된 문장 딕셔너리
    """
    # Driver 타입에 따라 톤 결정
    driver_code = industry_edge.source_driver_code or company_edge.target_id
    
    # MACRO/STYLE/FLOW → 가능성 톤
    # COMPANY → 직접 노출 톤
    if driver_code and any(x in driver_code.upper() for x in ['MACRO', 'STYLE', 'FLOW', 'MARKET']):
        tone = "가능성"
        prefix = "시장에서는"
    else:
        tone = "직접 노출"
        prefix = ""
    
    # ⭐ 3-hop 문장 품질 강제: Industry 로직 요약 (더 간결하게)
    industry_logic = industry_edge.logic_summary or industry_edge.key_sentence or ""
    # 줄바꿈 제거 및 길이 제한 (각 hop 1~2문장, 총 320~450자 이내)
    industry_logic = industry_logic.replace('\n', ' ').replace('\r', ' ').strip()
    # 중복 제거: 같은 구절 2회 이상 반복 금지
    industry_logic = _remove_duplicate_phrases(industry_logic)
    # Semantic Compression 사용 (의미 중심 문장 선택)
    try:
        from utils.semantic_compression import compress_semantically
        industry_logic = compress_semantically(
            industry_logic=industry_logic,
            key_sentence=industry_edge.key_sentence,
            logic_summary=industry_edge.logic_summary,
            logic_fingerprint=industry_edge.logic_fingerprint,
            max_sentences=2,
            max_length=220
        )
    except Exception as e:
        logger.warning(f"Semantic Compression 실패, rule-based fallback: {e}")
        # Fallback: 기존 rule-based 로직
        if len(industry_logic) > 150:
            sentences = industry_logic.split('.')
            industry_logic = '. '.join(sentences[:2]).strip()
            if not industry_logic.endswith('.'):
                industry_logic += '.'
    
    # Company 정보 (ticker → company_name 조회)
    ticker = company_sector.ticker
    company_name = ticker  # 기본값
    
    # stocks 테이블에서 company_name 조회
    stock = db.query(Stock).filter(Stock.ticker == ticker).first()
    if stock:
        company_name = stock.stock_name or ticker
    
    # Evidence 개수 및 근거 링크 추출
    evidence_count = 0
    evidence_ids = []
    report_ids = []
    
    # ⭐ Industry Edge의 report_id 추가 (최소 1개 보장)
    if hasattr(industry_edge, 'report_id') and industry_edge.report_id:
        report_ids.append(str(industry_edge.report_id))
    
    if company_edge.properties:
        evidence_layer = company_edge.properties.get("evidence_layer", [])
        if isinstance(evidence_layer, list):
            evidence_count = len(evidence_layer)
            for evidence in evidence_layer:
                if isinstance(evidence, dict):
                    # report_fingerprint를 evidence_id로 사용
                    fingerprint = evidence.get("report_fingerprint", "")
                    if fingerprint:
                        evidence_ids.append(fingerprint[:16])  # 짧은 버전
                    # report_id 수집
                    report_id = evidence.get("report_id", "")
                    if report_id:
                        report_ids.append(report_id)
    
    # ⭐ evidence_layer가 비어있으면 최소한 key_sentence를 evidence로 추가
    if evidence_count == 0:
        # company_edge에 key_sentence가 있으면 사용
        if company_edge.properties:
            key_sentence = company_edge.properties.get("key_sentence", "")
            if not key_sentence:
                # industry_edge의 key_sentence 사용
                key_sentence = industry_edge.key_sentence or industry_edge.logic_summary or ""
            
            if key_sentence:
                # ⭐ deterministic hash 생성 (플레이스홀더 제거)
                import hashlib
                from utils.text_normalizer import normalize_text_for_fingerprint
                
                report_id = str(industry_edge.report_id) if hasattr(industry_edge, 'report_id') and industry_edge.report_id else ""
                # ⭐ stable key 사용 (company_edge.id는 사용하지 않음 - 멱등성 보장)
                # ticker는 company_sector에서 가져옴
                ticker = company_sector.ticker if hasattr(company_sector, 'ticker') else ""
                driver_code = industry_edge.source_driver_code or ""
                edge_stable_key = f"{ticker}|{driver_code}|DRIVEN_BY"
                normalized_key = normalize_text_for_fingerprint(key_sentence)
                evidence_id = hashlib.sha256(
                    f"{report_id}|{edge_stable_key}|{normalized_key}".encode()
                ).hexdigest()[:16]
                
                evidence_ids.append(evidence_id)
                evidence_count = 1
    
    # 중복 제거
    evidence_ids = list(set(evidence_ids))[:3]  # 최대 3개
    report_ids = list(set(report_ids))[:3]  # 최대 3개
    
    # ⭐ 3-hop 문장 품질 강제: 문장 생성 (길이 제한, 중복 제거)
    if tone == "가능성":
        sentence = (
            f"{prefix} {industry_logic}가 나타나는 경향이 있습니다. "
            f"이러한 시장 환경 속에서 {industry_edge.target_sector_code} 업종은 "
            f"관련 인사이트가 언급되고 있으며, {company_name}은(는) "
            f"해당 업종 내에서 상대적으로 관심을 받을 수 있는 기업으로 평가됩니다."
        )
    else:
        sentence = (
            f"{industry_logic}에 따라 {industry_edge.target_sector_code} 업종에 "
            f"영향이 예상되며, {company_name}은(는) 해당 업종 내에서 "
            f"직접적인 노출이 있는 기업으로 평가됩니다."
        )
    
    # ⭐ 길이 제한: 총 320~450자 이내
    if len(sentence) > 450:
        # 문장 단위로 자르기
        sentences = sentence.split('.')
        sentence = '. '.join(sentences[:3]).strip()
        if not sentence.endswith('.'):
            sentence += '.'
    
    # ⭐ 중복 제거: 같은 구절 2회 이상 반복 금지
    sentence = _remove_duplicate_phrases(sentence)
    
    # ⭐ 품질 검증: 최종 문장 길이 및 중복 체크
    final_sentence = sentence.strip()
    if len(final_sentence) < 50 or len(final_sentence) > 450:
        logger.warning(f"문장 길이 이상: {len(final_sentence)}자 (50~450자 범위)")
        return None
    
    # ⭐ Check 2: 2-hop과 3-hop 스키마 일관성 유지
    return {
        "type": f"{tone} 톤",
        "sentence": final_sentence,
        "driver": driver_code,
        "industry": industry_edge.target_sector_code,
        "industry_logic": industry_logic,
        "company": company_name,  # 3-hop: company 정보 있음
        "ticker": ticker,  # 3-hop: ticker 정보 있음
        "evidence_count": evidence_count,
        "industry_edge_id": industry_edge.id if hasattr(industry_edge, 'id') else None,
        "company_edge_id": company_edge.id if hasattr(company_edge, 'id') else None,  # 3-hop: company_edge_id 있음
        "evidence_ids": evidence_ids,
        "report_ids": report_ids,
        "created_at": industry_edge.created_at.isoformat() if industry_edge.created_at else None,
        "is_macro": False,  # ⭐ 3-hop은 is_macro=False
        "hop_count": 3  # ⭐ 3-hop 명시
    }


def _remove_duplicate_phrases(text: str) -> str:
    """
    같은 구절 2회 이상 반복 제거
    
    Args:
        text: 원본 텍스트
    
    Returns:
        중복 제거된 텍스트
    """
    # 문장 단위로 분리
    sentences = re.split(r'[.!?]\s+', text)
    
    # 중복 문장 제거
    seen = set()
    unique_sentences = []
    for sentence in sentences:
        sentence_clean = sentence.strip().lower()
        if sentence_clean and sentence_clean not in seen:
            seen.add(sentence_clean)
            unique_sentences.append(sentence.strip())
    
    # 문장 재조합
    result = '. '.join(unique_sentences)
    if result and not result.endswith(('.', '!', '?')):
        result += '.'
    
    # 단어 레벨 중복 제거 (같은 단어가 3회 이상 연속 반복)
    words = result.split()
    dedup_words = []
    prev_word = None
    prev_count = 0
    for word in words:
        if word == prev_word:
            prev_count += 1
            if prev_count < 2:  # 최대 2회까지 허용
                dedup_words.append(word)
        else:
            prev_word = word
            prev_count = 1
            dedup_words.append(word)
    
    return ' '.join(dedup_words)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="3-hop 유저 문장 생성")
    parser.add_argument("--max_samples", type=int, default=10, help="최대 샘플 수")
    parser.add_argument("--output", type=str, help="출력 파일 경로 (JSON 형식)")
    args = parser.parse_args()
    
    sentences = generate_3hop_sentences(max_samples=args.max_samples)
    
    # 출력 파일 저장
    if args.output:
        import json
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(sentences, f, ensure_ascii=False, indent=2)
        print(f"\n출력 파일 저장: {output_path}")

