"""
Edge Enricher (Phase 2.0 P0)

목적: 추출된 인사이트를 edges.properties.evidence_layer에 추가

프로세스:
1. 리포트에서 ticker, driver_code 매칭
2. 해당하는 DRIVEN_BY 엣지 찾기
3. evidence_layer에 인사이트 추가 (중복 방지)
4. alignment 판정 (KG와의 정합성)
"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging
import hashlib
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from sqlalchemy.orm import Session
from app.db import SessionLocal
from app.models.edge import Edge
from app.models.economic_variable import EconomicVariable
from app.models.driver_candidate import DriverCandidate
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_report_fingerprint(
    title: str, 
    text_head: str, 
    n_chars: int = 500,
    parser_version: str = "v1.0",
    cleaning_profile: str = "default"
) -> str:
    """
    리포트 지문 생성 (중복 방지용)
    
    P0+ 보강: parser_version과 cleaning_profile을 포함하여
    파서 로직 변경 시 재처리 가능하도록 함
    
    Args:
        title: 리포트 제목
        text_head: 리포트 본문 앞부분
        n_chars: 본문에서 사용할 문자 수
        parser_version: 파서 버전 (예: "v1.0")
        cleaning_profile: 클린업 프로필 (예: "default")
    
    Returns:
        SHA256 해시값 (hex)
    """
    content = f"{title}|{text_head[:n_chars]}|{parser_version}|{cleaning_profile}"
    return hashlib.sha256(content.encode('utf-8')).hexdigest()


def generate_report_uid(pdf_url: Optional[str] = None, naver_doc_id: Optional[str] = None) -> str:
    """
    리포트 원본 단위 식별자 생성 (report_uid)
    
    P0+ 보강: 파서 변경과 무관하게 같은 리포트를 식별
    
    Args:
        pdf_url: PDF URL
        naver_doc_id: 네이버 문서 ID
    
    Returns:
        SHA256 해시값 (hex)
    """
    if pdf_url:
        return hashlib.sha256(pdf_url.encode('utf-8')).hexdigest()
    elif naver_doc_id:
        return hashlib.sha256(naver_doc_id.encode('utf-8')).hexdigest()
    else:
        # 둘 다 없으면 타임스탬프 기반 (비권장)
        import time
        return hashlib.sha256(str(time.time()).encode('utf-8')).hexdigest()


def determine_alignment(
    kg_mechanism: str,
    kg_polarity: str,
    report_logic: str,
    report_conditions: Dict[str, str]
) -> tuple[str, str]:
    """
    KG와 리포트의 정합성 판정
    
    Args:
        kg_mechanism: KG의 mechanism 값
        kg_polarity: KG의 polarity 값
        report_logic: 리포트의 analyst_logic
        report_conditions: 리포트의 conditions
    
    Returns:
        (alignment, conflict_note) 튜플
    """
    # 간단한 휴리스틱 (향후 LLM 기반으로 개선 가능)
    alignment = "UNKNOWN"
    conflict_note = ""
    
    # 긍정/부정 키워드 체크
    positive_keywords = ["증가", "회복", "개선", "상승", "긍정", "호재", "기대"]
    negative_keywords = ["감소", "하락", "부정", "악재", "우려", "리스크"]
    
    report_text = f"{report_logic} {report_conditions.get('positive', '')} {report_conditions.get('negative', '')}"
    report_text_lower = report_text.lower()
    
    has_positive = any(kw in report_text_lower for kw in positive_keywords)
    has_negative = any(kw in report_text_lower for kw in negative_keywords)
    
    if kg_polarity == "POSITIVE" and has_positive and not has_negative:
        alignment = "ALIGNED"
    elif kg_polarity == "NEGATIVE" and has_negative and not has_positive:
        alignment = "ALIGNED"
    elif kg_polarity == "MIXED" and (has_positive and has_negative):
        alignment = "ALIGNED"
    elif (kg_polarity == "POSITIVE" and has_negative) or (kg_polarity == "NEGATIVE" and has_positive):
        alignment = "CONFLICT"
        conflict_note = f"KG는 {kg_polarity}인데 리포트는 반대 방향 언급"
    elif has_positive or has_negative:
        alignment = "PARTIAL"
    else:
        alignment = "UNKNOWN"
    
    return alignment, conflict_note


def enrich_edge_with_evidence(
    db: Session,
    ticker: str,
    driver_code: str,
    insights: Dict[str, Any],
    report_metadata: Dict[str, Any],
    kg_snapshot: Optional[Dict[str, str]] = None
) -> tuple[bool, Optional[str], Optional[str]]:
    """
    DRIVEN_BY 엣지에 evidence_layer 추가
    
    Args:
        db: DB 세션
        ticker: 종목코드
        driver_code: 드라이버 코드
        insights: 추출된 인사이트 (extract_insights 결과)
        report_metadata: 리포트 메타데이터
            {
                "title": "...",
                "broker_name": "...",
                "report_date": "YYYY-MM-DD",
                "report_id": "...",
                "text_head": "..."  # 지문 생성용
            }
        kg_snapshot: KG 스냅샷 (None이면 DB에서 조회)
            {
                "mechanism": "...",
                "polarity": "...",
                "impact_nature": "..."
            }
    
    Returns:
        (성공 여부, alignment, failure_reason) 튜플
        failure_reason: 실패 시 원인 (NO_DRIVER_EXTRACTED, DRIVER_PENDING_APPROVAL, DRIVER_NOT_IN_UNIVERSE, BLOCKED_BY_GATE)
    """
    try:
        # DRIVEN_BY 엣지 찾기
        edge_id = f"{ticker}_{driver_code}_DRIVEN_BY"
        edge = db.query(Edge).filter(Edge.id == edge_id).first()
        
        if not edge:
            # ⭐ DRIVEN_BY 실패 원인 세분화
            failure_reason = _determine_driven_by_failure_reason(
                ticker, driver_code, db, report_metadata
            )
            logger.warning(f"DRIVEN_BY 엣지를 찾을 수 없습니다: {edge_id} (reason: {failure_reason})")
            return False, None, failure_reason
        
        # properties 초기화
        props = edge.properties or {}
        
        # ⭐ evidence_layer가 비어있으면 최소한 key_sentence를 포함한 evidence 생성
        evidence_layer = props.get("evidence_layer", [])
        
        # evidence_layer가 비어있고 insights에 key_sentence가 있으면 최소 evidence 생성
        if not evidence_layer and insights.get("key_sentence"):
            key_sentence = insights.get("key_sentence", "")
            if key_sentence:
                # ⭐ deterministic hash 생성 (플레이스홀더 제거)
                from utils.text_normalizer import normalize_text_for_fingerprint
                
                report_id = report_metadata.get("report_id", "")
                # ⭐ stable key 사용 (edge.id는 사용하지 않음 - 멱등성 보장)
                edge_stable_key = f"{ticker}|{driver_code}|DRIVEN_BY"
                normalized_key = normalize_text_for_fingerprint(key_sentence)
                evidence_id = hashlib.sha256(
                    f"{report_id}|{edge_stable_key}|{normalized_key}".encode()
                ).hexdigest()[:16]
                
                # 최소한의 evidence 항목 생성 (HOLD_EVIDENCE_MISSING 방지)
                min_evidence = {
                    "source_type": "REPORT",
                    "source_name": f"{report_metadata.get('broker_name', 'Unknown')}_{report_metadata.get('report_date', '')}_{ticker}",
                    "broker_name": report_metadata.get("broker_name", ""),
                    "report_date": report_metadata.get("report_date", ""),
                    "report_id": report_id,
                    "report_fingerprint": evidence_id,  # ⭐ deterministic hash 사용
                    "key_sentence": key_sentence[:200],  # 최대 200자
                    "evidence_snippet_type": "SHORT_QUOTE",
                    "extracted_at": datetime.utcnow().isoformat() + "Z",
                    "version": 1
                }
                evidence_layer = [min_evidence]
                if edge.properties is None:
                    edge.properties = {}
                edge.properties["evidence_layer"] = evidence_layer
                props = edge.properties
                logger.info(f"최소 evidence 생성: {ticker} → {driver_code} (evidence_id: {evidence_id})")
        
        # KG 스냅샷 조회 (없으면)
        if kg_snapshot is None:
            # properties에서 가져오기
            kg_snapshot = {
                "mechanism": props.get("mechanism", "UNKNOWN"),
                "polarity": props.get("polarity", "UNKNOWN"),
                "impact_nature": props.get("impact_nature", "UNKNOWN")
            }
        
        # 리포트 지문 생성
        # ⭐ P0+ 보강: parser_version과 cleaning_profile 포함
        parser_version = report_metadata.get("parser_version", "v1.0")
        cleaning_profile = report_metadata.get("cleaning_profile", "default")
        
        fingerprint = generate_report_fingerprint(
            title=report_metadata.get("title", ""),
            text_head=report_metadata.get("text_head", ""),
            parser_version=parser_version,
            cleaning_profile=cleaning_profile
        )
        
        # 중복 체크 (같은 fingerprint가 이미 있는지)
        evidence_layer = props.get("evidence_layer", [])
        existing_fingerprints = [e.get("report_fingerprint") for e in evidence_layer if isinstance(e, dict)]
        
        if fingerprint in existing_fingerprints:
            logger.info(f"중복 리포트 감지 (fingerprint: {fingerprint[:16]}...), 스킵")
            # 기존 alignment 반환
            existing_evidence = [e for e in evidence_layer if isinstance(e, dict) and e.get("report_fingerprint") == fingerprint]
            if existing_evidence:
                return True, existing_evidence[0].get("alignment", "UNKNOWN")
            return True, "UNKNOWN"  # 중복이지만 성공으로 처리
        
        # alignment 판정
        alignment, conflict_note = determine_alignment(
            kg_snapshot.get("mechanism", ""),
            kg_snapshot.get("polarity", ""),
            insights.get("analyst_logic", ""),
            insights.get("conditions", {})
        )
        
        # evidence_layer 항목 생성
        # ⭐ P0+ 보강: KIRS 힌트 타입 표시 (mechanism 변경 없음)
        source_type = insights.get("source_type", "REPORT")  # KIRS_HINT 또는 REPORT
        hint_type = insights.get("hint_type")  # MOAT 등 (KIRS인 경우)
        
        evidence_item = {
            # 기본 정보
            "source_type": source_type,  # REPORT 또는 KIRS_HINT
            "hint_type": hint_type,  # KIRS인 경우: MOAT 등
            "source_name": f"{report_metadata.get('broker_name', 'Unknown')}_{report_metadata.get('report_date', '')}_{ticker}",
            "broker_name": report_metadata.get("broker_name", ""),
            "report_date": report_metadata.get("report_date", ""),
            "report_id": report_metadata.get("report_id", ""),
            "report_fingerprint": fingerprint,
            "section_fingerprint": insights.get("section_fingerprint"),  # 섹션 단위 dedup용
            "version": 1,
            
            # KG 스냅샷
            "kg_driver_code": driver_code,
            "kg_mechanism": kg_snapshot.get("mechanism", ""),
            "kg_polarity": kg_snapshot.get("polarity", ""),
            "kg_impact_nature": kg_snapshot.get("impact_nature", ""),
            
            # 정합성 검증
            "alignment": alignment,
            "conflict_note": conflict_note,
            "extraction_confidence": insights.get("extraction_confidence", "LOW"),
            
            # 추출된 인사이트
            "analyst_logic": insights.get("analyst_logic", ""),
            "conditions": insights.get("conditions", {}),
            "key_sentence": insights.get("key_sentence", ""),
            
            # 메타데이터
            "selection_reason": "driver_mapping",  # 향후 개선 가능
            # ⭐ Compliance: evidence_snippet_type 추가
            "evidence_snippet_type": "PARAPHRASE" if source_type == "KIRS_HINT" else "SHORT_QUOTE",  # KIRS는 PARAPHRASE 중심
            "extracted_at": datetime.utcnow().isoformat() + "Z",
            
            # 시간 축
            "temporal_hint": insights.get("temporal_hint", "MID_TERM"),
            "valid_from": None,
            "valid_to": None,
            "report_timestamp": None
        }
        
        # 시점 정보 정확한 계산 (report_date 파싱)
        try:
            report_date_str = report_metadata.get("report_date", "")
            if report_date_str:
                # "25.12.19" 형식 파싱
                from datetime import datetime
                if len(report_date_str) >= 8 and '.' in report_date_str:
                    # "25.12.19" -> "2025-12-19"
                    parts = report_date_str.split('.')
                    if len(parts) == 3:
                        year = int(parts[0])
                        if year < 100:
                            year = 2000 + year
                        month = int(parts[1])
                        day = int(parts[2])
                        report_date = datetime(year, month, day)
                        evidence_item["valid_from"] = report_date.strftime("%Y-%m")
                        
                        # ⭐ P0+ 보강: KIRS 힌트인 경우 유효기간 2년, 일반 리포트는 12개월
                        source_type = insights.get("source_type", "REPORT")
                        valid_to_override = report_metadata.get("valid_to")  # KIRS인 경우 2-3년으로 설정됨
                        
                        if source_type == "KIRS_HINT" and valid_to_override:
                            # valid_to_override는 YYYY-MM-DD 형식
                            try:
                                valid_to_date = datetime.strptime(valid_to_override, "%Y-%m-%d")
                                evidence_item["valid_to"] = valid_to_date.strftime("%Y-%m")
                            except ValueError:
                                # Fallback: 2년
                                from datetime import timedelta
                                valid_to_date = report_date + timedelta(days=730)  # 2년 유효
                                evidence_item["valid_to"] = valid_to_date.strftime("%Y-%m")
                        elif source_type == "KIRS_HINT":
                            # valid_to_override가 없으면 기본 2년
                            from datetime import timedelta
                            valid_to_date = report_date + timedelta(days=730)  # 2년 유효
                            evidence_item["valid_to"] = valid_to_date.strftime("%Y-%m")
                        else:
                            # 일반 리포트: 12개월 유효
                            valid_to_date = report_date.replace(year=report_date.year + 1)
                            evidence_item["valid_to"] = valid_to_date.strftime("%Y-%m")
                        
                        evidence_item["report_timestamp"] = report_date.isoformat()
        except Exception as e:
            logger.warning(f"시점 정보 파싱 실패: {e}")
        
        # evidence_layer에 추가
        if not isinstance(evidence_layer, list):
            evidence_layer = []
        
        evidence_layer.append(evidence_item)
        
        # properties 업데이트
        if edge.properties is None:
            edge.properties = {}
        
        edge.properties["evidence_layer"] = evidence_layer
        
        # KG 스냅샷도 properties에 저장 (없으면)
        if "mechanism" not in edge.properties:
            edge.properties["mechanism"] = kg_snapshot.get("mechanism", "")
        if "polarity" not in edge.properties:
            edge.properties["polarity"] = kg_snapshot.get("polarity", "")
        if "impact_nature" not in edge.properties:
            edge.properties["impact_nature"] = kg_snapshot.get("impact_nature", "")
        
        db.commit()
        logger.info(f"엣지 enrichment 완료: {edge_id} (alignment: {alignment})")
        return True, alignment, None
        
    except Exception as e:
        logger.error(f"엣지 enrichment 실패: {e}", exc_info=True)
        db.rollback()
        return False, None, "BLOCKED_BY_GATE"


def _determine_driven_by_failure_reason(
    ticker: str,
    driver_code: str,
    db: Session,
    report_metadata: Optional[Dict[str, Any]] = None
) -> str:
    """
    DRIVEN_BY 엣지 실패 원인 세분화
    
    ⭐ 개선: 분류 정확성 개선
    - driver_code가 실제로 master에 있는지 재확인
    - 우선순위 명확화
    
    Returns:
        NO_DRIVER_EXTRACTED: 본문에서 드라이버를 못 뽑음
        DRIVER_PENDING_APPROVAL: 후보는 있는데 master에 없어 연결 불가
        DRIVER_NOT_IN_UNIVERSE: ticker는 잡았지만 회사가 universe 밖
        BLOCKED_BY_GATE: quality/sanity/dedupe 등에서 막힘
    """
    # 1. Driver가 None이거나 빈 문자열인지 확인 (최우선)
    if not driver_code or driver_code.strip() == "":
        return "NO_DRIVER_EXTRACTED"
    
    # 2. Driver가 후보 상태인지 확인
    try:
        candidate = db.query(DriverCandidate).filter(
            DriverCandidate.candidate_text == driver_code,
            DriverCandidate.status == 'PENDING'
        ).first()
        
        if candidate:
            return "DRIVER_PENDING_APPROVAL"
    except Exception:
        pass  # 테이블이 없을 수 있음
    
    # 3. Ticker는 있지만 CompanyDetail이 없는지 확인 (universe 밖)
    try:
        stock = db.query(Stock).filter(Stock.ticker == ticker).first()
        if stock:
            company_detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == ticker
            ).first()
            if not company_detail:
                return "DRIVER_NOT_IN_UNIVERSE"
    except Exception:
        pass  # 테이블이 없을 수 있음
    
    # ⭐ 4. driver_code가 실제로 master에 있는지 재확인
    # (BLOCKED_BY_GATE로 분류하기 전에 확인)
    try:
        from app.models.economic_variable import EconomicVariable
        driver_exists = db.query(EconomicVariable).filter(
            EconomicVariable.code == driver_code
        ).first()
        
        if not driver_exists:
            # driver가 master에 없으면 DRIVER_PENDING_APPROVAL로 재분류
            # (추출은 됐지만 master에 없음 = 승인 대기 상태)
            logger.debug(f"Driver가 master에 없음: {driver_code} → DRIVER_PENDING_APPROVAL로 재분류")
            return "DRIVER_PENDING_APPROVAL"
    except Exception:
        pass  # 테이블이 없을 수 있음
    
    # 5. 그 외는 BLOCKED_BY_GATE (quality/sanity/dedupe 등)
    return "BLOCKED_BY_GATE"

