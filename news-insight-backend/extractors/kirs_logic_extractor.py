"""
KIRS 리포트 Logic Extractor

한국IR협의회 리포트를 힌트 데이터로 변환
- 기업 섹션 → edges 테이블 (힌트 타입으로만 저장, mechanism 변경 없음)
- 산업 섹션 → industry_edges 테이블
"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import logging
import re

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 표준 섹터 코드 매핑 (SECTOR_REFERENCES 사용)
from app.models.sector_reference import SECTOR_REFERENCES, get_all_sector_references

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_company_logic(
    checkpoint: Optional[str],
    sections: Dict[str, str],
    full_text: str
) -> Dict[str, Any]:
    """
    기업 분석 섹션에서 Logic 추출
    
    Args:
        checkpoint: 체크포인트 텍스트
        sections: 섹션별 텍스트
        full_text: 전체 텍스트
    
    Returns:
        {
            "analyst_logic": "...",
            "conditions": {"positive": [...], "negative": [...]},
            "key_sentence": "...",
            "tech_moats": [...],  # 기술 해자 목록
            "target_drivers": [...]  # 관련 드라이버 코드
        }
    """
    logic_parts = []
    positive_conditions = []
    negative_conditions = []
    tech_moats = []
    key_sentences = []
    
    # 1. 체크포인트 우선 활용
    if checkpoint:
        logic_parts.append(checkpoint)
        key_sentences.append(checkpoint[:200])  # 첫 200자
    
    # 2. 기술분석 섹션에서 기술 해자 추출
    tech_section = sections.get("기술분석", "")
    if tech_section:
        logic_parts.append(tech_section)
        
        # 기술 해자 키워드 추출
        tech_keywords = [
            "기술력", "기술 경쟁력", "핵심 기술", "특허", "노하우",
            "기술 해자", "기술 우위", "차별화 기술", "독점 기술"
        ]
        
        for keyword in tech_keywords:
            if keyword in tech_section:
                # 키워드 주변 문장 추출
                sentences = re.split(r'[.!?]\s+', tech_section)
                for sentence in sentences:
                    if keyword in sentence and len(sentence) > 20:
                        tech_moats.append(sentence.strip())
                        break
        
        # 기술분석 섹션의 핵심 문장 추출
        sentences = re.split(r'[.!?]\s+', tech_section)
        for sentence in sentences:
            if len(sentence) > 50 and any(kw in sentence for kw in ["기술", "경쟁력", "우위", "차별화"]):
                key_sentences.append(sentence.strip())
    
    # 3. 재무분석 섹션에서 조건 추출
    financial_section = sections.get("재무분석", "")
    if financial_section:
        # 긍정적 조건 추출
        positive_keywords = ["증가", "성장", "개선", "확대", "향상", "회복"]
        for keyword in positive_keywords:
            if keyword in financial_section:
                sentences = re.split(r'[.!?]\s+', financial_section)
                for sentence in sentences:
                    if keyword in sentence and len(sentence) > 30:
                        positive_conditions.append(sentence.strip())
                        break
        
        # 부정적 조건 추출
        negative_keywords = ["감소", "하락", "위축", "부담", "리스크", "우려"]
        for keyword in negative_keywords:
            if keyword in financial_section:
                sentences = re.split(r'[.!?]\s+', financial_section)
                for sentence in sentences:
                    if keyword in sentence and len(sentence) > 30:
                        negative_conditions.append(sentence.strip())
                        break
    
    # 4. 전체 텍스트에서 드라이버 코드 추출 (간단한 키워드 매칭)
    target_drivers = []
    driver_keywords = {
        "AI": "AI_TECH",
        "반도체": "SEMICONDUCTOR",
        "배터리": "BATTERY",
        "전기차": "EV",
        "수소": "HYDROGEN",
        "바이오": "BIO",
        "화학": "CHEMICAL",
        "소재": "MATERIAL"
    }
    
    combined_text = " ".join([checkpoint or "", tech_section, financial_section])
    for keyword, driver_code in driver_keywords.items():
        if keyword in combined_text:
            target_drivers.append(driver_code)
    
    # Logic 요약 생성
    analyst_logic = " ".join(logic_parts[:3])  # 최대 3개 섹션 결합
    if len(analyst_logic) > 500:
        analyst_logic = analyst_logic[:500] + "..."
    
    # 핵심 문장 선택 (가장 긴 것)
    key_sentence = max(key_sentences, key=len) if key_sentences else analyst_logic[:200]
    
    return {
        "analyst_logic": analyst_logic,
        "conditions": {
            "positive": positive_conditions[:3],  # 최대 3개
            "negative": negative_conditions[:3]
        },
        "key_sentence": key_sentence,
        "tech_moats": tech_moats[:5],  # 최대 5개
        "target_drivers": list(set(target_drivers)),  # 중복 제거
        "sentiment": "NEUTRAL"  # KIRS 리포트는 투자의견 없으므로 중립
    }


def extract_industry_logic(
    sections: Dict[str, str],
    full_text: str
) -> Dict[str, Any]:
    """
    산업/시장 동향 섹션에서 Logic 추출
    
    Args:
        sections: 섹션별 텍스트
        full_text: 전체 텍스트
    
    Returns:
        {
            "logic_summary": "...",
            "conditions": {"positive": [...], "negative": [...]},
            "key_sentence": "...",
            "target_sector": "...",  # 섹터 코드
            "target_value_chain": "...",  # 밸류체인 코드
            "target_drivers": [...]  # 관련 드라이버 코드
        }
    """
    logic_parts = []
    positive_conditions = []
    negative_conditions = []
    key_sentences = []
    
    # 1. 시장동향 섹션 우선 활용
    market_section = sections.get("시장동향", "")
    if market_section:
        logic_parts.append(market_section)
        
        # 핵심 문장 추출
        sentences = re.split(r'[.!?]\s+', market_section)
        for sentence in sentences:
            if len(sentence) > 50:
                key_sentences.append(sentence.strip())
        
        # 긍정적 조건 추출
        positive_keywords = ["성장", "확대", "증가", "기회", "수요 증가", "시장 확대"]
        for keyword in positive_keywords:
            if keyword in market_section:
                for sentence in sentences:
                    if keyword in sentence and len(sentence) > 30:
                        positive_conditions.append(sentence.strip())
                        break
        
        # 부정적 조건 추출
        negative_keywords = ["위축", "감소", "부담", "리스크", "불확실성"]
        for keyword in negative_keywords:
            if keyword in market_section:
                for sentence in sentences:
                    if keyword in sentence and len(sentence) > 30:
                        negative_conditions.append(sentence.strip())
                        break
    
    # 2. 기술분석 섹션도 산업 관점에서 활용
    tech_section = sections.get("기술분석", "")
    if tech_section and "산업" in tech_section or "시장" in tech_section:
        logic_parts.append(tech_section)
    
    # 3. 섹터/밸류체인 추출 (표준 섹터 코드 매핑 - 정밀도 향상)
    target_sector_code = None
    target_value_chain = None
    
    # 표준 섹터 코드 리스트 가져오기
    all_sector_codes = list(get_all_sector_references().keys())
    
    # 섹터 Reference 텍스트와 매칭 (더 정확한 매핑)
    combined_text = market_section + " " + tech_section
    combined_text_lower = combined_text.lower()
    
    # 각 섹터의 Reference 텍스트와 매칭 점수 계산
    sector_scores = {}
    for sector_code, reference_text in get_all_sector_references().items():
        # Reference 텍스트의 키워드들이 combined_text에 얼마나 포함되는지 계산
        reference_keywords = reference_text.split()
        match_count = sum(1 for kw in reference_keywords if kw.lower() in combined_text_lower)
        if match_count > 0:
            sector_scores[sector_code] = match_count / len(reference_keywords)  # 매칭 비율
    
    # 가장 높은 점수의 섹터 선택
    if sector_scores:
        target_sector_code = max(sector_scores.items(), key=lambda x: x[1])[0]
        if sector_scores[target_sector_code] < 0.1:  # 10% 미만이면 신뢰도 낮음
            target_sector_code = None
    
    # Fallback: 키워드 기반 매핑 (표준 섹터 코드만 사용)
    if not target_sector_code:
        sector_keywords = {
            "반도체": "SEC_SEMI",
            "화학": "SEC_CHEM",
            "바이오": "SEC_BIO",
            "IT": "SEC_IT",
            "전기차": "SEC_AUTO",
            "배터리": "SEC_BATTERY",
            "소프트웨어": "SEC_IT",
            "게임": "SEC_GAME",
            "제약": "SEC_BIO"
        }
        
        for keyword, sector_code in sector_keywords.items():
            if keyword in combined_text and sector_code in all_sector_codes:
                target_sector_code = sector_code
                break
    
    # 4. 드라이버 코드 추출
    target_drivers = []
    driver_keywords = {
        "AI": "AI_TECH",
        "반도체": "SEMICONDUCTOR",
        "배터리": "BATTERY",
        "전기차": "EV",
        "수소": "HYDROGEN",
        "바이오": "BIO"
    }
    
    for keyword, driver_code in driver_keywords.items():
        if keyword in combined_text:
            target_drivers.append(driver_code)
    
    # Logic 요약 생성
    logic_summary = " ".join(logic_parts[:2])  # 최대 2개 섹션
    if len(logic_summary) > 500:
        logic_summary = logic_summary[:500] + "..."
    
    # 핵심 문장 선택
    key_sentence = max(key_sentences, key=len) if key_sentences else logic_summary[:200]
    
    return {
        "logic_summary": logic_summary,
        "conditions": {
            "positive": positive_conditions[:3],
            "negative": negative_conditions[:3]
        },
        "key_sentence": key_sentence,
        "target_sector_code": target_sector_code,  # 표준 섹터 코드
        "target_value_chain": target_value_chain,
        "target_drivers": list(set(target_drivers)),
        "target_type": "SECTOR" if target_sector_code else "TECHNOLOGY"
    }


def route_kirs_report(
    parsed_report: Dict[str, Any]
) -> Dict[str, Any]:
    """
    KIRS 리포트를 기업/산업으로 라우팅
    
    ⭐ P0+ 보강: confidence + 근거 로그 추가
    
    Args:
        parsed_report: 파싱된 리포트 데이터
    
    Returns:
        {
            "route_type": "COMPANY" | "INDUSTRY" | "BOTH",
            "route_confidence": 0.0~1.0,
            "route_evidence": ["키워드", "섹션 헤더", "표현 패턴"],
            "company_logic": {...} (기업 리포트인 경우),
            "industry_logic": {...} (산업 리포트인 경우),
            "section_fingerprints": {...}  # 섹션 단위 dedup용
        }
    """
    report_type = parsed_report.get("report_type", "COMPANY")
    ticker = parsed_report.get("ticker")
    checkpoint = parsed_report.get("checkpoint")
    sections = parsed_report.get("sections", {})
    full_text = parsed_report.get("full_text", "")
    
    result = {
        "route_type": "COMPANY",
        "route_confidence": 0.0,
        "route_evidence": [],
        "company_logic": None,
        "industry_logic": None,
        "section_fingerprints": {}  # 섹션 단위 dedup용
    }
    
    # 라우팅 근거 수집
    evidence = []
    confidence_factors = []
    
    # 티커가 있으면 기업 리포트 (높은 신뢰도)
    has_company_section = False
    if ticker is not None:
        has_company_section = True
        evidence.append(f"티커 존재: {ticker}")
        confidence_factors.append(0.9)
    elif "기술분석" in sections:
        has_company_section = True
        evidence.append("기술분석 섹션 존재")
        confidence_factors.append(0.7)
    elif "재무분석" in sections:
        has_company_section = True
        evidence.append("재무분석 섹션 존재")
        confidence_factors.append(0.7)
    
    # 시장동향 섹션이 있으면 산업 리포트
    has_industry_section = False
    if "시장동향" in sections:
        has_industry_section = True
        evidence.append("시장동향 섹션 존재")
        confidence_factors.append(0.8)
    elif report_type == "INDUSTRY":
        has_industry_section = True
        evidence.append(f"리포트 타입: {report_type}")
        confidence_factors.append(0.6)
    elif any(kw in full_text.lower() for kw in ["산업", "시장", "업계", "섹터"]):
        has_industry_section = True
        evidence.append("산업 관련 키워드 발견")
        confidence_factors.append(0.5)
    
    # 섹션 단위 fingerprint 생성 (dedup용)
    import hashlib
    section_fingerprints = {}
    
    # 기업 섹션 처리
    if has_company_section:
        company_logic = extract_company_logic(
            checkpoint=checkpoint,
            sections=sections,
            full_text=full_text
        )
        result["company_logic"] = company_logic
        result["route_type"] = "COMPANY"
        
        # 기업 섹션 fingerprint 생성
        company_text = " ".join([
            checkpoint or "",
            sections.get("기술분석", ""),
            sections.get("재무분석", "")
        ])
        if company_text.strip():
            company_fp = hashlib.sha256(company_text[:1000].encode('utf-8')).hexdigest()
            section_fingerprints["company"] = company_fp
    
    # 산업 섹션 처리
    if has_industry_section:
        industry_logic = extract_industry_logic(
            sections=sections,
            full_text=full_text
        )
        result["industry_logic"] = industry_logic
        
        # 산업 섹션 fingerprint 생성
        industry_text = sections.get("시장동향", "") + " " + sections.get("기술분석", "")
        if industry_text.strip():
            industry_fp = hashlib.sha256(industry_text[:1000].encode('utf-8')).hexdigest()
            section_fingerprints["industry"] = industry_fp
        
        if result["route_type"] == "COMPANY":
            result["route_type"] = "BOTH"
        else:
            result["route_type"] = "INDUSTRY"
    
    # Confidence 계산 (근거의 평균)
    if confidence_factors:
        result["route_confidence"] = sum(confidence_factors) / len(confidence_factors)
    else:
        result["route_confidence"] = 0.3  # 낮은 신뢰도
    
    # ⭐ 점수 분해 필드 추가 (캘리브레이션용)
    result["route_confidence_breakdown"] = {
        "company_signal_score": 0.0,
        "industry_signal_score": 0.0,
        "sector_mapping_score": 0.0,
        "evidence_count": len(evidence),
        "ambiguous_terms_detected": []
    }
    
    # Company 신호 점수
    if has_company_section:
        if ticker:
            result["route_confidence_breakdown"]["company_signal_score"] = 0.9
        elif "기술분석" in sections or "재무분석" in sections:
            result["route_confidence_breakdown"]["company_signal_score"] = 0.7
        else:
            result["route_confidence_breakdown"]["company_signal_score"] = 0.5
    
    # Industry 신호 점수
    if has_industry_section:
        if "시장동향" in sections:
            result["route_confidence_breakdown"]["industry_signal_score"] = 0.8
        elif report_type == "INDUSTRY":
            result["route_confidence_breakdown"]["industry_signal_score"] = 0.6
        else:
            result["route_confidence_breakdown"]["industry_signal_score"] = 0.5
    
    # Sector 매핑 점수 (industry_logic에서 추출 가능하면)
    if has_industry_section and result.get("industry_logic"):
        target_sector = result["industry_logic"].get("target_sector_code")
        if target_sector:
            result["route_confidence_breakdown"]["sector_mapping_score"] = 0.8
        else:
            result["route_confidence_breakdown"]["sector_mapping_score"] = 0.3
    
    # 모호한 용어 감지
    ambiguous_keywords = ["당사", "동사", "Top pick", "핵심 기업", "대표 기업"]
    detected_ambiguous = [kw for kw in ambiguous_keywords if kw in full_text]
    result["route_confidence_breakdown"]["ambiguous_terms_detected"] = detected_ambiguous
    
    result["route_evidence"] = evidence
    result["section_fingerprints"] = section_fingerprints
    
    logger.info(f"KIRS 리포트 라우팅: {result['route_type']} (confidence: {result['route_confidence']:.2f}, evidence: {evidence})")
    logger.debug(f"Confidence breakdown: {result['route_confidence_breakdown']}")
    
    return result

