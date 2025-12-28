"""
Naver 리포트 라우터

Naver 리포트를 기업/산업으로 라우팅
"""
import sys
from pathlib import Path
from typing import Dict, Any, Optional
import logging
import hashlib

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# ⭐ DB 분석 기반 섹터 키워드 (자동 생성: scripts/analyze_sector_keywords.py)
# 생성일: 2024-12-24
# 총 분석 기업: 2,949개 / 31개 섹터
# ============================================================================
SECTOR_KEYWORDS_FROM_DB = {
    'SEC_SEMI': ['반도체장비', '반도체', '팹리스', '시스템반도체', 'R&D', '디스플레이장비'],
    'SEC_BIO': ['신약개발', 'CDMO', '바이오시밀러', 'CMO', '제약', '건강기능식품'],
    'SEC_IT': ['AI', '클라우드', '빅데이터', 'IT서비스', 'SaaS', '디지털트윈', 'IoT', '데이터센터'],
    'SEC_AUTO': ['자동차부품', '전동화', 'OEM', 'ESS', '글로벌생산'],
    'SEC_BATTERY': ['이차전지', '2차전지', '양극재', '이차전지소재', '전해액', '음극재'],
    'SEC_UTIL': ['신재생에너지', 'ESS', 'EPC', '태양광', '도시가스', '친환경'],
    # ⭐ 토스증권 77개 기업 분석 결과 반영 (2024-12-24)
    'SEC_COSMETIC': ['화장품', '기초화장품', '색조화장품', '스킨케어', '화장품소재', '뷰티', '코스메틱'],
    # ⭐ 토스증권 섬유/의류 95개 기업 분석 기반 신설 (2024-12-24)
    'SEC_FASHION': ['의류', '섬유', '봉제', '패션', '니트', '원단', '직물', '방적', '방직', '어패럴', '스포츠웨어', '란제리'],
    'SEC_CONSUMER': ['외주생산', 'D2C', '프리미엄브랜드'],
    'SEC_CONST': ['건설', '건설업', '토목', '레미콘', '플랜트', '종합건설업'],
    'SEC_MACH': ['물류', '종합물류', '항만하역', '항공운송', '공작기계'],
    'SEC_FOOD': ['HACCP', '수직계열화', '육가공', 'HMR', '배합사료'],
    'SEC_ENT': ['엔터테인먼트', '콘텐츠제작', '드라마제작', '매니지먼트', 'K-POP', 'OTT'],
    'SEC_GAME': ['모바일게임', '퍼블리싱', 'MMORPG', '게임개발', '온라인게임'],
    'SEC_TELECOM': ['5G', '광통신', '데이터센터', '통신장비', 'OpenRAN'],
    'SEC_DEFENSE': ['방위산업', '방산', '항공전자', '우주산업'],
    'SEC_SHIP': ['친환경선박', '해양플랜트', '조선기자재', 'LNG선', '조선'],
    'SEC_STEEL': ['후판', '형강', '비철금속', '골판지'],
    'SEC_MEDDEV': ['의료기기', '임플란트', 'HIFU', '체외진단', '미용의료기기'],
    'SEC_ELECTRONICS': ['OLED', 'FPCB', 'PCB', '베트남생산', 'IoT'],
    'SEC_RETAIL': ['HMR', '유통', '백화점'],
    'SEC_SEC': ['리츠', '벤처캐피탈', '자산운용', '벤처투자'],
    # ⭐ 지주회사 키워드 분석 기반 업데이트 (2024-12-24)
    # 핵심: 배당금수익, 임대수익, 브랜드사용료 등 지주회사 전형적 수익구조
    'SEC_HOLDING': ['지주회사', '금융지주', '지주', '배당금수익', '배당수익', '임대수익', '브랜드사용료', '로열티'],
    'SEC_INS': ['자동차보험', '장기보험', '생명보험', '손해보험', '퇴직연금'],
    'SEC_CARD': ['스마트카드', 'PG', '신용카드', '휴대폰결제'],
    'SEC_BANK': ['신기술사업금융', '부동산담보대출', '정기예금', '중소기업금융'],
    # ⭐ SEC_FINANCE: SPAC 오염으로 키워드 제거 (SPAC은 별도 필터링)
    # 'SEC_FINANCE': ['기업인수목적회사', 'SPAC', '합병'],  # SPAC 오염 - 비활성화
    'SEC_TRAVEL': ['호텔', '크루즈', '여행업', '복합리조트'],
    'SEC_EDU': ['에듀테크', '온라인강의', '온라인교육', '이러닝'],
    'SEC_TIRE': ['타이어', '고무', '계면활성제', '도료'],
    'SEC_CHEM': ['LPG', '계면활성제', '암모니아', '화학'],
}

# 밸류체인별 키워드 (UPSTREAM 중심)
VALUE_CHAIN_KEYWORDS_FROM_DB = {
    'SEC_SEMI': ['소재', '웨이퍼', '수입원재료'],
    'SEC_BATTERY': ['이차전지소재', '탄산리튬', '전해액'],
    'SEC_UTIL': ['신재생에너지', 'ESS'],
    'SEC_SHIP': ['조선기자재', '환경장비'],
    'SEC_CONST': ['시멘트', '건설자재'],
    'SEC_FASHION': ['원사', '원단', '염색', '방적', '섬유소재'],  # UPSTREAM: 원료/소재
}


def detect_company_signals(report: Dict[str, Any]) -> Dict[str, Any]:
    """
    ⭐ P0-α1: 기업 신호 감지 (오분류 방지 Safety Latch)
    
    ⭐ 개선: 가중합 점수 방식으로 오탐 방지 (티커=0.6, 기업명콜론=0.3, 키워드=0.2)
    - 0.7 이상만 뒤집기 (과잉 전환 방지)
    
    Args:
        report: 리포트 데이터
    
    Returns:
        {
            "has_strong_company_signal": bool,
            "signals_detected": List[str],
            "confidence": float,
            "signal_breakdown": {
                "ticker_score": float,
                "keyword_score": float,
                "company_colon_score": float
            }
        }
    """
    title = report.get("title", "")
    full_text = report.get("full_text", "")
    category = report.get("category", "")
    
    signals = []
    signal_breakdown = {
        "ticker_score": 0.0,
        "keyword_score": 0.0,
        "company_colon_score": 0.0
    }
    
    # 1. 6자리 티커 패턴 감지 (가중치: 0.6) - 가장 강한 신호
    import re
    ticker_pattern = r'\b\d{6}\b'
    ticker_matches = re.findall(ticker_pattern, f"{title} {full_text[:1000]}")
    if ticker_matches:
        signals.append(f"6자리 티커 패턴 발견: {ticker_matches[:3]}")
        signal_breakdown["ticker_score"] = 0.6
    
    # 2. 기업 리포트 표현 키워드 (가중치: 0.2) - 가장 약한 신호 (오탐 가능성 높음)
    company_keywords = ["투자의견", "목표주가", "실적", "컨센서스", "추정치", "목표가", "PER", "PBR"]
    detected_keywords = [kw for kw in company_keywords if kw in title or kw in full_text[:500]]
    if detected_keywords:
        signals.append(f"기업 리포트 표현: {', '.join(detected_keywords[:3])}")
        signal_breakdown["keyword_score"] = 0.2
    
    # 3. 기업명 + 콜론 패턴 (가중치: 0.3) - 중간 신호
    # 주요 기업명 패턴 (간단한 휴리스틱)
    major_companies = [
        "삼성", "SK", "LG", "현대", "포스코", "한화", "롯데", "신세계", "CJ", "GS",
        "KT", "네이버", "카카오", "엔씨소프트", "넥슨", "한화솔루션", "아모레퍼시픽",
        "셀트리온", "유한양행", "대한항공", "아시아나", "한진", "현대해상", "삼성화재"
    ]
    text_to_check = f"{title} {full_text[:500]}"
    for company in major_companies:
        if f"{company}:" in text_to_check or f"{company} :" in text_to_check:
            signals.append(f"기업명+콜론 패턴: {company}")
            signal_breakdown["company_colon_score"] = 0.3
            break
    
    # 가중합 점수 계산
    confidence = (
        signal_breakdown["ticker_score"] +
        signal_breakdown["keyword_score"] +
        signal_breakdown["company_colon_score"]
    )
    
    # ⭐ 개선: 0.7 이상만 뒤집기 (과잉 전환 방지)
    return {
        "has_strong_company_signal": confidence >= 0.7,  # 0.5 → 0.7로 상향
        "signals_detected": signals,
        "confidence": min(confidence, 1.0),
        "signal_breakdown": signal_breakdown
    }


def determine_report_type(report: Dict[str, Any]) -> str:
    """
    Naver 리포트의 report_type 결정
    
    ⭐ 개선: 1순위를 breadcrumb/메뉴 경로 기반으로 변경
    [1순위] 네이버 breadcrumb / 메뉴 경로 (category 필드)
    [2순위] 페이지 상단 태그 (종목명/분류 태그)
    [3순위] 제목 패턴 (대괄호 [], 괄호 () 패턴)
    [4순위] Safety Latch (기업 신호)
    [5순위] 제목/본문 휴리스틱
    
    Args:
        report: 리포트 데이터 (category 필드 포함)
    
    Returns:
        "COMPANY" | "INDUSTRY" | "MACRO" | "MIXED"
    """
    category = report.get("category", "").strip()
    title = report.get("title", "")
    
    # ⭐ [1순위] 네이버 breadcrumb / 메뉴 경로 기반 판단 (정확한 텍스트 매칭)
    # 네이버 증권의 정확한 카테고리명: "종목분석", "산업분석", "경제분석", "투자정보"
    # ⭐ 종목분석은 하드 오버라이드: COMPANY로 강제 분류
    if category == "종목분석":
        # 종목분석 리포트는 무조건 COMPANY로 분류 (휴리스틱 불필요)
        return "COMPANY"
    elif category == "산업분석":
        initial_type = "INDUSTRY"
    elif category == "경제분석":
        initial_type = "MACRO"
    elif category == "투자정보":
        # ⭐ 이미지 분석 힌트: "투자정보" 리포트는 대부분 시장 전반/스타일/전략 리포트
        # 예: "[Quant] 소외되어 온 종목들을 재점검해볼 시간, 1월" → MACRO (1월 효과 분석)
        # 예: "KOSPI200 수시변경 전략" → MACRO (전략 리포트)
        # 예: "Shinhan China Weekly" → MACRO (주간 리포트)
        # 
        # 특정 기업 리포트인 경우는 매우 드묾 (제목에 기업명 + 티커가 명시적으로 있는 경우만)
        # 
        # 기본값: MACRO (시장 전반/전략 리포트)
        # 제목/메타데이터에서 강한 기업 신호가 있으면 COMPANY로 전환
        initial_type = "MACRO"  # 기본값을 MACRO로 설정
    else:
        # 카테고리가 없거나 모호한 경우
        initial_type = None
    
    # ⭐ [2순위] 페이지 상단 태그 (상세 페이지 메타데이터)
    stock_name = report.get("stock_name")  # 상세 페이지에서 추출된 종목명 태그
    industry_category = report.get("industry_category")  # 상세 페이지에서 추출된 분류 태그
    target_price = report.get("target_price")  # 상세 페이지에서 추출된 목표가
    investment_opinion = report.get("investment_opinion")  # 상세 페이지에서 추출된 투자의견
    ticker_hint = report.get("ticker_hint")  # ⭐ 이미지 힌트: PDF 파일명에서 추출한 티커 힌트
    
    # ⭐ 이미지 힌트: "투자정보" 리포트의 경우, 상세 페이지 메타데이터가 없으면 MACRO 유지
    if initial_type == "MACRO" and category == "투자정보":
        # 목표가/투자의견이 있으면 종목분석 리포트로 전환
        if target_price or investment_opinion:
            initial_type = "COMPANY"
        # 종목명 태그가 있으면 종목분석 리포트로 전환
        elif stock_name:
            initial_type = "COMPANY"
        # 티커 힌트가 있으면 종목분석 리포트로 전환
        elif ticker_hint:
            initial_type = "COMPANY"
        # 그 외는 MACRO 유지
    
    if initial_type is None:
        # 목표가/투자의견이 있으면 종목분석 리포트로 확정
        if target_price or investment_opinion:
            initial_type = "COMPANY"
        # 종목명 태그가 있으면 종목분석 리포트로 확정
        elif stock_name:
            initial_type = "COMPANY"
        # 티커 힌트가 있으면 종목분석 리포트로 확정
        elif ticker_hint:
            initial_type = "COMPANY"
        # 분류 태그가 있으면 산업분석 리포트로 확정
        elif industry_category:
            initial_type = "INDUSTRY"
    
    # ⭐ [3순위] 제목 패턴 기반 판단
    if initial_type is None or initial_type == "MACRO":  # MACRO인 경우에도 재확인
        import re
        # 대괄호 [] 패턴 분석
        bracket_pattern = r'\[([^\]]+)\]'
        bracket_matches = re.findall(bracket_pattern, title)
        if bracket_matches:
            bracket_content = bracket_matches[0]
            # 산업 키워드 확인
            industry_keywords = ["반도체", "IT", "통신", "자동차", "타이어", "유통", "게임", "제약", "화학", "바이오"]
            if any(kw in bracket_content for kw in industry_keywords):
                initial_type = "INDUSTRY"
            # ⭐ 이미지 힌트: "[Quant]", "[신흥국 전략]", "[QWER]" 같은 태그는 MACRO/전략 리포트
            elif any(tag in bracket_content for tag in ["Quant", "전략", "QWER", "Daily", "Weekly", "Monitor", "Alert"]):
                initial_type = "MACRO"
        
        # 괄호 () 내 숫자 패턴: 기업 리포트의 강력한 신호 (예: "삼성전자(005930)")
        ticker_pattern = r'\((\d{6})\)'
        ticker_matches = re.findall(ticker_pattern, title)
        if ticker_matches:
            initial_type = "COMPANY"
        
        # 콜론(:) 패턴: 기업 리포트의 신호 (예: "삼성전자: 실적 호조")
        if ":" in title and any(company in title for company in ["삼성", "SK", "LG", "현대", "포스코"]):
            initial_type = "COMPANY"
        
        # ⭐ 이미지 힌트: "투자정보" 리포트의 MACRO 신호 키워드
        if initial_type == "MACRO" or category == "투자정보":
            macro_keywords = [
                "효과", "전략", "스타일", "로테이션", "Weekly", "Daily", "Monitor", "Alert",
                "시장", "증시", "KOSPI", "KOSDAQ", "전략", "전술", "스크리닝", "체크업",
                "China", "Global", "Macro", "Quant", "펀드", "ETF", "수시변경"
            ]
            if any(kw in title for kw in macro_keywords):
                initial_type = "MACRO"
        
        # ⭐ 이미지 힌트: "종목"이라는 단어가 있어도 특정 기업명이 없으면 MACRO일 가능성 높음
        if "종목" in title and initial_type != "COMPANY":
            # 특정 기업명이 있는지 확인
            major_companies = [
                "삼성", "SK", "LG", "현대", "포스코", "한화", "롯데", "신세계", "CJ", "GS",
                "KT", "네이버", "카카오", "엔씨소프트", "넥슨", "셀트리온", "유한양행"
            ]
            if not any(company in title for company in major_companies):
                # 특정 기업명이 없으면 MACRO 유지
                if initial_type is None:
                    initial_type = "MACRO"
    
    # ⭐ [4순위] 기존 카테고리 기반 판단 (fallback)
    if initial_type is None:
        if "종목" in category or "company" in category.lower():
            initial_type = "COMPANY"
        elif "산업" in category or "industry" in category.lower():
            initial_type = "INDUSTRY"
        elif "경제" in category or "economy" in category.lower() or "macro" in category.lower():
            initial_type = "MACRO"
        elif "투자정보" in category or "invest" in category.lower():
            # 투자정보는 제목에 특정 기업이 있으면 company, 없으면 macro/industry
            if any(keyword in title for keyword in ["기업", "주식", "종목", "회사"]):
                initial_type = "COMPANY"
            else:
                initial_type = "MACRO"
    
    # ⭐ [5순위] 제목/본문 휴리스틱 (최종 fallback)
    if initial_type is None:
        if any(keyword in title for keyword in ["시장", "산업", "업계", "섹터", "경제", "매크로"]):
            initial_type = "INDUSTRY"
        elif any(keyword in title for keyword in ["기업", "주식", "종목", "회사"]):
            initial_type = "COMPANY"
        else:
            initial_type = "COMPANY"  # 기본값은 COMPANY (보수적 접근)
    
    # ⭐ P0-α1: Safety Latch - INDUSTRY/MACRO로 분류됐지만 강한 기업 신호가 있으면 COMPANY 또는 MIXED로 전환
    # ⭐ 중요: "투자정보" 리포트(MACRO)에서는 Safety Latch 비활성화
    # 이유: 투자정보 리포트는 시장 전반/전략 리포트로, 기업명 언급 ≠ 기업 분석
    # 예: "[Quant] 소외되어 온 종목들을 재점검해볼 시간, 1월" → NVIDIA 언급 있지만 기업 분석 아님
    if initial_type in ["INDUSTRY", "MACRO"]:
        # ⭐ 투자정보 리포트에서는 Safety Latch 비활성화
        if category == "투자정보" and initial_type == "MACRO":
            logger.debug(f"투자정보 리포트(MACRO): Safety Latch 비활성화 (시장 전반/전략 리포트)")
            return initial_type
        
        company_signals = detect_company_signals(report)
        if company_signals["has_strong_company_signal"]:  # 이미 0.7 이상만 True
            logger.warning(
                f"오분류 방지 Safety Latch 작동: {initial_type} → COMPANY/MIXED "
                f"(신호: {', '.join(company_signals['signals_detected'][:2])}, "
                f"confidence: {company_signals['confidence']:.2f})"
            )
            # 신호가 매우 강하면 COMPANY (0.9 이상), 그 외는 MIXED
            if company_signals["confidence"] >= 0.9:
                return "COMPANY"
            else:
                return "MIXED"
    
    return initial_type


def check_sector_sanity(
    title: str,
    full_text: str,
    target_sector_code: str,
    company_name: Optional[str] = None
) -> tuple[bool, Optional[str], Optional[str]]:
    """
    ⭐ Sector Sanity Check v3: DB 분석 기반 자동 검증
    
    개선: SECTOR_KEYWORDS_FROM_DB (2,949개 기업 분석 결과) 활용
    - 해당 섹터의 Top 키워드가 텍스트에 있는지 확인
    - 경쟁 섹터 점수가 더 높으면 실패
    
    Args:
        title: 리포트 제목
        full_text: 전체 텍스트
        target_sector_code: 매핑된 섹터 코드
        company_name: 기업명 (선택)
    
    Returns:
        (is_valid, reason, rule_id): (True, None, None) 또는 (False, "이유", "rule_id")
    """
    if not target_sector_code:
        return True, None, None
    
    # 텍스트 준비 (제목 + 본문 앞부분)
    text_combined = (title + " " + full_text[:1500]).lower()
    
    # ============================================================
    # 방법 1: 금지된 조합 체크 (기존 로직 유지)
    # ============================================================
    forbidden_combinations = {
        "SEC_TIRE": ["삼성전기", "삼성전자", "LG전자", "SK하이닉스"],
        "SEC_DEFENSE": ["삼성전기", "삼성전자", "LG전자"],
    }
    
    if target_sector_code in forbidden_combinations:
        forbidden_keywords = forbidden_combinations[target_sector_code]
        for keyword in forbidden_keywords:
            if keyword.lower() in text_combined:
                reason = f"금지된 섹터-기업 조합: {keyword} → {target_sector_code}"
                rule_id = f"FORBIDDEN_COMBO_{target_sector_code}_{keyword.upper().replace(' ', '_')}"
                logger.warning(f"Sector Sanity Check 실패: {reason} (rule_id: {rule_id})")
                return False, reason, rule_id
    
    # ============================================================
    # 방법 2: DB 기반 키워드 점수 계산 (신규)
    # ============================================================
    target_score = 0
    matched_keywords = []
    
    if target_sector_code in SECTOR_KEYWORDS_FROM_DB:
        sector_keywords = SECTOR_KEYWORDS_FROM_DB[target_sector_code]
        for kw in sector_keywords:
            # 해시태그 제거 후 검색
            kw_clean = kw.lstrip('#').lower()
            if kw_clean in text_combined:
                target_score += 1
                matched_keywords.append(kw)
    
    # ============================================================
    # 방법 3: 경쟁 섹터 점수 계산
    # ============================================================
    competing_sectors = []
    for sector_code, keywords in SECTOR_KEYWORDS_FROM_DB.items():
        if sector_code == target_sector_code:
            continue
        
        score = 0
        for kw in keywords[:5]:  # 상위 5개만 비교
            kw_clean = kw.lstrip('#').lower()
            if kw_clean in text_combined:
                score += 1
        
        if score > 0:
            competing_sectors.append((sector_code, score))
    
    # 점수 기준 정렬
    competing_sectors.sort(key=lambda x: x[1], reverse=True)
    
    # ============================================================
    # 검증 규칙
    # ============================================================
    
    # 규칙 1: 대상 섹터 키워드가 하나도 없으면 경고 (실패는 아님)
    if target_score == 0 and target_sector_code in SECTOR_KEYWORDS_FROM_DB:
        # 키워드 기반 섹터만 검증 (DB에 없는 섹터는 스킵)
        logger.debug(f"Sector Sanity Check 경고: {target_sector_code} 키워드 없음 (통과 처리)")
        # 일단 통과 (너무 strict하면 많이 탈락)
        return True, None, None
    
    # 규칙 2: 경쟁 섹터 점수가 대상보다 2배 이상 높으면 실패
    if competing_sectors and target_score > 0:
        best_competitor, best_score = competing_sectors[0]
        if best_score >= target_score * 2:
            reason = f"섹터 불일치 의심: {best_competitor}({best_score}점) > {target_sector_code}({target_score}점)"
            rule_id = f"SECTOR_MISMATCH_{target_sector_code}_VS_{best_competitor}"
            logger.warning(f"Sector Sanity Check 실패: {reason}")
            return False, reason, rule_id
    
    # 규칙 3: 특정 섹터는 필수 키워드 필요 (강화)
    strict_required_keywords = {
        "SEC_TIRE": ["타이어", "고무", "넥센타이어", "한국타이어"],
        "SEC_DEFENSE": ["방산", "방위", "국방", "방위산업"],
        "SEC_SHIP": ["조선", "선박", "해양", "해운"],
    }
    
    if target_sector_code in strict_required_keywords and target_score == 0:
        required = strict_required_keywords[target_sector_code]
        has_required = any(kw.lower() in text_combined for kw in required)
        if not has_required:
            reason = f"필수 키워드 없음: {target_sector_code}는 {required} 중 하나 필요"
            rule_id = f"REQUIRED_KEYWORD_{target_sector_code}"
            logger.warning(f"Sector Sanity Check 실패: {reason}")
            return False, reason, rule_id
    
    logger.debug(f"Sector Sanity Check 통과: {target_sector_code} (score: {target_score}, matched: {matched_keywords[:3]})")
    return True, None, None


def map_industry_category_to_sector_code(industry_category: str) -> Optional[str]:
    """
    산업분석 리포트의 분류를 sector_code로 매핑
    
    ⭐ 개선: HTML 태그에서 직접 추출한 산업 분류를 sector_code로 변환
    
    Args:
        industry_category: "반도체", "IT", "통신", "자동차" 등
    
    Returns:
        sector_code: "SEC_SEMI", "SEC_IT", "SEC_TELECOM", "SEC_AUTO" 등
    """
    if not industry_category:
        return None
    
    # 네이버 리포트의 산업 태그 → sector_code 매핑
    mapping = {
        "반도체": "SEC_SEMI",
        "IT": "SEC_IT",
        "IT부품": "SEC_IT",  # ⭐ 추가: IT하드웨어/부품
        "IT하드웨어": "SEC_IT",  # ⭐ 추가
        "IT하드웨어산업": "SEC_IT",  # ⭐ 추가: "IT하드웨어산업(카메라부품)" 패턴
        "전기전자": "SEC_ELECTRONICS",  # ⭐ 추가
        "전자": "SEC_ELECTRONICS",  # ⭐ 추가
        "통신": "SEC_TELECOM",
        "자동차": "SEC_AUTO",
        "타이어": "SEC_TIRE",  # ⭐ 수정: SEC_AUTO에서 SEC_TIRE로 변경 (타이어는 별도 섹터)
        "유통": "SEC_RETAIL",
        "게임": "SEC_GAME",
        "제약": "SEC_BIO",
        "화학": "SEC_CHEM",
        "바이오": "SEC_BIO",
        "금융": "SEC_FINANCE",
        "에너지": "SEC_ENERGY",
        "건설": "SEC_CONSTRUCTION",
        "철강": "SEC_STEEL",
        "섬유": "SEC_TEXTILE",
        "식품": "SEC_FOOD",
        "의료": "SEC_MEDICAL",
        "교육": "SEC_EDUCATION",
        "미디어": "SEC_MEDIA",
        "물류": "SEC_LOGISTICS"
    }
    
    # 정확한 매칭 우선
    if industry_category in mapping:
        return mapping[industry_category]
    
    # 부분 매칭 (예: "반도체 장비" → "SEC_SEMI")
    for key, sector_code in mapping.items():
        if key in industry_category:
            return sector_code
    
    return None


def route_naver_report(
    parsed_report: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Naver 리포트를 기업/산업으로 라우팅
    
    ⭐ 개선: 산업 태그를 활용한 sector_code 매핑 추가
    
    Args:
        parsed_report: 파싱된 리포트 데이터
    
    Returns:
        {
            "route_type": "COMPANY" | "INDUSTRY" | "MACRO" | "MIXED",
            "route_confidence": 0.0~1.0,
            "route_evidence": ["키워드", "카테고리", "제목 패턴"],
            "industry_logic": {...} (산업 리포트인 경우),
            "target_sector_code": "SEC_SEMI" 등 (산업 리포트인 경우),
            "section_fingerprints": {...},  # 섹션 단위 dedup용
            "safety_latch_applied": bool,  # ⭐ P0-α1: Safety Latch 적용 여부
            "original_type": str,  # ⭐ P0-α1: 원래 분류 타입
            "company_signals": {...}  # ⭐ P0-α1: 기업 신호 정보
        }
    """
    # ⭐ P0-α1: Safety Latch 적용 (determine_report_type 내부에서 처리됨)
    report_type = determine_report_type(parsed_report)
    category = parsed_report.get("category", "")
    title = parsed_report.get("title", "")
    full_text = parsed_report.get("full_text", "")
    
    # 원래 분류 타입 추정 (Safety Latch 적용 전)
    original_type = "UNKNOWN"
    if "종목" in category or "company" in category.lower():
        original_type = "COMPANY"
    elif "산업" in category or "industry" in category.lower():
        original_type = "INDUSTRY"
    elif "경제" in category or "economy" in category.lower() or "macro" in category.lower():
        original_type = "MACRO"
    
    # Safety Latch 적용 여부 확인
    safety_latch_applied = False
    company_signals_info = None
    if original_type in ["INDUSTRY", "MACRO"] and report_type in ["COMPANY", "MIXED"]:
        safety_latch_applied = True
        company_signals_info = detect_company_signals(parsed_report)
    
    result = {
        "route_type": report_type,
        "route_confidence": 0.0,
        "route_evidence": [],
        "industry_logic": None,
        "section_fingerprints": {},
        "safety_latch_applied": safety_latch_applied,  # ⭐ P0-α1
        "original_type": original_type,  # ⭐ P0-α1
        "company_signals": company_signals_info  # ⭐ P0-α1
    }
    
    # 라우팅 근거 수집
    evidence = []
    confidence_factors = []
    
    # 카테고리 기반 신뢰도
    if category:
        evidence.append(f"카테고리: {category}")
        if "종목" in category:
            confidence_factors.append(0.9)
        elif "산업" in category:
            confidence_factors.append(0.8)
        elif "경제" in category:
            confidence_factors.append(0.8)
        else:
            confidence_factors.append(0.5)
    
    # 제목 패턴 기반 신뢰도
    if report_type == "COMPANY":
        if any(keyword in title for keyword in ["기업", "주식", "종목", "회사"]):
            evidence.append("제목에 기업 키워드 발견")
            confidence_factors.append(0.7)
    elif report_type in ["INDUSTRY", "MACRO"]:
        if any(keyword in title for keyword in ["시장", "산업", "업계", "섹터", "경제", "매크로"]):
            evidence.append("제목에 산업/경제 키워드 발견")
            confidence_factors.append(0.7)
    
    # 산업/매크로 리포트인 경우 industry_logic 추출 및 sector_code 매핑
    if report_type in ["INDUSTRY", "MACRO"]:
        # ⭐ 중요: MACRO 리포트(투자정보)는 sector_code를 MARKET or L1 only로 제한
        # 이유: 투자정보 리포트는 시장 전반/전략 리포트로, L2/L3 섹터 추론 금지
        if report_type == "MACRO" and category == "투자정보":
            # 투자정보 리포트는 기본적으로 MARKET
            target_sector_code = "MARKET"
            evidence.append("투자정보 리포트: MARKET (시장 전반/전략 리포트, L2/L3 추론 금지)")
            confidence_factors.append(0.9)  # 투자정보 리포트는 MARKET으로 확정
            result["target_sector_code"] = target_sector_code
            result["sector_code_level"] = "MARKET"
            logger.info(f"투자정보 리포트(MACRO): target_sector_code = MARKET (L2/L3 추론 금지)")
        else:
            # 산업 리포트는 기존 로직 유지
            # ⭐ [1순위] HTML 태그에서 추출한 산업 분류를 sector_code로 매핑
            # ⭐ P0-β2: L1 확정, L2/L3 후보로 강제
            industry_category = parsed_report.get("industry_category")  # 상세 페이지에서 추출
            target_sector_code = None
            sector_code_level = None  # L1, L2, L3 구분
            
            if industry_category:
                target_sector_code = map_industry_category_to_sector_code(industry_category)
                if target_sector_code:
                    # HTML 태그 매핑은 L1 확정 (0.9~0.95 OK)
                    sector_code_level = "L1"
                    # L1 확정값은 suffix 규칙으로 명시 (예: SEC_SEMI__L1)
                    target_sector_code_with_level = f"{target_sector_code}__L1"
                    evidence.append(f"HTML 산업 태그 (L1 확정): {industry_category} → {target_sector_code}")
                    confidence_factors.append(0.95)  # HTML 태그는 매우 높은 신뢰도
                    result["target_sector_code"] = target_sector_code  # 원본 코드 유지 (L1)
                    result["target_sector_code_with_level"] = target_sector_code_with_level  # 레벨 포함 버전
                    result["sector_code_level"] = sector_code_level
                    logger.info(f"산업 태그에서 sector_code 매핑 성공 (L1 확정): {industry_category} → {target_sector_code}")
        
        industry_logic = extract_industry_logic_from_naver(
            title=title,
            full_text=full_text,
            category=category,
            report_type=report_type  # ⭐ MACRO 리포트 구분을 위해 전달
        )
        result["industry_logic"] = industry_logic
        
        # target_sector_code가 없으면 industry_logic에서 추출 시도 (MACRO가 아닌 경우만)
        if not target_sector_code and industry_logic and report_type != "MACRO":
            # industry_logic에 sector_code가 포함되어 있을 수 있음
            logic_sector = industry_logic.get("target_sector_code")
            if logic_sector:
                # ⭐ STEP 0: 기업-섹터 상식 체크 (최우선 차단)
                is_valid, sanity_reason, rule_id = check_sector_sanity(
                    title=title,
                    full_text=full_text,
                    target_sector_code=logic_sector,
                    company_name=None
                )
                
                if not is_valid:
                    # Sanity check 실패 → HOLD
                    result["target_sector_code"] = None
                    result["hold_reason"] = f"HOLD_SECTOR_MAPPING_SANITY_CHECK_FAILED: {sanity_reason}"
                    result["sanity_check_rule_id"] = rule_id  # ⭐ P1: 룰 식별자 추가
                    evidence.append(f"Sector Sanity Check 실패: {sanity_reason} (rule_id: {rule_id})")
                    logger.warning(f"Sector Sanity Check 실패로 HOLD: {sanity_reason} (rule_id: {rule_id})")
                else:
                    # ⭐ Fallback으로 추출된 경우: confidence 깎기, L2/L3 확정 금지
                    result["target_sector_code"] = logic_sector
                    result["sector_code_level"] = "L1"  # Fallback은 L1까지만 허용
                    evidence.append(f"Fallback 키워드 매칭 (L1 후보): {logic_sector}")
                    # ⭐ 원칙 2: fallback은 confidence를 깎는다
                    confidence_factors.append(0.65)  # 0.8에서 0.15 감소
                    logger.warning(f"Fallback 섹터 매핑 (confidence 감소): {logic_sector}")
                    
                    # ⭐ 원칙 3: HTML 태그 없으면 L2/L3 확정 금지
                    if not industry_category:
                        logger.warning(f"HTML 태그 없음, L2/L3 확정 금지: {logic_sector}")
                        # L2/L3는 HOLD 처리 (나중에 수동 검토 필요)
                        if logic_sector.startswith("SEC_") and "_" in logic_sector[4:]:  # L2/L3 패턴 체크
                            result["target_sector_code"] = None  # L2/L3는 None으로 처리
                            result["hold_reason"] = "HOLD_SECTOR_MAPPING_L2L3_WITHOUT_HTML_TAG"
                            logger.warning(f"L2/L3 섹터는 HTML 태그 없이 확정 불가: {logic_sector}")
        
        # ⭐ 추가: HTML 태그로 매핑된 경우에도 Sanity Check 수행
        if target_sector_code and report_type != "MACRO":
            is_valid, sanity_reason, rule_id = check_sector_sanity(
                title=title,
                full_text=full_text,
                target_sector_code=target_sector_code,
                company_name=None
            )
            if not is_valid:
                result["target_sector_code"] = None
                result["hold_reason"] = f"HOLD_SECTOR_MAPPING_SANITY_CHECK_FAILED: {sanity_reason}"
                result["sanity_check_rule_id"] = rule_id  # ⭐ P1: 룰 식별자 추가
                evidence.append(f"Sector Sanity Check 실패: {sanity_reason} (rule_id: {rule_id})")
                logger.warning(f"HTML 태그 매핑 후 Sanity Check 실패로 HOLD: {sanity_reason} (rule_id: {rule_id})")
        
        # 산업 섹션 fingerprint 생성
        industry_text = f"{title} {full_text[:1000]}"
        if industry_text.strip():
            industry_fp = hashlib.sha256(industry_text.encode('utf-8')).hexdigest()
            result["section_fingerprints"]["industry"] = industry_fp
    
    # Confidence 계산 (근거의 평균)
    if confidence_factors:
        result["route_confidence"] = sum(confidence_factors) / len(confidence_factors)
    else:
        result["route_confidence"] = 0.5  # 중간 신뢰도
    
    # ⭐ 점수 분해 필드 추가 (캘리브레이션용) - 개선: 모든 feature를 0~1로 정규화
    result["route_confidence_breakdown"] = {
        "category_score": 0.0,
        "title_pattern_score": 0.0,
        "sector_mapping_score": 0.0,
        "evidence_count": len(evidence),
        "ambiguous_terms_detected": [],
        "missing_features": [],  # ⭐ 추가: 부족한 feature 목록
        "feature_scores": {}  # ⭐ 추가: 각 feature별 점수 (0~1)
    }
    
    # 카테고리 점수 (0~1 정규화)
    category_score = 0.0
    if category:
        if "종목" in category:
            category_score = 0.9
        elif "산업" in category or "경제" in category:
            category_score = 0.8
        else:
            category_score = 0.5
    result["route_confidence_breakdown"]["category_score"] = category_score
    result["route_confidence_breakdown"]["feature_scores"]["category"] = category_score
    
    # 제목 패턴 점수 (0~1 정규화)
    title_pattern_score = 0.0
    if report_type == "COMPANY":
        if any(keyword in title for keyword in ["기업", "주식", "종목", "회사"]):
            title_pattern_score = 0.7
    elif report_type in ["INDUSTRY", "MACRO"]:
        if any(keyword in title for keyword in ["시장", "산업", "업계", "섹터"]):
            title_pattern_score = 0.7
    result["route_confidence_breakdown"]["title_pattern_score"] = title_pattern_score
    result["route_confidence_breakdown"]["feature_scores"]["title_pattern"] = title_pattern_score
    
    # 섹터 매핑 점수 (0~1 정규화)
    sector_mapping_score = 0.0
    if result.get("industry_logic") and result["industry_logic"].get("target_sector_code"):
        sector_mapping_score = 0.8
    result["route_confidence_breakdown"]["sector_mapping_score"] = sector_mapping_score
    result["route_confidence_breakdown"]["feature_scores"]["sector_mapping"] = sector_mapping_score
    
    # 모호한 용어 감지
    ambiguous_keywords = ["당사", "동사", "Top pick", "핵심 기업", "대표 기업"]
    detected_ambiguous = [kw for kw in ambiguous_keywords if kw in full_text[:2000]]
    result["route_confidence_breakdown"]["ambiguous_terms_detected"] = detected_ambiguous
    
    # ⭐ 추가: 부족한 feature 추적 (0.6~0.7 구간에서 어떤 feature가 부족한지)
    # 각 feature의 최대 점수 대비 현재 점수 비율 계산
    feature_max_scores = {
        "category": 0.9,
        "title_pattern": 0.7,
        "sector_mapping": 0.8
    }
    
    missing_features = []
    for feature_name, max_score in feature_max_scores.items():
        current_score = result["route_confidence_breakdown"]["feature_scores"].get(feature_name, 0.0)
        if current_score < max_score * 0.5:  # 최대 점수의 50% 미만이면 부족
            missing_features.append({
                "feature": feature_name,
                "current_score": current_score,
                "max_score": max_score,
                "gap": max_score - current_score
            })
    
    result["route_confidence_breakdown"]["missing_features"] = missing_features
    
    result["route_evidence"] = evidence
    
    logger.info(f"Naver 리포트 라우팅: {result['route_type']} (confidence: {result['route_confidence']:.2f}, evidence: {evidence})")
    logger.debug(f"Confidence breakdown: {result['route_confidence_breakdown']}")
    
    return result


def extract_industry_logic_from_naver(
    title: str,
    full_text: str,
    category: str,
    report_type: str = "INDUSTRY"  # ⭐ MACRO 리포트 구분을 위해 추가
) -> Dict[str, Any]:
    """
    Naver 산업/매크로 리포트에서 Logic 추출
    
    Args:
        title: 리포트 제목
        full_text: 전체 텍스트
        category: 카테고리
    
    Returns:
        {
            "logic_summary": "...",
            "conditions": {"positive": [...], "negative": [...]},
            "key_sentence": "...",
            "target_sector_code": "...",  # 섹터 코드
            "target_drivers": [...]  # 관련 드라이버 코드
        }
    """
    from extractors.kirs_logic_extractor import get_all_sector_references
    
    # 텍스트 전처리
    combined_text = f"{title} {full_text[:2000]}"  # 앞부분 2000자만 사용
    combined_text_lower = combined_text.lower()
    
    # 1. 섹터 코드 추출
    target_sector_code = None
    all_sector_codes = list(get_all_sector_references().keys())
    
    # 섹터 Reference 텍스트와 매칭
    sector_scores = {}
    for sector_code, reference_text in get_all_sector_references().items():
        reference_keywords = reference_text.split()
        match_count = sum(1 for kw in reference_keywords if kw.lower() in combined_text_lower)
        if match_count > 0:
            sector_scores[sector_code] = match_count / len(reference_keywords)
    
    if sector_scores:
        target_sector_code = max(sector_scores.items(), key=lambda x: x[1])[0]
        if sector_scores[target_sector_code] < 0.1:
            target_sector_code = None
    
    # Fallback: 키워드 기반 매핑 (후보만 허용, confidence 깎기)
    # ⭐ 원칙 1: 좁은 섹터 단독 키워드 금지
    # ⭐ 원칙 2: fallback은 확정이 아니라 관측 대상
    narrow_sectors = ["SEC_TIRE", "SEC_DEFENSE", "SEC_TELECOM"]  # 좁은 섹터 목록
    
    if not target_sector_code:
        sector_keywords = {
            "반도체": "SEC_SEMI",
            "화학": "SEC_CHEM",
            "바이오": "SEC_BIO",
            "IT": "SEC_IT",
            "IT부품": "SEC_IT",  # ⭐ 추가
            "IT하드웨어": "SEC_IT",  # ⭐ 추가
            "IT하드웨어산업": "SEC_IT",  # ⭐ 추가: "IT하드웨어산업(카메라부품)" 패턴
            "전기전자": "SEC_ELECTRONICS",  # ⭐ 추가
            "전자": "SEC_ELECTRONICS",  # ⭐ 추가
            "카메라": "SEC_IT",  # ⭐ 추가: 카메라부품은 IT하드웨어
            "카메라부품": "SEC_IT",  # ⭐ 추가
            "전기차": "SEC_AUTO",
            "배터리": "SEC_BATTERY",
            "소프트웨어": "SEC_IT",
            "게임": "SEC_GAME",
            "제약": "SEC_BIO"
        }
        
        # 좁은 섹터는 2개 이상 키워드 필요
        matched_keywords = []
        for keyword, sector_code in sector_keywords.items():
            if keyword in combined_text and sector_code in all_sector_codes:
                matched_keywords.append((keyword, sector_code))
        
        # 좁은 섹터 체크: 타이어는 "타이어" + "고무" 등 2개 이상 필요
        if matched_keywords:
            candidate_sector = matched_keywords[0][1]
            if candidate_sector in narrow_sectors:
                # 좁은 섹터는 키워드가 2개 이상이어야 함
                if len(matched_keywords) < 2:
                    logger.warning(f"좁은 섹터 단독 키워드 매칭 금지: {matched_keywords[0][0]} → {candidate_sector}")
                    target_sector_code = None  # 매칭 실패
                else:
                    target_sector_code = candidate_sector
            else:
                target_sector_code = candidate_sector
    
    # ⭐ 중요: MACRO 리포트(투자정보)는 driver_type을 MACRO/STYLE/FLOW로 제한
    # 이유: 투자정보 리포트는 시장 전반/전략 리포트로, 기업/산업 특화 드라이버가 아님
    if report_type == "MACRO" and category == "투자정보":
        # 투자정보 리포트는 MACRO/STYLE/FLOW 드라이버만 추출
        # 예: "1월 효과", "스타일 로테이션", "펀드 플로우" → MACRO/STYLE/FLOW 드라이버
        macro_driver_keywords = {
            "효과": "MACRO_EFFECT",
            "전략": "MACRO_STRATEGY",
            "스타일": "STYLE_FACTOR",
            "로테이션": "STYLE_ROTATION",
            "플로우": "FLOW_FACTOR",
            "펀드": "FLOW_FUND",
            "ETF": "FLOW_ETF",
            "Weekly": "MACRO_WEEKLY",
            "Daily": "MACRO_DAILY",
            "Monitor": "MACRO_MONITOR",
            "Alert": "MACRO_ALERT"
        }
        target_drivers = []
        combined_text_lower = combined_text.lower()
        for keyword, driver_code in macro_driver_keywords.items():
            if keyword in combined_text_lower:
                target_drivers.append(driver_code)
        
        # 기본적으로 MACRO 드라이버 추가
        if not target_drivers:
            target_drivers = ["MACRO_MARKET"]  # 기본 MACRO 드라이버
        
        # ⭐ logic_summary, key_sentence, conditions 정의 (MACRO 리포트용)
        paragraphs = [p.strip() for p in full_text.split('\n\n') if p.strip() and len(p.strip()) > 50]
        key_sentence = paragraphs[0] if paragraphs else title
        
        logic_summary = f"{title} {paragraphs[0] if paragraphs else ''}"
        if len(logic_summary) > 500:
            logic_summary = logic_summary[:500] + "..."
        
        # 조건 추출 (간단한 휴리스틱)
        positive_keywords = ["증가", "상승", "성장", "확대", "개선", "호조"]
        negative_keywords = ["감소", "하락", "축소", "악화", "부진", "우려"]
        
        positive_conditions = []
        negative_conditions = []
        
        for para in paragraphs[:5]:  # 앞 5개 문단만
            for keyword in positive_keywords:
                if keyword in para and len(para) > 30:
                    positive_conditions.append(para[:200].strip())
                    break
            for keyword in negative_keywords:
                if keyword in para and len(para) > 30:
                    negative_conditions.append(para[:200].strip())
                    break
        
        return {
            "logic_summary": logic_summary,
            "conditions": {
                "positive": positive_conditions[:3],
                "negative": negative_conditions[:3]
            },
            "key_sentence": key_sentence[:300],
            "target_sector_code": "MARKET",  # 투자정보 리포트는 MARKET
            "target_drivers": list(set(target_drivers)),
            "target_type": "MARKET",  # MARKET 타입
            "driver_type": "MACRO"  # MACRO 드라이버 타입
        }
    
    # 2. 드라이버 코드 추출 (산업 리포트용)
    target_drivers = []
    driver_keywords = {
        "AI": "AI_TECH",
        "반도체": "SEMICONDUCTOR",
        "배터리": "BATTERY",
        "전기차": "EV",
        "수소": "HYDROGEN",
        "바이오": "BIO",
        "유가": "OIL_PRICE_WTI",
        "환율": "EXCHANGE_RATE",
        "금리": "INTEREST_RATE"
    }
    
    for keyword, driver_code in driver_keywords.items():
        if keyword in combined_text:
            target_drivers.append(driver_code)
    
    # 3. 핵심 문장 추출 (첫 문단 또는 제목)
    paragraphs = [p.strip() for p in full_text.split('\n\n') if p.strip() and len(p.strip()) > 50]
    key_sentence = paragraphs[0] if paragraphs else title
    
    # 4. Logic 요약 (제목 + 첫 문단)
    logic_summary = f"{title} {paragraphs[0] if paragraphs else ''}"
    if len(logic_summary) > 500:
        logic_summary = logic_summary[:500] + "..."
    
    # 5. 조건 추출 (간단한 휴리스틱)
    positive_keywords = ["증가", "상승", "성장", "확대", "개선", "호조"]
    negative_keywords = ["감소", "하락", "축소", "악화", "부진", "우려"]
    
    positive_conditions = []
    negative_conditions = []
    
    for para in paragraphs[:5]:  # 앞 5개 문단만
        for keyword in positive_keywords:
            if keyword in para and len(para) > 30:
                positive_conditions.append(para[:200].strip())
                break
        for keyword in negative_keywords:
            if keyword in para and len(para) > 30:
                negative_conditions.append(para[:200].strip())
                break
    
    return {
        "logic_summary": logic_summary,
        "conditions": {
            "positive": positive_conditions[:3],
            "negative": negative_conditions[:3]
        },
        "key_sentence": key_sentence[:300],
        "target_sector_code": target_sector_code,
        "target_drivers": list(set(target_drivers)),
        "target_type": "SECTOR" if target_sector_code else "TECHNOLOGY"
    }

