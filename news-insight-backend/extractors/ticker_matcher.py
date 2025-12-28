"""
Ticker Matcher (Phase 2.0 P0)

목적: 리포트의 주인(Subject) 찾기

3단계 매칭:
1. Rule: 제목/파일명에서 종목코드(6자리 숫자) 추출
2. Fuzzy: stocks 테이블 company_name/alias fuzzy 매칭
3. LLM: 기업명 1개만 추출 → 다시 fuzzy 재검증

Context-Aware:
- 기업 리포트: subject 확정 + 대명사 치환(동사/당사 등)
- 산업 리포트: 명시적 언급 기업만 태깅
"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import re
import logging

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from sqlalchemy.orm import Session
from sqlalchemy import or_
from app.db import SessionLocal
from app.models.stock import Stock
from app.models.company_alias import CompanyAlias

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from rapidfuzz import fuzz, process
    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False
    logger.warning("rapidfuzz가 설치되지 않았습니다. pip install rapidfuzz 실행하세요.")


# 일반적인 단어 제외 리스트 (오탐 방지)
COMMON_WORDS_BLACKLIST = {
    "삼성", "LG", "현대", "SK", "롯데", "한화", "두산", "포스코"
    # 너무 일반적인 단어는 confidence 크게 낮춤
}

# 회사 약어 사전 (수동 보완)
COMPANY_ALIASES = {
    "삼전": "삼성전자",
    "하이닉스": "SK하이닉스",
    "현차": "현대자동차",
    "기아": "기아",
    "엘지엔솔": "LG에너지솔루션",
    "엘지화학": "LG화학",
    "엘지디스플레이": "LG디스플레이",
    "엘지전자": "LG전자",
    "포스코홀딩스": "포스코홀딩스",
    "포스코케미칼": "포스코케미칼",
    "한화솔루션": "한화솔루션",
    "한화케미칼": "한화케미칼",
    "두산에너빌리티": "두산에너빌리티",
    "두산중공업": "두산중공업",
    "S-Oil": "S-Oil",
    "에스오일": "S-Oil",
    "GS": "GS",
    "GS칼텍스": "GS칼텍스",
    "대한항공": "대한항공",
    "아시아나항공": "아시아나항공",
    "카카오": "카카오",
    "네이버": "네이버",
    "크래프톤": "크래프톤",
    "넥슨": "넥슨",
    "NC소프트": "NC소프트"
}


def extract_ticker_by_rule(title: str, filename: str = "") -> Optional[str]:
    """
    1단계: 룰 기반 종목코드 추출
    
    Args:
        title: 리포트 제목
        filename: 파일명 (선택)
    
    Returns:
        종목코드 (6자리 숫자) 또는 None
    """
    # 제목에서 종목코드 패턴 찾기
    patterns = [
        r'\((\d{6})\)',  # (005930)
        r'\[(\d{6})\]',  # [005930]
        r'종목코드[:\s]*(\d{6})',  # 종목코드: 005930
        r'티커[:\s]*(\d{6})',  # 티커: 005930
        r'\b(\d{6})\b'  # 6자리 숫자 (단어 경계)
    ]
    
    for pattern in patterns:
        match = re.search(pattern, title)
        if match:
            ticker = match.group(1)
            if len(ticker) == 6 and ticker.isdigit():
                logger.debug(f"룰 기반 종목코드 추출: {ticker} (패턴: {pattern})")
                return ticker
    
    # 파일명에서도 찾기
    if filename:
        for pattern in patterns:
            match = re.search(pattern, filename)
            if match:
                ticker = match.group(1)
                if len(ticker) == 6 and ticker.isdigit():
                    logger.debug(f"파일명에서 종목코드 추출: {ticker}")
                    return ticker
    
    return None


def fuzzy_match_company_name(
    company_name: str,
    db: Optional[Session] = None,
    threshold: float = 85.0
) -> List[Dict[str, Any]]:
    """
    2단계: Fuzzy 매칭 (stocks 테이블)
    
    Args:
        company_name: 회사명 (또는 약어)
        db: DB 세션
        threshold: 유사도 임계값 (기본 85%)
    
    Returns:
        [{"ticker": "...", "company_name": "...", "confidence": 0.9, "method": "FUZZY"}, ...]
    """
    if not RAPIDFUZZ_AVAILABLE:
        logger.warning("rapidfuzz가 없어 fuzzy 매칭을 건너뜁니다.")
        return []
    
    if db is None:
        db = SessionLocal()
        should_close = True
    else:
        should_close = False
    
    try:
        # 1. 수동 약어 사전 체크
        if company_name in COMPANY_ALIASES:
            normalized_name = COMPANY_ALIASES[company_name]
            logger.debug(f"수동 약어 사전 매칭: {company_name} → {normalized_name}")
        else:
            normalized_name = company_name
        
        # 2. CompanyAlias 테이블에서 별칭 체크
        alias_match = db.query(CompanyAlias).filter(
            CompanyAlias.alias_name.ilike(f"%{company_name}%")
        ).first()
        
        if alias_match:
            normalized_name = alias_match.official_name
            logger.debug(f"DB 별칭 매칭: {company_name} → {normalized_name} (ticker: {alias_match.ticker})")
        
        # 3. stocks 테이블에서 모든 회사명 조회
        all_stocks = db.query(Stock).all()
        
        candidates = []
        
        for stock in all_stocks:
            stock_name = stock.stock_name or ""
            
            # synonyms/alias 체크 (ARRAY 타입)
            synonyms = []
            if stock.synonyms:
                if isinstance(stock.synonyms, list):
                    synonyms = stock.synonyms
                elif isinstance(stock.synonyms, (tuple, set)):
                    synonyms = list(stock.synonyms)
            
            # 검색 대상: stock_name + synonyms
            search_terms = [stock_name] + synonyms
            
            for term in search_terms:
                if not term or not isinstance(term, str):
                    continue
                
                # Fuzzy 매칭
                ratio = fuzz.ratio(normalized_name.lower(), term.lower())
                
                if ratio >= threshold:
                    # 일반적인 단어는 confidence 낮춤
                    confidence = ratio / 100.0
                    if normalized_name in COMMON_WORDS_BLACKLIST:
                        confidence *= 0.7  # 30% 감소
                    
                    candidates.append({
                        "ticker": stock.ticker,
                        "company_name": stock_name,
                        "confidence": confidence,
                        "method": "FUZZY",
                        "matched_term": term,
                        "fuzzy_ratio": ratio
                    })
        
        # confidence 내림차순 정렬
        candidates.sort(key=lambda x: x["confidence"], reverse=True)
        
        # 중복 제거 (같은 ticker는 confidence 높은 것만)
        seen_tickers = set()
        unique_candidates = []
        for candidate in candidates:
            if candidate["ticker"] not in seen_tickers:
                seen_tickers.add(candidate["ticker"])
                unique_candidates.append(candidate)
        
        return unique_candidates[:5]  # Top-5 반환
    
    finally:
        if should_close:
            db.close()


def extract_company_name_with_llm(
    title: str,
    text_head: str = "",
    max_length: int = 500
) -> Optional[str]:
    """
    3단계: LLM으로 기업명 추출 (최후수단)
    
    Args:
        title: 리포트 제목
        text_head: 리포트 본문 앞부분
        max_length: 본문 최대 길이
    
    Returns:
        기업명 (1개만) 또는 None
    """
    try:
        from openai import OpenAI
        import os
        
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.warning("OPENAI_API_KEY가 없어 LLM 매칭을 건너뜁니다.")
            return None
        
        client = OpenAI(api_key=api_key)
        
        # 입력 텍스트 구성
        input_text = title
        if text_head:
            input_text += "\n\n" + text_head[:max_length]
        
        prompt = f"""리포트 제목과 본문에서 언급된 기업명을 1개만 추출하세요.

제목: {title}
본문: {text_head[:max_length]}

응답 형식:
- 기업명만 출력 (예: "삼성전자")
- 여러 기업이 언급되면 가장 주요한 기업 1개만
- 찾을 수 없으면 "NONE" 출력
- 약어(삼전, 하이닉스 등)는 정식 명칭으로 변환하지 말고 그대로 출력
"""
        
        response = client.chat.completions.create(
            model="gpt-5-mini",
            messages=[
                {"role": "system", "content": "리포트에서 기업명을 1개만 추출하는 전문가. 기업명만 출력하고 설명은 하지 않습니다."},
                {"role": "user", "content": prompt}
            ],
            max_completion_tokens=50
        )
        
        company_name = response.choices[0].message.content.strip()
        
        if company_name.upper() == "NONE" or not company_name:
            return None
        
        # 따옴표 제거
        company_name = company_name.strip('"\'')
        
        logger.debug(f"LLM 기업명 추출: {company_name}")
        return company_name
    
    except Exception as e:
        logger.error(f"LLM 기업명 추출 실패: {e}")
        return None


def match_ticker(
    title: str,
    text_head: str = "",
    filename: str = "",
    report_type: str = "company",  # "company" or "industry"
    use_llm: bool = True,
    confidence_threshold: float = 0.85,
    report_metadata: Optional[Dict[str, Any]] = None,  # 리포트 메타데이터 (로그용)
    db: Optional[Session] = None,
    stock_name: Optional[str] = None  # ⭐ 종목분석 리스트에서 추출한 종목명 (1순위)
) -> Dict[str, Any]:
    """
    Ticker 매칭 (3단계)
    
    Args:
        title: 리포트 제목
        text_head: 리포트 본문 앞부분
        filename: 파일명 (선택)
        report_type: 리포트 유형 ("company" or "industry")
        use_llm: LLM 사용 여부
        confidence_threshold: 최소 confidence (이상이면 LLM 스킵)
        db: DB 세션
    
    Returns:
        {
            "ticker": "005930",
            "company_name": "삼성전자",
            "method": "RULE",  # RULE, FUZZY, LLM
            "confidence": 0.95,
            "candidates": [...],  # Top-5 후보
            "debug": {...}
        }
        또는
        {
            "ticker": None,
            "reason": "TICKER_NOT_FOUND",
            "debug": {...}
        }
    """
    debug_info = {
        "step0_stock_name": None,  # ⭐ 종목명 기반 매칭 추가
        "step1_rule": None,
        "step2_fuzzy": None,
        "step3_llm": None
    }
    
    # ⭐ 0단계: 종목분석 리스트에서 추출한 종목명 기반 매칭 (최우선)
    if stock_name and stock_name.strip():
        if db is None:
            db = SessionLocal()
            should_close = True
        else:
            should_close = False
        
        try:
            # 종목명으로 정확 매칭 시도
            from sqlalchemy import func
            stock = db.query(Stock).filter(
                func.replace(func.replace(Stock.stock_name, ' ', ''), '㈜', '') == 
                stock_name.replace(' ', '').replace('㈜', '')
            ).first()
            
            if not stock:
                # 부분 매칭 시도
                stocks = db.query(Stock).filter(
                    Stock.stock_name.ilike(f"%{stock_name}%")
                ).all()
                
                # ⭐ 동명이인 체크: 복수 후보면 AMBIGUOUS_NAME
                if len(stocks) > 1:
                    debug_info["step0_stock_name"] = "AMBIGUOUS"
                    return {
                        "ticker": None,
                        "company_name": stock_name,
                        "method": "STOCK_NAME_LIST",
                        "confidence": 0.0,
                        "match_category": "AMBIGUOUS_NAME",  # ⭐ 동명이인
                        "reason": "AMBIGUOUS_NAME",
                        "candidates": [{
                            "ticker": s.ticker,
                            "company_name": s.stock_name,
                            "confidence": 0.0,
                            "method": "STOCK_NAME_LIST"
                        } for s in stocks],
                        "debug": debug_info
                    }
                elif len(stocks) == 1:
                    stock = stocks[0]
            
            if stock:
                # ⭐ 2단계 검증: CompanyDetail 존재 여부 확인 (universe 체크)
                from app.models.company_detail import CompanyDetail
                company_detail = db.query(CompanyDetail).filter(
                    CompanyDetail.ticker == stock.ticker
                ).first()
                
                # 매칭 결과 분류
                if company_detail:
                    match_category = "MATCHED_IN_UNIVERSE"  # DB에 기업 상세 정보 있음
                    confidence = 1.0
                else:
                    match_category = "MATCHED_OUT_OF_UNIVERSE"  # ticker는 찾았지만 DB에 상세 정보 없음
                    confidence = 0.85  # ⭐ universe 밖이면 confidence 낮춤
                
                debug_info["step0_stock_name"] = "SUCCESS"
                debug_info["match_category"] = match_category
                return {
                    "ticker": stock.ticker,
                    "company_name": stock.stock_name,
                    "method": "STOCK_NAME_LIST",  # 리스트에서 추출한 종목명
                    "confidence": confidence,  # ⭐ universe 체크 반영
                    "match_category": match_category,  # ⭐ 4분류 추가
                    "candidates": [{
                        "ticker": stock.ticker,
                        "company_name": stock.stock_name,
                        "confidence": confidence,
                        "method": "STOCK_NAME_LIST"
                    }],
                    "debug": debug_info
                }
        except Exception as e:
            logger.warning(f"종목명 기반 매칭 실패: {e}")
        finally:
            if should_close:
                db.close()
    
    debug_info["step0_stock_name"] = "NOT_PROVIDED" if not stock_name else "NOT_FOUND"
    
    # 1단계: 룰 기반 종목코드 추출
    ticker_rule = extract_ticker_by_rule(title, filename)
    if ticker_rule:
        # 종목코드로 stocks 테이블에서 회사명 조회
        if db is None:
            db = SessionLocal()
            should_close = True
        else:
            should_close = False
        
        try:
            stock = db.query(Stock).filter(Stock.ticker == ticker_rule).first()
            if stock:
                debug_info["step1_rule"] = "SUCCESS"
                return {
                    "ticker": ticker_rule,
                    "company_name": stock.stock_name,
                    "method": "RULE",
                    "confidence": 0.95,
                    "candidates": [{
                        "ticker": ticker_rule,
                        "company_name": stock.stock_name,
                        "confidence": 0.95,
                        "method": "RULE"
                    }],
                    "debug": debug_info
                }
        finally:
            if should_close:
                db.close()
    
    debug_info["step1_rule"] = "NOT_FOUND"
    
    # 2단계: 제목에서 회사명 추출 시도 (키워드 매칭)
    # 약어 사전 체크
    for alias, full_name in COMPANY_ALIASES.items():
        if alias in title or full_name in title:
            fuzzy_results = fuzzy_match_company_name(full_name, db, threshold=80.0)
            if fuzzy_results:
                top_result = fuzzy_results[0]
                if top_result["confidence"] >= confidence_threshold:
                    debug_info["step2_fuzzy"] = "SUCCESS"
                    return {
                        "ticker": top_result["ticker"],
                        "company_name": top_result["company_name"],
                        "method": "FUZZY_ALIAS",
                        "confidence": top_result["confidence"],
                        "candidates": fuzzy_results,
                        "debug": debug_info
                    }
    
    # 제목에서 회사명 패턴 찾기 (간단한 휴리스틱)
    # "XXX 투자포인트", "XXX 분석" 같은 패턴
    title_patterns = [
        r'^(.+?)\s+(투자포인트|분석|전망|리포트)',
        r'^(.+?)\s+\(',
        r'\[(.+?)\]',
    ]
    
    for pattern in title_patterns:
        match = re.search(pattern, title)
        if match:
            potential_name = match.group(1).strip()
            if len(potential_name) > 1 and len(potential_name) < 20:
                fuzzy_results = fuzzy_match_company_name(potential_name, db, threshold=80.0)
                if fuzzy_results:
                    top_result = fuzzy_results[0]
                    if top_result["confidence"] >= confidence_threshold:
                        debug_info["step2_fuzzy"] = "SUCCESS"
                        return {
                            "ticker": top_result["ticker"],
                            "company_name": top_result["company_name"],
                            "method": "FUZZY_TITLE",
                            "confidence": top_result["confidence"],
                            "candidates": fuzzy_results,
                            "debug": debug_info
                        }
    
    debug_info["step2_fuzzy"] = "NOT_FOUND"
    
    # 3단계: LLM 최후수단
    if use_llm:
        company_name_llm = extract_company_name_with_llm(title, text_head)
        if company_name_llm:
            # LLM 결과를 fuzzy로 재검증
            fuzzy_results = fuzzy_match_company_name(company_name_llm, db, threshold=75.0)
            if fuzzy_results:
                top_result = fuzzy_results[0]
                debug_info["step3_llm"] = "SUCCESS"
                return {
                    "ticker": top_result["ticker"],
                    "company_name": top_result["company_name"],
                    "method": "LLM_FUZZY",
                    "confidence": top_result["confidence"] * 0.9,  # LLM은 confidence 낮춤
                    "candidates": fuzzy_results,
                    "debug": debug_info
                }
            else:
                debug_info["step3_llm"] = "FUZZY_VALIDATION_FAILED"
        else:
            debug_info["step3_llm"] = "LLM_EXTRACTION_FAILED"
    else:
        debug_info["step3_llm"] = "SKIPPED"
    
    # 매칭 실패 - 로그 기록 (report_metadata가 있으면)
    if report_metadata:
        try:
            from utils.unmatched_logger import log_ticker_match_failure
            
            log_ticker_match_failure(
                report_title=report_metadata.get("title", title),
                report_broker=report_metadata.get("broker_name", ""),
                report_date=report_metadata.get("report_date", ""),
                failure_reason="TICKER_NOT_FOUND",
                extracted_company_name=None,
                debug_info=debug_info,
                suggested_aliases=[]
            )
        except ImportError:
            logger.warning("unmatched_logger를 사용할 수 없습니다.")
    
    return {
        "ticker": None,
        "company_name": None,
        "method": "NONE",
        "confidence": 0.0,
        "match_category": "NOT_FOUND",  # ⭐ 4분류 추가
        "reason": "TICKER_NOT_FOUND",
        "candidates": [],
        "debug": debug_info
    }


def get_report_subject(
    title: str,
    text_head: str = "",
    filename: str = "",
    report_type: str = "company",
    db: Optional[Session] = None
) -> Dict[str, Any]:
    """
    리포트 Subject 확정 (Context-Aware)
    
    기업 리포트: subject 확정 + 대명사 치환 정보 제공
    산업 리포트: 명시적 언급 기업만 태깅
    
    Args:
        title: 리포트 제목
        text_head: 리포트 본문 앞부분
        filename: 파일명
        report_type: 리포트 유형
        db: DB 세션
    
    Returns:
        {
            "ticker": "...",
            "company_name": "...",
            "confidence": 0.9,
            "report_subject": {...},  # 확정된 subject
            "pronoun_replacements": {...}  # 대명사 치환 정보
        }
    """
    # Ticker 매칭
    match_result = match_ticker(title, text_head, filename, report_type, db=db)
    
    if not match_result.get("ticker"):
        return {
            "ticker": None,
            "company_name": None,
            "confidence": 0.0,
            "report_subject": None,
            "pronoun_replacements": {},
            "reason": match_result.get("reason", "TICKER_NOT_FOUND")
        }
    
    # 기업 리포트: subject 확정
    if report_type == "company":
        report_subject = {
            "ticker": match_result["ticker"],
            "company_name": match_result["company_name"],
            "confidence": match_result["confidence"]
        }
        
        # 대명사 치환 정보
        pronoun_replacements = {
            "동사": match_result["company_name"],
            "당사": match_result["company_name"],
            "회사": match_result["company_name"],
            # "당사(증권사)" 패턴은 예외 처리 필요
        }
        
        return {
            "ticker": match_result["ticker"],
            "company_name": match_result["company_name"],
            "confidence": match_result["confidence"],
            "report_subject": report_subject,
            "pronoun_replacements": pronoun_replacements,
            "method": match_result["method"]
        }
    else:
        # 산업 리포트: 명시적 언급만
        return {
            "ticker": match_result["ticker"],
            "company_name": match_result["company_name"],
            "confidence": match_result["confidence"],
            "report_subject": None,  # 산업 리포트는 subject 없음
            "pronoun_replacements": {},
            "method": match_result["method"]
        }


if __name__ == "__main__":
    # 테스트 코드
    test_cases = [
        {
            "title": "삼성전자(005930) 투자포인트",
            "text_head": "삼성전자는 반도체 및 IT 기기 제조 기업입니다.",
            "report_type": "company"
        },
        {
            "title": "S-Oil 분석 리포트",
            "text_head": "S-Oil은 정유 및 석유화학 사업을 영위합니다.",
            "report_type": "company"
        },
        {
            "title": "반도체 산업 전망",
            "text_head": "삼성전자, SK하이닉스 등 메모리 반도체 기업들이...",
            "report_type": "industry"
        }
    ]
    
    for test in test_cases:
        result = get_report_subject(**test)
        print(f"\n제목: {test['title']}")
        print(f"결과: {result['ticker']} ({result['company_name']}) - confidence: {result['confidence']:.2f}, method: {result.get('method', 'NONE')}")

