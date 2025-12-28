# -*- coding: utf-8 -*-
"""
우선주 판단 유틸리티

Phase 2: 본주 존재 여부 검증 포함
"""
import re
import logging
from typing import Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import func, or_

logger = logging.getLogger(__name__)

# 우선주 패턴 정규식 (우, 2우B, 3우B, 우선주 등)
PREFERRED_PATTERN = re.compile(r'(우|우B|우\(전\)|[0-9]우B?|우선주)$')

# 예외 케이스 목록: '우'가 앞이나 중간에 있는 일반 기업명
EXCEPTION_COMPANIES = {
    '우성',  # '우'가 앞에 있는 일반 기업명
    '대우',  # 일반 기업명 (대우그룹 관련)
    '성우',  # 일반 기업명
    '배우',  # 일반 기업명
    '화우'   # 일반 기업명
}


def normalize_name(name: str) -> str:
    """
    이름 정규화 (공백 정규화)
    - 여러 공백을 하나로
    - 앞뒤 공백 제거
    """
    if not name:
        return ""
    # 여러 공백을 하나로, 앞뒤 공백 제거
    normalized = ' '.join(name.split())
    return normalized.strip()


def is_preferred_stock_smart(
    stock_name: str,
    db_session: Optional[Session] = None
) -> Tuple[bool, Optional[str]]:
    """
    우선주 여부 판단 + 본주 티커 반환 (Phase 2: 스마트 검증)
    
    Args:
        stock_name: 주식 이름
        db_session: DB 세션 (본주 검증용, None이면 패턴만 체크)
    
    Returns:
        (is_preferred, parent_ticker)
        - is_preferred: 우선주 여부
        - parent_ticker: 본주 티커 (우선주인 경우), None (우선주가 아닌 경우)
    """
    if not stock_name:
        return False, None
    
    stock_name = normalize_name(stock_name)  # 공백 정규화
    if not stock_name:
        return False, None
    
    # 0. 예외 케이스 처리: '우'가 앞에 오거나 특정 기업명은 제외
    if stock_name in EXCEPTION_COMPANIES:
        logger.debug(f"예외 기업명으로 일반 주식 처리: {stock_name}")
        return False, None
    
    # 0-1. '우'가 문자열 앞에 오는 경우 제외 (단, '우선주'는 예외)
    if stock_name.startswith('우') and stock_name != '우선주':
        if len(stock_name) > 1:  # '우' + 다른 글자
            logger.debug(f"'우'가 앞에 있어 일반 주식 처리: {stock_name}")
            return False, None
    
    # 1. 패턴 체크
    match = PREFERRED_PATTERN.search(stock_name)
    
    if not match:
        # 우선주 패턴이 아니면 일반 주식
        return False, None
    
    # 2. 본주 이름 추출 (공백 정규화)
    parent_name_candidate = normalize_name(stock_name[:match.start()])
    
    if not parent_name_candidate:
        # '우'만 있는 경우는 이상하지만, 일단 False 반환
        logger.warning(f"이상한 주식명: {stock_name} (본주 이름 추출 실패)")
        return False, None
    
    # 2-1. 본주 이름 검증 강화: 본주 이름이 너무 짧으면 일반 주식으로 처리
    if len(parent_name_candidate) < 2:
        logger.debug(f"본주 이름 후보가 너무 짧음: {stock_name} -> {parent_name_candidate} (일반 주식으로 처리)")
        return False, None
    
    # 3. DB 세션이 없으면 패턴만 체크 (본주 검증 불가)
    if db_session is None:
        logger.debug(f"DB 세션 없음, 패턴만 체크: {stock_name} -> 우선주로 추정 (본주: {parent_name_candidate})")
        return True, None  # 우선주로 추정하지만 본주 티커는 모름
    
    # 4. 본주 존재 여부 확인 (Stock 테이블에서 검색 - 공백 무시 비교)
    try:
        from sqlalchemy import text
        
        # 컬럼 없는 오류 방지를 위해 raw SQL 사용
        # 기본 컬럼만 선택 (is_preferred_stock, parent_ticker 제외)
        
        # Raw SQL로 기본 컬럼만 조회 (컬럼 없는 오류 방지)
        sql = text("""
            SELECT ticker, stock_name, market 
            FROM stocks 
            WHERE country = 'KR' 
            AND market IN ('KOSPI', 'KOSDAQ')
        """)
        result = db_session.execute(sql)
        all_stocks_list = [(row[0], row[1], row[2]) for row in result]  # (ticker, name, market)
        
        # 1단계: 정확히 일치
        parent_stock_info = None
        for ticker, name, market in all_stocks_list:
            if name == parent_name_candidate:
                parent_stock_info = (ticker, name, market)
                break
        
        # 2단계: 정규화된 이름으로 비교
        if not parent_stock_info:
            for ticker, name, market in all_stocks_list:
                if normalize_name(name) == parent_name_candidate:
                    parent_stock_info = (ticker, name, market)
                    break
        
        # 3단계: 공백 제거 후 비교
        if not parent_stock_info:
            parent_name_no_space = parent_name_candidate.replace(' ', '')
            for ticker, name, market in all_stocks_list:
                stock_name_no_space = normalize_name(name).replace(' ', '')
                if stock_name_no_space == parent_name_no_space:
                    parent_stock_info = (ticker, name, market)
                    break
        
        # 본주 찾음
        if parent_stock_info:
            parent_ticker_found = parent_stock_info[0]
            logger.debug(f"우선주 확인: {stock_name} -> 본주: {parent_ticker_found} ({parent_name_candidate})")
            return True, parent_ticker_found
        
        # 5. 오탐 가능성 (예: '대우', '배우', '성우' 등)
        # 본주가 없으면 일반 주식으로 처리
        logger.debug(f"본주 미발견 (Stock 테이블): {stock_name} -> {parent_name_candidate} (일반 주식으로 처리)")
        return False, None
        
    except Exception as e:
        logger.warning(f"우선주 판단 중 오류: {stock_name}, 오류: {e}")
        return False, None

