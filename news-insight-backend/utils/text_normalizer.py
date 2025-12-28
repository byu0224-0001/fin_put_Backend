"""
텍스트 정규화 유틸리티

logic_fingerprint 안정성을 위한 텍스트 정규화 함수
report_id 생성 안정성을 위한 날짜/URL 정규화 함수
"""
import re
import logging
from datetime import datetime
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger(__name__)

# ⭐ report_id 생성 규칙 버전 (변경 금지)
# v0.1-rc1: broker_name|title|normalized_date|canonicalized_url
# 이 버전을 변경하면 기존 report_id와 충돌 가능성이 있으므로,
# 변경 시에는 report_uid 같은 별도 키를 사용하거나 마이그레이션 전략 필요
REPORT_ID_GENERATION_VERSION = "v0.1-rc1"


def normalize_text_for_fingerprint(text: str) -> str:
    """
    logic_fingerprint 생성을 위한 텍스트 정규화
    
    목적: 공백/줄바꿈/숫자 포맷 차이를 무시하여 동일한 논리는 동일한 fingerprint를 생성
    
    Args:
        text: 원본 텍스트
    
    Returns:
        정규화된 텍스트
    """
    if not text:
        return ""
    
    # 1. 소문자 변환
    normalized = text.lower()
    
    # 2. 연속된 공백/줄바꿈을 단일 공백으로 변환
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # 3. 앞뒤 공백 제거
    normalized = normalized.strip()
    
    # 4. 숫자 포맷 정규화 (예: 1,000 → 1000, 1.5% → 1.5)
    # 주의: 의미 있는 숫자 차이는 유지해야 하므로 완전히 제거하지 않음
    # 단, 천단위 구분자만 제거
    normalized = re.sub(r'(\d),(\d)', r'\1\2', normalized)
    
    # 5. 특수문자 정규화 (예: "→", "->" → "→")
    normalized = normalized.replace('->', '→')
    normalized = normalized.replace('=>', '→')
    
    return normalized


def normalize_date_for_report_id(date_str: str) -> str:
    """
    날짜를 YYYY-MM-DD 형식으로 정규화 (report_id 생성용)
    
    Args:
        date_str: 원본 날짜 문자열 (다양한 포맷 가능)
    
    Returns:
        YYYY-MM-DD 형식의 날짜 문자열, 실패 시 "NA"
    """
    if not date_str or date_str.strip() == "":
        return "NA"
    
    try:
        # 다양한 포맷 처리
        date_formats = [
            "%Y-%m-%d",
            "%Y.%m.%d",
            "%Y/%m/%d",
            "%y.%m.%d",  # ⭐ 2자리 연도 추가 (25.12.19)
            "%y-%m-%d",
            "%y/%m/%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y.%m.%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%S.%f",
        ]
        
        for fmt in date_formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                # 2자리 연도는 2000년대로 가정
                if fmt.startswith("%y") and dt.year < 2000:
                    dt = dt.replace(year=dt.year + 2000)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        
        # 모든 포맷 실패 시 "NA" 반환
        logger.warning(f"날짜 파싱 실패: {date_str}")
        return "NA"
    except Exception as e:
        logger.warning(f"날짜 정규화 오류: {date_str}, {e}")
        return "NA"


def canonicalize_url(url: str) -> str:
    """
    URL에서 쿼리스트링과 fragment 제거하여 canonicalize (report_id 생성용)
    
    Args:
        url: 원본 URL
    
    Returns:
        canonicalized URL (scheme + netloc + path만), 실패 시 "NA"
    """
    if not url or url.strip() == "":
        return "NA"
    
    try:
        parsed = urlparse(url.strip())
        # scheme + netloc + path만 사용 (query, fragment 제거)
        canonical = urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))
        return canonical if canonical else "NA"
    except Exception as e:
        logger.warning(f"URL canonicalize 오류: {url}, {e}")
        return "NA"


def normalize_text_for_fuzzy_fingerprint(text: str) -> str:
    """
    fuzzy_fingerprint 생성을 위한 강화된 텍스트 정규화
    
    목적: 숫자/기업명/티커 차이를 무시하여 "같은 논리"를 식별
    
    사용 사례: sources>=3 안 쌓이는 원인 분리
    - A: 데이터 부족 (진짜 같은 논리가 없음)
    - B: 과도 세분화 (같은 논리인데 다른 fingerprint)
    
    Args:
        text: 원본 텍스트
    
    Returns:
        fuzzy 정규화된 텍스트
    """
    if not text:
        return ""
    
    # 1. 기본 정규화 (기존 함수 활용)
    normalized = normalize_text_for_fingerprint(text)
    
    # 2. 숫자를 <NUM>으로 치환 (소수점, 퍼센트, 통화 포함)
    # 예: 2025년 → <NUM>년, 1.5% → <NUM>%, 1조원 → <NUM>조원
    normalized = re.sub(r'\d+(\.\d+)?%?', '<NUM>', normalized)
    
    # 3. 날짜 패턴을 <DATE>로 치환
    # 예: 2025-12-23, 25.12.23, 12/23 등
    normalized = re.sub(r'\d{2,4}[-./]\d{1,2}[-./]\d{1,2}', '<DATE>', normalized)
    
    # 4. 한글 기업명 패턴을 <ENTITY>로 치환 (흔한 접미사)
    # 예: 삼성전자, 현대건설, SK하이닉스 등
    # 주의: 너무 많이 치환하면 의미가 사라짐 - 최소한만 적용
    normalized = re.sub(r'[가-힣]+(?:전자|건설|증권|화학|제약|반도체|통신|금융|은행|보험|자동차|중공업|에너지|물산|상사)', '<ENTITY>', normalized)
    
    # 5. 영문 티커 패턴을 <TICKER>로 치환 (괄호 안 대문자)
    # 예: (AAPL), (005930) 등
    normalized = re.sub(r'\([A-Z]{2,5}\)', '<TICKER>', normalized)
    normalized = re.sub(r'\(\d{6}\)', '<TICKER>', normalized)
    
    # 6. 연속된 치환 토큰 정리
    normalized = re.sub(r'(<NUM>\s*)+', '<NUM> ', normalized)
    normalized = re.sub(r'(<ENTITY>\s*)+', '<ENTITY> ', normalized)
    
    # 7. 최종 공백 정리
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized

