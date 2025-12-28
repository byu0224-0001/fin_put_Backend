import requests
from bs4 import BeautifulSoup
from typing import Optional, Dict
import logging
import re
import json

logger = logging.getLogger(__name__)

# trafilatura는 필수 (지능형 본문 추출)
try:
    import trafilatura
    TRAFILATURA_AVAILABLE = True
except ImportError:
    TRAFILATURA_AVAILABLE = False
    logger.warning("trafilatura가 설치되지 않았습니다. 지능형 본문 추출이 제한됩니다.")

# Playwright는 선택적 (동적 사이트만)
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logger.warning("Playwright가 설치되지 않았습니다. 동적 사이트 파싱이 제한됩니다.")

# HTTP 요청 헤더
REQUEST_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}


def clean_text(text: str) -> str:
    """
    텍스트 정리 (불필요한 공백, 줄바꿈 제거)
    
    Args:
        text: 원본 텍스트
    
    Returns:
        정리된 텍스트
    """
    # 연속된 공백 제거
    text = re.sub(r'\s+', ' ', text)
    # 연속된 줄바꿈 제거
    text = re.sub(r'\n\s*\n', '\n\n', text)
    # 앞뒤 공백 제거
    text = text.strip()
    
    return text


def _extract_with_trafilatura(html: str, url: Optional[str] = None) -> Optional[Dict]:
    """
    trafilatura를 사용하여 본문 및 메타데이터 추출
    
    Args:
        html: HTML 문자열
        url: 원본 URL (선택적, 메타데이터 추출에 도움)
    
    Returns:
        {
            'content': 본문 텍스트,
            'title': 제목,
            'date': 발행일,
            'author': 작성자,
            'source': 'trafilatura'
        } 또는 None
    """
    if not TRAFILATURA_AVAILABLE:
        return None
    
    try:
        # trafilatura로 본문 + 메타데이터 추출
        if url:
            # URL이 있으면 직접 URL 사용 (더 정확함)
            result = trafilatura.extract(
                url,
                include_comments=False,
                with_metadata=True,
                output_format='json'
            )
        else:
            # HTML 문자열만 있으면 HTML 사용
            result = trafilatura.extract(
                html,
                include_comments=False,
                with_metadata=True,
                output_format='json'
            )
        
        if not result:
            return None
        
        # JSON 파싱
        data = json.loads(result)
        
        # 본문 텍스트 확인
        text = data.get('text', '').strip()
        if not text or len(text) < 200:
            return None
        
        return {
            'content': clean_text(text),
            'title': data.get('title', '').strip() or None,
            'date': data.get('date', '').strip() or None,
            'author': data.get('author', '').strip() or None,
            'source': 'trafilatura'
        }
        
    except Exception as e:
        logger.debug(f"trafilatura 추출 실패: {e}")
        return None


def _extract_with_beautifulsoup(html: str) -> Optional[str]:
    """
    BeautifulSoup + rule-based로 본문 추출 (최종 fallback)
    
    Args:
        html: HTML 문자열
    
    Returns:
        본문 텍스트 또는 None
    """
    try:
        soup = BeautifulSoup(html, 'lxml')
        
        # 불필요한 태그 제거
        for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 
                        'advertisement', 'ad', 'comment', 'comments', 'sidebar',
                        'related-articles', 'recommend', 'trending', 'popular']):
            tag.decompose()
        
        # 클래스/ID 기반으로 광고/댓글 영역 제거
        for selector in ['.ad', '.advertisement', '.ads', '.comment', '.comments',
                         '.sidebar', '.related', '.recommend', '.trending']:
            for element in soup.select(selector):
                element.decompose()
        
        # 일반적인 선택자로 본문 추출 (사용자 요청대로)
        content = None
        for selector in ['article', 'main', '.content']:
            element = soup.select_one(selector)
            if element:
                content = element.get_text(separator='\n', strip=True)
                # 최소 길이 확인
                if content and len(content) >= 200:
                    break
        
        if content and len(content) >= 200:
            return clean_text(content)
        
        return None
        
    except Exception as e:
        logger.debug(f"BeautifulSoup 추출 실패: {e}")
        return None


def _fetch_with_playwright(url: str) -> Optional[str]:
    """
    Playwright를 사용하여 렌더링된 HTML 다운로드
    
    Args:
        url: 기사 URL
    
    Returns:
        렌더링된 HTML 문자열 또는 None
    """
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning("Playwright가 사용 불가능합니다.")
        return None
    
    try:
        with sync_playwright() as p:
            # 브라우저 실행 (headless 모드)
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=REQUEST_HEADERS['User-Agent']
            )
            page = context.new_page()
            
            # 페이지 로드 (최대 15초 대기)
            page.goto(url, wait_until='networkidle', timeout=15000)
            
            # 추가 대기 (동적 콘텐츠 로딩)
            page.wait_for_timeout(2000)  # 2초 대기
            
            # HTML 가져오기
            html = page.content()
            
            browser.close()
            
            return html
            
    except PlaywrightTimeout:
        logger.warning(f"Playwright 타임아웃: {url}")
        return None
    except Exception as e:
        logger.error(f"Playwright HTML 다운로드 실패 ({url}): {e}")
        return None


def parse_article_content(url: str) -> Optional[str]:
    """
    웹 페이지에서 본문 텍스트 추출 (하이브리드 방식)
    
    파이프라인:
    1. requests로 HTML 다운로드 → trafilatura로 본문 추출 시도
    2. (1단계 실패 시) Playwright로 렌더링된 HTML 다운로드
    3. (2단계의 HTML 사용) trafilatura로 본문 추출 재시도
    4. (3단계 실패 시) 같은 HTML을 BeautifulSoup에 넣어 rule-based로 마지막 추출 시도
    5. (최종 실패) None 반환
    
    Args:
        url: 기사 URL
    
    Returns:
        본문 텍스트 (없으면 None)
    """
    # ============================================
    # [1단계] requests로 HTML 다운로드 → trafilatura로 본문 추출 시도
    # ============================================
    try:
        response = requests.get(url, headers=REQUEST_HEADERS, timeout=10)
        response.raise_for_status()
        html = response.text
        
        # trafilatura로 본문 추출 시도 (URL 직접 사용)
        result = _extract_with_trafilatura(html, url=url)
        if result and result.get('content'):
            logger.info(f"[1단계 성공] trafilatura: {url} ({len(result['content'])}자)")
            return result['content']
        
        # HTML 문자열로도 시도
        result = _extract_with_trafilatura(html)
        if result and result.get('content'):
            logger.info(f"[1단계 성공] trafilatura (HTML): {url} ({len(result['content'])}자)")
            return result['content']
            
    except requests.RequestException as e:
        logger.debug(f"[1단계 실패] HTTP 오류 ({url}): {e}")
    except Exception as e:
        logger.debug(f"[1단계 실패] trafilatura 추출 오류 ({url}): {e}")
    
    # ============================================
    # [2단계] Playwright로 렌더링된 HTML 다운로드
    # ============================================
    logger.info(f"[2단계 시작] Playwright로 HTML 다운로드: {url}")
    html = _fetch_with_playwright(url)
    
    if not html:
        logger.warning(f"[2단계 실패] Playwright HTML 다운로드 실패: {url}")
        return None
    
    # ============================================
    # [3단계] trafilatura로 본문 추출 재시도
    # ============================================
    result = _extract_with_trafilatura(html)
    if result and result.get('content'):
        logger.info(f"[3단계 성공] trafilatura (Playwright HTML): {url} ({len(result['content'])}자)")
        return result['content']
    
    # ============================================
    # [4단계] BeautifulSoup + rule-based로 마지막 추출 시도
    # ============================================
    logger.info(f"[4단계 시작] BeautifulSoup rule-based 추출: {url}")
    content = _extract_with_beautifulsoup(html)
    
    if content:
        logger.info(f"[4단계 성공] BeautifulSoup: {url} ({len(content)}자)")
        return content
    
    # ============================================
    # [5단계] 최종 실패
    # ============================================
    logger.warning(f"[최종 실패] 모든 파싱 방법 실패: {url}")
    return None


def parse_article_with_metadata(url: str) -> Optional[Dict]:
    """
    웹 페이지에서 본문 텍스트 및 메타데이터 추출 (하이브리드 방식)
    
    파이프라인:
    1. requests로 HTML 다운로드 → trafilatura로 본문 + 메타데이터 추출 시도
    2. (1단계 실패 시) Playwright로 렌더링된 HTML 다운로드
    3. (2단계의 HTML 사용) trafilatura로 본문 + 메타데이터 추출 재시도
    4. (3단계 실패 시) 같은 HTML을 BeautifulSoup에 넣어 rule-based로 본문만 추출
    5. (최종 실패) None 반환
    
    Args:
        url: 기사 URL
    
    Returns:
        {
            'content': 본문 텍스트,
            'title': 제목 (없으면 None),
            'date': 발행일 (없으면 None),
            'author': 작성자 (없으면 None),
            'source': 추출 방법 ('trafilatura' 또는 'beautifulsoup')
        } 또는 None
    """
    # ============================================
    # [1단계] requests로 HTML 다운로드 → trafilatura로 본문 + 메타데이터 추출 시도
    # ============================================
    try:
        response = requests.get(url, headers=REQUEST_HEADERS, timeout=10)
        response.raise_for_status()
        html = response.text
        
        # trafilatura로 본문 + 메타데이터 추출 시도 (URL 직접 사용)
        result = _extract_with_trafilatura(html, url=url)
        if result and result.get('content'):
            logger.info(f"[1단계 성공] trafilatura (메타데이터 포함): {url}")
            return result
        
        # HTML 문자열로도 시도
        result = _extract_with_trafilatura(html)
        if result and result.get('content'):
            logger.info(f"[1단계 성공] trafilatura (HTML, 메타데이터 포함): {url}")
            return result
            
    except requests.RequestException as e:
        logger.debug(f"[1단계 실패] HTTP 오류 ({url}): {e}")
    except Exception as e:
        logger.debug(f"[1단계 실패] trafilatura 추출 오류 ({url}): {e}")
    
    # ============================================
    # [2단계] Playwright로 렌더링된 HTML 다운로드
    # ============================================
    logger.info(f"[2단계 시작] Playwright로 HTML 다운로드: {url}")
    html = _fetch_with_playwright(url)
    
    if not html:
        logger.warning(f"[2단계 실패] Playwright HTML 다운로드 실패: {url}")
        return None
    
    # ============================================
    # [3단계] trafilatura로 본문 + 메타데이터 추출 재시도
    # ============================================
    result = _extract_with_trafilatura(html)
    if result and result.get('content'):
        logger.info(f"[3단계 성공] trafilatura (Playwright HTML, 메타데이터 포함): {url}")
        return result
    
    # ============================================
    # [4단계] BeautifulSoup + rule-based로 본문만 추출
    # ============================================
    logger.info(f"[4단계 시작] BeautifulSoup rule-based 추출: {url}")
    content = _extract_with_beautifulsoup(html)
    
    if content:
        logger.info(f"[4단계 성공] BeautifulSoup: {url} ({len(content)}자)")
        return {
            'content': content,
            'title': None,
            'date': None,
            'author': None,
            'source': 'beautifulsoup'
        }
    
    # ============================================
    # [5단계] 최종 실패
    # ============================================
    logger.warning(f"[최종 실패] 모든 파싱 방법 실패: {url}")
    return None
