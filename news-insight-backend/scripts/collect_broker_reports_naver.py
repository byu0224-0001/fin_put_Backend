"""
네이버 증권 리포트 수집 스크립트 (Phase 2.1 파일럿)

기존 economic_report_ai_v2의 NaverResearchScraperTool 로직을 활용하여
news-insight-backend 프로젝트에 맞게 재구성

목적: Edge Enrichment를 위한 리포트 수집
- 기존 엣지에 evidence_layer 추가
- Hidden Edge 후보 수집 (자동 생성 금지)
"""
import sys
import os
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
import time
import re
import json
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Windows 환경에서 인코딩 문제 방지
if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

import logging
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 설정
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

MOBILE_USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 10; SM-G973F) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Mobile Safari/537.36"
)

# Phase 2.1 파일럿 설정
PILOT_MODE = True  # 파일럿 모드
INITIAL_COLLECTION = True  # 초기 수집 모드 (11월~12월)
RECENT_DAYS = 7 if PILOT_MODE else 1  # 파일럿: 최근 7일, 프로덕션: 오늘만
TARGET_COMPANIES_LIMIT = 100  # 파일럿: 상위 100개 기업 대상

# 초기 수집 기간 설정 (2025년 11월 1일 ~ 12월 19일)
INITIAL_START_YEAR = 2025
INITIAL_START_MONTH = 11
INITIAL_START_DAY = 1
# 오늘 날짜를 동적으로 사용
_today = datetime.now()
INITIAL_END_YEAR = _today.year
INITIAL_END_MONTH = _today.month
INITIAL_END_DAY = _today.day


def create_selenium_driver(force_mobile=False):
    """Selenium 드라이버 생성"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    if force_mobile:
        chrome_options.add_argument(f"user-agent={MOBILE_USER_AGENT}")
    else:
        chrome_options.add_argument(f"user-agent={HEADERS['User-Agent']}")
    
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)


def get_target_dates(recent_days: int = 7, initial_collection: bool = False) -> List[str]:
    """
    수집 대상 날짜 리스트 생성
    
    Args:
        recent_days: 최근 N일 (일반 모드)
        initial_collection: 초기 수집 모드 (11월~12월 전체)
    
    Returns:
        날짜 리스트 (평일만, 주말 제외)
    """
    dates = []
    
    if initial_collection:
        # 초기 수집 모드: 11월 1일 ~ 12월 31일 (평일만)
        current_date = datetime(INITIAL_START_YEAR, INITIAL_START_MONTH, INITIAL_START_DAY)
        end_date = datetime(INITIAL_END_YEAR, INITIAL_END_MONTH, INITIAL_END_DAY)
        
        while current_date <= end_date:
            # 주말 제외 (월요일=0, 일요일=6)
            weekday = current_date.weekday()
            if weekday < 5:  # 월요일(0) ~ 금요일(4)만
                # 4자리 연도 형식
                dates.append(current_date.strftime("%Y.%m.%d"))
                # 2자리 연도 형식
                dates.append(current_date.strftime("%y.%m.%d"))
            current_date += timedelta(days=1)
        
        start_date_str = datetime(INITIAL_START_YEAR, INITIAL_START_MONTH, INITIAL_START_DAY).strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        logger.info(f"초기 수집 모드: {start_date_str} ~ {end_date_str} (평일만)")
    else:
        # 일반 모드: 최근 N일 (평일만)
        date_obj = datetime.now()
        collected_days = 0
        days_back = 0
        
        while collected_days < recent_days:
            check_date = date_obj - timedelta(days=days_back)
            weekday = check_date.weekday()
            
            # 평일만 수집 (월요일=0, 일요일=6)
            if weekday < 5:  # 월요일(0) ~ 금요일(4)
                # 4자리 연도 형식
                dates.append(check_date.strftime("%Y.%m.%d"))
                # 2자리 연도 형식
                dates.append(check_date.strftime("%y.%m.%d"))
                collected_days += 1
            
            days_back += 1
            
            # 무한 루프 방지 (최대 30일 전까지)
            if days_back > 30:
                break
    
    # 중복 제거
    dates = list(set(dates))
    dates.sort(reverse=True)  # 최신순 정렬
    
    logger.info(f"수집 대상 날짜: {len(dates)}개 (평일만, 주말 제외)")
    return dates


def get_target_dates_range(start_dt: datetime, end_dt: datetime) -> List[str]:
    """
    ⭐ P0: 절대 기간 모드 - 지정된 날짜 범위의 대상 날짜 리스트 생성 (GPT 피드백)
    
    Args:
        start_dt: 시작일 (datetime)
        end_dt: 종료일 (datetime)
    
    Returns:
        날짜 리스트 (평일만, 주말 제외)
    """
    dates = []
    current_date = start_dt
    
    while current_date <= end_dt:
        # 주말 제외 (월요일=0, 일요일=6)
        weekday = current_date.weekday()
        if weekday < 5:  # 월요일(0) ~ 금요일(4)만
            # 4자리 연도 형식
            dates.append(current_date.strftime("%Y.%m.%d"))
            # 2자리 연도 형식
            dates.append(current_date.strftime("%y.%m.%d"))
        current_date += timedelta(days=1)
    
    # 중복 제거
    dates = list(set(dates))
    dates.sort(reverse=True)  # 최신순 정렬
    
    logger.info(f"절대기간 수집: {start_dt.strftime('%Y-%m-%d')} ~ {end_dt.strftime('%Y-%m-%d')} ({len(dates)}개 형식, 평일만)")
    return dates


def scrape_naver_research(
    driver: webdriver.Chrome, 
    target_dates: List[str],
    target_category: Optional[str] = None,
    limit_per_category: Optional[int] = None,
    validate_urls: bool = False,  # ⭐ P0: URL 검증 게이트 (기본 OFF - 상세 페이지 404 회피)
    max_pages: int = 20  # ⭐ 페이지네이션: 최대 페이지 수 (2개월치 ≈ 20페이지)
) -> List[Dict[str, Any]]:
    """
    네이버 금융 리서치 리포트 수집
    
    Args:
        driver: Selenium WebDriver
        target_dates: 수집 대상 날짜 리스트
        target_category: 수집할 카테고리 (None이면 전체)
        limit_per_category: 카테고리별 최대 수집 개수 (None이면 제한 없음)
        validate_urls: URL 검증 여부 (기본 OFF - 상세 페이지 404 회피)
        max_pages: 페이지네이션 최대 페이지 수 (기본 20)
    
    Returns:
        리포트 리스트 (메타데이터만)
    """
    base_url = "https://finance.naver.com/research/"
    categories = {
        "투자정보": "invest_list.naver",
        "종목분석": "company_list.naver",
        "산업분석": "industry_list.naver",
        "경제분석": "economy_list.naver"
    }
    
    reports = []
    logger.info(f"네이버 리포트 수집 시작 - 대상 날짜: {target_dates[:2]}...")
    
    try:
        # ⭐ P0.7: 카테고리 필터 적용
        categories_to_scrape = categories
        if target_category:
            if target_category not in categories:
                logger.warning(f"지정한 카테고리 '{target_category}'가 없습니다. 전체 카테고리 수집합니다.")
            else:
                categories_to_scrape = {target_category: categories[target_category]}
                logger.info(f"카테고리 필터 적용: {target_category}만 수집")
        
        for cat, path in categories_to_scrape.items():
            base_category_url = base_url + path
            category_reports_count = 0
            consecutive_empty_pages = 0  # ⭐ P1: 연속 empty 페이지 카운터
            
            # ⭐ 페이지네이션: 1페이지부터 max_pages까지 순회
            for page_num in range(1, max_pages + 1):
                # 페이지 URL 생성 (?page=N 파라미터)
                url = f"{base_category_url}?page={page_num}"
                logger.info(f"{cat} 페이지 {page_num}/{max_pages} 접속: {url}")
                
                driver.get(url)
                time.sleep(2)  # 로딩 대기 (페이지당 2초)
                
                page_source = driver.page_source
                if "table" not in page_source.lower():
                    logger.warning(f"{cat} 페이지 {page_num}: 페이지 소스에 'table' 없음")
                    break  # 더 이상 페이지 없음
                
                soup = BeautifulSoup(page_source, "html.parser")
                
                # 다양한 선택자 시도
                rows = soup.select("table.type_1 tbody tr")
                if not rows:
                    rows = soup.select("table tbody tr")
                if not rows:
                    rows = soup.select("tbody tr")
                if not rows:
                    rows = soup.find_all("tr")
                
                if not rows or len(rows) == 0:
                    logger.info(f"{cat} 페이지 {page_num}: 더 이상 데이터 없음, 페이지네이션 종료")
                    break
                
                logger.info(f"{cat} 페이지 {page_num}: {len(rows)}개 row 발견")
                
                # 이 페이지에서 날짜 필터 통과한 리포트 수
                page_matched_count = 0
                page_dates = []  # ⭐ P0: 페이지 내 날짜 수집 (중단 조건 검증용)
                
                for i, row in enumerate(rows):
                    cols = row.find_all("td")
                    if len(cols) < 4:
                        continue
                    
                    # 종목분석 카테고리: 종목명 컬럼 추출 (첫 번째 컬럼)
                    stock_name = None
                    if cat == "종목분석":
                        # 종목분석 리스트: 종목명(0), 제목(1), 증권사(2), 첨부(3), 작성일(4), 조회수(5)
                        stock_name = cols[0].get_text(strip=True) if len(cols) > 0 else None
                        # 제목은 두 번째 컬럼
                        title_tag = cols[1].find("a") if len(cols) > 1 else None
                        # 증권사는 세 번째 컬럼
                        company = cols[2].get_text(strip=True) if len(cols) > 2 else "N/A"
                    else:
                        # 다른 카테고리: 제목 추출
                        title_tag = cols[0].find("a")
                        if not title_tag and len(cols) > 1:
                            title_tag = cols[1].find("a")
                        
                        # 증권사 추출
                        company = cols[2].get_text(strip=True) if len(cols) > 2 else "N/A"
                        if not company or company == "":
                            company = cols[1].get_text(strip=True) if len(cols) > 1 else "N/A"
                    
                    # 날짜 추출
                    date = ""
                    if cat == "종목분석":
                        # 종목분석: 작성일은 4번째 컬럼 (인덱스 4)
                        if len(cols) >= 5:
                            date = cols[4].get_text(strip=True)
                    else:
                        # 다른 카테고리
                        if len(cols) >= 6:
                            date = cols[4].get_text(strip=True)
                        elif len(cols) >= 5:
                            date = cols[3].get_text(strip=True)
                        else:
                            date = cols[-2].get_text(strip=True)
                    
                    # 날짜 필터
                    date_clean = date.replace(" ", "").replace(".", ".").strip()
                    
                    # ⭐ P0: 페이지 내 날짜 수집 (중단 조건 검증용)
                    if date_clean:
                        page_dates.append(date_clean)
                    
                    if date_clean not in target_dates:
                        continue
                    
                    # 날짜 필터 통과 카운트
                    page_matched_count += 1
                    
                    if not title_tag:
                        continue
                    
                    # href 추출
                    href = title_tag.get("href", "")
                    if not href or href == "#":
                        continue
                    
                    # 금지된 패턴 필터
                    excluded_patterns = ["/chart/", "/quote/", "/news/"]
                    if cat != "종목분석":
                        excluded_patterns.append("/item/")
                        excluded_patterns.append("/frgn/")
                    
                    if any(pattern in href for pattern in excluded_patterns):
                        continue
                    
                    # URL 정규화
                    detail_url = urljoin("https://finance.naver.com", href)
                    
                    # ⭐ P0-2 (v10): 목록 페이지 텍스트 저장 (GPT 피드백)
                    # 상세 페이지가 404여도 최소 입력 확보
                    list_row_text = ""
                    list_row_title = ""
                    list_row_broker = ""
                    list_row_date = ""
                    try:
                        # 행 전체 텍스트 추출
                        list_row_text = row.get_text(separator=' ', strip=True)
                        # ⭐ P1 (v11): list_row 구조화 (GPT 피드백)
                        list_row_title = title_tag.get_text(strip=True) if title_tag else ""
                        list_row_broker = company
                        list_row_date = date_clean
                    except:
                        pass
                    
                    # PDF URL 추출 (목록에서 직접 - run_daily_briefing.py 방식)
                    pdf_url = None
                    for col_idx, col in enumerate(cols):
                        # 1. <a> 태그에서 href 찾기
                        pdf_link = col.find("a", href=re.compile(r"\.pdf|download|filekey|attach|report|view", re.IGNORECASE))
                        if pdf_link:
                            pdf_href = pdf_link.get("href", "")
                            if pdf_href:
                                pdf_url = urljoin("https://finance.naver.com", pdf_href)
                                break
                        
                        # 2. 이미지 alt/title에서 PDF 확인
                        img = col.find("img")
                        if img and ("pdf" in (img.get("alt", "") + img.get("title", "")).lower()):
                            parent_a = col.find("a")
                            if parent_a:
                                pdf_href = parent_a.get("href", "")
                                if pdf_href:
                                    pdf_url = urljoin("https://finance.naver.com", pdf_href)
                                    break
                        
                        # 3. svg 아이콘 확인
                        svg = col.find("svg")
                        if svg:
                            parent_a = col.find("a")
                            if parent_a:
                                pdf_href = parent_a.get("href", "")
                                if pdf_href and (".pdf" in pdf_href.lower() or "download" in pdf_href.lower() or "filekey" in pdf_href.lower()):
                                    pdf_url = urljoin("https://finance.naver.com", pdf_href)
                                    break
                    
                    # ⭐ P0-2 (v11): row_has_pdf_candidate - "pdf_url=None 이유" 추적 (GPT 피드백)
                    row_has_pdf_candidate = pdf_url is not None
                    
                    # ⭐ P0-A (v12): row_links_count - "목록에 링크가 정말 없었는지" 증거 (GPT 피드백)
                    row_links = row.find_all("a", href=True)
                    row_links_count = len(row_links)
                    
                    # ⭐ P0-1 (v9): 상세 페이지 접근 + PDF URL + HTML 텍스트 **동시 추출**
                    # 【핵심 변경】HTML은 "나중에 가져오는 데이터"가 아니라 "수집 시점에 박제해야 하는 데이터"
                    # 
                    # 【목적】
                    #   - PDF URL 탐색과 HTML 텍스트 추출을 **한 번의 요청**으로 처리
                    #   - 상세 페이지가 휘발성이어도 텍스트는 이미 저장됨
                    # 【주의】이 규칙을 풀면 파싱 실패율이 급증할 수 있음!
                    
                    # P0-2: 상세 페이지 상태 추적 필드
                    detail_fetch_status = None      # 200/404/403/timeout/error
                    detail_fetch_error = None       # 에러 상세
                    detail_html_text = ""           # 추출된 HTML 텍스트 (스냅샷)
                    detail_html_text_len = 0        # HTML 텍스트 길이
                    detail_fetch_at = None          # ⭐ P0-1 (v11): 상세 접근 시각
                    detail_content_type = None      # ⭐ P0-B (v12): 위장 404 구분용
                    detail_body_hash_8 = None       # ⭐ P0-1 (v12 Final): 404 시그니처 (앞 512B md5[:8])
                    
                    if not pdf_url and detail_url:
                        try:
                            fallback_headers = {**HEADERS, "Referer": detail_url}
                            detail_fetch_at = datetime.now().isoformat()  # ⭐ P0-1 (v11): 상세 접근 시각
                            d_res = requests.get(detail_url, headers=fallback_headers, timeout=10)
                            detail_fetch_status = d_res.status_code
                            detail_content_type = d_res.headers.get('content-type', '')[:50]  # ⭐ P0-B (v12)
                            # ⭐ P0-1 (v12 Final): 404 시그니처 해시 (위장 vs 진짜 구분)
                            detail_body_hash_8 = hashlib.md5(d_res.content[:512]).hexdigest()[:8]
                            
                            if d_res.status_code == 200:
                                d_soup = BeautifulSoup(d_res.text, "html.parser")
                                
                                # ⭐ P0-1: HTML 텍스트 **즉시 추출** (휘발성 대응)
                                content_area = d_soup.find("div", class_="contentArea")
                                if not content_area:
                                    content_area = d_soup.find("div", id="content")
                                if not content_area:
                                    content_area = d_soup.find("article")
                                if not content_area:
                                    content_area = d_soup.body
                                
                                if content_area:
                                    # 스크립트/스타일 제거 후 텍스트 추출
                                    for tag in content_area.find_all(['script', 'style', 'nav', 'footer', 'header']):
                                        tag.decompose()
                                    detail_html_text = content_area.get_text(separator=' ', strip=True)
                                    detail_html_text_len = len(detail_html_text)
                                
                                # ⭐ P0: PDF 확정 규칙 (GPT 피드백)
                                # 1순위: .pdf로 끝나는 URL
                                # 2순위: pstatic.net/ssl.pstatic.net/stock.pstatic.net 도메인
                                # 3순위: 기타 download/filekey 패턴 (후보만)
                                pdf_candidates = []
                                
                                for a_tag in d_soup.find_all("a", href=True):
                                    href = a_tag.get("href", "")
                                    if not href:
                                        continue
                                    
                                    # 절대 URL로 변환
                                    if not href.startswith("http"):
                                        candidate_url = urljoin("https://finance.naver.com", href)
                                    else:
                                        candidate_url = href
                                    
                                    # 1순위: .pdf 확장자
                                    if href.lower().endswith('.pdf'):
                                        pdf_candidates.insert(0, ('pdf_ext', candidate_url))
                                        continue
                                    
                                    # 2순위: 네이버 리서치 PDF 서버
                                    if any(domain in candidate_url for domain in ['pstatic.net', 'ssl.pstatic.net', 'stock.pstatic.net']):
                                        if 'research' in candidate_url.lower():
                                            pdf_candidates.insert(0, ('pstatic', candidate_url))
                                            continue
                                    
                                    # 3순위: 기타 패턴 (낮은 우선순위)
                                    if re.search(r'download|filekey|attach', href, re.IGNORECASE):
                                        pdf_candidates.append(('pattern', candidate_url))
                                
                                # 가장 높은 우선순위 후보 선택
                                if pdf_candidates:
                                    pdf_url = pdf_candidates[0][1]
                            elif d_res.status_code == 404:
                                detail_fetch_error = "DETAIL_404"
                            elif d_res.status_code in [403, 429]:
                                detail_fetch_error = "DETAIL_BLOCKED"
                            else:
                                detail_fetch_error = f"DETAIL_HTTP_{d_res.status_code}"
                        except requests.exceptions.Timeout:
                            detail_fetch_status = 0
                            detail_fetch_error = "DETAIL_TIMEOUT"
                        except requests.exceptions.ConnectionError:
                            detail_fetch_status = 0
                            detail_fetch_error = "DETAIL_CONN_ERROR"
                        except Exception as e:
                            detail_fetch_status = 0
                            detail_fetch_error = f"DETAIL_ERROR:{str(e)[:30]}"
                    
                    # PDF가 없는 경우 HTML URL 사용
                    valid_url = detail_url
                    if not pdf_url:
                        if detail_url and ("read.naver" in detail_url or "/research/" in detail_url):
                            valid_url = detail_url
                        elif valid_url and "/item/" in valid_url:
                            valid_url = None
                    else:
                        if valid_url and "/item/" in valid_url:
                            valid_url = None
                    
                    if not valid_url:
                        continue
                    
                    # ⭐ P0: URL 검증 게이트 (기본 OFF - 상세 페이지 404 회피)
                    url_status = "VALID"
                    url_validation_error = None
                    if validate_urls:  # 기본 False
                        try:
                            # 먼저 HEAD로 빠르게 확인
                            head_response = requests.head(valid_url, headers=HEADERS, timeout=5, allow_redirects=True)
                            if head_response.status_code == 200:
                                url_status = "VALID"
                            elif head_response.status_code in [301, 302, 303, 307, 308]:
                                # 리다이렉트는 GET으로 재시도
                                with requests.get(valid_url, headers=HEADERS, timeout=10, allow_redirects=True, stream=True) as get_response:
                                    if get_response.status_code == 200:
                                        url_status = "VALID"
                                    else:
                                        url_status = "INVALID_URL"
                                        url_validation_error = f"GET {get_response.status_code} (HEAD→{head_response.status_code})"
                            else:
                                # HEAD가 404/403이면 GET으로 재시도
                                with requests.get(valid_url, headers=HEADERS, timeout=10, allow_redirects=True, stream=True) as get_response:
                                    content_type = get_response.headers.get('content-type', '').lower()
                                    
                                    if get_response.status_code == 200:
                                        url_status = "VALID"
                                    else:
                                        url_status = "INVALID_URL"
                                        url_validation_error = f"HEAD {head_response.status_code} → GET {get_response.status_code}"
                        except requests.exceptions.Timeout:
                            url_status = "INVALID_URL"
                            url_validation_error = "TIMEOUT"
                        except requests.exceptions.ConnectionError:
                            url_status = "INVALID_URL"
                            url_validation_error = "CONNECTION_ERROR"
                        except Exception as e:
                            url_status = "INVALID_URL"
                            url_validation_error = f"EXCEPTION: {str(e)[:50]}"
                    
                    # ⭐ P0: acquisition_status 세분화 (GPT 피드백 v9)
                    # 
                    # 【상호배타적 판정 흐름도 (v9: HTML 수집 시점 즉시 저장)】
                    # ┌─ pdf_url 있음? ─────────────────────────────────────────────────┐
                    # │   YES → pdf_url로 probe 시도                                    │
                    # │         ├─ HTTP 200/206 + %PDF? → ACQUIRED_PDF                  │
                    # │         ├─ HTTP 200/206 - %PDF  → 이미 저장된 HTML 사용          │
                    # │         ├─ HTTP 403/429 → PDF_BLOCKED → RETRY                   │
                    # │         ├─ HTTP 404 → 이미 저장된 HTML 사용                      │
                    # │         ├─ Timeout → PDF_TIMEOUT → RETRY                        │
                    # │         └─ Error → PDF_ERROR → DROP                             │
                    # │   NO  → 이미 저장된 HTML 사용                                    │
                    # │         ├─ detail_fetch_status == 200 + 길이 OK → ACQUIRED_HTML  │
                    # │         ├─ detail_fetch_status == 404 → DETAIL_404              │
                    # │         ├─ detail_fetch_status == 403 → DETAIL_BLOCKED          │
                    # │         └─ 기타 → HTML_EMPTY                                    │
                    # └─────────────────────────────────────────────────────────────────┘
                    # 
                    # 【status 의미 (v9)】
                    # - ACQUIRED_PDF: PDF 획득 성공 (PDF 파서로 전달)
                    # - ACQUIRED_HTML: HTML 텍스트 획득 성공 (HTML 파서로 전달)
                    # - HTML_EMPTY: HTML 텍스트 부족 (파싱 제외)
                    # - DETAIL_404: 상세 페이지 404 (URL 만료)
                    # - DETAIL_BLOCKED: 상세 페이지 차단 (403/429)
                    # - PDF_BLOCKED/PDF_TIMEOUT: PDF 차단/타임아웃 (RETRY)
                    
                    # ⭐ P0-3: 브로커별 HTML 최소 길이 기준 (GPT 피드백)
                    BROKER_HTML_MIN_LENGTH = {
                        "신한금융투자": 200,
                        "신한투자증권": 200,
                        "한국투자증권": 200,
                    }
                    DEFAULT_HTML_MIN_LENGTH = 400
                    
                    # 브로커별 기준 적용
                    html_min_length = BROKER_HTML_MIN_LENGTH.get(company, DEFAULT_HTML_MIN_LENGTH)
                    
                    acquisition_status = "HTML_EMPTY"  # 기본값: HTML 텍스트 확보 실패
                    html_text_length = detail_html_text_len  # 이미 추출한 HTML 텍스트 길이 사용
                    acquisition_final_url = None
                    acquisition_content_type = None
                    acquisition_redirect_count = 0
                    acquisition_has_pdf_sig = None
                    
                    # ⭐ P0-1 (v9): HTML 텍스트 판정은 이미 추출한 데이터 사용 (다시 요청 안 함!)
                    def check_html_acquired():
                        """이미 추출한 HTML 텍스트로 획득 여부 판정"""
                        if detail_fetch_status == 200 and detail_html_text_len >= html_min_length:
                            return "ACQUIRED_HTML"
                        elif detail_fetch_status == 404:
                            return "DETAIL_404"
                        elif detail_fetch_status in [403, 429]:
                            return "DETAIL_BLOCKED"
                        elif detail_fetch_status == 0:  # timeout/error
                            return "DETAIL_ERROR"
                        elif detail_fetch_status == 200 and detail_html_text_len < html_min_length:
                            return "HTML_EMPTY"  # 텍스트 부족
                        else:
                            return "HTML_EMPTY"
                    
                    if pdf_url:
                        try:
                            range_headers = {**HEADERS, "Range": "bytes=0-1024"}
                            with requests.get(pdf_url, headers=range_headers, timeout=10, 
                                             allow_redirects=True, stream=True) as resp:
                                acquisition_final_url = resp.url
                                acquisition_content_type = resp.headers.get('content-type', '').lower()
                                acquisition_redirect_count = len(resp.history)
                                
                                # %PDF 시그니처 체크
                                body_start = resp.content[:16] if resp.content else b''
                                acquisition_has_pdf_sig = body_start.startswith(b'%PDF')
                                
                                if resp.status_code in [200, 206]:
                                    if acquisition_has_pdf_sig:
                                        acquisition_status = "ACQUIRED_PDF"
                                    else:
                                        # ⭐ P0-1 (v9): 이미 저장된 HTML 텍스트 사용
                                        acquisition_status = check_html_acquired()
                                elif resp.status_code in [403, 429]:
                                    acquisition_status = "PDF_BLOCKED"  # 차단/제한 → RETRY
                                elif resp.status_code == 404:
                                    # ⭐ P0-1 (v9): 이미 저장된 HTML 텍스트 사용
                                    acquisition_status = check_html_acquired()
                                else:
                                    acquisition_status = f"PDF_HTTP_{resp.status_code}"
                        except requests.exceptions.Timeout:
                            acquisition_status = "PDF_TIMEOUT"
                        except requests.exceptions.ConnectionError:
                            acquisition_status = "PDF_CONNECTION_ERROR"
                        except Exception as e:
                            acquisition_status = f"PDF_ERROR:{str(e)[:20]}"
                    else:
                        # ⭐ P0-1 (v9): PDF URL 없음 → 이미 저장된 HTML 텍스트 사용
                        acquisition_status = check_html_acquired()
                    
                    # ⭐ P0-4: content_source + retry_policy 결정 (GPT 피드백 v9)
                    # 
                    # 【acquisition_status → content_source 매핑 규칙 (v9)】
                    # ┌─────────────────────────────────────────────────────────────┐
                    # │ acquisition_status   │ content_source │ retry_policy       │
                    # ├─────────────────────────────────────────────────────────────┤
                    # │ ACQUIRED_PDF         │ PDF            │ none (완료)        │
                    # │ ACQUIRED_HTML        │ DETAIL_HTML    │ none (HTML 정식)   │
                    # │ HTML_EMPTY           │ DROP           │ none (텍스트 부족) │
                    # │ DETAIL_404           │ DROP           │ none (URL 만료)    │
                    # │ DETAIL_BLOCKED       │ RETRY          │ retryable          │
                    # │ DETAIL_ERROR         │ DROP           │ non_retryable      │
                    # │ PDF_TIMEOUT          │ RETRY          │ retryable          │
                    # │ PDF_CONN_ERROR       │ RETRY          │ retryable          │
                    # │ PDF_BLOCKED          │ RETRY          │ retryable          │
                    # │ PDF_ERROR/*          │ DROP           │ non_retryable      │
                    # └─────────────────────────────────────────────────────────────┘
                    # 
                    # 【핵심 원칙 (v9)】
                    # - HTML은 "수집 시점에 박제"되어 있음 (다시 요청 안 함)
                    # - DETAIL_404는 "URL 만료"로 명확히 분리
                    # - retry_policy 스펙: retryable은 2회 재시도, 이후 HTML fallback
                    
                    RETRYABLE_STATUSES = {"PDF_TIMEOUT", "PDF_CONNECTION_ERROR", "PDF_BLOCKED", "DETAIL_BLOCKED"}
                    
                    if acquisition_status == "ACQUIRED_PDF":
                        content_source = "PDF"
                        retry_policy = "none"
                    elif acquisition_status == "ACQUIRED_HTML":
                        content_source = "DETAIL_HTML"
                        retry_policy = "none"
                    elif acquisition_status == "HTML_EMPTY":
                        content_source = "DROP"  # 텍스트 부족 → 파싱 제외
                        retry_policy = "none"
                    elif acquisition_status == "DETAIL_404":
                        content_source = "DROP"  # URL 만료 → 파싱 제외
                        retry_policy = "none"
                    elif acquisition_status in RETRYABLE_STATUSES:
                        content_source = "RETRY"  # 재시도 후 PDF 또는 HTML fallback
                        retry_policy = "retryable"  # 2회 재시도 후 HTML fallback
                    else:
                        # PDF_ERROR, PDF_HTTP_xxx, DETAIL_ERROR 등 예외 케이스
                        content_source = "DROP"  # 코드 버그 가능성 → 파싱 제외
                        retry_policy = "non_retryable"
                    
                    # 리포트 데이터 생성
                    report_data = {
                        "schema_version": "naver_reports_v10",  # ⭐ v10 (Final): row_links_count + detail_content_type
                        "source": "네이버",
                        "category": cat,
                        "title": title_tag.get_text(strip=True) if title_tag else "",
                        "broker_name": company,
                        "date": date_clean,
                        "url": valid_url,
                        "pdf_url": pdf_url,
                        "collected_at": datetime.now().isoformat(),
                        "url_status": url_status,
                        "url_validation_error": url_validation_error,
                        # ⭐ P0: acquisition 추적 필드
                        "acquisition_status": acquisition_status,
                        "acquisition_final_url": acquisition_final_url,
                        "acquisition_content_type": acquisition_content_type,
                        "acquisition_redirect_count": acquisition_redirect_count,
                        "acquisition_has_pdf_sig": acquisition_has_pdf_sig,
                        # ⭐ P0-4: content_source + retry_policy (GPT 피드백 v8)
                        "content_source": content_source,  # PDF | DETAIL_HTML | RETRY | DROP
                        "retry_policy": retry_policy,      # none | retryable | non_retryable
                        # ⭐ P0-1 (v9): HTML 스냅샷 필드
                        "html_text_length": html_text_length,       # HTML 텍스트 길이
                        "detail_fetch_status": detail_fetch_status, # 상세 페이지 HTTP 상태
                        "detail_fetch_error": detail_fetch_error,   # 상세 페이지 에러 타입
                        "detail_html_text": detail_html_text[:2000] if detail_html_text else None,  # HTML 텍스트 스냅샷 (최대 2000자)
                        # ⭐ P0-2 (v10): 목록 페이지 최소 입력 (GPT 피드백)
                        "detail_url": detail_url,                   # 상세 페이지 URL (URL 검증용)
                        "list_row_text": list_row_text[:500] if list_row_text else None,  # 목록 행 텍스트 (최대 500자)
                        # ⭐ P0-1/P0-2/P1 (v11): 수집 시점 증거 + row 구조화 (GPT 피드백)
                        "detail_fetch_at": detail_fetch_at,         # 상세 접근 시각
                        "row_has_pdf_candidate": row_has_pdf_candidate,  # 목록에서 PDF 후보 있었는지
                        "list_row_title": list_row_title,           # 구조화된 제목
                        "list_row_broker": list_row_broker,         # 구조화된 브로커
                        "list_row_date": list_row_date,             # 구조화된 날짜
                        # ⭐ P0-A/P0-B (v12 Final): 증거 강화 (GPT 피드백)
                        "row_links_count": row_links_count,         # row 내 링크 개수
                        "detail_content_type": detail_content_type, # 404 위장 구분용
                        "detail_body_hash_8": detail_body_hash_8,   # 404 시그니처 해시
                    }
                    
                    # INVALID_URL인 경우 수집 대상에서 제외
                    if url_status == "INVALID_URL":
                        logger.warning(f"URL 검증 실패로 제외: {title_tag.get_text(strip=True)[:50]}... ({url_validation_error})")
                        continue
                    
                    # 종목분석 리포트: 종목명 추가
                    if cat == "종목분석" and stock_name:
                        report_data["stock_name"] = stock_name
                    
                    reports.append(report_data)
                    category_reports_count += 1
                    
                    # ⭐ P0.7: 카테고리별 limit 적용
                    if limit_per_category and category_reports_count >= limit_per_category:
                        logger.info(f"{cat}: {limit_per_category}개 수집 완료 (limit 도달)")
                        break
                
                # ⭐ P0: 페이지 날짜 범위 로그 (중단 조건 검증용)
                if page_dates:
                    page_dates_sorted = sorted(set(page_dates))
                    logger.debug(f"{cat} 페이지 {page_num}: 날짜 범위 {page_dates_sorted[0]} ~ {page_dates_sorted[-1]} (고유 {len(set(page_dates))}개)")
                
                # ⭐ P1: 연속 empty 페이지 체크 (안전장치)
                if page_matched_count == 0:
                    consecutive_empty_pages += 1
                    page_date_info = f", 페이지 날짜 범위: {min(page_dates) if page_dates else 'N/A'} ~ {max(page_dates) if page_dates else 'N/A'}" if page_dates else ""
                    
                    if consecutive_empty_pages >= 2:
                        logger.info(f"{cat} 페이지 {page_num}: 연속 {consecutive_empty_pages}페이지 대상 날짜 없음{page_date_info}, 페이지네이션 종료")
                        break
                    else:
                        logger.info(f"{cat} 페이지 {page_num}: 대상 날짜 리포트 없음{page_date_info}, 다음 페이지 시도 (연속 {consecutive_empty_pages})")
                else:
                    consecutive_empty_pages = 0  # 리셋
                
                # limit 도달 시 페이지네이션 종료
                if limit_per_category and category_reports_count >= limit_per_category:
                    break
            
            logger.info(f"{cat}: 총 {category_reports_count}개 수집 완료")
            time.sleep(1)  # 카테고리 간 요청 간격
    
    except Exception as e:
        logger.error(f"네이버 리포트 수집 중 오류: {e}", exc_info=True)
    
    logger.info(f"네이버: 총 {len(reports)}개 리포트 수집 완료")
    return reports


def match_ticker_from_title(title: str, broker_name: str) -> Optional[str]:
    """
    리포트 제목에서 티커 매칭 시도
    
    Args:
        title: 리포트 제목
        broker_name: 증권사명
    
    Returns:
        티커 (매칭 실패 시 None)
    """
    # TODO: 향후 구현
    # 1. 제목에서 회사명 추출 (LLM 또는 키워드 매칭)
    # 2. 회사명으로 stocks 테이블에서 ticker 조회
    # 3. 매칭 실패 시 None 반환
    
    # 임시: 제목에 종목코드가 포함된 경우 추출
    ticker_match = re.search(r'\((\d{6})\)', title)
    if ticker_match:
        return ticker_match.group(1)
    
    return None


def main():
    """메인 실행 함수"""
    import argparse
    
    # ⭐ P0: 카테고리 영문 enum (Windows PowerShell 한글 인코딩 문제 해결)
    CATEGORY_MAP = {
        "COMPANY": "종목분석",
        "INDUSTRY": "산업분석",
        "ECONOMY": "경제분석",
        "INVEST": "투자정보",
        # 한글도 호환 지원
        "종목분석": "종목분석",
        "산업분석": "산업분석",
        "경제분석": "경제분석",
        "투자정보": "투자정보"
    }
    
    parser = argparse.ArgumentParser(description="네이버 증권 리포트 수집")
    parser.add_argument("--category", type=str, 
                        help="수집할 카테고리 (COMPANY|INDUSTRY|ECONOMY|INVEST) - 영문 권장, 한글도 지원")
    parser.add_argument("--limit", type=int, help="최대 수집 개수 (카테고리별)")
    parser.add_argument("--max-pages", type=int, default=20, help="페이지네이션 최대 페이지 수 (기본: 20)")
    parser.add_argument("--days-back", type=int, default=7, 
                        help="최근 N일 수집 모드 (기본: 7일, 대량 수집은 --initial 사용)")
    parser.add_argument("--validate-urls", action="store_true", default=False, 
                        help="URL 검증 게이트 활성화 (기본: OFF)")
    parser.add_argument("--no-validate-urls", dest="validate_urls", action="store_false", 
                        help="URL 검증 게이트 비활성화 (기본)")
    parser.add_argument("--probe-pdfs", type=int, default=0,
                        help="PDF URL 획득 가능성 샘플링 (0=비활성, N=N개 샘플)")
    parser.add_argument("--initial", action="store_true", default=False,
                        help="초기 대량 수집 모드 (11월~현재, --days-back 무시)")
    # ⭐ P0: 절대 기간 모드 (GPT 피드백)
    parser.add_argument("--start-date", type=str, default=None,
                        help="수집 시작일 (YYYY-MM-DD, 예: 2025-12-01)")
    parser.add_argument("--end-date", type=str, default=None,
                        help="수집 종료일 (YYYY-MM-DD, 예: 2025-12-23)")
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info("네이버 증권 리포트 수집 시작 (Phase 2.1 파일럿)")
    logger.info("=" * 80)
    
    # ⭐ P0.7: 카테고리 필터 (영문 → 한글 변환)
    target_category = None
    if args.category:
        if args.category.upper() in CATEGORY_MAP:
            target_category = CATEGORY_MAP[args.category.upper()]
        elif args.category in CATEGORY_MAP:
            target_category = CATEGORY_MAP[args.category]
        else:
            logger.warning(f"알 수 없는 카테고리: {args.category}. 사용 가능: COMPANY, INDUSTRY, ECONOMY, INVEST")
    
    limit_per_category = args.limit if args.limit else None
    
    if target_category:
        logger.info(f"카테고리 필터 모드: {target_category}만 수집")
    if limit_per_category:
        logger.info(f"카테고리별 최대 수집 개수: {limit_per_category}개")
    logger.info(f"페이지네이션: 최대 {args.max_pages}페이지")
    logger.info(f"URL 검증 게이트: {'ON' if args.validate_urls else 'OFF'}")
    if args.probe_pdfs > 0:
        logger.info(f"PDF 획득 샘플링: {args.probe_pdfs}개")
    
    # ⭐ P0: 수집 모드 결정 - 우선순위/상호배제 규칙 (GPT 피드백)
    # 우선순위: initial > start-date/end-date > days-back
    # 규칙:
    #   - --initial이 켜지면 다른 날짜 옵션 무시 (경고 출력)
    #   - --start-date만 있으면 end=today
    #   - --end-date만 있으면 에러
    #   - --start-date/--end-date가 있으면 --days-back 무시
    
    custom_date_range = None
    collection_mode = None  # 'initial' | 'absolute' | 'relative'
    
    if args.initial:
        # 최우선: --initial 모드
        collection_mode = 'initial'
        if args.start_date or args.end_date:
            logger.warning("--initial 플래그가 설정되어 --start-date/--end-date가 무시됩니다.")
        if args.days_back != 7:  # 기본값이 아닌 경우만 경고
            logger.warning("--initial 플래그가 설정되어 --days-back이 무시됩니다.")
        logger.info("초기 수집 모드: 2025년 11월 ~ 현재 (--initial 플래그)")
    
    elif args.start_date or args.end_date:
        # 절대 기간 모드
        collection_mode = 'absolute'
        
        # --end-date만 있으면 에러
        if args.end_date and not args.start_date:
            logger.error("--end-date만 지정할 수 없습니다. --start-date도 함께 지정하세요.")
            return
        
        # --start-date만 있으면 end=today
        if args.start_date and not args.end_date:
            args.end_date = datetime.now().strftime("%Y-%m-%d")
            logger.info(f"--end-date 미지정, 오늘({args.end_date})로 설정")
        
        try:
            start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")
            
            if start_dt > end_dt:
                logger.error(f"시작일({args.start_date})이 종료일({args.end_date})보다 늦습니다.")
                return
            
            custom_date_range = (start_dt, end_dt)
            logger.info(f"절대기간 모드: {args.start_date} ~ {args.end_date}")
            
            if args.days_back != 7:  # 기본값이 아닌 경우만 경고
                logger.warning("절대기간이 설정되어 --days-back이 무시됩니다.")
                
        except ValueError as e:
            logger.error(f"날짜 형식 오류: {e}. YYYY-MM-DD 형식을 사용하세요.")
            return
    else:
        # 상대 기간 모드 (기본)
        collection_mode = 'relative'
        logger.info(f"상대기간 모드: 최근 {args.days_back}일 (평일만)")
    
    # 대상 날짜 생성 (평일만, 주말 제외)
    if collection_mode == 'absolute':
        target_dates = get_target_dates_range(custom_date_range[0], custom_date_range[1])
    elif collection_mode == 'initial':
        target_dates = get_target_dates(RECENT_DAYS, initial_collection=True)
    else:  # 'relative'
        target_dates = get_target_dates(args.days_back, initial_collection=False)
    
    if target_dates:
        logger.info(f"수집 대상 날짜 범위: {target_dates[-1]} ~ {target_dates[0]} (총 {len(target_dates)}개 형식)")
    else:
        logger.warning("수집 대상 날짜가 없습니다.")
        return
    
    # Selenium 드라이버 생성
    driver = create_selenium_driver()
    
    try:
        # 리포트 수집
        reports = scrape_naver_research(
            driver, 
            target_dates,
            target_category=target_category,
            limit_per_category=limit_per_category,
            validate_urls=args.validate_urls,  # ⭐ P0: URL 검증 게이트 (기본 OFF)
            max_pages=args.max_pages  # ⭐ 페이지네이션 (기본 20페이지)
        )
        
        if not reports:
            logger.warning("수집된 리포트가 없습니다.")
            return
        
        logger.info(f"\n총 {len(reports)}개 리포트 수집 완료")
        
        # 결과 출력
        logger.info("\n수집된 리포트 샘플 (최대 10개):")
        for i, report in enumerate(reports[:10], 1):
            logger.info(f"{i}. [{report['date']}] {report['title'][:50]}... ({report['broker_name']})")
            if report['pdf_url']:
                logger.info(f"   PDF: {report['pdf_url'][:80]}...")
            else:
                logger.info(f"   URL: {report['url'][:80]}...")
        
        # JSON 저장 (임시)
        output_file = project_root / "reports" / f"naver_reports_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        output_file.parent.mkdir(exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(reports, f, ensure_ascii=False, indent=2)
        
        logger.info(f"\n결과 저장: {output_file}")
        
        # ⭐ P1: 수집 결과 summary 분포 출력 (GPT 피드백)
        from collections import Counter
        
        logger.info("\n=== 수집 결과 분포 ===")
        
        # ⭐ P1: 수집 메타 정보 (GPT 피드백)
        logger.info("  [수집 메타]")
        logger.info(f"    수집 모드: {collection_mode}")
        logger.info(f"    collected/limit: {len(reports)}/{args.limit if args.limit else 'unlimited'}")
        logger.info(f"    max_pages: {args.max_pages}")
        
        # acquisition_status 분포
        acq_status_counts = Counter(r.get('acquisition_status', 'UNKNOWN') for r in reports)
        logger.info("  [Acquisition Status]")
        for status, count in sorted(acq_status_counts.items(), key=lambda x: -x[1]):
            pct = count / len(reports) * 100
            logger.info(f"    {status}: {count}개 ({pct:.1f}%)")
        
        # ⭐ P0-4: content_source 분포 (GPT 피드백 v6)
        content_source_counts = Counter(r.get('content_source', 'UNKNOWN') for r in reports)
        logger.info("  [Content Source (다운스트림 전달 타입)]")
        for source, count in sorted(content_source_counts.items(), key=lambda x: -x[1]):
            pct = count / len(reports) * 100
            logger.info(f"    {source}: {count}개 ({pct:.1f}%)")
        
        # ⭐ P0-4: retry_policy 분포 (GPT 피드백 v6)
        retry_policy_counts = Counter(r.get('retry_policy', 'UNKNOWN') for r in reports)
        logger.info("  [Retry Policy]")
        for policy, count in sorted(retry_policy_counts.items(), key=lambda x: -x[1]):
            pct = count / len(reports) * 100
            logger.info(f"    {policy}: {count}개 ({pct:.1f}%)")
        
        # PDF vs HTML-only
        pdf_count = sum(1 for r in reports if r.get('pdf_url'))
        html_only_count = len(reports) - pdf_count
        logger.info(f"  [PDF 추출]")
        logger.info(f"    PDF 있음: {pdf_count}개 ({pdf_count/len(reports)*100:.1f}%)")
        logger.info(f"    HTML-only: {html_only_count}개 ({html_only_count/len(reports)*100:.1f}%)")
        
        # 실패한 경우 broker_name별 분포 (ACQUIRED_PDF가 아닌 경우)
        failed_reports = [r for r in reports if r.get('acquisition_status') != 'ACQUIRED_PDF']
        if failed_reports:
            broker_fail_counts = Counter(r.get('broker_name', 'UNKNOWN') for r in failed_reports)
            if broker_fail_counts:
                logger.info(f"  [비정상 Acquisition - 증권사별 Top 5]")
                for broker, count in broker_fail_counts.most_common(5):
                    logger.info(f"    {broker}: {count}개")
        
        # 카테고리별 분포
        cat_counts = Counter(r.get('category', 'UNKNOWN') for r in reports)
        logger.info(f"  [카테고리별]")
        for cat, count in sorted(cat_counts.items(), key=lambda x: -x[1]):
            logger.info(f"    {cat}: {count}개")
        
        # ⭐ P1: PDF URL 획득 가능성 샘플링
        if args.probe_pdfs > 0:
            import random
            pdf_reports = [r for r in reports if r.get('pdf_url')]
            sample_size = min(args.probe_pdfs, len(pdf_reports))
            
            if sample_size > 0:
                logger.info(f"\n=== PDF 획득 샘플링 ({sample_size}개) ===")
                samples = random.sample(pdf_reports, sample_size)
                
                probe_results = {"200": 0, "206": 0, "404": 0, "other": 0, "error": 0}
                content_types = {"pdf": 0, "html": 0, "other": 0}
                signature_check = {"pdf_sig": 0, "not_pdf": 0}  # ⭐ P0: %PDF 시그니처 체크
                
                for i, sample in enumerate(samples, 1):
                    pdf_url = sample['pdf_url']
                    try:
                        range_headers = {**HEADERS, "Range": "bytes=0-1024"}
                        with requests.get(pdf_url, headers=range_headers, timeout=10, 
                                         allow_redirects=True, stream=True) as resp:
                            status = resp.status_code
                            ct = resp.headers.get('content-type', '').lower()
                            
                            # ⭐ P0: %PDF 시그니처 체크 (첫 16바이트)
                            body_start = resp.content[:16] if resp.content else b''
                            has_pdf_sig = body_start.startswith(b'%PDF')
                            
                            if status == 200:
                                probe_results["200"] += 1
                            elif status == 206:
                                probe_results["206"] += 1
                            elif status == 404:
                                probe_results["404"] += 1
                            else:
                                probe_results["other"] += 1
                            
                            if 'pdf' in ct:
                                content_types["pdf"] += 1
                            elif 'html' in ct:
                                content_types["html"] += 1
                            else:
                                content_types["other"] += 1
                            
                            # ⭐ P0: 시그니처 체크 결과
                            if has_pdf_sig:
                                signature_check["pdf_sig"] += 1
                                sig_status = "PDF_SIG"
                            else:
                                signature_check["not_pdf"] += 1
                                sig_status = "NOT_PDF"
                            
                            logger.info(f"  [{i}/{sample_size}] {status} | {ct[:20]} | {sig_status} | {pdf_url[:55]}...")
                    except Exception as e:
                        probe_results["error"] += 1
                        logger.warning(f"  [{i}/{sample_size}] ERROR: {str(e)[:50]} | {pdf_url[:60]}...")
                
                # 샘플링 결과 요약
                success_rate = (probe_results["200"] + probe_results["206"]) / sample_size * 100
                pdf_sig_rate = signature_check["pdf_sig"] / sample_size * 100 if sample_size > 0 else 0
                
                logger.info(f"\n=== 샘플링 결과 ===")
                logger.info(f"  상태 코드: 200={probe_results['200']}, 206={probe_results['206']}, 404={probe_results['404']}, other={probe_results['other']}, error={probe_results['error']}")
                logger.info(f"  Content-Type: pdf={content_types['pdf']}, html={content_types['html']}, other={content_types['other']}")
                logger.info(f"  %PDF 시그니처: pdf_sig={signature_check['pdf_sig']}, not_pdf={signature_check['not_pdf']}")
                logger.info(f"  HTTP 성공률: {success_rate:.1f}% | 실제 PDF 비율: {pdf_sig_rate:.1f}%")
            else:
                logger.warning("PDF URL이 있는 리포트가 없어 샘플링을 건너뜁니다.")
        
        logger.info(f"\n다음 단계: 리포트 본문 다운로드 및 파싱")
        
    except Exception as e:
        logger.error(f"오류 발생: {e}", exc_info=True)
    finally:
        driver.quit()
        logger.info("Selenium 드라이버 종료")


if __name__ == "__main__":
    main()

