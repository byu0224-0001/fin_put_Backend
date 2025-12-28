"""
한국IR협의회(KIRS) 기업분석보고서 수집 스크립트 (Phase 2.1)

목적: 중소형주 기술분석보고서 수집 (Long-tail Coverage)
- 네이버 증권사 리포트와 보완 관계
- Industry Insight 수집에 최적화

법적 고려사항:
- 원문 PDF 재배포 금지
- 분석 데이터(KG)만 서비스에 사용
- 원문은 링크로만 제공
"""
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import time
import re
import json
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin, urlparse, parse_qs

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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

BASE_URL = "https://kirs.or.kr"
RESEARCH_LIST_URL = "https://kirs.or.kr/research/research22_1.html"

# Phase 2.1 파일럿 설정
PILOT_MODE = True
INITIAL_COLLECTION = True  # 초기 수집 모드
INITIAL_START_YEAR = 2025
INITIAL_START_MONTH = 11
INITIAL_START_DAY = 1
INITIAL_END_YEAR = 2025
INITIAL_END_MONTH = 12
INITIAL_END_DAY = 19

# 페이지당 리포트 수 (KIRS 사이트 구조)
REPORTS_PER_PAGE = 20


def create_selenium_driver():
    """Selenium 드라이버 생성"""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(f"user-agent={HEADERS['User-Agent']}")
    
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)


def extract_ticker_from_title(title: str) -> Optional[str]:
    """
    리포트 제목에서 티커 추출
    
    예: "에스피소프트(443670) - MS 생태계 확장..." -> "443670"
    """
    # 괄호 안의 6자리 숫자 패턴
    match = re.search(r'\((\d{6})\)', title)
    if match:
        return match.group(1)
    
    # 제목 끝에 붙은 티커 패턴 (예: "에스피소프트 443670")
    match = re.search(r'\s+(\d{6})\s*$', title)
    if match:
        return match.group(1)
    
    return None


def get_target_date_range() -> tuple:
    """
    수집 대상 날짜 범위 반환 (평일만)
    
    Returns:
        (start_date, end_date) 튜플
    """
    if INITIAL_COLLECTION:
        start_date = datetime(INITIAL_START_YEAR, INITIAL_START_MONTH, INITIAL_START_DAY)
        end_date = datetime(INITIAL_END_YEAR, INITIAL_END_MONTH, INITIAL_END_DAY)
    else:
        # 일반 모드: 최근 30일 (평일만)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
    
    return start_date, end_date


def scrape_kirs_research_list(driver: webdriver.Chrome, max_pages: int = 10) -> List[Dict[str, Any]]:
    """
    KIRS 기업분석보고서 목록 수집
    
    Args:
        driver: Selenium WebDriver
        max_pages: 최대 수집 페이지 수
    
    Returns:
        리포트 리스트 (메타데이터만)
    """
    reports = []
    start_date, end_date = get_target_date_range()
    
    logger.info(f"KIRS 리포트 수집 시작 - 대상 기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    
    try:
        for page in range(1, max_pages + 1):
            # 페이지 URL 구성
            if page == 1:
                url = RESEARCH_LIST_URL
            else:
                url = f"{RESEARCH_LIST_URL}?page={page}"
            
            logger.info(f"페이지 {page} 접속: {url}")
            
            driver.get(url)
            time.sleep(2)  # 로딩 대기
            
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, "html.parser")
            
            # 테이블 찾기
            table = soup.find("table")
            if not table:
                logger.warning(f"페이지 {page}: 테이블을 찾을 수 없음")
                break
            
            # 테이블 행 추출
            rows = table.find_all("tr")
            if len(rows) <= 1:  # 헤더만 있거나 비어있음
                logger.info(f"페이지 {page}: 더 이상 리포트 없음")
                break
            
            page_reports = 0
            
            for row in rows[1:]:  # 헤더 제외
                cols = row.find_all("td")
                if len(cols) < 4:
                    continue
                
                # 종목명 추출
                stock_name_tag = cols[0].find("a") or cols[0]
                stock_name = stock_name_tag.get_text(strip=True) if hasattr(stock_name_tag, 'get_text') else str(stock_name_tag).strip()
                
                # 제목 추출
                title_tag = cols[1].find("a")
                if not title_tag:
                    continue
                
                title = title_tag.get_text(strip=True)
                
                # 티커 추출 (제목 또는 종목명에서)
                ticker = extract_ticker_from_title(title)
                if not ticker:
                    # 종목명 컬럼에서도 시도
                    ticker_match = re.search(r'\((\d{6})\)', stock_name)
                    if ticker_match:
                        ticker = ticker_match.group(1)
                
                # 작성자 추출
                author = cols[2].get_text(strip=True) if len(cols) > 2 else ""
                
                # 등록일자 추출
                date_str = cols[3].get_text(strip=True) if len(cols) > 3 else ""
                
                # 날짜 파싱 및 필터링
                try:
                    if date_str:
                        # "2025-12-19" 형식
                        report_date = datetime.strptime(date_str, "%Y-%m-%d")
                        
                        # 평일만 필터링
                        if report_date.weekday() >= 5:  # 토요일(5), 일요일(6)
                            continue
                        
                        # 날짜 범위 필터링
                        if report_date < start_date or report_date > end_date:
                            continue
                    else:
                        continue
                except ValueError:
                    logger.warning(f"날짜 파싱 실패: {date_str}")
                    continue
                
                # 첨부파일 링크 추출
                attachment_tag = cols[4].find("a") if len(cols) > 4 else None
                pdf_url = None
                if attachment_tag:
                    href = attachment_tag.get("href", "")
                    if href:
                        pdf_url = urljoin(BASE_URL, href)
                
                # 상세 페이지 URL 추출
                detail_url = urljoin(BASE_URL, title_tag.get("href", ""))
                
                # 리포트 타입 판단 (제목 기반)
                report_type = "COMPANY"
                if any(keyword in title.lower() for keyword in ["산업", "섹터", "시장", "산업분석"]):
                    report_type = "INDUSTRY"
                
                reports.append({
                    "source": "한국IR협의회",
                    "report_type": report_type,
                    "stock_name": stock_name,
                    "ticker": ticker,
                    "title": title,
                    "author": author,
                    "date": report_date.strftime("%Y-%m-%d"),
                    "report_date": report_date.strftime("%Y-%m-%d"),
                    "url": detail_url,
                    "pdf_url": pdf_url,
                    "collected_at": datetime.now().isoformat()
                })
                
                page_reports += 1
            
            logger.info(f"페이지 {page}: {page_reports}개 리포트 수집")
            
            if page_reports == 0:
                logger.info(f"페이지 {page}: 수집된 리포트 없음, 종료")
                break
            
            time.sleep(1)  # 요청 간격
    
    except Exception as e:
        logger.error(f"KIRS 리포트 수집 중 오류: {e}", exc_info=True)
    
    logger.info(f"KIRS: 총 {len(reports)}개 리포트 수집 완료")
    return reports


def main():
    """메인 실행 함수"""
    logger.info("=" * 80)
    logger.info("한국IR협의회(KIRS) 기업분석보고서 수집 시작 (Phase 2.1)")
    logger.info("=" * 80)
    
    # 수집 모드 확인
    if INITIAL_COLLECTION:
        logger.info("초기 수집 모드: 2025년 11월~12월 (평일만)")
    else:
        logger.info("일반 수집 모드: 최근 30일 (평일만)")
    
    # Selenium 드라이버 생성
    driver = create_selenium_driver()
    
    try:
        # 리포트 수집
        reports = scrape_kirs_research_list(driver, max_pages=50)  # 최대 50페이지
        
        if not reports:
            logger.warning("수집된 리포트가 없습니다.")
            return
        
        logger.info(f"\n총 {len(reports)}개 리포트 수집 완료")
        
        # 리포트 타입별 통계
        company_reports = [r for r in reports if r.get("report_type") == "COMPANY"]
        industry_reports = [r for r in reports if r.get("report_type") == "INDUSTRY"]
        
        logger.info(f"기업 리포트: {len(company_reports)}개")
        logger.info(f"산업 리포트: {len(industry_reports)}개")
        
        # 결과 출력
        logger.info("\n수집된 리포트 샘플 (최대 10개):")
        for i, report in enumerate(reports[:10], 1):
            ticker_str = f"[{report['ticker']}]" if report.get('ticker') else "[티커없음]"
            logger.info(f"{i}. {ticker_str} {report['title'][:50]}... ({report['author']})")
            if report['pdf_url']:
                logger.info(f"   PDF: {report['pdf_url'][:80]}...")
            else:
                logger.info(f"   URL: {report['url'][:80]}...")
        
        # JSON 저장
        output_file = project_root / "reports" / f"kirs_reports_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        output_file.parent.mkdir(exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(reports, f, ensure_ascii=False, indent=2)
        
        logger.info(f"\n결과 저장: {output_file}")
        logger.info(f"다음 단계: 리포트 본문 다운로드 및 파싱")
        
    except Exception as e:
        logger.error(f"오류 발생: {e}", exc_info=True)
    finally:
        driver.quit()
        logger.info("Selenium 드라이버 종료")


if __name__ == "__main__":
    main()

