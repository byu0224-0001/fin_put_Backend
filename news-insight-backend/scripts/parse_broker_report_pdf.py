"""
증권사 리포트 PDF 파싱 스크립트 (Phase 2.1 파일럿)

기존 economic_report_ai_v2의 PDF 파싱 로직을 활용하여
문단 단위로 의미 있는 텍스트 추출

목적: Edge Enrichment를 위한 리포트 본문 추출
"""
import sys
import os
from pathlib import Path
from typing import Dict, List, Optional, Any
import re
import hashlib
import json

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
import requests
import fitz  # PyMuPDF
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Smart PDF Parser (pdfplumber) - Phase 2.0
try:
    from parsers.smart_pdf_parser import extract_text_from_pdf_smart
    SMART_PARSER_AVAILABLE = True
except ImportError:
    SMART_PARSER_AVAILABLE = False
    logger.warning("Smart PDF Parser (pdfplumber)를 사용할 수 없습니다. PyMuPDF로 폴백합니다.")

# 설정
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
}

# PDF 캐시 디렉토리
PDF_CACHE_DIR = project_root / "data" / "pdf_cache"
PDF_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def get_pdf_hash(pdf_url: str) -> str:
    """PDF URL의 해시값 생성 (캐시 키)"""
    return hashlib.md5(pdf_url.encode()).hexdigest()


def download_pdf(pdf_url: str, cache_dir: Path = PDF_CACHE_DIR) -> tuple[Optional[bytes], Optional[str]]:
    """
    PDF 다운로드 (캐시 활용)
    
    Args:
        pdf_url: PDF URL
        cache_dir: 캐시 디렉토리
    
    Returns:
        (PDF 바이너리 데이터, parse_fail_reason) - 실패 시 (None, reason)
    """
    # 캐시 확인
    pdf_hash = get_pdf_hash(pdf_url)
    cache_file = cache_dir / f"{pdf_hash}.pdf"
    
    if cache_file.exists():
        logger.info(f"캐시에서 PDF 로드: {pdf_url[:60]}...")
        try:
            with open(cache_file, 'rb') as f:
                content = f.read()
                # 캐시된 파일도 PDF 형식 검증
                if not content.startswith(b'%PDF'):
                    logger.warning(f"캐시된 파일이 PDF 형식이 아님: {pdf_url[:60]}...")
                    return None, "NOT_PDF_CONTENT"
                return content, None
        except Exception as e:
            logger.error(f"캐시 파일 읽기 실패: {pdf_url[:60]}... - {e}")
            return None, "CACHE_READ_FAIL"
    
    try:
        logger.info(f"PDF 다운로드: {pdf_url[:60]}...")
        response = requests.get(pdf_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        # Content-Type 검증
        content_type = response.headers.get('content-type', '').lower()
        if 'application/pdf' not in content_type and not response.content.startswith(b'%PDF'):
            logger.warning(f"다운로드된 파일이 PDF가 아님: {content_type}")
            return None, "NOT_PDF_CONTENT"
        
        # PDF 검증
        if not response.content.startswith(b'%PDF'):
            logger.warning(f"PDF 형식이 아닙니다: {pdf_url[:60]}...")
            return None, "NOT_PDF_CONTENT"
        
        # 캐시 저장
        try:
            with open(cache_file, 'wb') as f:
                f.write(response.content)
        except Exception as e:
            logger.warning(f"캐시 저장 실패 (계속 진행): {e}")
        
        return response.content, None
    
    except requests.exceptions.Timeout:
        logger.error(f"PDF 다운로드 타임아웃: {pdf_url[:60]}...")
        return None, "DOWNLOAD_TIMEOUT"
    except requests.exceptions.RequestException as e:
        logger.error(f"PDF 다운로드 실패 (네트워크/HTTP 오류): {pdf_url[:60]}... - {e}")
        return None, "DOWNLOAD_FAIL"
    except Exception as e:
        logger.error(f"PDF 다운로드 실패 (기타 오류): {pdf_url[:60]}... - {e}")
        return None, "DOWNLOAD_FAIL"


def extract_text_from_pdf(pdf_bytes: bytes, report_type: str = "company") -> Dict[str, Any]:
    """
    PDF에서 텍스트 추출 (문단 단위)
    
    Args:
        pdf_bytes: PDF 바이너리 데이터
        report_type: 리포트 유형 ("company" or "industry")
    
    Returns:
        {
            "full_text": 전체 텍스트,
            "paragraphs": 문단 리스트,
            "pages": 페이지별 텍스트,
            "tables": 표 데이터 (선택적),
            "metadata": PDF 메타데이터,
            "parse_fail_reason": 실패 원인 (선택적)
        }
    """
    # Smart PDF Parser 우선 사용 (pdfplumber)
    if SMART_PARSER_AVAILABLE:
        try:
            logger.info("Smart PDF Parser (pdfplumber) 사용")
            result = extract_text_from_pdf_smart(pdf_bytes, report_type)
            
            # 기존 형식으로 변환
            return {
                "full_text": result.get("full_text", ""),
                "paragraphs": result.get("paragraphs", []),
                "pages": result.get("pages", []),
                "metadata": {},
                "total_pages": result.get("total_pages", 0),
                "total_paragraphs": result.get("total_paragraphs", 0),
                "parse_quality": result.get("parse_quality", "UNKNOWN"),
                "error": result.get("error"),
                "parse_fail_reason": result.get("parse_fail_reason")
            }
        except Exception as e:
            logger.warning(f"Smart PDF Parser 실패, PyMuPDF로 폴백: {e}")
            # Smart Parser 실패 시에도 parse_fail_reason 기록
            parse_fail_reason = "SMART_PARSER_EXCEPTION"
    
    # 폴백: PyMuPDF
    try:
        logger.info("PyMuPDF 사용 (폴백)")
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        
        # PDF 암호화 확인
        if doc.is_encrypted:
            logger.warning("PDF가 암호화되어 있습니다.")
            doc.close()
            return {
                "full_text": "",
                "paragraphs": [],
                "pages": [],
                "metadata": {},
                "total_pages": 0,
                "total_paragraphs": 0,
                "error": "PDF is encrypted",
                "parse_fail_reason": "PDF_ENCRYPTED"
            }
        
        full_text = ""
        paragraphs = []
        pages = []
        
        for page_num in range(len(doc)):
            page = doc[page_num]
            page_text = page.get_text()
            
            if page_text.strip():
                pages.append({
                    "page_number": page_num + 1,
                    "text": page_text.strip()
                })
                
                full_text += page_text + "\n\n"
                
                # 문단 단위로 분리 (빈 줄 기준)
                page_paragraphs = [p.strip() for p in page_text.split('\n\n') if p.strip()]
                for para in page_paragraphs:
                    # 의미 있는 문단만 (최소 길이 체크)
                    if len(para) > 50:  # 50자 이상만
                        paragraphs.append({
                            "page_number": page_num + 1,
                            "text": para
                        })
        
        # 메타데이터 추출
        metadata = doc.metadata
        
        doc.close()
        
        # 텍스트가 없으면 실패로 처리
        if not full_text.strip():
            return {
                "full_text": "",
                "paragraphs": [],
                "pages": [],
                "metadata": metadata,
                "total_pages": len(doc),
                "total_paragraphs": 0,
                "error": "No text extracted",
                "parse_fail_reason": "ZERO_TEXT"
            }
        
        return {
            "full_text": full_text.strip(),
            "paragraphs": paragraphs,
            "pages": pages,
            "metadata": metadata,
            "total_pages": len(doc),
            "total_paragraphs": len(paragraphs),
            "parse_quality": "HIGH" if len(paragraphs) > 10 else "LOW"
        }
    
    except fitz.fitz.FileDataError as e:
        logger.error(f"PDF 파일 데이터 오류: {e}")
        return {
            "full_text": "",
            "paragraphs": [],
            "pages": [],
            "metadata": {},
            "total_pages": 0,
            "total_paragraphs": 0,
            "error": str(e),
            "parse_fail_reason": "PDF_CORRUPTED"
        }
    except Exception as e:
        logger.error(f"PDF 파싱 실패: {e}")
        return {
            "full_text": "",
            "paragraphs": [],
            "pages": [],
            "metadata": {},
            "total_pages": 0,
            "total_paragraphs": 0,
            "error": str(e),
            "parse_fail_reason": "PYMUPDF_EXCEPTION"
        }


def extract_metadata_from_detail_page(html_url: str, retry_count: int = 3) -> Dict[str, Any]:
    """
    네이버 리포트 상세 페이지에서 메타데이터 추출
    
    ⭐ 개선: 상세 페이지의 종목명 태그, 분류 태그, 목표가/투자의견 추출
    ⭐ P0-β4: HTML_DOWNLOAD_FAIL 원인 재발 방지 (헤더/세션/리트라이)
    
    Args:
        html_url: 네이버 리포트 상세 페이지 URL
        retry_count: 재시도 횟수 (기본값: 3)
    
    Returns:
        {
            "stock_name": 종목명 (종목분석 리포트인 경우),
            "industry_category": 분류 태그 (산업분석 리포트인 경우),
            "target_price": 목표가,
            "investment_opinion": 투자의견,
            "analyst_name": 애널리스트 이름,
            "extraction_success": bool,  # ⭐ P0-β3: 추출 성공 여부
            "fields_extracted": List[str]  # ⭐ P0-β3: 추출된 필드 목록
        }
    """
    metadata = {
        "stock_name": None,
        "industry_category": None,
        "target_price": None,
        "investment_opinion": None,
        "analyst_name": None,
        "extraction_success": False,
        "fields_extracted": []
    }
    
    # ⭐ P0-β4: 강화된 헤더 (User-Agent 고정, Referer 추가)
    enhanced_headers = {
        **HEADERS,
        "Referer": "https://finance.naver.com/",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }
    
    # ⭐ P0-β4: 지수 백오프 재시도
    import time
    for attempt in range(retry_count):
        try:
            logger.debug(f"상세 페이지 메타데이터 추출 (시도 {attempt + 1}/{retry_count}): {html_url[:60]}...")
            response = requests.get(
                html_url, 
                headers=enhanced_headers, 
                timeout=15,
                allow_redirects=True
            )
            
            # ⭐ P0-β4: 429/403 처리
            if response.status_code == 429:
                wait_time = (2 ** attempt) * 1  # 지수 백오프: 1초, 2초, 4초
                logger.warning(f"Rate limit (429) 감지, {wait_time}초 대기 후 재시도...")
                time.sleep(wait_time)
                continue
            
            if response.status_code == 403:
                logger.warning(f"접근 거부 (403): {html_url[:60]}... (robots/정책 위반 가능)")
                metadata["extraction_success"] = False
                return metadata
            
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 1. 종목명 태그 추출 (빨간색 테두리 태그)
            # 예: <span class="tag">유한양행</span> 또는 <div class="stock_name">유한양행</div>
            stock_tag = soup.find("span", class_=re.compile(r"tag|stock|name", re.I))
            if not stock_tag:
                stock_tag = soup.find("div", class_=re.compile(r"stock|name|tag", re.I))
            if stock_tag:
                stock_text = stock_tag.get_text(strip=True)
                # 6자리 숫자가 아닌 경우만 종목명으로 간주
                if not re.match(r'^\d{6}$', stock_text):
                    metadata["stock_name"] = stock_text
            
            # 2. 분류 태그 추출 (산업분석 리포트의 경우)
            # 예: <span class="tag">반도체</span>
            industry_tag = soup.find("span", class_=re.compile(r"tag|category|industry", re.I))
            if not industry_tag:
                industry_tag = soup.find("div", class_=re.compile(r"category|industry|tag", re.I))
            if industry_tag:
                industry_text = industry_tag.get_text(strip=True)
                # 산업 키워드 확인
                industry_keywords = ["반도체", "IT", "통신", "자동차", "타이어", "유통", "게임", "제약", "화학", "바이오", "금융", "에너지"]
                if any(kw in industry_text for kw in industry_keywords):
                    metadata["industry_category"] = industry_text
            
            # 3. 목표가 추출
            # 예: "목표가 150,000" 또는 "목표주가 150,000"
            target_price_pattern = re.compile(r'목표(?:가|주가)\s*[:：]?\s*([\d,]+)', re.I)
            target_price_match = target_price_pattern.search(response.text)
            if target_price_match:
                metadata["target_price"] = target_price_match.group(1).replace(",", "")
            
            # 4. 투자의견 추출
            # 예: "투자의견 Buy" 또는 "투자의견 매수"
            opinion_pattern = re.compile(r'투자\s*의견\s*[:：]?\s*([가-힣A-Za-z\s]+)', re.I)
            opinion_match = opinion_pattern.search(response.text)
            if opinion_match:
                metadata["investment_opinion"] = opinion_match.group(1).strip()
            
            # 5. 애널리스트 이름 추출
            # 예: "애널리스트 이수림"
            analyst_pattern = re.compile(r'애널리스트\s+([가-힣\s]+)', re.I)
            analyst_match = analyst_pattern.search(response.text)
            if analyst_match:
                metadata["analyst_name"] = analyst_match.group(1).strip()
            
            # ⭐ P0-β3: 추출 성공 여부 및 필드 목록 기록
            extracted_fields = [k for k, v in metadata.items() if v is not None and k not in ["extraction_success", "fields_extracted"]]
            metadata["extraction_success"] = len(extracted_fields) > 0
            metadata["fields_extracted"] = extracted_fields
            
            logger.debug(f"메타데이터 추출 완료: {metadata}")
            break  # 성공 시 루프 종료
            
        except requests.exceptions.Timeout:
            if attempt < retry_count - 1:
                wait_time = (2 ** attempt) * 1
                logger.warning(f"타임아웃 발생, {wait_time}초 대기 후 재시도...")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"상세 페이지 메타데이터 추출 타임아웃 (최종 실패): {html_url[:60]}...")
                metadata["extraction_success"] = False
                return metadata
        
        except requests.exceptions.RequestException as e:
            if attempt < retry_count - 1:
                wait_time = (2 ** attempt) * 1
                logger.warning(f"요청 오류 발생 ({e}), {wait_time}초 대기 후 재시도...")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"상세 페이지 메타데이터 추출 실패 (최종 실패): {e}")
                metadata["extraction_success"] = False
                return metadata
        
        except Exception as e:
            logger.error(f"상세 페이지 메타데이터 추출 중 예외 발생: {e}")
            metadata["extraction_success"] = False
            return metadata
    
    return metadata


def extract_text_from_html(html_url: str) -> Dict[str, Any]:
    """
    HTML 리포트에서 텍스트 추출
    
    Args:
        html_url: HTML 리포트 URL
    
    Returns:
        {
            "full_text": 전체 텍스트,
            "paragraphs": 문단 리스트,
            "sections": 섹션별 텍스트
        }
    """
    try:
        logger.info(f"HTML 리포트 다운로드: {html_url[:60]}...")
        response = requests.get(html_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 다양한 선택자 시도 (기존 로직 활용)
        content_selectors = [
            "td.view_cnt",
            "div.view_con",
            "div.report_view",
            "div.report-content",
            "div.report-body",
            "div.content",
            "div.article"
        ]
        
        full_text = ""
        paragraphs = []
        sections = []
        
        for selector in content_selectors:
            elements = soup.select(selector)
            if elements:
                for elem in elements:
                    text = elem.get_text(strip=True)
                    if text and len(text) > 50:
                        full_text += text + "\n\n"
                        
                        # 문단 단위로 분리
                        para_list = [p.strip() for p in text.split('\n\n') if p.strip() and len(p.strip()) > 50]
                        paragraphs.extend(para_list)
        
        # 섹션 추출 (제목 기준)
        headings = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
        for heading in headings:
            section_text = ""
            for sibling in heading.next_siblings:
                if sibling.name and sibling.name.startswith('h'):
                    break
                if hasattr(sibling, 'get_text'):
                    section_text += sibling.get_text(strip=True) + " "
            
            if section_text.strip():
                sections.append({
                    "title": heading.get_text(strip=True),
                    "text": section_text.strip()
                })
        
        return {
            "full_text": full_text.strip(),
            "paragraphs": paragraphs,
            "sections": sections,
            "total_paragraphs": len(paragraphs)
        }
    
    except requests.exceptions.RequestException as e:
        logger.error(f"HTML 다운로드 실패: {html_url[:60]}... - {e}")
        return {
            "full_text": "",
            "paragraphs": [],
            "sections": [],
            "total_paragraphs": 0,
            "error": str(e),
            "error_type": "HTML_DOWNLOAD_FAIL"
        }
    except Exception as e:
        logger.error(f"HTML 파싱 실패: {html_url[:60]}... - {e}")
        return {
            "full_text": "",
            "paragraphs": [],
            "sections": [],
            "total_paragraphs": 0,
            "error": str(e),
            "error_type": "HTML_PARSER_EXCEPTION"
        }


def parse_report(report: Dict[str, Any]) -> Dict[str, Any]:
    """
    리포트 파싱 (PDF 또는 HTML)
    
    Args:
        report: 리포트 메타데이터 (collect_broker_reports_naver.py에서 수집)
    
    Returns:
        리포트 메타데이터 + 파싱된 텍스트 + report_type + parse_fail_reason
    """
    pdf_url = report.get("pdf_url")
    html_url = report.get("url")
    category = report.get("category", "")
    
    # 리포트 유형 판단 (P0-1)
    # ⭐ 종목분석은 하드 오버라이드: COMPANY로 강제 분류
    if "종목" in category or "company" in category.lower() or category == "종목분석":
        report_type = "COMPANY"
    elif "산업" in category or "industry" in category.lower():
        report_type = "INDUSTRY"
    elif "경제" in category or "economy" in category.lower() or "macro" in category.lower():
        report_type = "MACRO"
    else:
        report_type = "UNKNOWN"  # 기본값 또는 추가 분류 필요
    
    # report_type을 리포트 데이터에 저장
    report["report_type"] = report_type
    
    parsed_content = {
        "source_type": None,
        "full_text": "",
        "paragraphs": [],
        "total_paragraphs": 0,
        "error": None,
        "parse_fail_reason": None,  # P0-2: 파싱 실패 원인 추가
        "report_type": report_type  # P0-1: report_type 추가
    }
    
    # ⭐ 종목분석 리포트: 리스트에서 추출한 stock_name 보존
    if category == "종목분석" and report.get("stock_name"):
        # 리스트에서 추출한 stock_name이 있으면 우선 사용
        parsed_content["stock_name"] = report.get("stock_name")
        logger.info(f"리스트에서 추출한 종목명 보존: {report.get('stock_name')}")
    
    # PDF 우선 처리
    if pdf_url:
        pdf_bytes, download_error_reason = download_pdf(pdf_url)  # P0-2: download_pdf 반환값 처리
        if download_error_reason:
            parsed_content["error"] = f"PDF 다운로드 실패: {download_error_reason}"
            parsed_content["parse_fail_reason"] = download_error_reason
            return {**report, **parsed_content}
        
        if pdf_bytes:
            pdf_result = extract_text_from_pdf(pdf_bytes, report_type)
            parsed_content.update({
                "source_type": "PDF",
                "full_text": pdf_result.get("full_text", ""),
                "paragraphs": pdf_result.get("paragraphs", []),
                "total_paragraphs": pdf_result.get("total_paragraphs", 0),
                "pages": pdf_result.get("pages", []),
                "metadata": pdf_result.get("metadata", {}),
                "parse_quality": pdf_result.get("parse_quality", "UNKNOWN"),
                "error": pdf_result.get("error"),
                "parse_fail_reason": pdf_result.get("parse_fail_reason")  # P0-2: parse_fail_reason 전파
            })
            
            if parsed_content["total_paragraphs"] > 0:
                logger.info(f"PDF 파싱 성공: {parsed_content['total_paragraphs']}개 문단 추출 (품질: {parsed_content.get('parse_quality', 'UNKNOWN')})")
                return {**report, **parsed_content}
    
    # ⭐ 상세 페이지 메타데이터 추출 (네이버 리포트인 경우)
    if report.get("source") == "네이버" and html_url:
        detail_metadata = extract_metadata_from_detail_page(html_url)
        parsed_content.update(detail_metadata)
        logger.info(f"상세 페이지 메타데이터 추출: stock_name={detail_metadata.get('stock_name')}, "
                   f"industry_category={detail_metadata.get('industry_category')}")
    
    # HTML 폴백
    if html_url and not parsed_content.get("full_text"):
        html_result = extract_text_from_html(html_url)
        parsed_content.update({
            "source_type": "HTML",
            "full_text": html_result.get("full_text", ""),
            "paragraphs": html_result.get("paragraphs", []),
            "total_paragraphs": html_result.get("total_paragraphs", 0),
            "sections": html_result.get("sections", []),
            "error": html_result.get("error"),
            "parse_fail_reason": html_result.get("error_type", "HTML_PARSER_EXCEPTION")  # P0-2: HTML 파싱 실패 원인
        })
        
        if parsed_content["total_paragraphs"] > 0:
            logger.info(f"HTML 파싱 성공: {parsed_content['total_paragraphs']}개 문단 추출")
            return {**report, **parsed_content}
    
    # 파싱 실패
    logger.warning(f"리포트 파싱 실패: {report.get('title', 'N/A')[:50]}...")
    if not parsed_content["parse_fail_reason"]:  # 특정 원인이 없으면
        parsed_content["error"] = "PDF 및 HTML 파싱 모두 실패"
        parsed_content["parse_fail_reason"] = "ALL_PARSING_FAILED"
    return {**report, **parsed_content}


def main():
    """메인 실행 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description="증권사 리포트 PDF/HTML 파싱")
    parser.add_argument("--input", type=str, help="수집된 리포트 JSON 파일 경로")
    parser.add_argument("--output", type=str, help="파싱 결과 JSON 파일 경로")
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info("증권사 리포트 파싱 시작 (Phase 2.1 파일럿)")
    logger.info("=" * 80)
    
    # 입력 파일 로드
    if args.input:
        input_file = Path(args.input)
    else:
        # 최신 리포트 파일 찾기
        reports_dir = project_root / "reports"
        json_files = list(reports_dir.glob("naver_reports_*.json"))
        if not json_files:
            logger.error("수집된 리포트 파일이 없습니다. 먼저 collect_broker_reports_naver.py를 실행하세요.")
            return
        
        input_file = max(json_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"최신 리포트 파일 사용: {input_file}")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reports = json.load(f)
    
    logger.info(f"총 {len(reports)}개 리포트 파싱 시작")
    
    # 리포트 파싱
    parsed_reports = []
    for i, report in enumerate(reports, 1):
        logger.info(f"[{i}/{len(reports)}] 파싱 중: {report.get('title', 'N/A')[:50]}...")
        parsed_report = parse_report(report)
        parsed_reports.append(parsed_report)
        
        # 진행 상황 출력
        if i % 10 == 0:
            success_count = sum(1 for r in parsed_reports if r.get("total_paragraphs", 0) > 0)
            logger.info(f"진행 상황: {i}/{len(reports)} ({success_count}개 성공)")
    
    # 결과 저장
    if args.output:
        output_file = Path(args.output)
    else:
        output_file = project_root / "reports" / f"parsed_reports_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(parsed_reports, f, ensure_ascii=False, indent=2)
    
    # 통계 출력
    success_count = sum(1 for r in parsed_reports if r.get("total_paragraphs", 0) > 0)
    total_paragraphs = sum(r.get("total_paragraphs", 0) for r in parsed_reports)
    low_quality_count = sum(1 for r in parsed_reports if r.get("parse_quality") == "LOW")
    total_text_length = sum(len(r.get("full_text", "")) for r in parsed_reports)
    
    # P0-2: 파싱 실패 원인 분해
    parse_fail_reasons = {}
    quality_status_distribution = {"PASS": 0, "HOLD": 0, "DROP": 0, "UNKNOWN": 0}
    
    for r in parsed_reports:
        if r.get("total_paragraphs", 0) == 0:  # Parsing failed
            reason = r.get("parse_fail_reason", "UNKNOWN_FAILURE")
            parse_fail_reasons[reason] = parse_fail_reasons.get(reason, 0) + 1
        else:  # Parsing succeeded, check quality
            # Note: Quality Gate is applied in enrich_edges_from_reports.py,
            # so here we just use the parse_quality from smart_pdf_parser
            quality = r.get("parse_quality", "UNKNOWN")
            if quality == "HIGH":
                quality_status_distribution["PASS"] += 1
            elif quality == "MEDIUM":
                quality_status_distribution["HOLD"] += 1  # Treat MEDIUM as HOLD for initial observation
            elif quality == "LOW":
                quality_status_distribution["DROP"] += 1  # Treat LOW as DROP for initial observation
            else:
                quality_status_distribution["UNKNOWN"] += 1
    
    logger.info("\n" + "=" * 80)
    logger.info("파싱 완료")
    logger.info("=" * 80)
    logger.info(f"총 리포트 수: {len(parsed_reports)}")
    logger.info(f"파싱 성공: {success_count}개 ({success_count/len(parsed_reports)*100:.1f}%)")
    logger.info(f"파싱 실패: {len(parsed_reports) - success_count}개")
    if parse_fail_reasons:
        logger.info(f"  - 실패 원인별:")
        for reason, count in sorted(parse_fail_reasons.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"    - {reason}: {count}개")
    logger.info(f"LOW 품질: {low_quality_count}개 ({low_quality_count/len(parsed_reports)*100:.1f}%)")
    logger.info(f"총 문단 수: {total_paragraphs}개")
    logger.info(f"평균 텍스트 길이: {total_text_length//max(success_count, 1):,}자")
    logger.info(f"파싱 품질 분포 (Smart Parser 기준):")
    for status, count in quality_status_distribution.items():
        logger.info(f"  - {status}: {count}개")
    logger.info(f"결과 저장: {output_file}")
    logger.info(f"다음 단계: Logic Extraction 및 Edge Enrichment")


if __name__ == "__main__":
    from datetime import datetime
    main()

