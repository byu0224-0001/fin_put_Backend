"""
KIRS 리포트 PDF 파싱 스크립트

한국IR협의회 기업분석보고서 전용 파서
- SmallCap 전용 파싱 로직 적용
- 체크포인트 우선 추출
- 섹션별 분리 (기술분석/시장동향)
"""
import sys
import os
from pathlib import Path
from datetime import datetime
import json
import time
from typing import List, Dict, Optional, Any

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
from parsers.smallcap_pdf_parser import extract_text_from_kirs_pdf

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


def download_pdf(pdf_url: str, max_retries: int = 3) -> Optional[bytes]:
    """
    PDF 다운로드
    
    Args:
        pdf_url: PDF URL
        max_retries: 최대 재시도 횟수
    
    Returns:
        PDF 바이너리 데이터 또는 None
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(pdf_url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            
            # Content-Type 확인
            content_type = response.headers.get('Content-Type', '')
            if 'pdf' in content_type.lower() or pdf_url.lower().endswith('.pdf'):
                return response.content
            else:
                logger.warning(f"PDF가 아닌 파일: {pdf_url} (Content-Type: {content_type})")
                return None
        
        except requests.exceptions.RequestException as e:
            logger.warning(f"PDF 다운로드 실패 (시도 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # 지수 백오프
            else:
                logger.error(f"PDF 다운로드 최종 실패: {pdf_url}")
                return None
    
    return None


def parse_single_report(report: Dict[str, Any]) -> Dict[str, Any]:
    """
    단일 리포트 파싱
    
    Args:
        report: 리포트 메타데이터
    
    Returns:
        파싱된 리포트 데이터
    """
    report_id = report.get("report_id", f"kirs_{report.get('date', '')}_{hash(report.get('title', ''))}")
    
    parsed_report = {
        "report_id": report_id,
        "source": report.get("source", "한국IR협의회"),
        "report_type": report.get("report_type", "COMPANY"),
        "title": report.get("title", ""),
        "ticker": report.get("ticker"),
        "stock_name": report.get("stock_name", ""),
        "author": report.get("author", ""),
        "date": report.get("date", ""),
        "url": report.get("url", ""),
        "pdf_url": report.get("pdf_url", ""),
        "parsed_at": datetime.now().isoformat()
    }
    
    # PDF 다운로드 및 파싱
    pdf_url = report.get("pdf_url")
    if pdf_url:
        logger.info(f"[{report_id}] PDF 다운로드 시작: {pdf_url[:80]}...")
        pdf_bytes = download_pdf(pdf_url)
        
        if pdf_bytes:
            logger.info(f"[{report_id}] PDF 파싱 시작 ({len(pdf_bytes)} bytes)")
            
            # KIRS 전용 파서 사용
            report_type = report.get("report_type", "company").lower()
            parse_result = extract_text_from_kirs_pdf(pdf_bytes, report_type=report_type)
            
            # 파싱 결과 병합
            parsed_report.update({
                "full_text": parse_result.get("full_text", ""),
                "checkpoint": parse_result.get("checkpoint"),
                "sections": parse_result.get("sections", {}),
                "paragraphs": parse_result.get("paragraphs", []),
                "total_paragraphs": parse_result.get("total_paragraphs", 0),
                "total_pages": parse_result.get("total_pages", 0),
                "parse_quality": parse_result.get("parse_quality", "LOW"),
                "parsing_error": parse_result.get("error")
            })
            
            logger.info(f"[{report_id}] 파싱 완료: {parse_result.get('total_paragraphs', 0)}개 문단, 품질: {parse_result.get('parse_quality', 'LOW')}")
        else:
            logger.warning(f"[{report_id}] PDF 다운로드 실패")
            parsed_report["parsing_error"] = "PDF 다운로드 실패"
    else:
        logger.warning(f"[{report_id}] PDF URL 없음")
        parsed_report["parsing_error"] = "PDF URL 없음"
    
    return parsed_report


def main():
    """메인 실행 함수"""
    logger.info("=" * 80)
    logger.info("KIRS 리포트 PDF 파싱 시작")
    logger.info("=" * 80)
    
    # 입력 파일 찾기
    reports_dir = project_root / "reports"
    json_files = list(reports_dir.glob("kirs_reports_*.json"))
    
    if not json_files:
        logger.error("수집된 KIRS 리포트 파일이 없습니다. 먼저 collect_broker_reports_kirs.py를 실행하세요.")
        return
    
    # 최신 파일 사용
    input_file = max(json_files, key=lambda p: p.stat().st_mtime)
    logger.info(f"입력 파일: {input_file}")
    
    # 리포트 로드
    with open(input_file, 'r', encoding='utf-8') as f:
        reports = json.load(f)
    
    logger.info(f"총 {len(reports)}개 리포트 파싱 시작")
    
    # 리포트 파싱
    parsed_reports = []
    failed_reports = []
    
    for i, report in enumerate(reports, 1):
        try:
            logger.info(f"\n[{i}/{len(reports)}] 리포트 파싱: {report.get('title', 'N/A')[:50]}...")
            parsed_report = parse_single_report(report)
            
            if parsed_report.get("parsing_error"):
                failed_reports.append(parsed_report)
            else:
                parsed_reports.append(parsed_report)
            
            # 요청 간격
            time.sleep(1)
        
        except Exception as e:
            logger.error(f"리포트 파싱 중 오류: {e}", exc_info=True)
            failed_reports.append({
                "report_id": report.get("report_id", "unknown"),
                "title": report.get("title", ""),
                "error": str(e)
            })
    
    # 결과 저장
    output_file = reports_dir / f"parsed_kirs_reports_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(parsed_reports, f, ensure_ascii=False, indent=2)
    
    logger.info(f"\n파싱 완료: {len(parsed_reports)}개 성공, {len(failed_reports)}개 실패")
    logger.info(f"결과 저장: {output_file}")
    
    # 실패 리포트 저장
    if failed_reports:
        failed_file = reports_dir / f"failed_kirs_reports_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(failed_file, 'w', encoding='utf-8') as f:
            json.dump(failed_reports, f, ensure_ascii=False, indent=2)
        logger.warning(f"실패 리포트 저장: {failed_file}")


if __name__ == "__main__":
    main()

