# -*- coding: utf-8 -*-
"""
Top20 실패 12개 기업 원인 분석 스크립트

분석 항목:
1. 원문 HTML에서 표 존재 여부 확인
2. extract_key_sections가 그 표를 포함했는지 확인
3. 포함했는데도 LLM이 못 뽑는지 확인
"""
import sys
import os
import json
import re
import time
import requests
from bs4 import BeautifulSoup
from typing import Dict, List, Optional, Tuple

# Windows 환경에서 UTF-8 인코딩 설정
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'
else:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

sys.path.insert(0, '.')

from app.services.dart_parser import DartParser
from app.services.llm_handler import LLMHandler
from app.services.embedding_filter import select_relevant_chunks
from dotenv import load_dotenv
import logging

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DART_API_KEY = os.getenv('DART_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
MAX_LLM_CHARS = 50000

# 🆕 실패한 기업 목록 (refetch_revenue_monitoring.json에서 로드)
FAILED_TICKERS = [
    "105560",  # KB금융
    "000270",  # 기아
    "012450",  # 한화에어로스페이스
    "055550",  # 신한지주
    "032830",  # 삼성생명
    "086790",  # 하나금융지주
    "005490",  # POSCO홀딩스
    "000810",  # 삼성화재
    "011200",  # HMM
    "138040",  # 메리츠금융지주
    "096770",  # SK이노베이션
    "024110",  # 기업은행
]


def check_table_in_html(html_content: str, ticker: str, company_name: str) -> Dict:
    """
    원문 HTML에서 표 존재 여부 확인
    
    Returns:
        {
            'has_table': bool,
            'table_count': int,
            'revenue_keywords_found': List[str],
            'table_preview': str  # 첫 번째 표의 미리보기
        }
    """
    if not html_content:
        return {
            'has_table': False,
            'table_count': 0,
            'revenue_keywords_found': [],
            'table_preview': None
        }
    
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')
    
    # 매출/수익 관련 키워드
    revenue_keywords = [
        '매출', '수익', '영업', '부문', '사업부문', '영업의 현황', '영업의 종류',
        '영업수익', '당기손익', '당기순이익', '보험수익', '이자수익',
        '은행부문', '금융투자부문', '보험부문', '생명보험', '손해보험',
        '사업부문', '부문별', '세그먼트', '영업종류', '영업부문'
    ]
    
    revenue_keywords_found = []
    table_preview = None
    
    for table in tables:
        table_text = table.get_text()
        table_text_lower = table_text.lower()
        
        # 키워드 확인
        for keyword in revenue_keywords:
            if keyword in table_text_lower and keyword not in revenue_keywords_found:
                revenue_keywords_found.append(keyword)
        
        # 첫 번째 표 미리보기 저장
        if not table_preview and len(table_text) > 50:
            table_preview = table_text[:500]  # 처음 500자만
    
    return {
        'has_table': len(tables) > 0,
        'table_count': len(tables),
        'revenue_keywords_found': revenue_keywords_found,
        'table_preview': table_preview
    }


def check_table_in_extracted_text(extracted_text: str, ticker: str, company_name: str) -> Dict:
    """
    extract_key_sections로 추출한 텍스트에서 표 존재 여부 확인
    
    Returns:
        {
            'has_table_markers': bool,  # 마크다운 표 마커(|) 존재 여부
            'table_marker_count': int,
            'revenue_keywords_found': List[str],
            'text_preview': str  # 관련 섹션 미리보기
        }
    """
    if not extracted_text:
        return {
            'has_table_markers': False,
            'table_marker_count': 0,
            'revenue_keywords_found': [],
            'text_preview': None
        }
    
    # 마크다운 표 마커 확인
    table_markers = re.findall(r'\|', extracted_text)
    table_marker_count = len(table_markers)
    has_table_markers = table_marker_count > 10  # 최소 10개 이상의 |가 있어야 표로 간주
    
    # 매출/수익 관련 키워드 확인
    revenue_keywords = [
        '매출', '수익', '영업', '부문', '사업부문', '영업의 현황', '영업의 종류',
        '영업수익', '당기손익', '당기순이익', '보험수익', '이자수익',
        '은행부문', '금융투자부문', '보험부문', '생명보험', '손해보험',
        '사업부문', '부문별', '세그먼트', '영업종류', '영업부문'
    ]
    
    revenue_keywords_found = []
    text_lower = extracted_text.lower()
    
    for keyword in revenue_keywords:
        if keyword in text_lower and keyword not in revenue_keywords_found:
            revenue_keywords_found.append(keyword)
    
    # 관련 섹션 미리보기 (키워드 주변 텍스트)
    text_preview = None
    if revenue_keywords_found:
        # 첫 번째 키워드 주변 텍스트 추출
        first_keyword = revenue_keywords_found[0]
        idx = text_lower.find(first_keyword)
        if idx != -1:
            start = max(0, idx - 200)
            end = min(len(extracted_text), idx + 500)
            text_preview = extracted_text[start:end]
    
    return {
        'has_table_markers': has_table_markers,
        'table_marker_count': table_marker_count,
        'revenue_keywords_found': revenue_keywords_found,
        'text_preview': text_preview
    }


def test_llm_extraction(extracted_text: str, ticker: str, company_name: str, llm_handler: LLMHandler) -> Dict:
    """
    LLM 추출 시도 및 결과 확인
    
    Returns:
        {
            'llm_success': bool,
            'revenue_by_segment': Optional[Dict],
            'error': Optional[str]
        }
    """
    if not extracted_text:
        return {
            'llm_success': False,
            'revenue_by_segment': None,
            'error': 'NO_EXTRACTED_TEXT'
        }
    
    try:
        # 필터링
        filtered_text = select_relevant_chunks(extracted_text, ticker=ticker)
        effective_text = filtered_text if filtered_text and len(filtered_text) > 200 else extracted_text
        if len(effective_text) > MAX_LLM_CHARS:
            effective_text = effective_text[:MAX_LLM_CHARS]
        
        # LLM 추출 시도
        structured_data = llm_handler.extract_structured_data(
            effective_text,
            ticker=ticker,
            company_name=company_name
        )
        
        if structured_data and structured_data.get('revenue_by_segment'):
            revenue_data = structured_data.get('revenue_by_segment')
            if isinstance(revenue_data, dict) and len(revenue_data) > 0:
                return {
                    'llm_success': True,
                    'revenue_by_segment': revenue_data,
                    'error': None
                }
        
        return {
            'llm_success': False,
            'revenue_by_segment': None,
            'error': 'NO_REVENUE_DATA_IN_RESPONSE'
        }
    except Exception as e:
        return {
            'llm_success': False,
            'revenue_by_segment': None,
            'error': str(e)
        }


def get_raw_html_from_dart(dart_parser: DartParser, ticker: str, year: int = 2024) -> Optional[str]:
    """
    DART에서 원문 HTML 가져오기
    
    Returns:
        HTML 문자열 또는 None
    """
    try:
        # 사업보고서 찾기
        report_info = dart_parser.find_business_report(ticker, year)
        if not report_info:
            logger.warning(f"[{ticker}] 사업보고서를 찾을 수 없습니다.")
            return None
        
        rcept_no = report_info['rcept_no']
        
        # 하위 문서 목록 조회
        sub_docs = dart_parser.get_sub_docs(rcept_no)
        if sub_docs is None or len(sub_docs) == 0:
            logger.warning(f"[{ticker}] 하위 문서 없음.")
            return None
        
        # "영업의 현황" 또는 "주요 제품 및 서비스" 관련 문서 찾기
        target_keywords = ['영업의 현황', '영업의 종류', '주요 제품', '매출', '사업부문', '영업개황']
        
        html_content = None
        for idx, row in sub_docs.iterrows():
            title = str(row.get('title', ''))
            url = row.get('url', '')
            
            # 관련 섹션 찾기
            if any(keyword in title for keyword in target_keywords):
                try:
                    # URL에서 HTML 가져오기
                    response = requests.get(url, timeout=30)
                    if response.status_code == 200:
                        html_content = response.text
                        logger.info(f"[{ticker}] 관련 섹션 HTML 추출 성공: {title}")
                        break
                except Exception as e:
                    logger.warning(f"[{ticker}] HTML 추출 실패 ({title}): {e}")
        
        # 관련 섹션을 못 찾으면 "사업의 내용" 섹션 사용
        if not html_content:
            for idx, row in sub_docs.iterrows():
                title = str(row.get('title', ''))
                url = row.get('url', '')
                
                if '사업의 내용' in title or '사업의 개요' in title:
                    try:
                        response = requests.get(url, timeout=30)
                        if response.status_code == 200:
                            html_content = response.text
                            logger.info(f"[{ticker}] 사업의 내용 섹션 HTML 추출 성공: {title}")
                            break
                    except Exception as e:
                        logger.warning(f"[{ticker}] HTML 추출 실패 ({title}): {e}")
        
        # 그래도 없으면 첫 번째 문서 사용
        if not html_content and len(sub_docs) > 0:
            first_url = sub_docs.iloc[0].get('url', '')
            if first_url:
                try:
                    response = requests.get(first_url, timeout=30)
                    if response.status_code == 200:
                        html_content = response.text
                        logger.info(f"[{ticker}] 첫 번째 문서 HTML 추출 성공")
                except Exception as e:
                    logger.warning(f"[{ticker}] HTML 추출 실패: {e}")
        
        return html_content
    except Exception as e:
        logger.error(f"[{ticker}] 원문 HTML 가져오기 실패: {e}")
        return None


def analyze_failure_causes():
    """
    Top20 실패 12개 기업 원인 분석
    """
    if not DART_API_KEY:
        print("❌ DART_API_KEY가 설정되지 않았습니다.", flush=True)
        return None
    
    if not OPENAI_API_KEY:
        print("❌ OPENAI_API_KEY가 설정되지 않았습니다.", flush=True)
        return None
    
    dart_parser = DartParser(DART_API_KEY)
    llm_handler = LLMHandler()
    
    print("=" * 80, flush=True)
    print("Top20 실패 12개 기업 원인 분석", flush=True)
    print("=" * 80, flush=True)
    
    # 실패한 기업 정보 로드
    report_file = 'reports/refetch_revenue_monitoring.json'
    failed_companies = []
    
    if os.path.exists(report_file):
        with open(report_file, 'r', encoding='utf-8') as f:
            report = json.load(f)
        
        results = report.get('results', [])
        for result in results:
            if result.get('status') == 'FAIL' and result.get('ticker') in FAILED_TICKERS:
                failed_companies.append({
                    'ticker': result.get('ticker'),
                    'name': result.get('name'),
                    'error_code': result.get('error_code'),
                    'failure_cause': result.get('failure_cause')
                })
    else:
        # 리포트 파일이 없으면 티커만으로 분석
        print(f"⚠️  리포트 파일이 없습니다. 티커만으로 분석합니다.", flush=True)
        for ticker in FAILED_TICKERS:
            failed_companies.append({
                'ticker': ticker,
                'name': f"기업_{ticker}",
                'error_code': 'NO_REVENUE_DATA',
                'failure_cause': 'B'
            })
    
    print(f"\n분석 대상: {len(failed_companies)}개 기업", flush=True)
    print("=" * 80, flush=True)
    
    analysis_results = []
    
    for idx, company in enumerate(failed_companies, 1):
        ticker = company['ticker']
        name = company['name']
        
        print(f"\n[{idx}/{len(failed_companies)}] {name} ({ticker}) 분석 중...", flush=True)
        
        result = {
            'ticker': ticker,
            'name': name,
            'error_code': company.get('error_code'),
            'failure_cause': company.get('failure_cause'),
            'step1_html_table_check': None,
            'step2_extracted_text_check': None,
            'step3_llm_extraction_test': None,
            'diagnosis': None
        }
        
        # Step 1: 원문 HTML에서 표 존재 여부 확인
        print(f"  [Step 1] 원문 HTML에서 표 존재 여부 확인 중...", flush=True)
        try:
            raw_html = get_raw_html_from_dart(dart_parser, ticker, year=2024)
            html_check = check_table_in_html(raw_html, ticker, name) if raw_html else {
                'has_table': False,
                'table_count': 0,
                'revenue_keywords_found': [],
                'table_preview': None
            }
            result['step1_html_table_check'] = html_check
            
            if html_check['has_table']:
                print(f"    ✅ 표 발견: {html_check['table_count']}개", flush=True)
                if html_check['revenue_keywords_found']:
                    print(f"    ✅ 매출 관련 키워드: {', '.join(html_check['revenue_keywords_found'][:5])}", flush=True)
            else:
                print(f"    ❌ 표 없음", flush=True)
        except Exception as e:
            print(f"    ❌ 오류: {e}", flush=True)
            result['step1_html_table_check'] = {'error': str(e)}
        
        # Step 2: extract_key_sections로 추출한 텍스트 확인
        print(f"  [Step 2] extract_key_sections 추출 텍스트 확인 중...", flush=True)
        try:
            extracted_text = dart_parser.extract_key_sections(ticker, target_year=2024)
            text_check = check_table_in_extracted_text(extracted_text, ticker, name) if extracted_text else {
                'has_table_markers': False,
                'table_marker_count': 0,
                'revenue_keywords_found': [],
                'text_preview': None
            }
            result['step2_extracted_text_check'] = text_check
            
            if text_check['has_table_markers']:
                print(f"    ✅ 표 마커 발견: {text_check['table_marker_count']}개", flush=True)
            else:
                print(f"    ❌ 표 마커 없음", flush=True)
            
            if text_check['revenue_keywords_found']:
                print(f"    ✅ 매출 관련 키워드: {', '.join(text_check['revenue_keywords_found'][:5])}", flush=True)
            else:
                print(f"    ❌ 매출 관련 키워드 없음", flush=True)
        except Exception as e:
            print(f"    ❌ 오류: {e}", flush=True)
            result['step2_extracted_text_check'] = {'error': str(e)}
        
        # Step 3: LLM 추출 시도
        print(f"  [Step 3] LLM 추출 시도 중...", flush=True)
        try:
            extracted_text = dart_parser.extract_key_sections(ticker, target_year=2024)
            llm_test = test_llm_extraction(extracted_text, ticker, name, llm_handler) if extracted_text else {
                'llm_success': False,
                'revenue_by_segment': None,
                'error': 'NO_EXTRACTED_TEXT'
            }
            result['step3_llm_extraction_test'] = llm_test
            
            if llm_test['llm_success']:
                revenue_data = llm_test['revenue_by_segment']
                print(f"    ✅ LLM 추출 성공: {len(revenue_data)}개 세그먼트", flush=True)
                print(f"    세그먼트: {list(revenue_data.keys())[:3]}...", flush=True)
            else:
                print(f"    ❌ LLM 추출 실패: {llm_test.get('error', 'UNKNOWN')}", flush=True)
        except Exception as e:
            print(f"    ❌ 오류: {e}", flush=True)
            result['step3_llm_extraction_test'] = {'error': str(e)}
        
        # 진단
        diagnosis = []
        step1 = result.get('step1_html_table_check', {})
        step2 = result.get('step2_extracted_text_check', {})
        step3 = result.get('step3_llm_extraction_test', {})
        
        if step1.get('has_table'):
            if not step2.get('has_table_markers') and not step2.get('revenue_keywords_found'):
                diagnosis.append("원인 A: 원문에 표가 있지만 extract_key_sections가 표를 포함하지 않음")
            elif step2.get('has_table_markers') or step2.get('revenue_keywords_found'):
                if not step3.get('llm_success'):
                    diagnosis.append("원인 B: 표는 포함되었지만 LLM이 revenue_by_segment를 추출하지 못함")
                else:
                    diagnosis.append("✅ 모든 단계 통과: LLM 추출 성공 (재수집 가능)")
            else:
                diagnosis.append("원인 불명: 표는 포함되었지만 키워드/마커 없음")
        else:
            diagnosis.append("원인 A: 원문에 표가 없음 (DART 보고서 구조 문제)")
        
        result['diagnosis'] = diagnosis
        print(f"  [진단] {', '.join(diagnosis)}", flush=True)
        
        analysis_results.append(result)
        
        # Rate limiting
        time.sleep(1)
    
    # 결과 저장
    os.makedirs('reports', exist_ok=True)
    output_file = 'reports/failure_cause_analysis.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(analysis_results, f, ensure_ascii=False, indent=2, default=str)
    
    print("\n" + "=" * 80, flush=True)
    print("분석 결과 요약", flush=True)
    print("=" * 80, flush=True)
    
    # 원인별 통계
    cause_a_count = sum(1 for r in analysis_results if '원인 A' in str(r.get('diagnosis', [])))
    cause_b_count = sum(1 for r in analysis_results if '원인 B' in str(r.get('diagnosis', [])))
    success_count = sum(1 for r in analysis_results if '✅ 모든 단계 통과' in str(r.get('diagnosis', [])))
    
    print(f"\n[원인별 분포]", flush=True)
    print(f"  원인 A (DART/섹션 추출 문제): {cause_a_count}개", flush=True)
    print(f"  원인 B (LLM 추출 문제): {cause_b_count}개", flush=True)
    print(f"  재수집 가능 (LLM 추출 성공): {success_count}개", flush=True)
    
    print(f"\n✅ 분석 결과 저장: {output_file}", flush=True)
    print("=" * 80, flush=True)
    
    return analysis_results


if __name__ == '__main__':
    analyze_failure_causes()

