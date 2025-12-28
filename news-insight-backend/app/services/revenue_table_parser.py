"""
Revenue Table Parser Service

LLM 없이 HTML 테이블에서 매출 비중을 직접 추출하는 결정론적 파서
하이브리드 전략: HTML 파싱 → 정규식 → LLM Fallback
"""
import re
import logging
from typing import Dict, Optional, List, Tuple, Any
from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)


def detect_consolidated_structure(html_content: str, markdown_text: str) -> Tuple[bool, List[str], Dict[str, Any]]:
    """
    연결 재무제표 구조 감지 (표 구조 신호 중심)
    
    Returns:
        (is_consolidated, subsidiary_names, evidence)
    """
    evidence = {}
    subsidiary_names = []
    
    if not html_content and not markdown_text:
        return False, [], {}
    
    # 텍스트 결합
    combined_text = (markdown_text or "") + (html_content or "")
    
    # 신호 1: 법인명 패턴 (보조 신호)
    subsidiary_patterns = [
        r'([가-힣A-Za-z0-9\s]+㈜)',  # SK온㈜
        r'\[([가-힣A-Za-z0-9\s]+㈜)\]',  # [SK온㈜]
        r'\(([가-힣A-Za-z0-9\s]+㈜)\)',  # (SK온㈜)
    ]
    for pattern in subsidiary_patterns:
        matches = re.findall(pattern, combined_text)
        subsidiary_names.extend(matches)
    
    unique_subsidiaries = list(set([s.strip() for s in subsidiary_names if s.strip()]))
    evidence['subsidiary_count'] = len(unique_subsidiaries)
    evidence['subsidiaries'] = unique_subsidiaries
    
    # 신호 2: 연결 재무제표 키워드 (주 신호)
    consolidated_keywords = [
        '연결조정', '내부거래', '조정전', '조정후', '연결합계',
        '연결 재무제표', '연결매출', '종속회사', '자회사', '계열사'
    ]
    found_keywords = [kw for kw in consolidated_keywords if kw in combined_text]
    evidence['consolidated_keywords'] = found_keywords
    
    # 신호 3: 법인 헤더 반복 패턴 (주 신호)
    # 예: "[SK온㈜]", "[SK에너지㈜]" 같은 헤더가 반복되는 경우
    header_pattern = r'\[([가-힣A-Za-z0-9\s]+(?:㈜|\(주\)|주식회사))\]'
    header_matches = re.findall(header_pattern, combined_text)
    evidence['header_pattern_count'] = len(set(header_matches))
    
    # 신호 4: 매출/비중 컬럼 존재 (주 신호)
    revenue_keywords = ['매출', '수익', '비중', '%', '사업부문', '영업의 종류', '주요 제품']
    found_revenue_keywords = [kw for kw in revenue_keywords if kw in combined_text]
    evidence['revenue_keywords'] = found_revenue_keywords
    
    # 판정: 주 신호 2개 이상 또는 (보조 신호 + 주 신호 1개)
    main_signals = (
        len(found_keywords) > 0,
        evidence['header_pattern_count'] >= 2,
        len(found_revenue_keywords) >= 2
    )
    main_signal_count = sum(main_signals)
    
    is_consolidated = (
        main_signal_count >= 2 or  # 주 신호 2개 이상
        (len(unique_subsidiaries) >= 2 and main_signal_count >= 1)  # 보조 신호 + 주 신호
    )
    
    evidence['is_consolidated'] = is_consolidated
    evidence['main_signal_count'] = main_signal_count
    
    return is_consolidated, unique_subsidiaries, evidence


def detect_unit_scale(table: Tag, context_text: str = "") -> float:
    """
    단위 인식 (단위: 백만원, 억원, 천원 등)
    
    Returns:
        scale factor (예: 1000000 for 백만원, 100000000 for 억원)
    """
    # 테이블 주변 텍스트에서 단위 찾기
    unit_patterns = [
        (r'단위\s*:\s*백만원', 1000000),
        (r'단위\s*:\s*억원', 100000000),
        (r'단위\s*:\s*천원', 1000),
        (r'단위\s*:\s*원', 1),
        (r'\(백만원\)', 1000000),
        (r'\(억원\)', 100000000),
        (r'\(천원\)', 1000),
    ]
    
    # 테이블 내부에서 단위 찾기
    table_text = table.get_text()
    search_text = context_text + " " + table_text
    
    for pattern, scale in unit_patterns:
        if re.search(pattern, search_text, re.IGNORECASE):
            logger.debug(f"단위 감지: {pattern} → scale={scale}")
            return scale
    
    # 기본값: 백만원 (대부분의 DART 보고서)
    return 1000000


def select_candidate_tables(html_content: str) -> List[Tag]:
    """
    1단계: 후보 테이블 선택
    
    조건:
    - 매출/수익/비중 키워드 2개 이상 포함
    - 숫자열(금액) + 비중열(%) 존재
    """
    if not html_content:
        return []
    
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')
    
    candidate_tables = []
    revenue_keywords = ['매출', '수익', '영업', '비중', '%', '사업부문', '영업의 종류', '주요 제품', '부문별']
    
    for table in tables:
        table_text = table.get_text()
        table_text_lower = table_text.lower()
        
        # 키워드 확인 (2개 이상)
        keyword_count = sum(1 for kw in revenue_keywords if kw in table_text_lower)
        if keyword_count < 2:
            continue
        
        # 숫자와 % 존재 확인
        has_numbers = bool(re.search(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?', table_text))
        has_percentages = bool(re.search(r'\d{1,2}(?:\.\d+)?\s*%', table_text))
        
        if has_numbers and has_percentages:
            candidate_tables.append(table)
            logger.debug(f"후보 테이블 선택: 키워드 {keyword_count}개, 숫자/비율 존재")
    
    return candidate_tables


def identify_table_schema(table: Tag) -> Optional[Dict[str, Any]]:
    """
    2단계: 테이블 스키마 판별
    
    Returns:
        {
            'has_ratio_column': bool,
            'has_amount_column': bool,
            'period_columns': List[str],  # 기간별 컬럼 (예: ['2025Q3', '2024', '2023'])
            'segment_column_idx': int,  # 사업부문명 컬럼 인덱스
            'ratio_column_idx': int,  # 비중 컬럼 인덱스
            'adjustment_rows': List[int],  # 조정/내부거래 행 인덱스
        } 또는 None
    """
    rows = table.find_all('tr')
    if len(rows) < 2:
        return None
    
    # 헤더 행 찾기
    header_row = rows[0]
    headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
    
    if not headers:
        return None
    
    schema = {
        'has_ratio_column': False,
        'has_amount_column': False,
        'period_columns': [],
        'segment_column_idx': 0,  # 기본값: 첫 번째 컬럼
        'ratio_column_idx': -1,
        'adjustment_rows': [],
        'headers': headers
    }
    
    # 🆕 P0-1: 사업 부문 컬럼 자동 감지 (다단 헤더 대응)
    segment_column_idx = 0  # 기본값
    segment_keywords_priority = [
        # 1순위: 명확한 사업부문 키워드
        ['사업 부문', '사업부문', 'business segment', 'segment', '영업부문'],
        # 2순위: 구분/부문
        ['구분', '부문', 'category', 'division'],
        # 3순위: 회사명/법인명 (지주사형 구조 대응)
        ['회사명', '법인명', '주요 종속회사', 'subsidiary', 'company'],
        # 4순위: 품목/제품 (최후 수단)
        ['품 목', '품목', 'item', '제품', 'product']
    ]
    
    # 다단 헤더 처리: 상단 1-3행을 세로로 병합
    header_rows = rows[:min(3, len(rows))]
    merged_headers = []
    
    # 컬럼 수 확인 (첫 번째 행 기준)
    max_cols = len(headers)
    for col_idx in range(max_cols):
        col_texts = []
        for header_row in header_rows:
            cells = header_row.find_all(['th', 'td'])
            if col_idx < len(cells):
                cell_text = cells[col_idx].get_text(strip=True)
                if cell_text:
                    col_texts.append(cell_text)
        merged_header = ' '.join(col_texts).lower()
        merged_headers.append(merged_header)
    
    # 우선순위에 따라 사업 부문 컬럼 찾기
    for priority_group in segment_keywords_priority:
        for i, merged_header in enumerate(merged_headers):
            if any(kw.lower() in merged_header for kw in priority_group):
                segment_column_idx = i
                logger.debug(f"사업부문 컬럼 감지: 인덱스 {i}, 헤더: {merged_header}")
                break
        if segment_column_idx != 0 or any(kw in merged_headers[0] for kw in segment_keywords_priority[0]):  # 기본값이 아니거나 1순위 키워드가 첫 컬럼에 있으면
            break
    
    schema['segment_column_idx'] = segment_column_idx
    
    # 비중 컬럼 찾기
    for i, header in enumerate(headers):
        header_lower = header.lower()
        if '비중' in header_lower or '%' in header_lower:
            schema['has_ratio_column'] = True
            schema['ratio_column_idx'] = i
        if '매출' in header_lower or '수익' in header_lower or '금액' in header_lower:
            schema['has_amount_column'] = True
        # 기간 패턴 (2025, 2024, Q3, 분기 등)
        if re.search(r'20\d{2}|Q\d|분기|기', header):
            schema['period_columns'].append(header)
    
    # 조정/내부거래 행 찾기
    for i, row in enumerate(rows[1:], start=1):
        row_text = row.get_text().lower()
        if any(kw in row_text for kw in ['조정', '내부거래', '연결조정', '제거', '합계']):
            schema['adjustment_rows'].append(i)
    
    # 스키마 유효성 검증
    if not (schema['has_ratio_column'] or schema['has_amount_column']):
        return None
    
    return schema


def extract_revenue_from_table(table: Tag, schema: Dict[str, Any], unit_scale: float = 1000000) -> Optional[Dict[str, float]]:
    """
    3단계: 값 추출
    
    Args:
        table: BeautifulSoup Table 태그
        schema: identify_table_schema() 결과
        unit_scale: 단위 스케일 (기본값: 백만원)
    
    Returns:
        {"석유사업": 67.0, "화학사업": 14.0, ...} 또는 None
    """
    rows = table.find_all('tr')
    if len(rows) < 2:
        return None
    
    revenue_data = {}
    segment_column_idx = schema.get('segment_column_idx', 0)
    ratio_column_idx = schema.get('ratio_column_idx', -1)
    adjustment_rows = set(schema.get('adjustment_rows', []))
    
    # 🆕 P0-2: 계층 구조 처리 - 현재 사업 부문 추적
    current_business_segment = None
    
    # 현재 기간 컬럼 선택 (가장 왼쪽 또는 가장 최근)
    # TODO: 기간별 컬럼이 여러 개일 때 우선순위 로직 추가
    
    for i, row in enumerate(rows[1:], start=1):
        # 조정/내부거래 행 제외
        if i in adjustment_rows:
            continue
        
        cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
        if len(cells) <= segment_column_idx:
            continue
        
        # 🆕 P0-2: 사업 부문 추출 (계층 구조 처리)
        segment_name = cells[segment_column_idx].strip()
        
        # 상위 행이 "소계/합계"인지 확인 (상속 금지)
        is_total_row = any(kw in segment_name for kw in ['합계', '소계', 'Total', 'Subtotal', '연결조정', '내부거래'])
        
        # 사업 부문 컬럼이 비어있으면 이전 행의 사업 부문 상속
        if not segment_name or segment_name == '':
            if current_business_segment and not is_total_row:
                segment_name = current_business_segment
            else:
                continue
        else:
            # 사업 부문이 있으면 업데이트
            # "배터리사업", "석유사업" 같은 상위 부문 저장
            if any(kw in segment_name for kw in ['사업', '부문', 'business']) and not is_total_row:
                current_business_segment = segment_name
            # 하위 품목인 경우 (들여쓰기, '-', '·' 패턴)
            elif current_business_segment and any(indicator in segment_name for indicator in ['-', '·', '①', '②', '③']):
                # parent::child 형태로 보존
                segment_name = f"{current_business_segment}::{segment_name}"
        
        # 기존 필터링 로직
        if segment_name in ['합계', '소계', '기타']:
            continue
        
        # 조정항목 키워드 확인
        adjustment_keywords = ['연결조정', '내부거래', '조정', '제거']
        if any(kw in segment_name for kw in adjustment_keywords):
            continue
        
        # 비중(%) 추출
        percentage = None
        
        # 방법 1: 비중 컬럼이 있으면 직접 사용
        if ratio_column_idx >= 0 and ratio_column_idx < len(cells):
            ratio_text = cells[ratio_column_idx]
            pct_match = re.search(r'(\d{1,2}(?:\.\d+)?)\s*%', ratio_text)
            if pct_match:
                percentage = float(pct_match.group(1))
        
        # 방법 2: 비중 컬럼이 없으면 모든 셀에서 % 찾기
        if percentage is None:
            for cell in cells:
                pct_match = re.search(r'(\d{1,2}(?:\.\d+)?)\s*%', cell)
                if pct_match:
                    candidate_pct = float(pct_match.group(1))
                    # 합리적인 범위 (0-100%)
                    if 0 < candidate_pct <= 100:
                        percentage = candidate_pct
                        break
        
        # 방법 3: 금액 컬럼이 있으면 비중 계산
        if percentage is None and schema.get('has_amount_column'):
            # TODO: 금액 기반 비중 계산 로직 추가
            pass
        
        if percentage and 0 < percentage <= 100:
            # 중복 제거 (같은 세그먼트면 더 큰 값 사용)
            if segment_name not in revenue_data or revenue_data[segment_name] < percentage:
                revenue_data[segment_name] = percentage
    
    # 최소 2개 세그먼트 필요
    if len(revenue_data) < 2:
        return None
    
    # 합계 검증 (70-130% 범위)
    total = sum(revenue_data.values())
    if not (70.0 <= total <= 130.0):
        logger.debug(f"테이블 파싱: 합계 범위 초과 ({total:.1f}%)")
        return None
    
    return revenue_data


def split_by_company_headers(html_content: str) -> List[Dict[str, Any]]:
    """
    법인 헤더 기반 분할 (SK이노 케이스)
    
    Returns:
        [
            {
                'company_name': 'SK온㈜',
                'html_section': str,
                'tables': List[Tag]
            },
            ...
        ]
    """
    if not html_content:
        return []
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # 법인 헤더 패턴 찾기
    header_pattern = r'\[([가-힣A-Za-z0-9\s]+(?:㈜|\(주\)|주식회사))\]'
    
    # HTML을 텍스트로 변환하여 헤더 위치 찾기
    html_text = str(soup)
    header_matches = list(re.finditer(header_pattern, html_text))
    
    if len(header_matches) < 2:
        # 법인 헤더가 2개 미만이면 분할 불필요
        return [{
            'company_name': None,
            'html_section': html_content,
            'tables': soup.find_all('table')
        }]
    
    # 헤더 위치 기반으로 HTML 분할
    sections = []
    for i, match in enumerate(header_matches):
        start_pos = match.start()
        end_pos = header_matches[i + 1].start() if i + 1 < len(header_matches) else len(html_text)
        
        section_html = html_text[start_pos:end_pos]
        section_soup = BeautifulSoup(section_html, 'html.parser')
        
        sections.append({
            'company_name': match.group(1),
            'html_section': section_html,
            'tables': section_soup.find_all('table')
        })
    
    return sections


def consolidate_by_business_segment(revenue_data_list: List[Dict[str, float]]) -> Dict[str, float]:
    """
    자회사별 매출을 사업부문별로 통합
    
    Args:
        revenue_data_list: [{'SK온': {'배터리': 100}}, {'SK에너지': {'석유': 100}}, ...]
    
    Returns:
        {'석유사업': 67.0, '화학사업': 14.0, '배터리사업': 19.0}
    """
    # 사업부문 매핑 (자회사 → 상위 사업부문)
    company_to_segment = {
        'SK온': '배터리',
        'SK에너지': '석유',
        'SK지오센트릭': '화학',
        'SK인천석유화학': '화학',
        'SK엔무브': '석유',
        # TODO: 더 많은 매핑 추가
    }
    
    consolidated = {}
    
    for revenue_data in revenue_data_list:
        for segment, pct in revenue_data.items():
            # 자회사명이 세그먼트명에 포함되어 있으면 매핑
            mapped_segment = segment
            for company, business_segment in company_to_segment.items():
                if company in segment:
                    mapped_segment = f"{business_segment}사업"
                    break
            
            # 통합
            if mapped_segment not in consolidated:
                consolidated[mapped_segment] = 0.0
            consolidated[mapped_segment] += pct
    
    # 100%로 정규화
    total = sum(consolidated.values())
    if total > 0:
        consolidated = {k: (v / total * 100) for k, v in consolidated.items()}
    
    return consolidated


def extract_revenue_from_financial_holding_tables(html_content: str, ticker: Optional[str] = None) -> Optional[Dict[str, float]]:
    """
    금융지주 전용: 사업부문별 매출 비중 추출 (KB금융, 신한지주 등)
    
    타겟: "영업의 현황 > 영업의 종류" 테이블에서 은행/보험/증권/카드 부문 추출
    자회사명 포함 매핑: "KB국민은행", "신한라이프" 등도 "은행", "보험"으로 매핑
    
    Returns:
        {"은행부문": 62.3, "보험부문": 18.1, "증권부문": 12.5, "카드부문": 7.1} 또는 None
    """
    if not html_content:
        return None
    
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')
    
    # 금융지주 사업부문 키워드 (은행/보험/증권/카드)
    financial_business_keywords = [
        '은행', '보험', '증권', '카드', '캐피탈', '저축은행',
        '생명', '손해', '화재', '금융투자', '자산운용'
    ]
    
    # 자회사명 → 사업부문 매핑 (KB국민은행 → 은행, 신한라이프 → 보험 등)
    company_to_segment_map = {
        '국민은행': '은행', 'KB국민은행': '은행', 'KB': '은행',
        '신한은행': '은행', '하나은행': '은행', '우리은행': '은행',
        '라이프': '보험', '생명': '보험', '화재': '보험',
        '신한라이프': '보험', '삼성생명': '보험', '삼성화재': '보험',
        '증권': '증권', '투자증권': '증권', '금융투자': '증권',
        '카드': '카드', '신용카드': '카드'
    }
    
    # "영업의 현황 > 영업의 종류" 또는 "사업부문별" 테이블 찾기
    for table in tables:
        table_text = table.get_text()
        
        # 금융지주 사업부문 키워드 확인
        has_business_segments = any(kw in table_text for kw in financial_business_keywords)
        has_segment_keywords = any(kw in table_text for kw in ['사업부문', '영업의 종류', '부문별', '영업부문'])
        
        if not (has_business_segments and has_segment_keywords):
            continue
        
        # 테이블 파싱
        rows = table.find_all('tr')
        if len(rows) < 2:
            continue
        
        revenue_data = {}
        
        # 헤더 행 확인
        header_row = rows[0]
        headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
        
        # 비중(%) 컬럼 찾기
        ratio_col_idx = -1
        for i, header in enumerate(headers):
            if '비중' in header or '%' in header:
                ratio_col_idx = i
                break
        
        # 매출액/영업수익 컬럼 찾기
        amount_col_idx = -1
        for i, header in enumerate(headers):
            if any(kw in header for kw in ['매출', '영업수익', '수익', '금액']):
                amount_col_idx = i
                break
        
        # 각 행 파싱
        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
            if len(cells) < 2:
                continue
            
            # 첫 번째 셀이 부문명/자회사명
            first_cell = cells[0].strip()
            
            # 사업부문명 추출 (은행, 보험, 증권, 카드 등)
            segment_name = None
            
            # 방법 1: 직접 키워드 매칭
            for keyword in financial_business_keywords:
                if keyword in first_cell:
                    if keyword in ['생명', '손해', '화재']:
                        segment_name = '보험'
                    elif keyword in ['금융투자', '자산운용']:
                        segment_name = '증권'
                    else:
                        segment_name = keyword
                    break
            
            # 방법 2: 자회사명 매핑
            if not segment_name:
                for company, segment in company_to_segment_map.items():
                    if company in first_cell:
                        segment_name = segment
                        break
            
            if not segment_name:
                continue
            
            # 비중(%) 추출
            percentage = None
            
            # 방법 1: 비중 컬럼에서 직접 추출
            if ratio_col_idx >= 0 and ratio_col_idx < len(cells):
                ratio_text = cells[ratio_col_idx]
                pct_match = re.search(r'(\d{1,2}(?:\.\d+)?)\s*%', ratio_text)
                if pct_match:
                    percentage = float(pct_match.group(1))
            
            # 방법 2: 모든 셀에서 % 찾기
            if percentage is None:
                for cell in cells:
                    pct_match = re.search(r'(\d{1,2}(?:\.\d+)?)\s*%', cell)
                    if pct_match:
                        candidate_pct = float(pct_match.group(1))
                        if 0 < candidate_pct <= 100:
                            percentage = candidate_pct
                            break
            
            # 방법 3: 금액 기반 비중 계산
            if percentage is None and amount_col_idx >= 0 and amount_col_idx < len(cells):
                # 금액 추출 및 비중 계산 (전체 합계 필요)
                pass  # TODO: 금액 기반 비중 계산 로직 추가
            
            if percentage and 0 < percentage <= 100:
                # 부문명 정규화 (은행부문, 은행 부문 → 은행부문)
                normalized_segment = f"{segment_name}부문"
                if normalized_segment not in revenue_data:
                    revenue_data[normalized_segment] = 0.0
                revenue_data[normalized_segment] += percentage
        
        # 최소 2개 부문 필요
        if len(revenue_data) >= 2:
            # 합계 검증
            total_pct = sum(revenue_data.values())
            if 70.0 <= total_pct <= 130.0:
                logger.info(f"[{ticker or 'N/A'}] 금융지주 사업부문별 파싱 성공: {revenue_data}")
                return revenue_data
    
    return None


def extract_revenue_from_financial_tables(html_content: str, ticker: Optional[str] = None) -> Optional[Dict[str, float]]:
    """
    금융사 전용: 부문별 영업수지 테이블에서 매출 비중 추출
    
    카카오뱅크 케이스: 이자 부문, 수수료 부문, 신탁 부문, 기타 부문
    (수익원 기반 - 이건 revenue_driver로 별도 저장 필요)
    
    Returns:
        {"이자부문": 85.0, "수수료부문": 10.0, "기타부문": 5.0} 또는 None
    """
    if not html_content:
        return None
    
    soup = BeautifulSoup(html_content, 'html.parser')
    tables = soup.find_all('table')
    
    # "부문별 영업수지" 또는 "부문별 영업실적" 테이블 찾기
    for table in tables:
        table_text = table.get_text()
        
        # 금융사 부문 키워드 확인
        financial_segment_keywords = ['이자 부문', '수수료 부문', '신탁 부문', '기타', '부문별 영업수지', '부문별 영업실적']
        has_financial_segments = any(kw in table_text for kw in financial_segment_keywords)
        
        if not has_financial_segments:
            continue
        
        # 테이블 파싱
        rows = table.find_all('tr')
        if len(rows) < 2:
            continue
        
        revenue_data = {}
        segment_names = []
        
        # 헤더 행에서 부문명 찾기
        header_row = rows[0]
        headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
        
        # 부문명 행 찾기 (이자 부문, 수수료 부문 등)
        for i, row in enumerate(rows[1:], start=1):
            cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
            if len(cells) < 2:
                continue
            
            # 첫 번째 셀이 부문명인지 확인
            first_cell = cells[0].strip()
            
            # 금융사 부문명 패턴
            if any(kw in first_cell for kw in ['이자', '수수료', '신탁', '기타']) and '부문' in first_cell:
                segment_name = first_cell.replace('부문', '').strip()
                if segment_name:
                    segment_names.append((i, segment_name, cells))
        
        # 부문별 소계 행 찾기 (A-B, C-D 등)
        for row_idx, segment_name, cells in segment_names:
            # 소계 행 찾기 (보통 부문명 다음 행)
            if row_idx + 1 < len(rows):
                subtotal_row = rows[row_idx + 1]
                subtotal_cells = [td.get_text(strip=True) for td in subtotal_row.find_all(['td', 'th'])]
                
                # 소계 값 찾기 (숫자)
                for cell in subtotal_cells[1:]:  # 첫 번째 셀 제외
                    # 숫자 추출 (억원 단위)
                    amount_match = re.search(r'([\d,]+)', cell.replace(',', ''))
                    if amount_match:
                        try:
                            amount = float(amount_match.group(1).replace(',', ''))
                            if amount > 0:
                                revenue_data[segment_name] = amount
                                break
                        except:
                            continue
        
        # 금액을 비중(%)으로 변환
        if revenue_data and len(revenue_data) >= 2:
            total = sum(revenue_data.values())
            if total > 0:
                revenue_data = {k: (v / total * 100) for k, v in revenue_data.items()}
                
                # 합계 검증
                total_pct = sum(revenue_data.values())
                if 70.0 <= total_pct <= 130.0:
                    logger.info(f"[{ticker or 'N/A'}] 금융사 부문별 파싱 성공: {revenue_data}")
                    return revenue_data
    
    return None


def extract_revenue_from_consolidated_tables(
    html_content: str,
    markdown_text: str = "",
    ticker: Optional[str] = None,
    is_financial: bool = False
) -> Optional[Dict[str, float]]:
    """
    연결 재무제표 테이블에서 직접 매출 추출 (LLM 없이)
    
    하이브리드 전략:
    1. 금융사인 경우: 금융사 전용 파싱 시도
    2. 연결 구조 감지
    3. 법인 헤더 기반 분할 (여러 자회사가 있는 경우)
    4. 각 구간에서 테이블 파싱
    5. 사업부문별 통합
    
    Args:
        html_content: HTML 내용
        markdown_text: 마크다운 텍스트
        ticker: 종목코드
        is_financial: 금융사 여부
    
    Returns:
        {"석유사업": 67.0, "화학사업": 14.0, "배터리사업": 19.0} 또는 None
    """
    if not html_content:
        return None
    
    # Step 0: 금융사 전용 파싱 시도 (카카오뱅크 케이스)
    if is_financial:
        financial_revenue = extract_revenue_from_financial_tables(html_content, ticker)
        if financial_revenue:
            return financial_revenue
    
    # Step 1: 연결 구조 감지 (금융사가 아닌 경우 또는 금융사 파싱 실패 시)
    is_consolidated, subsidiaries, evidence = detect_consolidated_structure(html_content, markdown_text)
    
    # 연결 구조가 아니어도 단일 구간 파싱 시도 (SK이노베이션 케이스)
    if not is_consolidated:
        logger.debug(f"[{ticker or 'N/A'}] 연결 재무제표 구조 미감지, 단일 구간 파싱 시도")
        # 단일 구간 파싱 시도
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all('table')
        revenue_data = _parse_section_tables(tables, html_content)
        if revenue_data and len(revenue_data) >= 2:
            logger.info(f"[{ticker or 'N/A'}] 단일 구간 파싱 성공: {revenue_data}")
            return revenue_data
        return None
    
    logger.info(f"[{ticker or 'N/A'}] 연결 재무제표 구조 감지: {len(subsidiaries)}개 자회사, evidence: {evidence}")
    
    # Step 2: 법인 헤더 기반 분할
    sections = split_by_company_headers(html_content)
    
    if len(sections) >= 2:
        # 여러 자회사 구간이 있으면 각각 파싱 후 통합
        logger.info(f"[{ticker or 'N/A'}] 법인 헤더 기반 분할: {len(sections)}개 구간")
        
        all_revenue_data = []
        for section in sections:
            section_revenue = _parse_section_tables(section['tables'], section.get('html_section', ''))
            if section_revenue:
                all_revenue_data.append(section_revenue)
        
        if all_revenue_data:
            # 사업부문별 통합
            consolidated = consolidate_by_business_segment(all_revenue_data)
            if len(consolidated) >= 2:
                logger.info(f"[{ticker or 'N/A'}] 법인별 파싱 후 통합 성공: {consolidated}")
                return consolidated
    else:
        # 단일 구간이면 직접 파싱
        revenue_data = _parse_section_tables(sections[0]['tables'] if sections else [], html_content)
        if revenue_data and len(revenue_data) >= 2:
            logger.info(f"[{ticker or 'N/A'}] 단일 구간 파싱 성공: {revenue_data}")
            return revenue_data
    
    return None


def _parse_section_tables(tables: List[Tag], context_html: str = "") -> Optional[Dict[str, float]]:
    """
    특정 구간의 테이블들을 파싱
    
    Returns:
        {"석유": 67.0, "화학": 14.0, ...} 또는 None
    """
    if not tables:
        return None
    
    # 단위 감지
    unit_scale = detect_unit_scale(tables[0] if tables else None, context_html)
    
    # 후보 테이블 선택
    candidate_tables = []
    for table in tables:
        table_text = table.get_text()
        revenue_keywords = ['매출', '수익', '영업', '비중', '%', '사업부문']
        keyword_count = sum(1 for kw in revenue_keywords if kw in table_text.lower())
        if keyword_count >= 2:
            candidate_tables.append(table)
    
    if not candidate_tables:
        return None
    
    # 각 후보 테이블 파싱 시도
    for table in candidate_tables:
        schema = identify_table_schema(table)
        if not schema:
            continue
        
        revenue_data = extract_revenue_from_table(table, schema, unit_scale)
        if revenue_data and len(revenue_data) >= 2:
            return revenue_data
    
    return None

