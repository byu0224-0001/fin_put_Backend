"""
DART Parser Service

DART API 호출, HTML 파싱, 마크다운 변환, 핵심 섹션 추출
이중 매핑 전략: 회사명 → 고유번호로 자동 재시도
"""
import logging
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from markdownify import markdownify as md
from typing import Optional, Dict, Any
import OpenDartReader
from app.services.retry_handler import retry_dart_api, dart_rate_limiter
from app.services.dart_corp_code_mapper import DartCorpCodeMapper

logger = logging.getLogger(__name__)


class DartParser:
    """DART API 파서 (이중 매핑 전략 지원)"""
    
    def __init__(self, api_key: str):
        """
        Args:
            api_key: DART API Key
        """
        self.api_key = api_key  # 직접 API 호출을 위해 저장
        self.dart = OpenDartReader(api_key)
        self.corp_code_mapper = DartCorpCodeMapper(api_key)  # 고유번호 매핑 서비스
    
    @retry_dart_api
    @dart_rate_limiter
    def list_reports(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        kind: str = 'A',
        final: bool = True,
        business_report_only: bool = False
    ) -> Optional[pd.DataFrame]:
        """
        DART API로 보고서 목록 조회 (이중 시도: 회사명 → 고유번호)
        
        Args:
            ticker: 종목코드
            start_date: 시작일 (YYYY-MM-DD)
            end_date: 종료일 (YYYY-MM-DD)
            kind: 공시 종류 ('A': 정기공시)
            final: 최종보고서만 조회 여부
            business_report_only: 사업보고서만 조회 여부 (True이면 pblntf_detail_ty='A001' 사용)
        
        Returns:
            보고서 목록 DataFrame 또는 None
        """
        # 사업보고서만 조회하는 경우, 고유번호 기반 직접 호출
        if business_report_only:
            return self._list_business_reports_direct(ticker, start_date, end_date, final)
        
        # 1차 시도: 기존 방식 (OpenDartReader - 회사명 기반)
        try:
            reports = self.dart.list(ticker, start=start_date, end=end_date, kind=kind, final=final)
            if reports is not None and len(reports) > 0:
                logger.debug(f"{ticker}: 회사명 기반 검색 성공 ({len(reports)}개 보고서)")
                return reports
        except Exception as e:
            logger.warning(f"{ticker}: 회사명 기반 검색 실패: {e}")
        
        # 2차 시도: 고유번호 기반 직접 호출
        logger.info(f"{ticker}: 회사명 기반 검색 실패, 고유번호 기반 재시도")
        return self._list_reports_by_corp_code(ticker, start_date, end_date, kind, final)
    
    def _list_business_reports_direct(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        final: bool = True
    ) -> Optional[pd.DataFrame]:
        """
        DART API 직접 호출로 사업보고서만 조회 (고유번호 기반)
        
        Args:
            ticker: 종목코드
            start_date: 시작일 (YYYY-MM-DD)
            end_date: 종료일 (YYYY-MM-DD)
            final: 최종보고서만 조회 여부
        
        Returns:
            사업보고서 목록 DataFrame 또는 None
        """
        # 고유번호 조회
        corp_code = self.corp_code_mapper.get_corp_code(ticker)
        
        if not corp_code:
            logger.error(f"{ticker}: 고유번호를 찾을 수 없습니다.")
            return None
        
        return self._list_reports_by_corp_code(
            ticker, start_date, end_date, 
            kind='A', final=final, 
            pblntf_detail_ty='A001'  # 사업보고서만
        )
    
    def _list_reports_by_corp_code(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        kind: str = 'A',
        final: bool = True,
        pblntf_detail_ty: Optional[str] = None
    ) -> Optional[pd.DataFrame]:
        """
        고유번호 기반으로 보고서 목록 조회
        
        Args:
            ticker: 종목코드
            start_date: 시작일 (YYYY-MM-DD)
            end_date: 종료일 (YYYY-MM-DD)
            kind: 공시 종류 ('A': 정기공시)
            final: 최종보고서만 조회 여부
            pblntf_detail_ty: 공시 상세 유형 (예: 'A001' = 사업보고서)
        
        Returns:
            보고서 목록 DataFrame 또는 None
        """
        try:
            # 고유번호 조회
            corp_code = self.corp_code_mapper.get_corp_code(ticker)
            
            if not corp_code:
                logger.error(f"{ticker}: 고유번호를 찾을 수 없습니다.")
                return None
            
            # 날짜 형식 변환 (YYYY-MM-DD -> YYYYMMDD)
            start_date_formatted = start_date.replace('-', '')
            end_date_formatted = end_date.replace('-', '')
            
            # DART API 직접 호출
            url = "https://opendart.fss.or.kr/api/list.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_code': corp_code,  # 고유번호 사용
                'bgn_de': start_date_formatted,
                'end_de': end_date_formatted,
                'pblntf_ty': kind,  # 'A': 정기공시
                'page_no': '1',
                'page_count': '100',
                'last_reprt_at': 'Y' if final else 'N',
                'sort': 'date',
                'sort_mth': 'desc'
            }
            
            # 사업보고서만 조회하는 경우
            if pblntf_detail_ty:
                params['pblntf_detail_ty'] = pblntf_detail_ty
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if data.get('status') != '000':
                error_msg = data.get('message', 'Unknown error')
                logger.error(f"DART API 오류 ({ticker}): {error_msg}")
                return None
            
            reports_list = data.get('list', [])
            if not reports_list:
                logger.debug(f"{ticker}: 고유번호 기반 검색 결과 없음")
                return None
            
            # DataFrame으로 변환
            df = pd.DataFrame(reports_list)
            logger.info(f"{ticker}: 고유번호 기반 검색 성공 ({len(df)}개 보고서)")
            return df
            
        except requests.RequestException as e:
            logger.error(f"고유번호 기반 보고서 조회 실패 ({ticker}): 네트워크 오류 - {e}")
            return None
        except Exception as e:
            logger.error(f"고유번호 기반 보고서 조회 실패 ({ticker}): {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return None
    
    @retry_dart_api
    @dart_rate_limiter
    def get_sub_docs(self, rcept_no: str) -> Optional[pd.DataFrame]:
        """
        보고서의 하위 문서 목록 조회
        
        Args:
            rcept_no: 접수번호
        
        Returns:
            하위 문서 목록 DataFrame 또는 None
        """
        try:
            sub_docs = self.dart.sub_docs(rcept_no)
            return sub_docs
        except Exception as e:
            logger.error(f"DART API 하위 문서 조회 실패 ({rcept_no}): {e}")
            return None
    
    def find_business_report(
        self,
        ticker: str,
        target_year: int = None
    ) -> Optional[Dict[str, Any]]:
        """
        사업보고서 찾기 (날짜 제한 없이 최신 보고서 가져오기)
        
        Args:
            ticker: 종목코드
            target_year: 대상 연도 (None이면 최신 보고서 가져오기)
        
        Returns:
            {
                'rcept_no': 접수번호,
                'report_nm': 보고서명,
                'rcept_dt': 접수일자 (YYYYMMDD 형식),
                'report': 보고서 정보 DataFrame
            } 또는 None
        """
        from datetime import datetime
        import re
        
        # 날짜 범위 설정: 최근 3년치 검색 (사업보고서는 보통 다음 해에 제출)
        current_year = datetime.now().year
        if target_year:
            # 특정 연도 지정 시: 해당 연도부터 현재까지 검색
            start_date = f'{target_year}-01-01'
        else:
            # 연도 미지정 시: 최근 3년치 검색
            start_date = f'{current_year - 3}-01-01'
        
        # 종료일은 현재 날짜로 설정 (제한 없음)
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        # 먼저 사업보고서만 조회 시도 (pblntf_detail_ty='A001' 사용)
        business_reports_only = self.list_reports(ticker, start_date, end_date, kind='A', final=True, business_report_only=True)
        business_reports = None
        
        if business_reports_only is not None and len(business_reports_only) > 0:
            business_reports = business_reports_only
            logger.info(f"{ticker}: 사업보고서 {len(business_reports)}개 발견")
        else:
            # 사업보고서가 없으면 정기 공시 전체 조회 (A001, A002, A003)
            logger.info(f"{ticker}: 사업보고서 없음. 정기 공시 전체 조회 시도...")
            all_regular = self.list_reports(ticker, start_date, end_date, kind='A', final=True, business_report_only=False)
            if all_regular is not None and len(all_regular) > 0:
                # 정기 공시만 필터링 (A001: 사업보고서, A002: 반기보고서, A003: 분기보고서)
                regular_types = ['A001', 'A002', 'A003']
                if 'pblntf_detail_ty' in all_regular.columns:
                    filtered_regular = all_regular[all_regular['pblntf_detail_ty'].isin(regular_types)]
                    if len(filtered_regular) > 0:
                        business_reports = filtered_regular
                        logger.info(f"{ticker}: 정기 공시 {len(business_reports)}개 발견")
                    else:
                        logger.warning(f"{ticker}: 정기 공시를 찾을 수 없습니다.")
                        return None
                else:
                    # pblntf_detail_ty 컬럼이 없으면 모두 사용
                    business_reports = all_regular
                    logger.info(f"{ticker}: 정기 공시 {len(business_reports)}개 발견 (타입 필터링 없음)")
            else:
                logger.warning(f"{ticker}: 정기 공시를 찾을 수 없습니다.")
                return None
        
        if business_reports is None or len(business_reports) == 0:
            logger.warning(f"{ticker}: 보고서를 찾지 못했습니다.")
            return None
        
        # 보고서 제목에서 연도 추출 함수
        def extract_year_from_title(title: str) -> Optional[int]:
            """보고서 제목에서 연도 추출 (예: '사업보고서 (2024.12)' -> 2024)"""
            if not title:
                return None
            # 패턴: (YYYY.MM) 또는 (YYYY.MM.DD) 또는 YYYY.12 등
            patterns = [
                r'\((\d{4})\.\d{1,2}',  # (2024.12)
                r'\((\d{4})\.\d{1,2}\.\d{1,2}',  # (2024.12.31)
                r'(\d{4})\.\d{1,2}',  # 2024.12
                r'(\d{4})\s*년\s*사업보고서',  # 2024년 사업보고서
                r'(\d{4})\s*사업보고서',  # 2024 사업보고서
            ]
            for pattern in patterns:
                match = re.search(pattern, str(title))
                if match:
                    try:
                        return int(match.group(1))
                    except:
                        continue
            return None
        
        # target_year가 지정된 경우, 보고서 제목에서 연도 추출하여 필터링
        if target_year:
            # 모든 보고서에 대해 연도 추출
            business_reports['report_year'] = business_reports['report_nm'].apply(extract_year_from_title)
            
            # target_year와 일치하는 보고서만 필터링
            filtered_reports = business_reports[business_reports['report_year'] == target_year]
            
            if len(filtered_reports) > 0:
                # 필터링된 보고서 중 접수일자 기준 최신 것 선택
                business_reports = filtered_reports
                logger.info(f"{ticker}: {target_year}년 사업보고서 {len(business_reports)}개 발견")
            else:
                # target_year와 일치하는 보고서가 없으면 최신 정기 공시(분기/반기/사업보고서) 사용 (Fallback)
                available_years = business_reports['report_year'].dropna().unique().tolist()
                logger.warning(f"{ticker}: {target_year}년 사업보고서를 찾을 수 없습니다.")
                logger.warning(f"  사용 가능한 연도: {sorted(available_years)}")
                logger.warning(f"  사용 가능한 보고서: {business_reports['report_nm'].tolist()[:5]}")
                logger.info(f"{ticker}: 최신 정기 공시(분기/반기/사업보고서)를 찾습니다 (Fallback)")
                
                # 정기 공시 전체 조회 (A001: 사업보고서, A002: 반기보고서, A003: 분기보고서)
                all_regular_reports = self.list_reports(ticker, start_date, end_date, kind='A', final=True, business_report_only=False)
                if all_regular_reports is not None and len(all_regular_reports) > 0:
                    # 정기 공시만 필터링 (A001, A002, A003)
                    regular_types = ['A001', 'A002', 'A003']
                    if 'pblntf_detail_ty' in all_regular_reports.columns:
                        filtered_regular = all_regular_reports[all_regular_reports['pblntf_detail_ty'].isin(regular_types)]
                        if len(filtered_regular) > 0:
                            # 접수일자 기준 정렬 (최신순)
                            if 'rcept_dt' in filtered_regular.columns:
                                filtered_regular = filtered_regular.sort_values('rcept_dt', ascending=False)
                            business_reports = filtered_regular
                            logger.info(f"{ticker}: 최신 정기 공시 발견: {business_reports.iloc[0]['report_nm']}")
                        else:
                            logger.warning(f"{ticker}: 정기 공시를 찾을 수 없습니다. 기존 사업보고서 목록 사용")
                    else:
                        logger.warning(f"{ticker}: pblntf_detail_ty 컬럼 없음. 기존 사업보고서 목록 사용")
                else:
                    logger.warning(f"{ticker}: 정기 공시를 찾을 수 없습니다. 기존 사업보고서 목록 사용")
        
        # 접수일자 기준으로 정렬 (최신순) - 가장 최신 보고서 선택
        if 'rcept_dt' in business_reports.columns:
            business_reports = business_reports.sort_values('rcept_dt', ascending=False)
        
        # 최신 사업보고서 선택
        target_report = business_reports.iloc[0]
        
        return {
            'rcept_no': target_report['rcept_no'],
            'report_nm': target_report['report_nm'],
            'rcept_dt': target_report.get('rcept_dt'),  # 접수일자 (YYYYMMDD 형식)
            'report': target_report
        }
    
    def get_raw_html_for_revenue_sections(self, ticker: str, year: Optional[int] = None, is_financial: bool = False) -> Optional[str]:
        """
        매출 비중 추출을 위한 원문 HTML 가져오기
        
        Args:
            ticker: 종목코드
            year: 연도
            is_financial: 금융사 여부 (금융사는 "부문별 영업수지" 같은 특정 섹션 우선)
        
        Returns:
            HTML 문자열 또는 None
        """
        try:
            report_info = self.find_business_report(ticker, year)
            if not report_info:
                return None
            
            rcept_no = report_info['rcept_no']
            sub_docs = self.get_sub_docs(rcept_no)
            if sub_docs is None or len(sub_docs) == 0:
                return None
            
            # 금융사 전용: "부문별 영업수지" 같은 특정 섹션 우선 검색
            if is_financial:
                financial_specific_keywords = ['부문별 영업수지', '부문별 영업실적', '이자 부문', '수수료 부문', '영업수지']
                for idx, row in sub_docs.iterrows():
                    title = str(row.get('title', ''))
                    url = row.get('url', '')
                    
                    if any(keyword in title for keyword in financial_specific_keywords):
                        html = self.fetch_section_content(url)
                        if html:
                            logger.info(f"{ticker}: 금융사 전용 섹션 HTML 추출 성공: {title}")
                            return html
            
            # 🆕 P0-4: 하위 섹션 타겟팅 개선 (스코어링 기반 선택)
            # "영업의 현황" 메인 섹션에서 하위 섹션 탐색하며 타겟 키워드가 있는 섹션만 수집
            target_keywords = ['영업의 현황', '영업의 종류', '주요 제품', '매출', '사업부문', '영업개황']
            
            # 타겟 섹션 키워드 (부문별 영업실적 등)
            if is_financial:
                target_section_keywords = ['부문별 영업실적', '부문별 영업수지', '사업부문별', '영업의 종류']
            else:
                target_section_keywords = ['부문별 영업실적', '부문별 매출', '사업부문별', '매출 비중']
            
            # Step 1: "영업의 현황" 메인 섹션 찾기
            main_section_idx = None
            for idx, row in sub_docs.iterrows():
                title = str(row.get('title', ''))
                if any(keyword in title for keyword in target_keywords):
                    main_section_idx = idx
                    logger.info(f"{ticker}: '영업의 현황' 메인 섹션 발견: {title}")
                    break
            
            if main_section_idx is None:
                # 메인 섹션 없으면 일반 검색
                for idx, row in sub_docs.iterrows():
                    title = str(row.get('title', ''))
                    url = row.get('url', '')
                    
                    if any(keyword in title for keyword in target_section_keywords):
                        html = self.fetch_section_content(url)
                        if html:
                            logger.info(f"{ticker}: 타겟 섹션 직접 발견: {title}")
                            return html
                # 메인 섹션도 타겟 섹션도 없으면 None 반환
                return None
            
            # Step 2: 메인 섹션 다음의 하위 섹션들을 탐색하며 타겟 키워드 찾기
            target_sections = []
            
            # 메인 섹션도 포함
            main_row = sub_docs.iloc[main_section_idx]
            main_url = main_row.get('url', '')
            if main_url:
                main_html = self.fetch_section_content(main_url)
                if main_html:
                    # 메인 섹션 HTML에서 타겟 키워드 확인
                    if any(kw in main_html for kw in target_section_keywords):
                        target_sections.append((main_row.get('title', ''), main_html))
                        logger.info(f"{ticker}: 메인 섹션에 타겟 키워드 발견")
            
            # 하위 섹션 탐색
            for idx in range(main_section_idx + 1, len(sub_docs)):
                row = sub_docs.iloc[idx]
                title = str(row.get('title', ''))
                url = row.get('url', '')
                
                # 다음 큰 섹션이 나오면 중단
                if any(major in title for major in ['이사의 경영진단', '재무상태', '손익계산서', '현금흐름']):
                    logger.debug(f"{ticker}: 다음 큰 섹션 발견, 탐색 중단: {title}")
                    break
                
                # 타겟 키워드가 있는 하위 섹션 찾기
                if any(keyword in title for keyword in target_section_keywords):
                    html = self.fetch_section_content(url)
                    if html:
                        target_sections.append((title, html))
                        logger.info(f"{ticker}: 타겟 하위 섹션 발견: {title}")
                        # 타겟 섹션을 찾으면 즉시 반환 (가장 정확한 섹션)
                        return html
                
                # 하위 섹션 패턴이지만 키워드가 없는 경우, HTML 내용에서도 확인
                if re.search(r'[가-나다라마바사아자차카타파하]\.|\([0-9]\)|\([가-힣]\)', title):
                    html = self.fetch_section_content(url)
                    if html:
                        # HTML 내용에서 타겟 키워드 확인
                        if any(keyword in html for keyword in target_section_keywords):
                            target_sections.append((title, html))
                            logger.info(f"{ticker}: HTML 내용에서 타겟 키워드 발견: {title}")
                            return html
            
            # Step 3: 타겟 섹션을 찾았으면 반환, 없으면 메인 섹션 반환 (Fallback)
            if target_sections:
                # 가장 첫 번째 타겟 섹션 반환
                return target_sections[0][1]
            
            # 타겟 섹션을 못 찾으면 메인 섹션 반환
            if main_url:
                main_html = self.fetch_section_content(main_url)
                if main_html:
                    logger.info(f"{ticker}: 타겟 섹션 미발견, 메인 섹션 반환")
                    return main_html
            
            # 관련 섹션을 못 찾으면 "사업의 내용" 섹션 사용
            for idx, row in sub_docs.iterrows():
                title = str(row.get('title', ''))
                url = row.get('url', '')
                
                if '사업의 내용' in title or '사업의 개요' in title:
                    html = self.fetch_section_content(url)
                    if html:
                        logger.info(f"{ticker}: 사업의 내용 섹션 HTML 추출 성공: {title}")
                        return html
            
            return None
        except Exception as e:
            logger.error(f"{ticker}: 원문 HTML 가져오기 실패: {e}")
            return None
    
    @retry_dart_api
    @dart_rate_limiter
    def fetch_section_content(self, url: str, timeout: int = 30) -> Optional[str]:
        """
        섹션 URL에서 HTML 내용 가져오기
        
        SSL 오류 및 네트워크 오류에 대한 재시도 로직 포함
        
        Args:
            url: 섹션 URL
            timeout: 타임아웃 (초)
        
        Returns:
            HTML 내용 또는 None
        """
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        # Session을 사용하여 연결 재사용 및 재시도 설정
        session = requests.Session()
        
        # 재시도 전략 설정
        retry_strategy = Retry(
            total=3,  # 총 3번 재시도
            backoff_factor=1,  # 1초, 2초, 4초 간격으로 재시도
            status_forcelist=[429, 500, 502, 503, 504],  # HTTP 상태 코드별 재시도
            allowed_methods=["GET", "POST"]  # GET, POST만 재시도
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        try:
            # SSL 검증은 유지하되, 연결 재시도 강화
            response = session.get(
                url, 
                timeout=timeout,
                verify=True,  # SSL 검증 유지
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            )
            response.raise_for_status()
            return response.text
        except requests.exceptions.SSLError as ssl_err:
            logger.warning(f"SSL 오류 발생 ({url[:50]}...): {ssl_err}")
            # SSL 오류 시 한 번 더 시도 (검증 우회 옵션)
            try:
                logger.info(f"SSL 검증 우회하여 재시도: {url[:50]}...")
                response = session.get(
                    url,
                    timeout=timeout,
                    verify=False,  # SSL 검증 우회 (마지막 수단)
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
                )
                response.raise_for_status()
                logger.info(f"SSL 검증 우회 후 성공: {url[:50]}...")
                return response.text
            except Exception as e2:
                logger.error(f"SSL 검증 우회 후에도 실패 ({url[:50]}...): {e2}")
                return None
        except Exception as e:
            logger.warning(f"섹션 내용 가져오기 실패 ({url[:50]}...): {e}")
            return None
        finally:
            session.close()
    
    def clean_html_to_markdown(self, html_content: str) -> str:
        """
        HTML을 마크다운으로 변환
        
        Args:
            html_content: HTML 내용
        
        Returns:
            마크다운 텍스트
        """
        if not html_content:
            return ""
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 불필요한 태그 제거
        for tag in soup(['script', 'style', 'img', 'svg', 'path']):
            tag.decompose()
        
        cleaned_html = str(soup)
        text = md(cleaned_html, heading_style="ATX", strip=['a'], newline_style="BACKSLASH")
        
        # 정규화
        text = re.sub(r'\n\s+\n', '\n\n', text)
        text = re.sub(r' +', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)  # 연속된 빈 줄 압축
        text = re.sub(r'\|\s*\|\s*\|', '|', text)  # 표의 빈 셀 제거
        
        # 보일러플레이트 제거
        text = self._filter_boilerplate_references(text)
        
        return text
    
    def _filter_boilerplate_references(self, text: str) -> str:
        """'~참조 바랍니다' 등 네비게이션 문장 제거"""
        if not text:
            return ""
        
        nav_patterns = [
            r"참고하시기 바랍니다",
            r"참조하시기 바랍니다",
            r"참조 바랍니다",
            r"참조바랍니다",
            r"참고 바랍니다",
            r"참고바랍니다",
            r"보시기 바랍니다",
            r"확인하시기 바랍니다",
            r"기재되어 있습니다",
            r"기재되어있습니다"
        ]
        value_keywords = [
            "통화선도", "스왑", "스와프", "선물", "옵션", "파생", "헷지", "헤지",
            "위험회피", "매매", "계약", "체결", "평가", "손익", "잔액",
            "%", "원", "달러", "억원", "배럴", "톤"
        ]
        ref_patterns = [
            r"['\"「].+['\"」]\s*(을|를)?\s*참(조|고)",
            r"상세 내용은\s*['\"「].+['\"」]"
        ]
        
        lines = text.split('\n')
        filtered_lines = []
        for line in lines:
            clean_line = line.strip()
            if len(clean_line) < 2:
                continue
            is_nav = any(re.search(pat, clean_line) for pat in nav_patterns)
            has_value = any(keyword in clean_line for keyword in value_keywords)
            ref_hit = any(re.search(pat, clean_line) for pat in ref_patterns)
            if (is_nav or ref_hit):
                if has_value:
                    ref_positions = [idx for idx in (clean_line.find("참고"), clean_line.find("참조")) if idx != -1]
                    if ref_positions:
                        cut_idx = min(ref_positions)
                        trimmed = clean_line[:cut_idx].rstrip(" ,.-")
                        if trimmed:
                            filtered_lines.append(trimmed)
                        continue
                else:
                    continue
            filtered_lines.append(line)
        
        cleaned = '\n'.join(filtered_lines).strip()
        return cleaned or ""
    
    def extract_key_sections(
        self,
        ticker: str,
        target_year: Optional[int] = None
    ) -> Optional[str]:
        """
        DART에서 핵심 섹션 추출 (사업의 내용 + 이사의 경영진단)
        
        3단계 Fallback 전략:
        - Strategy A: 상위 목차 + URL 있음 → 바로 사용
        - Strategy B: 상위 목차 있지만 URL 없음 → 하위 목차 통합 수집
        - Strategy C: 상위 목차 없음 → 키워드 기반 직접 수집
        
        Args:
            ticker: 종목코드
            target_year: 대상 연도
        
        Returns:
            결합된 마크다운 텍스트 또는 None
        """
        # 사업보고서 찾기
        report_info = self.find_business_report(ticker, target_year)
        if report_info is None:
            # 보고서를 찾지 못한 경우, 분기/반기 보고서로 폴백
            logger.warning(f"{ticker}: 사업보고서를 찾을 수 없습니다. 분기/반기 보고서로 폴백 시도...")
            return self._fallback_to_quarterly_or_semi_annual(ticker, target_year)
        
        rcept_no = report_info['rcept_no']
        report_title = report_info['report_nm']
        logger.info(f"{ticker}: 대상 보고서 - {report_title} (No: {rcept_no})")
        
        # 하위 문서 목록 조회
        sub_docs = self.get_sub_docs(rcept_no)
        if sub_docs is None or len(sub_docs) == 0:
            logger.warning(f"{ticker}: 하위 문서 없음. 분기/반기 보고서로 폴백 시도...")
            return self._fallback_to_quarterly_or_semi_annual(ticker, target_year)
        
        if 'title' not in sub_docs.columns or 'url' not in sub_docs.columns:
            logger.error(f"{ticker}: sub_docs에 'title' 또는 'url' 컬럼이 없습니다. 분기/반기 보고서로 폴백 시도...")
            return self._fallback_to_quarterly_or_semi_annual(ticker, target_year)
        
        # 특수 보고서 형식 감지 및 처리 시도
        if self._is_special_report_format(sub_docs):
            logger.info(f"{ticker}: 특수 보고서 형식 감지. 특수 형식에서 추출 시도...")
            special_result = self._try_extract_from_special_format(sub_docs, ticker)
            if special_result:
                return special_result
            # 특수 형식에서 추출 실패 시 분기/반기 폴백
            logger.info(f"{ticker}: 특수 형식에서 추출 실패. 분기/반기 보고서로 폴백 시도...")
            return self._fallback_to_quarterly_or_semi_annual(ticker, target_year)
        
        combined_text = ""
        found_count = 0
        business_section_texts = []  # 하위 섹션 수집용
        business_section_idx = None
        business_section_has_url = False
        
        # Strategy A & B: 상위 목차 "사업의 내용" 찾기
        for idx, row in sub_docs.iterrows():
            title_str = str(row['title']).strip()
            clean_title = title_str.replace(" ", "").replace(".", "").strip()
            
            # "사업의 내용" 상위 섹션 매칭
            if self._matches_business_section(clean_title, title_str):
                business_section_idx = idx
                url = row.get('url')
                
                # Strategy A: URL이 있고 내용이 충분한 경우
                if pd.notna(url) and url and len(str(url)) > 5:
                    html = self.fetch_section_content(url)
                    if html:
                        md_text = self.clean_html_to_markdown(html)
                        md_text = self._extract_business_subsections(md_text)
                        
                        # 내용이 충분한 경우만 성공으로 간주 (최소 500자)
                        md_length = len(md_text.strip())
                        logger.debug(f"{ticker}: '사업의 내용' 추출 결과: {md_length}자")
                        if md_length > 500:
                            combined_text += f"# 1. 사업의 내용\n{md_text}\n\n"
                            found_count += 1
                            logger.info(f"{ticker}: '사업의 내용' 추출 성공 (상위 섹션, {md_length}자)")
                            break  # Strategy A 성공 시 즉시 종료
                        else:
                            logger.warning(f"{ticker}: '사업의 내용' 상위 섹션 URL 있으나 내용 부족 ({md_length}자). 하위 섹션 탐색...")
                            business_section_has_url = True
                else:
                    # Strategy B: URL이 없는 경우
                    logger.info(f"{ticker}: '사업의 내용' 상위 섹션 발견했으나 URL 없음. 하위 섹션 탐색...")
                    business_section_has_url = False
                break
        
        # Strategy B: 상위 목차는 있지만 URL이 없거나 내용이 부족한 경우 → 하위 목차 수집
        if business_section_idx is not None and found_count == 0:
            logger.info(f"{ticker}: 하위 섹션 수집 시작 (인덱스 {business_section_idx}부터)")
            
            # 현재 인덱스 다음부터 탐색 시작
            for sub_idx in range(business_section_idx + 1, len(sub_docs)):
                sub_row = sub_docs.iloc[sub_idx]
                sub_title = str(sub_row['title']).strip()
                sub_clean_title = sub_title.replace(" ", "").replace(".", "").strip()
                sub_url = sub_row.get('url')
                
                # 다음 큰 섹션이 나오면 중단
                if self._is_next_major_section(sub_title, sub_clean_title):
                    logger.info(f"{ticker}: 다음 대섹션 '{sub_title}' 발견. 하위 섹션 수집 종료")
                    break
                
                # 하위 섹션 최대 7개 제한 (성능 고려)
                if len(business_section_texts) >= 7:
                    logger.warning(f"{ticker}: 하위 섹션 수집 제한 도달 (7개). 수집 중단")
                    break
                
                # 하위 섹션 패턴 확인
                if self._is_business_subsection(sub_clean_title, sub_title):
                    if pd.notna(sub_url) and sub_url and len(str(sub_url)) > 5:
                        html = self.fetch_section_content(sub_url)
                        if html:
                            md_text = self.clean_html_to_markdown(html)
                            md_text = self._extract_structured_content(md_text, html)
                            
                            if len(md_text.strip()) > 100:  # 최소 100자 이상
                                business_section_texts.append({
                                    'title': sub_title,
                                    'content': md_text
                                })
                                logger.info(f"{ticker}: 하위 섹션 수집 - {sub_title} ({len(md_text)}자)")
            
            # 하위 섹션들을 통합
            if len(business_section_texts) > 0:
                combined_business = "# 1. 사업의 내용\n\n"
                for section in business_section_texts:
                    combined_business += f"## {section['title']}\n{section['content']}\n\n"
                combined_text = combined_business + combined_text
                found_count += 1
                logger.info(f"{ticker}: Strategy B - 하위 섹션 {len(business_section_texts)}개 통합 완료 (총 {len(combined_text)}자)")
        
        # Strategy C: 상위 목차조차 없는 경우 → 키워드 기반 직접 수집
        if found_count == 0:
            logger.warning(f"{ticker}: '사업의 내용' 상위 목차 없음. 키워드 기반 하위 섹션 직접 수집...")
            
            target_keywords = [
                "사업의 개요", "주요 제품", "주요 서비스", "원재료",
                "생산", "매출", "수주", "판매", "고객", "위험관리"
            ]
            
            for idx, row in sub_docs.iterrows():
                title_str = str(row['title']).strip()
                url = row.get('url')
                
                # 키워드 매칭 (하지만 재무 관련은 제외)
                clean_title = title_str.replace(" ", "").replace(".", "").strip()
                if '재무' in clean_title and '사업' not in clean_title:
                    continue  # 재무 섹션은 제외
                
                if any(kw in title_str for kw in target_keywords):
                    if pd.notna(url) and url and len(str(url)) > 5:
                        html = self.fetch_section_content(url)
                        if html:
                            md_text = self.clean_html_to_markdown(html)
                            md_text = self._extract_structured_content(md_text, html)
                            
                            if len(md_text.strip()) > 100:
                                business_section_texts.append({
                                    'title': title_str,
                                    'content': md_text
                                })
                                logger.info(f"{ticker}: 키워드 매칭 섹션 수집 - {title_str} ({len(md_text)}자)")
                                
                                # 최대 7개 제한
                                if len(business_section_texts) >= 7:
                                    break
            
            if len(business_section_texts) > 0:
                combined_business = "# 1. 사업의 내용\n\n"
                for section in business_section_texts:
                    combined_business += f"## {section['title']}\n{section['content']}\n\n"
                combined_text = combined_business + combined_text
                found_count += 1
                logger.info(f"{ticker}: Strategy C - 키워드 기반 섹션 {len(business_section_texts)}개 통합 완료 (총 {len(combined_text)}자)")
        
        # 이사의 경영진단 처리
        for idx, row in sub_docs.iterrows():
            title_str = str(row['title']).strip()
            clean_title = title_str.replace(" ", "").replace(".", "").strip()
            
            if '이사의경영진단' in clean_title or '경영진단' in clean_title or '분석의견' in clean_title:
                if pd.notna(row['url']) and row['url']:
                    html = self.fetch_section_content(row['url'])
                    if html:
                        md_text = self.clean_html_to_markdown(html)
                        md_text = self._extract_mda_subsections(md_text)
                        md_length = len(md_text.strip())
                        combined_text += f"# 2. 이사의 경영진단\n{md_text}\n\n"
                        found_count += 1
                        combined_length = len(combined_text.strip())
                        logger.info(f"{ticker}: '이사의 경영진단' 추출 성공 ({md_length}자, 전체: {combined_length}자)")
        
        # 최종 실패 처리
        if found_count == 0:
            logger.warning(f"{ticker}: 타겟 목차를 찾지 못했습니다.")
            # 디버깅: 전체 섹션 목록을 WARNING 레벨로 출력
            if 'title' in sub_docs.columns:
                all_titles = sub_docs['title'].tolist()
                logger.warning(f"전체 섹션 목록 ({len(all_titles)}개):")
                for i, title in enumerate(all_titles[:30], 1):
                    url_exists = pd.notna(sub_docs.iloc[i-1].get('url')) and sub_docs.iloc[i-1].get('url')
                    url_info = "✓" if url_exists else "✗"
                    logger.warning(f"  {i}. [{url_info}] {title}")
                if len(all_titles) > 30:
                    logger.warning(f"  ... (총 {len(all_titles)}개, 처음 30개만 표시)")
            
            # 사업보고서를 찾았지만 목차를 찾지 못한 경우, 분기/반기 보고서로 대체 시도
            logger.info(f"{ticker}: 사업보고서 목차 미발견. 분기/반기 보고서로 대체 시도...")
            return self._fallback_to_quarterly_or_semi_annual(ticker, target_year)
        
        # 최종 검증: combined_text가 비어있거나 너무 짧으면 폴백 시도
        combined_length = len(combined_text.strip()) if combined_text else 0
        logger.info(f"{ticker}: 최종 combined_text 길이: {combined_length}자")
        
        if combined_length < 100:
            logger.warning(f"{ticker}: combined_text가 너무 짧음 ({combined_length}자). 분기/반기 보고서로 폴백 시도...")
            return self._fallback_to_quarterly_or_semi_annual(ticker, target_year)
        
        # combined_text 내용 요약 로그 (처음 200자)
        preview = combined_text[:200].replace('\n', ' ') if combined_text else ""
        logger.info(f"{ticker}: combined_text 미리보기: {preview}...")
        
        return combined_text
    
    def _fallback_to_quarterly_or_semi_annual(
        self,
        ticker: str,
        target_year: int = 2024
    ) -> Optional[str]:
        """
        사업보고서 목차를 찾지 못한 경우, 분기/반기 보고서 중 최신 것을 가져오기
        
        Args:
            ticker: 종목코드
            target_year: 대상 연도
        
        Returns:
            결합된 마크다운 텍스트 (사업의 내용만, 이사의 경영진단 제외) 또는 None
        """
        from datetime import datetime
        
        # 날짜 범위 설정
        current_year = datetime.now().year
        if target_year:
            start_date = f'{target_year}-01-01'
        else:
            start_date = f'{current_year - 3}-01-01'
        
        end_date = datetime.now().strftime('%Y-%m-%d')
        
        # 정기 공시 전체 조회 (A002: 반기보고서, A003: 분기보고서)
        all_regular = self.list_reports(ticker, start_date, end_date, kind='A', final=True, business_report_only=False)
        if all_regular is None or len(all_regular) == 0:
            logger.warning(f"{ticker}: 분기/반기 보고서를 찾을 수 없습니다.")
            return None
        
        # 분기/반기 보고서만 필터링 (A002: 반기보고서, A003: 분기보고서)
        quarterly_types = ['A002', 'A003']
        if 'pblntf_detail_ty' in all_regular.columns:
            filtered_reports = all_regular[all_regular['pblntf_detail_ty'].isin(quarterly_types)]
            if len(filtered_reports) == 0:
                logger.warning(f"{ticker}: 분기/반기 보고서를 찾을 수 없습니다.")
                return None
        else:
            logger.warning(f"{ticker}: pblntf_detail_ty 컬럼 없음. 전체 보고서 사용")
            filtered_reports = all_regular
        
        # 접수일자 기준 정렬 (최신순)
        if 'rcept_dt' in filtered_reports.columns:
            filtered_reports = filtered_reports.sort_values('rcept_dt', ascending=False)
        
        # 최신 분기/반기 보고서 선택
        latest_report = filtered_reports.iloc[0]
        rcept_no = latest_report['rcept_no']
        report_title = latest_report['report_nm']
        report_type = latest_report.get('pblntf_detail_ty', 'UNKNOWN')
        
        logger.info(f"{ticker}: 최신 분기/반기 보고서 발견 - {report_title} (No: {rcept_no}, Type: {report_type})")
        
        # 하위 문서 목록 조회
        sub_docs = self.get_sub_docs(rcept_no)
        if sub_docs is None or len(sub_docs) == 0:
            logger.warning(f"{ticker}: 하위 문서 없음")
            return None
        
        if 'title' not in sub_docs.columns or 'url' not in sub_docs.columns:
            logger.error(f"{ticker}: sub_docs에 'title' 또는 'url' 컬럼이 없습니다.")
            return None
        
        combined_text = ""
        found_count = 0
        business_section_texts = []  # 하위 섹션 수집용
        business_section_idx = None
        
        # Strategy A & B: 상위 목차 "사업의 내용" 찾기
        for idx, row in sub_docs.iterrows():
            title_str = str(row['title']).strip()
            clean_title = title_str.replace(" ", "").replace(".", "").strip()
            
            # "사업의 내용" 상위 섹션 매칭
            if self._matches_business_section(clean_title, title_str):
                business_section_idx = idx
                url = row.get('url')
                
                # Strategy A: URL이 있고 내용이 충분한 경우
                if pd.notna(url) and url and len(str(url)) > 5:
                    html = self.fetch_section_content(url)
                    if html:
                        md_text = self.clean_html_to_markdown(html)
                        md_text = self._extract_business_subsections(md_text)
                        
                        # 내용이 충분한 경우만 성공으로 간주 (최소 500자)
                        if len(md_text.strip()) > 500:
                            combined_text += f"# 1. 사업의 내용\n{md_text}\n\n"
                            found_count += 1
                            logger.info(f"{ticker}: '사업의 내용' 추출 성공 (상위 섹션, {len(md_text)}자)")
                            break
                        else:
                            logger.warning(f"{ticker}: '사업의 내용' 상위 섹션 URL 있으나 내용 부족 ({len(md_text)}자). 하위 섹션 탐색...")
                else:
                    # Strategy B: URL이 없는 경우
                    logger.info(f"{ticker}: '사업의 내용' 상위 섹션 발견했으나 URL 없음. 하위 섹션 탐색...")
                break
        
        # Strategy B: 하위 목차 수집
        if business_section_idx is not None and found_count == 0:
            logger.info(f"{ticker}: 하위 섹션 수집 시작 (인덱스 {business_section_idx}부터)")
            
            for sub_idx in range(business_section_idx + 1, len(sub_docs)):
                sub_row = sub_docs.iloc[sub_idx]
                sub_title = str(sub_row['title']).strip()
                sub_clean_title = sub_title.replace(" ", "").replace(".", "").strip()
                sub_url = sub_row.get('url')
                
                # 다음 큰 섹션이 나오면 중단
                if self._is_next_major_section(sub_title, sub_clean_title):
                    logger.info(f"{ticker}: 다음 대섹션 '{sub_title}' 발견. 하위 섹션 수집 종료")
                    break
                
                # 하위 섹션 최대 7개 제한
                if len(business_section_texts) >= 7:
                    break
                
                # 하위 섹션 패턴 확인
                if self._is_business_subsection(sub_clean_title, sub_title):
                    if pd.notna(sub_url) and sub_url and len(str(sub_url)) > 5:
                        html = self.fetch_section_content(sub_url)
                        if html:
                            md_text = self.clean_html_to_markdown(html)
                            md_text = self._extract_structured_content(md_text, html)
                            
                            if len(md_text.strip()) > 100:
                                business_section_texts.append({
                                    'title': sub_title,
                                    'content': md_text
                                })
                                logger.info(f"{ticker}: 하위 섹션 수집 - {sub_title} ({len(md_text)}자)")
            
            # 하위 섹션들을 통합
            if len(business_section_texts) > 0:
                logger.info(f"{ticker}: 하위 섹션 {len(business_section_texts)}개 통합")
                combined_business = "# 1. 사업의 내용\n\n"
                for section in business_section_texts:
                    combined_business += f"## {section['title']}\n{section['content']}\n\n"
                combined_text = combined_business + combined_text
                found_count += 1
        
        # Strategy C: 키워드 기반 직접 수집
        if found_count == 0:
            logger.warning(f"{ticker}: '사업의 내용' 상위 목차 없음. 키워드 기반 하위 섹션 직접 수집...")
            
            target_keywords = [
                "사업의 개요", "주요 제품", "주요 서비스", "원재료",
                "생산", "매출", "수주", "판매", "고객"
            ]
            
            for idx, row in sub_docs.iterrows():
                title_str = str(row['title']).strip()
                url = row.get('url')
                clean_title = title_str.replace(" ", "").replace(".", "").strip()
                
                # 재무 관련은 제외
                if '재무' in clean_title and '사업' not in clean_title:
                    continue
                
                if any(kw in title_str for kw in target_keywords):
                    if pd.notna(url) and url and len(str(url)) > 5:
                        html = self.fetch_section_content(url)
                        if html:
                            md_text = self.clean_html_to_markdown(html)
                            md_text = self._extract_structured_content(md_text, html)
                            
                            if len(md_text.strip()) > 100:
                                business_section_texts.append({
                                    'title': title_str,
                                    'content': md_text
                                })
                                logger.info(f"{ticker}: 키워드 매칭 섹션 수집 - {title_str} ({len(md_text)}자)")
                                
                                if len(business_section_texts) >= 7:
                                    break
            
            if len(business_section_texts) > 0:
                combined_business = "# 1. 사업의 내용\n\n"
                for section in business_section_texts:
                    combined_business += f"## {section['title']}\n{section['content']}\n\n"
                combined_text = combined_business + combined_text
                found_count += 1
        
        if found_count == 0:
            logger.warning(f"{ticker}: 분기/반기 보고서에서도 '사업의 내용' 목차를 찾지 못했습니다.")
            # 디버깅 정보 출력
            if 'title' in sub_docs.columns:
                all_titles = sub_docs['title'].tolist()
                logger.warning(f"전체 섹션 목록 ({len(all_titles)}개):")
                for i, title in enumerate(all_titles[:20], 1):
                    url_exists = pd.notna(sub_docs.iloc[i-1].get('url')) and sub_docs.iloc[i-1].get('url')
                    url_info = "✓" if url_exists else "✗"
                    logger.warning(f"  {i}. [{url_info}] {title}")
            return None
        
        logger.info(f"{ticker}: 분기/반기 보고서에서 '사업의 내용' 추출 완료 (이사의 경영진단 제외)")
        return combined_text
    
    def _matches_business_section(self, clean_title: str, original_title: str) -> bool:
        """
        '사업의 내용' 섹션인지 판단 (다양한 형식 지원)
        
        지원 형식:
        - "II. 사업의 내용" (로마숫자)
        - "2. 사업의 내용" (아라비아숫자)
        - "II사업의내용" (점/공백 없음)
        - "사업의 내용" (숫자 없음)
        """
        # 기본 패턴
        if '사업의내용' in clean_title or '사업내용' in clean_title:
            return True
        
        # 로마숫자 + 사업의 내용 패턴
        if re.search(r'[IVX]+.*?사업.*?내용', clean_title):
            return True
        
        # 아라비아숫자 + 사업의 내용 패턴
        if re.search(r'^\d+.*?사업.*?내용', clean_title):
            return True
        
        # 원본 제목에서 직접 확인 (점, 공백 포함)
        if re.search(r'[IVX\d]*\s*\.?\s*사업\s*의\s*내용', original_title):
            return True
        
        return False
    
    def _is_next_major_section(self, title: str, clean_title: str) -> bool:
        """
        다음 대섹션인지 판단 (사업 관련 제외)
        
        핵심 키워드 기반 종료 조건:
        - "재무" 키워드가 있고 "사업" 키워드가 없으면 종료
        - 로마숫자/아라비아숫자로 시작하는 대섹션
        """
        # 로마숫자로 시작
        if re.match(r'^[IVX]+\.', title):
            # "사업" 키워드가 없으면 다른 섹션으로 간주
            if '사업' not in clean_title:
                return True
        
        # 아라비아숫자로 시작 (3 이상은 보통 다른 대섹션)
        if re.match(r'^[3-9]\.', title):
            if '사업' not in clean_title:
                return True
        
        # 핵심 종료 키워드: "재무"
        if '재무' in clean_title and '사업' not in clean_title:
            return True
        
        return False
    
    def _is_business_subsection(self, clean_title: str, original_title: str) -> bool:
        """
        '사업의 내용'의 하위 섹션인지 판단
        
        하위 섹션 패턴:
        - 숫자로 시작하는 하위 섹션
        - 사업 관련 키워드 포함
        - 재무 관련 키워드는 제외
        """
        # 로마숫자 또는 큰 아라비아숫자로 시작하면 제외 (대섹션)
        if re.match(r'^[IVX]+\.|^[3-9]\.', original_title):
            return False
        
        # 재무 관련 키워드가 있으면 제외
        if '재무' in clean_title:
            return False
        
        # 하위 섹션 패턴 확인
        subsection_patterns = [
            r'^\d+.*?사업.*?개요',      # "1. 사업의 개요"
            r'^\d+.*?주요.*?제품',      # "2. 주요 제품 및 서비스"
            r'^\d+.*?주요.*?서비스',    # "2. 주요 제품 및 서비스"
            r'^\d+.*?원재료',           # "3. 원재료 및 생산실비"
            r'^\d+.*?생산',             # "3. 원재료 및 생산실비"
            r'^\d+.*?매출',             # "4. 매출 및 수주상황"
            r'^\d+.*?수주',             # "4. 매출 및 수주상황"
            r'^\d+.*?판매',             # 판매 관련
            r'^\d+.*?고객',             # 고객 관련
            r'^\d+.*?위험관리',         # 위험관리
            r'^\d+.*?연구개발',         # 연구개발
        ]
        
        for pattern in subsection_patterns:
            if re.search(pattern, clean_title):
                return True
        
        return False
    
    def _extract_structured_content(self, markdown_text: str, html_content: str) -> str:
        """
        표 형태 데이터를 구조화된 텍스트로 추출
        
        표가 주요 내용인 경우, 표를 텍스트로 변환하여 추출
        최대 20행만 추출하여 토큰 절약
        """
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = soup.find_all('table')
        
        if len(tables) == 0:
            # 표가 없으면 기존 마크다운 텍스트 반환
            return markdown_text
        
        table_texts = []
        for table in tables:
            # 표 헤더 추출
            headers = []
            header_row = table.find('tr')
            if header_row:
                for th in header_row.find_all(['th', 'td']):
                    headers.append(th.get_text(strip=True))
            
            # 표 데이터 추출
            rows = []
            for tr in table.find_all('tr')[1:]:  # 헤더 제외
                cells = [td.get_text(strip=True) for td in tr.find_all('td')]
                if cells and any(cell for cell in cells):  # 빈 행 제외
                    rows.append(cells)
            
            # 구조화된 텍스트로 변환 (최대 20행)
            if headers and rows:
                table_text = f"표 헤더: {', '.join(headers)}\n"
                for i, row in enumerate(rows[:20], 1):
                    row_text = ', '.join([cell for cell in row if cell])
                    if row_text:
                        table_text += f"행 {i}: {row_text}\n"
                table_texts.append(table_text)
        
        # 표 내용이 있으면 표 텍스트를 추가
        if table_texts:
            table_summary = "\n\n".join(table_texts)
            # 마크다운 텍스트와 표 텍스트 결합
            return f"{markdown_text}\n\n### 표 데이터\n{table_summary}"
        
        return markdown_text
    
    def _extract_business_subsections(self, markdown_text: str) -> str:
        """사업의 내용 섹션에서 핵심 하위 섹션만 추출"""
        # 간단한 키워드 기반 필터링
        target_keywords = [
            '주요 제품', '주요 서비스', '원재료', '생산 설비',
            '매출', '수주', '판매', '고객'
        ]
        
        lines = markdown_text.split('\n')
        result_lines = []
        in_target_section = False
        
        for line in lines:
            stripped = line.strip()
            if stripped.startswith('#'):
                # 헤딩에서 키워드 확인
                if any(keyword in stripped for keyword in target_keywords):
                    in_target_section = True
                    result_lines.append(line)
                elif stripped.startswith('##'):  # 대섹션 변경
                    in_target_section = False
            elif in_target_section:
                result_lines.append(line)
        
        extracted = '\n'.join(result_lines)
        extracted_length = len(extracted.strip())
        original_length = len(markdown_text.strip())
        
        # 필터링 결과가 너무 짧으면 원문 전체 반환 (최소 200자 보장)
        if extracted_length < 200:
            logger.debug(f"필터링 결과가 너무 짧음 ({extracted_length}자). 원문 전체 사용 ({original_length}자)")
            return markdown_text
        
        # 최소 길이 보장 (100자 미만이면 원문 반환)
        if extracted_length < 100:
            logger.debug(f"필터링 결과가 최소 길이 미만 ({extracted_length}자). 원문 사용")
            return markdown_text
        
        logger.debug(f"필터링 성공: {extracted_length}자 (원문: {original_length}자)")
        return extracted
    
    def _extract_mda_subsections(self, markdown_text: str) -> str:
        """이사의 경영진단 섹션에서 핵심 부분만 추출"""
        lines = markdown_text.split('\n')
        result_lines = []
        stop_patterns = [r'회계감사인의\s*감사의견']
        
        for line in lines:
            stripped = line.strip()
            # 중지 패턴 확인
            if any(re.search(pattern, stripped, re.IGNORECASE) for pattern in stop_patterns):
                break
            # 표 제외
            if stripped.startswith('|'):
                continue
            result_lines.append(line)
        
        extracted = '\n'.join(result_lines)
        return extracted if len(extracted.strip()) > 100 else markdown_text
    
    def _is_special_report_format(self, sub_docs: pd.DataFrame) -> bool:
        """
        특수 보고서 형식인지 감지 (정정신고, 영업보고서 등)
        
        Args:
            sub_docs: 하위 문서 목록 DataFrame
        
        Returns:
            True: 특수 보고서 형식, False: 일반 보고서 형식
        """
        if sub_docs is None or len(sub_docs) == 0:
            return False
        
        # 특수 보고서 형식 패턴
        special_patterns = [
            '정정신고',
            '정정신고서',
            '정정신고(보고)',
            '영업보고서',
            '영업보고',
            '정관',
            '이사회의사록',
        ]
        
        # 사업보고서 일반 섹션 패턴
        normal_patterns = [
            '사업의 내용',
            '사업의내용',
            '재무에 관한 사항',
            '재무에관한사항',
            '이사의 경영진단',
            '이사의경영진단',
        ]
        
        # 모든 제목 확인
        titles = sub_docs['title'].astype(str).tolist() if 'title' in sub_docs.columns else []
        all_titles_text = ' '.join(titles)
        
        # 특수 패턴 발견 여부
        has_special = any(pattern in all_titles_text for pattern in special_patterns)
        has_normal = any(pattern in all_titles_text for pattern in normal_patterns)
        
        # 특수 패턴이 있고 일반 패턴이 없으면 특수 보고서 형식
        if has_special and not has_normal:
            logger.warning(f"특수 보고서 형식 감지: {titles[:5]}")
            return True
        
        # 섹션이 2개 이하이고 특수 패턴만 있으면 특수 보고서 형식
        if len(sub_docs) <= 2 and has_special:
            logger.warning(f"특수 보고서 형식 감지 (섹션 2개 이하): {titles}")
            return True
        
        return False
    
    def _try_extract_from_special_format(self, sub_docs: pd.DataFrame, ticker: str) -> Optional[str]:
        """
        특수 보고서 형식에서 사업 내용 추출 시도 (영업보고서 등)
        
        Args:
            sub_docs: 하위 문서 목록
            ticker: 종목코드
        
        Returns:
            추출된 텍스트 또는 None
        """
        if sub_docs is None or len(sub_docs) == 0:
            return None
        
        combined_text = ""
        
        # 영업보고서 섹션 찾기
        for idx, row in sub_docs.iterrows():
            title = str(row.get('title', '')).strip()
            
            # 영업보고서 관련 키워드 확인
            if '영업보고서' in title or '영업보고' in title:
                url = row.get('url')
                if pd.notna(url) and url and len(str(url)) > 5:
                    logger.info(f"{ticker}: 영업보고서 섹션 발견 - {title}")
                    html = self.fetch_section_content(url)
                    if html:
                        md_text = self.clean_html_to_markdown(html)
                        md_text = self._extract_structured_content(md_text, html)
                        
                        if len(md_text.strip()) > 500:
                            combined_text += f"# 1. 사업의 내용\n{md_text}\n\n"
                            logger.info(f"{ticker}: 영업보고서에서 사업 내용 추출 성공 ({len(md_text)}자)")
                            return combined_text
        
        return None

