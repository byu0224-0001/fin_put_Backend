"""
DART 고유번호 매핑 서비스

corpCode.xml을 다운로드하여 ticker → corp_code 매핑 테이블 구축
"""
import logging
import zipfile
import io
import xml.etree.ElementTree as ET
import requests
from typing import Optional, Dict
from functools import lru_cache
import time

logger = logging.getLogger(__name__)


class DartCorpCodeMapper:
    """DART 고유번호 매핑 서비스"""
    
    def __init__(self, api_key: str):
        """
        Args:
            api_key: DART API Key
        """
        self.api_key = api_key
        self._mapping_cache: Optional[Dict[str, str]] = None  # ticker -> corp_code
        self._last_update_time: Optional[float] = None
        self._cache_ttl: int = 86400  # 24시간 캐시 유지
    
    def _load_corp_code_mapping(self, force_reload: bool = False) -> Dict[str, str]:
        """
        corpCode.xml을 다운로드하여 매핑 테이블 구축
        
        Args:
            force_reload: 강제 재로드 여부
        
        Returns:
            {ticker: corp_code} 딕셔너리
        """
        # 캐시 확인
        current_time = time.time()
        if (
            not force_reload
            and self._mapping_cache is not None
            and self._last_update_time is not None
            and (current_time - self._last_update_time) < self._cache_ttl
        ):
            logger.debug(f"고유번호 매핑 캐시 사용 (캐시 시간: {current_time - self._last_update_time:.0f}초)")
            return self._mapping_cache
        
        try:
            logger.info("DART 고유번호 매핑 테이블 다운로드 중...")
            corp_code_url = "https://opendart.fss.or.kr/api/corpCode.xml"
            params = {'crtfc_key': self.api_key}
            
            response = requests.get(corp_code_url, params=params, timeout=60)
            response.raise_for_status()
            
            # ZIP 파일 압축 해제
            z = zipfile.ZipFile(io.BytesIO(response.content))
            xml_file = z.namelist()[0]
            xml_content = z.read(xml_file).decode('utf-8')
            
            # XML 파싱
            root = ET.fromstring(xml_content)
            mapping = {}
            
            for corp in root.findall('list'):
                stock_code_elem = corp.find('stock_code')
                corp_code_elem = corp.find('corp_code')
                
                if stock_code_elem is not None and stock_code_elem.text:
                    stock_code = stock_code_elem.text.strip()
                    if corp_code_elem is not None and corp_code_elem.text:
                        corp_code = corp_code_elem.text.strip()
                        mapping[stock_code] = corp_code
            
            self._mapping_cache = mapping
            self._last_update_time = current_time
            
            logger.info(f"고유번호 매핑 테이블 구축 완료: {len(mapping)}개 기업")
            return mapping
            
        except requests.RequestException as e:
            logger.error(f"고유번호 매핑 테이블 다운로드 실패 (네트워크 오류): {e}")
            if self._mapping_cache is not None:
                logger.warning("기존 캐시 사용")
                return self._mapping_cache
            return {}
        except Exception as e:
            logger.error(f"고유번호 매핑 테이블 구축 실패: {e}")
            if self._mapping_cache is not None:
                logger.warning("기존 캐시 사용")
                return self._mapping_cache
            return {}
    
    def get_corp_code(self, ticker: str, force_reload: bool = False) -> Optional[str]:
        """
        티커로 고유번호 조회
        
        Args:
            ticker: 종목코드 (6자리)
            force_reload: 강제 재로드 여부
        
        Returns:
            고유번호 (8자리) 또는 None
        """
        mapping = self._load_corp_code_mapping(force_reload=force_reload)
        return mapping.get(ticker)
    
    def get_all_mappings(self, force_reload: bool = False) -> Dict[str, str]:
        """
        전체 매핑 테이블 조회
        
        Args:
            force_reload: 강제 재로드 여부
        
        Returns:
            {ticker: corp_code} 딕셔너리
        """
        return self._load_corp_code_mapping(force_reload=force_reload)
    
    def clear_cache(self):
        """캐시 초기화"""
        self._mapping_cache = None
        self._last_update_time = None
        logger.info("고유번호 매핑 캐시 초기화 완료")

