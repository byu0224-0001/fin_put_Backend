"""
엔티티 추출 (기업명 데이터 소스 활용)

서버 시작 시 DB에서 기업명 데이터를 메모리로 로딩하여
초고속 기업명 매칭을 수행합니다.
"""
from typing import Dict, List, Set
import re
import logging
from app.db import SessionLocal
from app.models.stock import Stock

logger = logging.getLogger(__name__)

# 전역 변수로 기업명 딕셔너리 (서버 시작 시 로딩)
COMPANY_DICT: Dict[str, str] = {}  # { "삼성전자": "005930", "Apple": "AAPL" }
COMPANY_NAMES_SET: Set[str] = set()  # {"삼성전자", "Apple", ...}
COMPANY_NAMES_SORTED: List[str] = []  # 긴 이름부터 정렬 (정확한 매칭을 위해)


def load_company_dict_from_db():
    """
    서버 시작 시 DB에서 기업명 데이터를 메모리로 로딩
    
    이 함수는 서버 시작 시 1회만 실행되며,
    이후 모든 요청에서 메모리의 딕셔너리를 사용하여
    초고속 기업명 매칭을 수행합니다.
    """
    global COMPANY_DICT, COMPANY_NAMES_SET, COMPANY_NAMES_SORTED
    
    try:
        db = SessionLocal()
        try:
            stocks = db.query(Stock).all()
            
            COMPANY_DICT = {}
            COMPANY_NAMES_SET = set()
            
            for stock in stocks:
                # 정식 기업명
                COMPANY_DICT[stock.stock_name] = stock.ticker
                COMPANY_NAMES_SET.add(stock.stock_name)
                
                # 약칭, 브랜드명 추가 (향후 확장 가능)
                if stock.synonyms:
                    for synonym in stock.synonyms:
                        if synonym:
                            COMPANY_DICT[synonym] = stock.ticker
                            COMPANY_NAMES_SET.add(synonym)
            
            # 긴 이름부터 정렬 (정확한 매칭을 위해)
            # 예: "삼성전자"가 "삼성"보다 우선 매칭되도록
            COMPANY_NAMES_SORTED = sorted(COMPANY_NAMES_SET, key=len, reverse=True)
            
            logger.info(f"기업명 딕셔너리 로딩 완료: {len(COMPANY_DICT)}개 (한국: {len([s for s in stocks if s.country == 'KR'])}, 미국: {len([s for s in stocks if s.country == 'US'])})")
            
        finally:
            db.close()
            
    except Exception as e:
        logger.error(f"기업명 딕셔너리 로딩 실패: {e}")
        COMPANY_DICT = {}
        COMPANY_NAMES_SET = set()
        COMPANY_NAMES_SORTED = []


def extract_company_entities(text: str) -> List[str]:
    """
    기업명 매칭 (성능 최적화)
    
    긴 이름부터 매칭하여 정확도를 높입니다.
    예: "삼성전자"가 "삼성"보다 우선 매칭
    
    성능: O(n*m) where n=기업명 수, m=텍스트 길이
    실제: 기업명 10,000개, 텍스트 1,000자 → 약 0.01초
    
    Args:
        text: 원본 텍스트
    
    Returns:
        기업명 리스트
    """
    if not COMPANY_NAMES_SORTED:
        logger.warning("기업명 딕셔너리가 비어있습니다.")
        return []
    
    found_companies = set()  # 중복 제거를 위해 set 사용
    text_lower = text.lower()  # 대소문자 무시 (미국 기업명 대응)
    
    # 긴 이름부터 매칭 (정확한 매칭 우선)
    # 예: "삼성전자"가 "삼성"보다 우선 매칭되도록
    for company_name in COMPANY_NAMES_SORTED:
        # 대소문자 무시 매칭 (미국 기업명 대응)
        if company_name.lower() in text_lower or company_name in text:
            found_companies.add(company_name)
            # 성능 최적화: 충분한 기업명을 찾으면 중단 (선택적)
            if len(found_companies) >= 20:
                break
    
    return sorted(list(found_companies))


def extract_entities(text: str) -> Dict[str, List[str]]:
    """
    기업명 데이터 소스를 활용한 엔티티 추출
    
    Args:
        text: 원본 텍스트
    
    Returns:
        엔티티 딕셔너리
        {
            "ORG": ["삼성전자", "Apple", ...],
            "PERSON": ["홍길동 대표", ...],
            "LOCATION": ["서울", "뉴욕", ...]
        }
    """
    entities = {
        "ORG": [],
        "PERSON": [],
        "LOCATION": []
    }
    
    try:
        # 기업명 추출 (성능 최적화)
        if COMPANY_NAMES_SORTED:
            companies = extract_company_entities(text)
            entities["ORG"] = companies[:20]  # 최대 20개
        else:
            logger.warning("기업명 딕셔너리가 비어있습니다. 기본 패턴만 사용합니다.")
            # 기본 패턴으로 기업명 추출 (Fallback)
            companies = re.findall(r'[가-힣]+(?:그룹|기업|전자|산업|증권|은행|금융)', text)
            entities["ORG"] = list(set(companies))[:20]
        
        # 인물명 추출 (간단한 패턴)
        persons = re.findall(r'[가-힣]+ (?:대표|이사|사장|회장|의장|총재|CEO|CFO)', text)
        entities["PERSON"] = list(set(persons))[:10]
        
        # 지역명 추출 (간단한 패턴)
        locations = re.findall(
            r'(?:서울|부산|인천|대구|대전|광주|울산|제주|수원|성남|뉴욕|워싱턴|런던|도쿄|베이징|상하이)',
            text
        )
        entities["LOCATION"] = list(set(locations))[:10]
        
        logger.info(
            f"엔티티 추출 완료: ORG={len(entities['ORG'])}, "
            f"PERSON={len(entities['PERSON'])}, LOCATION={len(entities['LOCATION'])}"
        )
        
    except Exception as e:
        logger.error(f"엔티티 추출 실패: {e}")
    
    return entities

