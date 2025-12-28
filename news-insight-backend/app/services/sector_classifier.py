"""
Sector Classification Service

기업 데이터를 기반으로 섹터 자동 분류
- 키워드 기반 Rule-based 분류
- 매출 비중 기반 부스팅 (P0 개선)
- LLM 기반 분류 (Fallback)
- Multi-Sector 분류 지원 (Primary + Secondary)
"""
import logging
from typing import Optional, Dict, Any, Tuple, List
from sqlalchemy.orm import Session

from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.services.llm_handler import LLMHandler

logger = logging.getLogger(__name__)

# ========================================================================
# 🆕 P0: Neutral 세그먼트 정의 (점수 계산에서 제외)
# ========================================================================
NEUTRAL_SEGMENTS = {
    # 범용 토큰 (섹터 중립)
    '기타', 'other', '그외', '상품', '제품', '용역', '서비스',
    '기타사업', '기타부문', '기타매출', '기타제품', '기타사업부문',
    '기타 부문', '기타 매출', '기타 제품', '기타사업', '기타사업부문',
    '상품매출', '제품매출', '용역매출', '기타수입', '기타수익',
    '상품 및 기타', '상품 등', '기타 제품', '기타(제품)', '기타(주1)',
    # 지역 기반 (섹터 중립)
    '수출', '내수', '해외', '국내', '미국', '중국', '해외사업부문',
    # 기타 중립
    '완제품', '합계', '매출', '수수료매출',
    # 공백 변형
    '기 타', '기  타', '기   타', '상 품', '상  품',
}

# SEC_MACH 고유 단서 (덤핑 방지용)
SEC_MACH_REQUIRED_KEYWORDS = [
    '공작기계', '산업기계', '건설기계', '중장비', '플랜트', '중공업',
    '내화물', '석도강판', '농기계', '펌프', '발전기', '모터', '변압기',
    '전력기기', '산업용기계', '산업장비', '공정장비', '제조장비',
    '기계부품', '기계사업', '기계제조', '기계설비'
]

# ========================================================================
# 🆕 매출 비중 → 섹터 매핑 (P0 개선: 삼성물산 오분류 방지)
# ========================================================================
SEGMENT_TO_SECTOR_MAP = {
    # 건설/부동산
    '건설': 'SEC_CONST', '건설부문': 'SEC_CONST', '건축': 'SEC_CONST',
    '부동산': 'SEC_CONST', '인프라': 'SEC_CONST', 'EPC': 'SEC_CONST',
    '주택': 'SEC_CONST', '토목': 'SEC_CONST', '부동산임대': 'SEC_CONST',
    '건설사업': 'SEC_CONST', '부동산개발': 'SEC_CONST',
    
    # 상사/무역/유통
    '상사': 'SEC_RETAIL', '상사부문': 'SEC_RETAIL', '트레이딩': 'SEC_RETAIL',
    '무역': 'SEC_RETAIL', '유통': 'SEC_RETAIL', '도매': 'SEC_RETAIL',
    '지류유통': 'SEC_RETAIL', '물류': 'SEC_RETAIL', '유통사업': 'SEC_RETAIL',
    
    # 바이오/제약/의료기기 (🆕 의료기기 추가)
    '바이오': 'SEC_BIO', '바이오부문': 'SEC_BIO', '제약': 'SEC_BIO',
    '의약품': 'SEC_BIO', '약품사업': 'SEC_BIO', '의약': 'SEC_BIO',
    '헬스케어': 'SEC_BIO', '생명과학': 'SEC_BIO', '의료기기': 'SEC_BIO',
    '원료의약품': 'SEC_BIO', '전문의약품': 'SEC_BIO', '일반의약품': 'SEC_BIO',
    '건강기능식품': 'SEC_FOOD',  # 건강기능식품은 FOOD로 분류
    
    # 자동차
    '차량': 'SEC_AUTO', '차량부문': 'SEC_AUTO', '자동차': 'SEC_AUTO',
    '완성차': 'SEC_AUTO', '모빌리티': 'SEC_AUTO', '자동차부품': 'SEC_AUTO',
    
    # 반도체
    '반도체': 'SEC_SEMI', 'DS': 'SEC_SEMI', '메모리': 'SEC_SEMI',
    '파운드리': 'SEC_SEMI', '반도체부문': 'SEC_SEMI', '비메모리': 'SEC_SEMI',
    '반도체소재': 'SEC_SEMI', '반도체장비': 'SEC_SEMI',
    
    # 전자
    '가전': 'SEC_ELECTRONICS', 'DX': 'SEC_ELECTRONICS', 'CE': 'SEC_ELECTRONICS',
    '전자': 'SEC_ELECTRONICS', '전자제품': 'SEC_ELECTRONICS',
    '디스플레이': 'SEC_ELECTRONICS', '전자재료': 'SEC_ELECTRONICS',
    '전자부품': 'SEC_ELECTRONICS', '전자소재': 'SEC_ELECTRONICS',
    
    # 에너지 (정유/연료 중심) - 🆕 P0-1: 정유/석화 토큰 맵 긴급 수혈
    '석유': 'SEC_ENERGY',  # 정유 원료
    '정유': 'SEC_ENERGY',  # 정유 사업
    '정제유': 'SEC_ENERGY',  # 정제된 유류
    '휘발유': 'SEC_ENERGY',  # 휘발유
    '경유': 'SEC_ENERGY',  # 경유
    '등유': 'SEC_ENERGY',  # 등유
    '무연': 'SEC_ENERGY',  # 무연 휘발유
    '연료유': 'SEC_ENERGY',  # 연료유
    '윤활유': 'SEC_ENERGY',  # 윤활유
    '윤활기유': 'SEC_ENERGY',  # 윤활기유
    # 🆕 P0-1: 항공유 및 기타 연료 매핑 추가 (피드백 반영)
    '항공유': 'SEC_ENERGY',  # 항공 연료 (Jet Fuel)
    'Jet Fuel': 'SEC_ENERGY',  # Jet Fuel (영문)
    'JET FUEL': 'SEC_ENERGY',  # JET FUEL (대문자)
    '벙커C': 'SEC_ENERGY',  # 벙커C유 (B-C유)
    '중유': 'SEC_ENERGY',  # 중유
    
    # 화학 (석유화학 원료/제품 중심) - 🆕 P0-1: 정유/석화 토큰 맵 긴급 수혈
    '화학': 'SEC_CHEM', '석유화학': 'SEC_CHEM',
    '화학부문': 'SEC_CHEM', '케미칼': 'SEC_CHEM', '정밀화학': 'SEC_CHEM',
    '나프타': 'SEC_CHEM',  # 나프타 (석화 원료)
    '납사': 'SEC_CHEM',  # 납사 (석화 원료)
    'PX': 'SEC_CHEM',  # PX (Paraxylene)
    'B-C': 'SEC_CHEM',  # B-C (Fuel Oil)
    '벤젠': 'SEC_CHEM',  # 벤젠
    '톨루엔': 'SEC_CHEM',  # 톨루엔
    '자일렌': 'SEC_CHEM',  # 자일렌
    '올레핀': 'SEC_CHEM',  # 올레핀
    '아로마틱': 'SEC_CHEM',  # 아로마틱
    
    # 금융
    '금융': 'SEC_FINANCE', '금융부문': 'SEC_FINANCE', '금융업': 'SEC_FINANCE',
    # 🆕 P0-3: 금융 세그먼트 매핑 확장 (정규화 기반으로 자동 매핑되도록)
    '은행': 'SEC_BANK', '은행부문': 'SEC_BANK', '은행 부문': 'SEC_BANK',
    '여신': 'SEC_BANK', '여신부문': 'SEC_BANK', '저축은행': 'SEC_BANK',
    '보험': 'SEC_INS', '보험부문': 'SEC_INS', '보험 부문': 'SEC_INS',
    '생명': 'SEC_INS', '생명보험': 'SEC_INS', '생명부문': 'SEC_INS',
    '손해': 'SEC_INS', '손해보험': 'SEC_INS', '손해부문': 'SEC_INS',
    '화재': 'SEC_INS', '화재보험': 'SEC_INS',
    '증권': 'SEC_SEC', '증권부문': 'SEC_SEC', '증권 부문': 'SEC_SEC',
    '금융투자': 'SEC_SEC', '금융투자부문': 'SEC_SEC',
    '자산운용': 'SEC_SEC', '자산운용부문': 'SEC_SEC',
    '카드': 'SEC_CARD', '카드부문': 'SEC_CARD', '카드 부문': 'SEC_CARD',
    '신용카드': 'SEC_CARD', '캐피탈': 'SEC_CARD', '캐피탈부문': 'SEC_CARD',
    
    # 식품
    '식품': 'SEC_FOOD', '음료': 'SEC_FOOD', '급식': 'SEC_FOOD',
    '식자재': 'SEC_FOOD', '식품부문': 'SEC_FOOD', '식품사업': 'SEC_FOOD',
    
    # 🆕 화장품 (누락되어 있었음!)
    '화장품': 'SEC_COSMETIC', '뷰티': 'SEC_COSMETIC', '스킨케어': 'SEC_COSMETIC',
    '코스메틱': 'SEC_COSMETIC',
    
    # 섬유/패션
    '섬유': 'SEC_FASHION', '패션': 'SEC_FASHION', '의류': 'SEC_FASHION',
    '섬유부문': 'SEC_FASHION', '의류부문': 'SEC_FASHION', '패션부문': 'SEC_FASHION',
    
    # 철강
    '철강': 'SEC_STEEL', '금속': 'SEC_STEEL', '철강부문': 'SEC_STEEL',
    '철강제품': 'SEC_STEEL', '1차금속': 'SEC_STEEL',
    
    # 통신
    '통신': 'SEC_TELECOM', '이동통신': 'SEC_TELECOM', '통신부문': 'SEC_TELECOM',
    '정보통신': 'SEC_TELECOM',
    
    # 엔터테인먼트/미디어/게임 (🆕 게임 추가)
    '미디어': 'SEC_ENT', '엔터테인먼트': 'SEC_ENT', '방송': 'SEC_ENT',
    '콘텐츠': 'SEC_ENT', '게임': 'SEC_ENT', '모바일게임': 'SEC_ENT',
    
    # 여행/레저 (🆕 P0-4: 치명 오탐 방지를 위해 최소 길이 2자 이상만 허용)
    '리조트': 'SEC_TRAVEL', '레저': 'SEC_TRAVEL', '호텔': 'SEC_TRAVEL',
    '여행': 'SEC_TRAVEL', '항공': 'SEC_TRAVEL', '항공운수': 'SEC_TRAVEL',
    '여행사': 'SEC_TRAVEL', '항공사': 'SEC_TRAVEL', '관광': 'SEC_TRAVEL',
    
    # 에너지/유틸리티
    '전력': 'SEC_UTIL', '에너지': 'SEC_UTIL', '발전': 'SEC_UTIL',
    '가스': 'SEC_UTIL', '전기': 'SEC_UTIL', '도시가스': 'SEC_UTIL',
    '중전기': 'SEC_UTIL', '전력기기': 'SEC_UTIL',
    
    # 배터리/전지
    '배터리': 'SEC_BATTERY', '배터리사업': 'SEC_BATTERY', '이차전지': 'SEC_BATTERY', '2차전지': 'SEC_BATTERY',
    '리튬이온': 'SEC_BATTERY', '리튬이온전지': 'SEC_BATTERY',
    'Battery': 'SEC_BATTERY', 'battery': 'SEC_BATTERY',
    # 🆕 P0.5: 자회사명 → 사업부문 매핑 (지주사형 기업 대응)
    'SK온': 'SEC_BATTERY', '에스케이온': 'SEC_BATTERY',
    'SK energy': 'SEC_ENERGY', 'SK Energy': 'SEC_ENERGY', 'SK에너지': 'SEC_ENERGY',
    'SK지오센트릭': 'SEC_CHEM',
    
    # 조선/해운
    '조선': 'SEC_SHIP', '해운': 'SEC_SHIP', '선박': 'SEC_SHIP',
    '해운업': 'SEC_SHIP', '조선부문': 'SEC_SHIP',
    
    # 기계/농기계
    '기계': 'SEC_MACH', '중공업': 'SEC_MACH', '플랜트': 'SEC_MACH',
    '공작기계': 'SEC_MACH', '산업기계': 'SEC_MACH', '농기계': 'SEC_MACH',
    
    # 🆕 IT/소프트웨어
    '솔루션': 'SEC_IT', 'SI': 'SEC_IT', '시스템통합': 'SEC_IT',
    '소프트웨어': 'SEC_IT', 'IT서비스': 'SEC_IT', 'IT사업': 'SEC_IT',
    'IT사업부문': 'SEC_IT', 'IT': 'SEC_IT', 'AI': 'SEC_IT',
    '유지보수': 'SEC_IT', '모바일': 'SEC_IT',
    '파워솔루션': 'SEC_IT', 'MRO솔루션': 'SEC_IT', '에코솔루션': 'SEC_IT',
    '보안솔루션': 'SEC_IT', 'AM솔루션': 'SEC_IT', '디지털솔루션': 'SEC_IT',
    
    # 🆕 지주회사 (매출 비중 특수 패턴)
    '배당금수익': 'SEC_HOLDING', '배당수익': 'SEC_HOLDING',
    '상표권사용': 'SEC_HOLDING', '상표권': 'SEC_HOLDING',
    '로열티': 'SEC_HOLDING', '브랜드사용료': 'SEC_HOLDING',
    '임대수익': 'SEC_HOLDING', '임대사업': 'SEC_HOLDING',
    '임대': 'SEC_HOLDING', '임대부문': 'SEC_HOLDING',
    '투자부문': 'SEC_HOLDING', '자회사': 'SEC_HOLDING', '계열사': 'SEC_HOLDING',
    '지주회사': 'SEC_HOLDING', '지주부문': 'SEC_HOLDING', '지주사업': 'SEC_HOLDING',
    
    # 🆕 제조업 (기계/제조 관련) - ⚠️ '제조' 단독은 제거 (SEC_MACH 덤핑 방지)
    # '제조': 'SEC_MACH',  # ❌ 제거: 중립 세그먼트로 처리
    # '제조부문': 'SEC_MACH',  # ❌ 제거: 중립 세그먼트로 처리
    # '제조사업': 'SEC_MACH',  # ❌ 제거: 중립 세그먼트로 처리
    # '제조 부문': 'SEC_MACH',  # ❌ 제거: 중립 세그먼트로 처리
    # ✅ 구체적인 제조업만 매핑 (기계 고유 단서 포함)
    '임가공': 'SEC_MACH', '임가공매출': 'SEC_MACH',
    '내화물': 'SEC_MACH', '내화물 제조업': 'SEC_MACH',
    '석도강판': 'SEC_MACH', '석도강판의제조': 'SEC_MACH',
    '합성수지': 'SEC_MACH', '합성수지등 제조': 'SEC_MACH',
    '기계부품 제조업': 'SEC_MACH', '기계사업부문': 'SEC_MACH',
    
    # 🆕 자동차 부품 확장
    '타이어': 'SEC_AUTO', '블랙박스': 'SEC_AUTO', '전장부품': 'SEC_AUTO',
    '자동차부품 제조업': 'SEC_AUTO',
    
    # 🆕 바이오 확장
    '항생제': 'SEC_BIO',
    
    # 🆕 철강 확장
    '강관': 'SEC_STEEL', '철강제품': 'SEC_STEEL', '판재': 'SEC_STEEL',
    
    # 🆕 식품 확장
    '사료': 'SEC_FOOD', '사료부문': 'SEC_FOOD',
    
    # 🆕 전자 확장
    'OLED': 'SEC_ELECTRONICS',
    
    # 🆕 엔터테인먼트 확장
    '광고': 'SEC_ENT', '광고대행': 'SEC_ENT',
    
    # 🆕 유통/물류 확장
    '운송': 'SEC_RETAIL', '운송사업부': 'SEC_RETAIL', '운송사업': 'SEC_RETAIL',
    '물류사업부문': 'SEC_RETAIL', '물류 부문': 'SEC_RETAIL',
    
    # 🆕 소비재 확장
    '생활용품': 'SEC_CONSUMER',
    
    # 🆕 건설 확장
    '공사': 'SEC_CONST', '공사수익': 'SEC_CONST',
    
    # 🆕 유틸리티 확장
    '환경사업': 'SEC_UTIL', '환경사업부문': 'SEC_UTIL',
}

# ========================================================================
# 🆕 대기업 특별 처리 (시장 대표성 우선)
# ========================================================================
# 🆕 P0.5: Override를 ticker 우선으로 변경 (회사명 변형에 안정적)
MAJOR_COMPANY_SECTORS_BY_TICKER = {
    # 반도체 대표
    '005930': ('SEC_SEMI', 'MEMORY', 'MIDSTREAM', 'HIGH'),  # 삼성전자
    '000660': ('SEC_SEMI', 'MEMORY', 'MIDSTREAM', 'HIGH'),  # SK하이닉스
    
    # 배터리 대표
    '373220': ('SEC_BATTERY', 'CELL', 'MIDSTREAM', 'HIGH'),  # LG에너지솔루션
    '006400': ('SEC_BATTERY', 'CELL', 'MIDSTREAM', 'HIGH'),  # 삼성SDI
    
    # 자동차 대표
    '005380': ('SEC_AUTO', 'OEM', 'DOWNSTREAM', 'HIGH'),  # 현대자동차
    '000270': ('SEC_AUTO', 'OEM', 'DOWNSTREAM', 'HIGH'),  # 기아
    '012330': ('SEC_AUTO', 'PARTS', 'MIDSTREAM', 'HIGH'),  # 현대모비스
    
    # 바이오 대표
    '207940': ('SEC_BIO', 'CMO', 'MIDSTREAM', 'HIGH'),  # 삼성바이오로직스
    '068270': ('SEC_BIO', 'BIOTECH', 'MIDSTREAM', 'HIGH'),  # 셀트리온
    
    # 조선 대표
    '329180': ('SEC_SHIP', 'SHIPBUILDING', 'MIDSTREAM', 'HIGH'),  # HD현대중공업
    '042660': ('SEC_SHIP', 'SHIPBUILDING', 'MIDSTREAM', 'HIGH'),  # 한화오션
    '010140': ('SEC_SHIP', 'SHIPBUILDING', 'MIDSTREAM', 'HIGH'),  # 삼성중공업
    
    # 화학/에너지 대표 (지주회사)
    '096770': ('SEC_CHEM', 'PETROCHEM', 'MIDSTREAM', 'HIGH'),  # SK이노베이션
}

# 하위 호환성: 회사명 기반 매핑 (보조용)
MAJOR_COMPANY_SECTORS = {
    # 반도체 대표
    '삼성전자': ('SEC_SEMI', 'MEMORY', 'MIDSTREAM', 'HIGH'),
    'SK하이닉스': ('SEC_SEMI', 'MEMORY', 'MIDSTREAM', 'HIGH'),
    
    # 배터리 대표
    'LG에너지솔루션': ('SEC_BATTERY', 'CELL', 'MIDSTREAM', 'HIGH'),
    '삼성SDI': ('SEC_BATTERY', 'CELL', 'MIDSTREAM', 'HIGH'),
    
    # 자동차 대표
    '현대자동차': ('SEC_AUTO', 'OEM', 'DOWNSTREAM', 'HIGH'),
    '현대차': ('SEC_AUTO', 'OEM', 'DOWNSTREAM', 'HIGH'),
    '기아': ('SEC_AUTO', 'OEM', 'DOWNSTREAM', 'HIGH'),
    '현대모비스': ('SEC_AUTO', 'PARTS', 'MIDSTREAM', 'HIGH'),
    
    # 바이오 대표
    '삼성바이오로직스': ('SEC_BIO', 'CMO', 'MIDSTREAM', 'HIGH'),
    '셀트리온': ('SEC_BIO', 'BIOTECH', 'MIDSTREAM', 'HIGH'),
    
    # 조선 대표
    'HD현대중공업': ('SEC_SHIP', 'SHIPBUILDING', 'MIDSTREAM', 'HIGH'),
    '한화오션': ('SEC_SHIP', 'SHIPBUILDING', 'MIDSTREAM', 'HIGH'),
    '삼성중공업': ('SEC_SHIP', 'SHIPBUILDING', 'MIDSTREAM', 'HIGH'),
    
    # 화학/에너지 대표 (지주회사)
    'SK이노베이션': ('SEC_CHEM', 'PETROCHEM', 'MIDSTREAM', 'HIGH'),
    'SK이노': ('SEC_CHEM', 'PETROCHEM', 'MIDSTREAM', 'HIGH'),
}


def is_neutral_segment(segment: str) -> bool:
    """
    P0-1: Neutral 세그먼트 판정 (점수 계산에서 제외)
    
    Args:
        segment: 세그먼트명
    
    Returns:
        Neutral 여부
    """
    if not segment or not isinstance(segment, str):
        return False
    
    segment_lower = segment.lower().strip()
    normalized = normalize_segment_name(segment)
    
    # 직접 매칭
    if segment_lower in NEUTRAL_SEGMENTS or normalized in NEUTRAL_SEGMENTS:
        return True
    
    # 부분 매칭 (범용 토큰 포함 여부)
    for neutral in NEUTRAL_SEGMENTS:
        if neutral in segment_lower or neutral in normalized:
            return True
    
    return False


def normalize_segment_name(segment: str) -> str:
    """
    세그먼트명 표준화 (P0-3: 정규화 강화 + P0 피드백 반영)
    
    정규화 규칙:
    1. 유니코드 정규화 (NFKC) - 합성/분해 문자 통일
    2. Zero-width 문자 제거 (\u200b, \u200c, \u200d 등)
    3. NBSP/공백 통일 (일반 공백으로 변환)
    4. 괄호/특수문자 제거
    5. "부문/사업/사업부/Division/Segment" 제거
    6. 공백/중복 토큰 정리 (공백 변형 통일)
    7. 동의어 통합 (이차전지=2차전지, 코스메틱=화장품, SI=시스템통합 등)
    8. "기타/상품/제품/용역" 같은 범용 토큰 제거 후 핵심명만 추출
    
    Args:
        segment: 원본 세그먼트명 (예: "건설부문 (주택)", "기타 사업")
    
    Returns:
        정규화된 세그먼트명 (예: "건설", "주택")
    """
    import re
    import unicodedata
    
    if not segment or not isinstance(segment, str):
        return ""
    
    # 🆕 P0-3: 유니코드 정규화 (NFKC) - 합성/분해 문자 통일
    normalized = unicodedata.normalize('NFKC', segment)
    
    # 🆕 P0-3: Zero-width 문자 제거 (\u200b, \u200c, \u200d, \ufeff 등)
    zero_width_chars = [
        '\u200b',  # Zero Width Space
        '\u200c',  # Zero Width Non-Joiner
        '\u200d',  # Zero Width Joiner
        '\ufeff',  # Zero Width No-Break Space
        '\u200e',  # Left-to-Right Mark
        '\u200f',  # Right-to-Left Mark
        '\u202a',  # Left-to-Right Embedding
        '\u202b',  # Right-to-Left Embedding
        '\u202c',  # Pop Directional Formatting
        '\u202d',  # Left-to-Right Override
        '\u202e',  # Right-to-Left Override
    ]
    for zw_char in zero_width_chars:
        normalized = normalized.replace(zw_char, '')
    
    # 🆕 P0-3: NBSP 및 기타 공백 문자 통일 (일반 공백으로 변환)
    normalized = normalized.replace('\xa0', ' ')  # Non-breaking space
    normalized = normalized.replace('\u3000', ' ')  # Ideographic space
    normalized = normalized.replace('\u2000', ' ')  # En quad
    normalized = normalized.replace('\u2001', ' ')  # Em quad
    normalized = normalized.replace('\u2002', ' ')  # En space
    normalized = normalized.replace('\u2003', ' ')  # Em space
    normalized = normalized.replace('\u2004', ' ')  # Three-per-em space
    normalized = normalized.replace('\u2005', ' ')  # Four-per-em space
    normalized = normalized.replace('\u2006', ' ')  # Six-per-em space
    normalized = normalized.replace('\u2007', ' ')  # Figure space
    normalized = normalized.replace('\u2008', ' ')  # Punctuation space
    normalized = normalized.replace('\u2009', ' ')  # Thin space
    normalized = normalized.replace('\u200a', ' ')  # Hair space
    
    # 1. 소문자 변환
    normalized = normalized.lower().strip()
    
    # 2. 괄호 내용 제거 (예: "건설부문 (주택)" → "건설부문")
    normalized = re.sub(r'\([^)]*\)', '', normalized)
    normalized = re.sub(r'\[[^\]]*\]', '', normalized)
    normalized = re.sub(r'\{[^}]*\}', '', normalized)
    
    # 3. 특수문자 제거 (하이픈, 슬래시는 공백으로 변환)
    normalized = re.sub(r'[-/]', ' ', normalized)
    normalized = re.sub(r'[^\w\s가-힣]', '', normalized)  # 한글, 영문, 숫자, 공백만 유지
    
    # 4. 공백 변형 통일 (여러 공백을 하나로) + 🆕 P0-2: 기 타 같은 공백 포함 토큰 처리
    normalized = re.sub(r'\s+', ' ', normalized)
    normalized = normalized.strip()  # 앞뒤 공백 제거
    # 🆕 P0-2: "기 타" 같은 공백 포함 토큰을 "기타"로 통일
    normalized = normalized.replace('기 타', '기타')
    normalized = normalized.replace('기  타', '기타')  # 더블 공백도 처리
    
    # 5. 동의어 통합 (P0-3 강화 + P0.5: 석유/화학 도메인 최소 동의어)
    synonym_map = {
        '이차전지': '2차전지',
        '2차전지': '2차전지',
        '코스메틱': '화장품',
        'si': '시스템통합',
        'ict': 'it',
        'ict사업': 'it사업',
        'ict사업부': 'it사업부',
        'it 서비스': 'it서비스',
        'it사업부문': 'it사업',
        'it사업부': 'it사업',
        # 🆕 P0.5: 석유/화학 도메인 최소 동의어 (Top200 기반)
        '정제유': '정유',  # 정제유 → 정유로 통합
    }
    for old, new in synonym_map.items():
        if old in normalized:
            normalized = normalized.replace(old, new)
    
    # 6. 범용 토큰 제거 (기타, 상품, 제품, 용역 등)
    generic_tokens = [
        '기타', 'other', '기타사업', '기타부문', '기타매출', '기타제품',
        '상품', '제품', '용역', '서비스', '기타사업부문',
        '기타 부문', '기타 매출', '기타 제품', '기타사업', '기타사업부문'
    ]
    
    # 범용 토큰이 단독이거나 주된 단어면 제거
    words = normalized.split()
    filtered_words = []
    for word in words:
        if word not in generic_tokens:
            filtered_words.append(word)
    
    # 7. "부문/사업/사업부/Division/Segment" 제거
    business_suffixes = [
        '부문', '사업', '사업부', '사업부문', '부서', '영역',
        'division', 'segment', 'business', 'unit', 'sector'
    ]
    
    # 접미사 제거
    final_words = []
    for word in filtered_words:
        for suffix in business_suffixes:
            if word.endswith(suffix):
                word = word[:-len(suffix)]
                break
        if word:  # 빈 문자열이 아니면 추가
            final_words.append(word)
    
    # 8. 공백 정리 및 중복 제거
    normalized = ' '.join(final_words).strip()
    normalized = re.sub(r'\s+', ' ', normalized)  # 여러 공백을 하나로
    
    # 9. 빈 문자열이면 원본 반환 (최소한의 정보라도 보존)
    if not normalized:
        # 원본에서 특수문자만 제거한 버전 반환
        fallback = re.sub(r'[^\w\s가-힣]', '', segment.lower().strip())
        return fallback if fallback else segment.lower().strip()
    
    return normalized


def calculate_revenue_sector_scores(revenue_by_segment: Dict[str, float]) -> Tuple[Dict[str, float], Dict[str, Any]]:
    """
    매출 비중을 섹터별 점수로 변환 (비선형 가중치 + 마진 체크 적용)
    
    Args:
        revenue_by_segment: {"건설부문": 44.3, "상사부문": 30.9, ...}
    
    Returns:
        (sector_scores, audit_info)
        sector_scores: {'SEC_CONST': 0.643, 'SEC_RETAIL': 0.509, ...}
        audit_info: {'top1': 'SEC_CONST', 'top2': 'SEC_RETAIL', 'margin': 13.4, ...}
    """
    if not revenue_by_segment or not isinstance(revenue_by_segment, dict):
        return {}, {'coverage': 0, 'mapped_pct': 0}
    
    # 1차: 기본 점수 계산 (보너스 없이)
    sector_base_scores = {}
    segment_mapping = {}  # 어떤 세그먼트가 어떤 섹터로 매핑됐는지 기록
    unmapped_segments = []  # 🆕 P0-3: 매핑 실패한 세그먼트 기록
    mapped_pct = 0  # 매핑된 비중 합계
    total_pct = sum(pct for pct in revenue_by_segment.values() if isinstance(pct, (int, float)) and pct > 0)
    
    for segment, pct in revenue_by_segment.items():
        if not isinstance(pct, (int, float)) or pct <= 0:
            continue
        
        # 🆕 P0-1: Neutral 세그먼트는 점수 계산에서 제외
        if is_neutral_segment(segment):
            continue  # Neutral 세그먼트는 점수 계산에서 제외
        
        # 🆕 P0-1: 세그먼트명 표준화
        normalized_segment = normalize_segment_name(segment)
        
        # 🆕 P0-1: 사업부문명에서 섹터 매핑 (결정론적 + 설명 가능)
        # - 길이 우선 정렬 (긴 키워드가 먼저 매칭)
        # - 짧은 토큰(len<3)은 contains 금지 (exact만 허용)
        # - TRAVEL/ENT 등 치명 섹터는 최소 길이 가드레일
        matched_sector = None
        matched_keyword = None
        match_rule = None
        match_method = None
        
        # 🆕 P0-2: 짧은 키워드 whitelist (2-3자 키워드 중 contains 허용)
        # 피드백 반영: 2-3자 키워드는 exact 우선, whitelist만 contains 허용
        # 🆕 P0-1: JET 오탐 방어 - JET 단독은 exact만 허용, Jet Fuel/JET FUEL은 contains 허용
        SHORT_KEYWORD_WHITELIST = {
            '석유', '화학', '정유', '나프타', '납사', 'PX', 'B-C', '벤젠', '톨루엔', 
            '자일렌', '올레핀', '경유', '등유', '무연', '연료유', '윤활유', '정제유',
            '항공유', 'Jet Fuel', 'JET FUEL'  # 항공유 관련은 whitelist에 포함
        }
        
        # 🆕 P0-4: 치명 오탐 섹터 정의 (최소 길이 가드레일)
        # 피드백 반영: len<3이면 contains 금지이므로, TRAVEL/ENT는 최소 3자 이상만 contains 허용
        CRITICAL_SECTORS = {
            'SEC_TRAVEL': {'min_length': 3, 'exact_only': False},  # 3자 이상만 contains 허용 (여행=2자는 exact만)
            'SEC_ENT': {'min_length': 3, 'exact_only': False},  # 3자 이상만 contains 허용
        }
        
        # 🆕 P0-1: 키워드를 길이 내림차순으로 정렬 (긴 키워드 우선)
        sorted_keywords = sorted(SEGMENT_TO_SECTOR_MAP.items(), key=lambda x: len(x[0]), reverse=True)
        
        normalized_lower = normalized_segment.lower()
        segment_lower = str(segment).lower()
        
        # 🆕 P0.6-C: 항공 관련 오탐 방지 - "항공유"만 SEC_ENERGY로 매핑
        # "항공", "항공사업", "항공운수" 등은 SEC_ENERGY 매핑에서만 제외
        # 섹터는 기존 로직(매출 매핑/키워드/업종)이 결정 (SEC_TRAVEL 강제 금지)
        is_aviation_related = False
        if '항공' in normalized_segment or '항공' in segment:
            # "항공유"가 정확히 포함된 경우만 제외
            if '항공유' not in normalized_segment and '항공유' not in segment:
                # "항공사업", "항공운수", "항공사" 등은 SEC_ENERGY로 매핑 금지
                is_aviation_related = True
        
        # 정규화된 세그먼트명으로 매핑 시도
        for keyword, sector in sorted_keywords:
            keyword_lower = keyword.lower()
            keyword_len = len(keyword)
            
            # 🆕 P0.6-C: 항공 관련 오탐 방지
            # 항공 관련 세그먼트는 SEC_ENERGY 키워드 매핑만 제외 (항공유 제외)
            # 섹터는 기존 로직이 결정 (강제 할당 금지)
            if is_aviation_related and sector == 'SEC_ENERGY' and keyword != '항공유':
                continue  # 항공/항공사업 등은 연료로 매핑 금지 (섹터는 기존 로직이 결정)
            
            # 🆕 P0-2: 2-3자 키워드는 exact 우선, whitelist만 contains 허용
            if keyword_len < 4:
                # exact match 시도
                if keyword_lower == normalized_lower or keyword_lower == segment_lower:
                    matched_sector = sector
                    matched_keyword = keyword
                    match_rule = 'exact'
                    match_method = 'normalized' if keyword_lower == normalized_lower else 'raw'
                    break
                # whitelist에 있으면 contains 허용
                elif keyword in SHORT_KEYWORD_WHITELIST:
                    if keyword_lower in normalized_lower or keyword_lower in segment_lower:
                        matched_sector = sector
                        matched_keyword = keyword
                        match_rule = 'contains'
                        match_method = 'normalized' if keyword_lower in normalized_lower else 'raw'
                        break
                # whitelist에 없으면 exact만 허용 (이미 위에서 실패했으므로 continue)
                continue
            else:
                # 🆕 P0-4: 치명 섹터 가드레일 체크
                sector_guard = CRITICAL_SECTORS.get(sector)
                if sector_guard:
                    min_length = sector_guard.get('min_length', 2)
                    exact_only = sector_guard.get('exact_only', False)
                    
                    # 최소 길이 미달이면 스킵
                    if keyword_len < min_length:
                        continue
                    
                    # exact_only면 exact만 허용
                    if exact_only:
                        if keyword_lower == normalized_lower or keyword_lower == segment_lower:
                            matched_sector = sector
                            matched_keyword = keyword
                            match_rule = 'exact'
                            match_method = 'normalized' if keyword_lower == normalized_lower else 'raw'
                            break
                        continue
                
                # contains 매칭 (정규화된 버전 우선)
                if keyword_lower in normalized_lower or keyword in normalized_segment:
                    matched_sector = sector
                    matched_keyword = keyword
                    match_rule = 'contains'
                    match_method = 'normalized'
                    break
                # 원본으로 재시도
                elif keyword_lower in segment_lower or keyword in segment_lower:
                    matched_sector = sector
                    matched_keyword = keyword
                    match_rule = 'contains'
                    match_method = 'raw'
                    break
        
        if matched_sector:
            mapped_pct += pct
            # 🆕 P0-2: decision_trace에 매칭 근거 저장
            segment_mapping[segment] = {
                'sector': matched_sector,
                'pct': pct,
                'matched_keyword': matched_keyword,  # 🆕 P0-2: 어떤 키워드가 매칭됐는지
                'match_rule': match_rule,  # 🆕 P0-2: exact/contains
                'match_method': match_method,  # 🆕 P0-2: normalized/raw
                'segment_raw': segment,  # 🆕 P0-2: 원본 세그먼트
                'segment_norm': normalized_segment,  # 🆕 P0-2: 정규화된 세그먼트
            }
            
            # 기본 점수 누적
            current = sector_base_scores.get(matched_sector, 0)
            sector_base_scores[matched_sector] = current + (pct / 100.0)
        else:
            # 🆕 P0-3: 매핑 실패한 세그먼트 기록 (unmapped_top용)
            unmapped_segments.append({
                'segment': segment,
                'pct': pct,
                'normalized': normalized_segment,
                'reason': 'NO_KEYWORD'  # 매핑 실패 이유
            })
    
    # 커버리지 계산
    coverage = (mapped_pct / total_pct * 100) if total_pct > 0 else 0
    
    # 2차: 마진 체크 후 보너스 적용
    sector_scores = {}
    sorted_sectors = sorted(sector_base_scores.items(), key=lambda x: x[1], reverse=True)
    
    top1_sector = sorted_sectors[0] if sorted_sectors else (None, 0)
    top2_sector = sorted_sectors[1] if len(sorted_sectors) > 1 else (None, 0)
    margin = (top1_sector[1] - top2_sector[1]) * 100 if top2_sector[0] else top1_sector[1] * 100
    
    for sector, base_score in sector_base_scores.items():
        pct = base_score * 100
        
        # 🎯 P0-4: 마진 체크 - top1 vs top2 차이가 5% 미만이면 보너스 미적용
        if margin >= 5.0:
            bonus = 0.2 if pct >= 30.0 else (0.1 if pct >= 20.0 else 0.0)
        else:
            bonus = 0  # 마진이 좁으면 보너스 없이 순수 비율로 판단
        
        sector_scores[sector] = min(1.0, base_score + bonus)
    
    # 🆕 P0-3: unmapped_top 정렬 (비중 큰 순으로 Top3)
    unmapped_top = sorted(unmapped_segments, key=lambda x: x['pct'], reverse=True)[:3]
    
    # Audit 정보
    audit_info = {
        'top1': top1_sector[0],
        'top1_pct': top1_sector[1] * 100,
        'top2': top2_sector[0],
        'top2_pct': top2_sector[1] * 100 if top2_sector[0] else 0,
        'margin': margin,
        'bonus_applied': margin >= 5.0,
        'coverage': coverage,
        'mapped_pct': mapped_pct,
        'total_pct': total_pct,
        'segment_mapping': segment_mapping,
        'unmapped_top': unmapped_top  # 🆕 P0-3: 매핑 실패 Top3
    }
    
    return sector_scores, audit_info


def calculate_revenue_quality(
    revenue_by_segment: Dict[str, float],
    total_pct: float,
    neutral_ratio: float
) -> Tuple[str, Dict[str, Any]]:
    """
    🆕 P0: revenue_quality 표준화 (OK/WARN/BAD)
    
    Args:
        revenue_by_segment: 매출 비중 딕셔너리
        total_pct: 합계 비율
        neutral_ratio: Neutral 세그먼트 비율
    
    Returns:
        (quality, quality_meta)
        quality: 'OK' | 'WARN' | 'BAD'
        quality_meta: {'neutral_ratio', 'segments_count', 'sum_pct', 'quality_reason'}
    """
    if not revenue_by_segment or not isinstance(revenue_by_segment, dict) or len(revenue_by_segment) == 0:
        return 'BAD', {
            'neutral_ratio': 0.0,
            'segments_count': 0,
            'sum_pct': 0.0,
            'quality_reason': 'NO_REVENUE_DATA'
        }
    
    segments_count = len([s for s in revenue_by_segment.keys() if not is_neutral_segment(s)])
    
    # 품질 판정 기준
    quality = 'OK'
    quality_reason_codes = []  # 🆕 표준 코드 리스트
    
    # 1. 합계 비율 체크 (70-130% 범위)
    if total_pct < 70 or total_pct > 130:
        quality = 'BAD'
        quality_reason_codes.append('SUM_OUT_OF_RANGE')
    elif total_pct < 80 or total_pct > 120:
        quality = 'WARN'
        quality_reason_codes.append('SUM_MARGINAL')
    
    # 2. Neutral 비율 체크 (50% 이상이면 WARN)
    if neutral_ratio >= 50.0:
        if quality == 'OK':
            quality = 'WARN'
        quality_reason_codes.append('NEUTRAL_RATIO_HIGH')
    
    # 3. 세그먼트 개수 체크 (1개면 WARN)
    if segments_count <= 1:
        if quality == 'OK':
            quality = 'WARN'
        quality_reason_codes.append('SEGMENTS_TOO_FEW')
    
    # 🆕 표준 코드: NO_REVENUE_DATA | SUM_OUT_OF_RANGE | SUM_MARGINAL | NEUTRAL_RATIO_HIGH | SEGMENTS_TOO_FEW | TEXT_ONLY_PERCENT_UNCONFIRMED | LLM_STRUCT_FAIL
    quality_reason_code = ';'.join(quality_reason_codes) if quality_reason_codes else 'OK'
    
    quality_meta = {
        'neutral_ratio': neutral_ratio,
        'segments_count': segments_count,
        'sum_pct': total_pct,
        'quality_reason': quality_reason_code  # 🆕 표준 코드로 저장
    }
    
    return quality, quality_meta


def calculate_dynamic_weights(
    revenue_by_segment: Dict[str, float],
    revenue_quality: Optional[str] = None,
    quality_meta: Optional[Dict[str, Any]] = None
) -> Tuple[float, float, float]:
    """
    데이터 품질에 따른 동적 가중치 결정 (P1-1: 기타 세그먼트 처리 정책 포함)
    🆕 P0: revenue_quality 기반 preset 가중치 적용
    
    Args:
        revenue_by_segment: 매출 비중 딕셔너리
        revenue_quality: 'OK' | 'WARN' | 'BAD' (선택)
        quality_meta: 품질 메타데이터 (선택)
    
    Returns:
        (w_revenue, w_keyword, w_product)
    """
    if not revenue_by_segment or not isinstance(revenue_by_segment, dict) or len(revenue_by_segment) == 0:
        # 매출 비중 데이터 없음 → 키워드/제품 의존
        return (0.0, 0.50, 0.50)
    
    # 🆕 P0: revenue_quality 기반 preset 가중치 (우선 적용)
    if revenue_quality:
        if revenue_quality == 'OK':
            return (0.6, 0.2, 0.2)  # OK: revenue 신뢰
        elif revenue_quality == 'WARN':
            return (0.2, 0.4, 0.4)  # WARN: revenue 신뢰도 낮춤
        else:  # BAD
            return (0.0, 0.50, 0.50)  # BAD: revenue 무시
    
    # 매출 비중 합계 및 최대값 확인
    total_pct = sum(p for p in revenue_by_segment.values() if isinstance(p, (int, float)))
    max_pct = max((p for p in revenue_by_segment.values() if isinstance(p, (int, float))), default=0)
    
    # 🆕 P1-1: 기타 세그먼트 처리 정책
    # 기타/상품/제품/용역 같은 범용 토큰 확인
    generic_keywords = ['기타', 'other', '그외', '상품', '제품', '용역', '서비스']
    other_ratio = 0
    top1_is_generic = False
    
    # Top1 세그먼트가 범용 토큰인지 확인
    if revenue_by_segment:
        sorted_segments = sorted(
            revenue_by_segment.items(),
            key=lambda x: x[1] if isinstance(x[1], (int, float)) else 0,
            reverse=True
        )
        if sorted_segments:
            top1_segment_name = str(sorted_segments[0][0]).lower()
            top1_segment_pct = sorted_segments[0][1] if isinstance(sorted_segments[0][1], (int, float)) else 0
            
            # Top1이 범용 토큰인지 확인
            normalized_top1 = normalize_segment_name(sorted_segments[0][0])
            if any(kw in normalized_top1 for kw in generic_keywords) or any(kw in top1_segment_name for kw in generic_keywords):
                top1_is_generic = True
                other_ratio = top1_segment_pct
    
    # 기타 비율 확인 (전체)
    if not top1_is_generic:
        for seg, pct in revenue_by_segment.items():
            if isinstance(pct, (int, float)):
                normalized_seg = normalize_segment_name(str(seg))
                if any(kw in normalized_seg for kw in generic_keywords) or any(kw in str(seg).lower() for kw in generic_keywords):
                    other_ratio += pct
    
    # 🆕 P1-1: 기타 세그먼트 처리 정책
    # 기타가 Top1이면 → revenue 신뢰도 낮추고 키워드/제품 가중치로 전환
    if top1_is_generic:
        # 기타가 Top1 → revenue 신뢰도 매우 낮음
        return (0.15, 0.425, 0.425)
    
    # 동적 가중치 결정
    # 1. coverage 높고(기타 낮고) top1이 크면 → w_revenue ↑ (0.6~0.8)
    # 2. 기타가 크거나 세그먼트가 난잡하면 → w_revenue ↓ (0.2~0.4)
    
    if max_pct >= 50.0 and other_ratio < 20.0:
        # 지배적 섹터 존재, 데이터 품질 좋음
        return (0.70, 0.15, 0.15)
    elif max_pct >= 30.0 and other_ratio < 30.0:
        # 적절한 분포
        return (0.50, 0.25, 0.25)
    elif other_ratio >= 50.0:
        # 기타가 너무 큼 → 매출 비중 신뢰도 낮음
        return (0.20, 0.40, 0.40)
    else:
        # 기본값
        return (0.40, 0.30, 0.30)

# 섹터 키워드 매핑 (Rule-based)
SECTOR_KEYWORDS = {
    'SEC_SEMI': {
        'keywords': ['반도체', '메모리', 'DRAM', 'NAND', 'HBM', 'DDR', 'LPDDR', 'SSD', '웨이퍼', '칩', 'SoC', 'AP', '패키징', '전자부품'],
        'products': ['반도체', '메모리', 'DRAM', 'NAND', 'HBM', 'DDR', 'SSD', '웨이퍼', '칩'],
        'sub_sectors': {
            'MEMORY': ['DRAM', 'NAND', 'HBM', 'DDR', 'LPDDR', 'SSD', '메모리'],
            'FOUNDRY': ['파운드리', '웨이퍼', '공정', 'TSMC'],
            'DISPLAY': ['디스플레이', 'OLED', 'LCD', '패널', 'QD-OLED'],
            'SEMI_EQUIP': ['반도체장비', '장비', '설비', '공정장비']
        }
    },
    'SEC_IT': {
        'keywords': ['IT', '소프트웨어', '플랫폼', '클라우드', 'SaaS', '보안', '인터넷', '네트워크', 'SI', '솔루션'],
        'products': ['소프트웨어', '플랫폼', '앱', '보안', '클라우드'],
        'sub_sectors': {
            'SOFTWARE': ['소프트웨어', 'SaaS', '플랫폼', '클라우드'],
            'SECURITY': ['보안', '네트워크', 'SI', '솔루션'],
            'INTERNET': ['인터넷', '플랫폼', '서비스']
        }
    },
    # ⭐ 신규: 게임 섹터
    'SEC_GAME': {
        'keywords': ['게임', '모바일게임', 'PC게임', '콘솔게임', '온라인게임', '모바일앱', '게임개발', '게임퍼블리싱', 'e스포츠', 'e-스포츠'],
        'products': ['게임', '모바일게임', 'PC게임', '온라인게임'],
        'sub_sectors': {
            'MOBILE_GAME': ['모바일게임', '모바일앱'],
            'PC_CONSOLE': ['PC게임', '콘솔게임'],
            'ESPORTS': ['e스포츠', 'e-스포츠']
        }
    },
    'SEC_AUTO': {
        'keywords': ['자동차', '차량', '전기차', 'EV', '배터리', '충전', '자동차부품', '전장', '모터', '인버터'],
        'products': ['자동차', '전기차', '배터리', '부품', '모터'],
        'sub_sectors': {
            'EV': ['전기차', 'EV', '배터리', '충전'],
            'AUTO_PARTS': ['자동차부품', '전장', '모터', '인버터']
        }
    },
    'SEC_BATTERY': {
        'keywords': ['배터리', '2차전지', '양극재', '음극재', '전해액', '리튬', '니켈', '코발트', 'LFP', 'NCM'],
        'products': ['배터리', '2차전지', '양극재', '음극재', '전해액'],
        'sub_sectors': {
            'CELL': ['셀', '배터리팩'],
            'MATERIAL': ['양극재', '음극재', '전해액', '분리막']
        }
    },
    # 🆕 P0-1: 에너지 섹터 추가 (정유/연료 중심)
    'SEC_ENERGY': {
        'keywords': ['석유', '정유', '정제유', '휘발유', '경유', '등유', '무연', '연료유', '윤활유', '윤활기유'],
        'products': ['석유', '정유', '정제유', '휘발유', '경유', '등유', '연료유'],
        'sub_sectors': {
            'REFINERY': ['정유', '정제유', '석유'],
            'FUEL': ['휘발유', '경유', '등유', '무연', '연료유'],
            'LUBRICANT': ['윤활유', '윤활기유']
        }
    },
    'SEC_CHEM': {
        'keywords': ['화학', '석유화학', '나프타', '납사', 'PX', 'B-C', '벤젠', '톨루엔', '자일렌', '올레핀', '아로마틱', 'LNG', '가스', '폴리머', '수지', '플라스틱', '합성섬유'],
        'products': ['화학', '석유화학', '나프타', '납사', 'PX', 'B-C', '벤젠', '톨루엔', '자일렌', '올레핀', '아로마틱', 'LNG', '폴리머'],
        'sub_sectors': {
            'PETROCHEMICAL': ['석유화학', '나프타', '납사', 'PX', 'B-C', '벤젠', '톨루엔', '자일렌', '올레핀', '아로마틱'],
            'SPECIALTY': ['특수화학', '폴리머', '수지']
        }
    },
    # 하위 호환성을 위해 기존 이름 유지
    'SEC_CHEMICAL': {
        'keywords': ['화학', '석유화학', '정유', 'LNG', '가스', '폴리머', '수지', '플라스틱', '합성섬유'],
        'products': ['화학', '석유화학', '정유', 'LNG', '폴리머'],
        'sub_sectors': {
            'PETROCHEMICAL': ['석유화학', '정유', 'LNG'],
            'SPECIALTY': ['특수화학', '폴리머', '수지']
        }
    },
    'SEC_STEEL': {
        'keywords': ['철강', '강판', '강재', '포스코', '제철', '압연'],
        'products': ['철강', '강판', '강재'],
        'sub_sectors': {
            'STEEL': ['철강', '강판', '강재']
        }
    },
    'SEC_CONST': {
        'keywords': ['건설', '건축', '토목', '인프라', '플랜트', '공사', '건축자재'],
        'products': ['건설', '건축', '토목', '인프라', '플랜트'],
        'sub_sectors': {
            'EPC': ['건설', 'EPC', '플랜트'],
            'CIVIL': ['토목', '인프라', '공사'],
            'BUILDING_MAT': ['건축자재']
        }
    },
    # 하위 호환성을 위해 기존 이름 유지
    'SEC_CONSTRUCTION': {
        'keywords': ['건설', '건축', '토목', '인프라', '부동산', 'PF', '개발'],
        'products': ['건설', '건축', '토목', '인프라'],
        'sub_sectors': {
            'CONSTRUCTION': ['건설', '건축', '토목'],
            'REAL_ESTATE': ['부동산', 'PF', '개발']
        }
    },
    'SEC_BANK': {
        'keywords': ['은행', '대출', '예금', '저축', '은행지주', '기업은행', 'KB금융', '신한지주', '하나금융지주', '우리금융지주'],
        'products': ['은행', '대출', '예금', '저축'],
        'sub_sectors': {
            'BANK': ['은행', '저축은행']
        }
    },
    'SEC_INS': {
        'keywords': ['보험', '생명보험', '손해보험', '재보험'],
        'products': ['보험', '생명보험', '손해보험'],
        'sub_sectors': {
            'LIFE': ['생명보험'],
            'NON_LIFE': ['손해보험', '재보험']
        }
    },
    'SEC_SEC': {
        'keywords': ['증권', '투자', '자산운용', '증권사', '브로커', '미래에셋', '삼성증권', '키움증권'],
        'products': ['증권', '투자', '자산운용'],
        'sub_sectors': {
            'SECURITIES': ['증권', '증권사', '미래에셋'],
            'ASSET_MGMT': ['자산운용', '투자']
        }
    },
    'SEC_CARD': {
        'keywords': ['카드', '신용카드', '체크카드', '결제'],
        'products': ['카드', '신용카드', '결제'],
        'sub_sectors': {
            'CARD': ['카드', '신용카드']
        }
    },
    # ⭐ 지주회사 3분류 체계 (2024-12-24)
    # FINANCIAL_HOLDING: 금융지주 (은행/보험/증권 자회사)
    # PURE_HOLDING: 순수지주 (배당/임대/로열티 비중 ≥50%)
    # BUSINESS_HOLDING: 사업지주 (자체 사업부문 비중 ≥50%)
    'SEC_HOLDING': {
        'keywords': [
            # 기업명 패턴
            '지주', '금융지주', '홀딩스', '홀딩', 'Holdings',
            # 금융지주 기업명
            'KB금융', '신한지주', '하나금융지주', '우리금융지주', '메리츠금융지주',
            # 지주회사 특성 수익 구조
            '배당금수익', '배당수익', '임대수익', '브랜드사용료', '로열티수익', '상표권사용료',
            # 지주회사 특성 키워드
            '자회사관리', '계열사', '경영컨설팅'
        ],
        'products': ['배당금수익', '임대수익', '브랜드사용료', '로열티', '경영관리용역'],
        'sub_sectors': {
            # 금융지주: 은행/보험/증권 자회사 보유
            'FINANCIAL_HOLDING': ['금융지주', 'KB금융', '신한지주', '하나금융지주', '우리금융지주', '메리츠금융지주', '은행지주', '보험지주'],
            # 순수지주: 배당/임대/로열티 비중 ≥50% (LG, 한진칼 등)
            'PURE_HOLDING': ['배당금수익', '임대수익', '상표권사용료', '브랜드사용료', '로열티', '지주사업'],
            # 사업지주: 자체 사업부문 비중 ≥50% (SK, CJ 등)
            'BUSINESS_HOLDING': ['사업부문', '사업지주', '투자부문', '홀딩스', '지주']
        }
    },
    'SEC_ELECTRONICS': {
        'keywords': ['전자', '가전', 'TV', '냉장고', '세탁기', '에어컨', '모니터', 'PC', '노트북'],
        'products': ['TV', '냉장고', '세탁기', '에어컨', '모니터', 'PC'],
        'sub_sectors': {
            'CONSUMER': ['TV', '냉장고', '세탁기', '에어컨'],
            'IT_DEVICE': ['PC', '노트북', '모니터']
        }
    },
    'SEC_MEDICAL': {
        'keywords': ['의료', '바이오', '제약', '백신', '신약', '바이오의약', '진단'],
        'products': ['의료', '바이오', '제약', '백신'],
        'sub_sectors': {
            'PHARMA': ['제약', '신약'],
            'BIO': ['바이오', '바이오의약'],
            'MEDICAL_DEVICE': ['의료기기', '진단']
        }
    },
    'SEC_RETAIL': {
        'keywords': ['유통', '소매', '마트', '편의점', '온라인쇼핑', '이커머스'],
        'products': ['유통', '소매', '마트', '편의점'],
        'sub_sectors': {
            'RETAIL': ['유통', '소매', '마트'],
            'E_COMMERCE': ['온라인쇼핑', '이커머스']
        }
    },
    'SEC_TELECOM': {
        'keywords': ['통신', '이동통신', '인터넷', '5G', '6G', '통신사', '통신망'],
        'products': ['통신', '이동통신', '인터넷', '5G'],
        'sub_sectors': {
            'TELECOM': ['통신', '이동통신', '통신사']
        }
    },
    # 경기소비재 (Discretionary Consumer)
    'SEC_DISCRETIONARY': {
        'keywords': ['미디어', '엔터테인먼트', '게임', 'OTT', '영화', '방송', '뷰티', '화장품', '코스메틱', '의류', '패션', '섬유', '의복', '신발', '소비재', '레저', '관광', '호텔', '레스토랑'],
        'products': ['미디어', '엔터테인먼트', '게임', 'OTT', '뷰티', '화장품', '의류', '패션', '섬유', '신발'],
        'sub_sectors': {
            'MEDIA_ENTERTAINMENT': ['미디어', '엔터테인먼트', '게임', 'OTT', '영화', '방송'],
            'BEAUTY': ['뷰티', '화장품', '코스메틱'],
            'APPAREL': ['의류', '패션', '섬유', '의복', '신발'],
            'OTHER_CONSUMER': ['소비재', '레저', '관광', '호텔', '레스토랑']
        }
    },
    # 필수소비재 (Staple Consumer)
    'SEC_STAPLE': {
        'keywords': ['음식료', '식품', '음료', '식품제조', '가공식품', '냉동식품', '유제품', '농산물', '축산물', '수산물', '담배', '생필품'],
        'products': ['음식료', '식품', '음료', '가공식품', '냉동식품', '유제품', '담배'],
        'sub_sectors': {
            'FOOD_BEVERAGE': ['음식료', '식품', '음료', '가공식품', '냉동식품', '유제품']
        }
    },
    # 산업재 (Industrial)
    'SEC_INDUSTRIAL': {
        'keywords': ['운송', '운수', '물류', '항공', '해운', '철도', '택배', '방위', '방산', '항공기', '무기', '국방', '중공업', '기계', '공작기계', '산업기계', '조선', '선박', '조선소'],
        'products': ['운송', '운수', '물류', '항공', '해운', '조선', '선박', '기계', '공작기계'],
        'sub_sectors': {
            'TRANSPORTATION': ['운송', '운수', '물류', '항공', '해운', '철도', '택배'],
            'DEFENSE': ['방위', '방산', '항공기', '무기', '국방'],
            'HEAVY_INDUSTRY': ['중공업', '기계', '공작기계', '산업기계'],
            'SHIPBUILDING': ['조선', '선박', '조선소']
        }
    },
    # 유틸리티 (Utilities)
    'SEC_UTILITIES': {
        'keywords': ['전기', '가스', '수도', '열', '열공급', '발전', '송전', '배전', '전력', '원자력', '신재생에너지', '태양광', '풍력'],
        'products': ['전기', '가스', '수도', '열', '전력', '발전'],
        'sub_sectors': {
            'POWER': ['전기', '전력', '발전', '송전', '배전', '원자력'],
            'GAS_WATER': ['가스', '수도', '열', '열공급'],
            'RENEWABLE': ['신재생에너지', '태양광', '풍력']
        }
    },
    # 타이어 (Tire) - 모빌리티 하위 섹터
    'SEC_TIRE': {
        'keywords': ['타이어', '타이어제조', '고무', '고무제품', '산업용 고무'],
        'products': ['타이어', '고무제품', '산업용 고무'],
        'sub_sectors': {
            'TIRE': ['타이어', '타이어제조'],
            'RUBBER': ['고무', '고무제품']
        }
    },
    # ⭐ 신규 섹터들 (28개 체계)
    'SEC_SHIP': {
        'keywords': ['조선', '선박', '조선소', '해운', '컨테이너선', 'LNG선', '유조선', '해상운송', '조선기자재'],
        'products': ['조선', '선박', 'LNG선', '컨테이너선'],
        'sub_sectors': {
            'SHIPBUILDING': ['조선', '선박', '조선소'],
            'SHIPPING': ['해운', '해상운송'],
            'SHIP_EQUIPMENT': ['조선기자재']
        }
    },
    'SEC_DEFENSE': {
        'keywords': ['방산', '방위', '무기', '국방', '항공기', '미사일', '레이더', '위성', '우주', '군수물자', 'KFX', 'KF-21'],
        'products': ['무기', '항공기', '미사일', '레이더'],
        'sub_sectors': {
            'DEFENSE': ['방산', '무기', '국방'],
            'AEROSPACE': ['항공기', '위성', '우주']
        }
    },
    'SEC_MACH': {
        # 🆕 P0-2: SEC_MACH 덤핑 방지 - 기계 고유 단서만 포함 ('기계' 단독 제거)
        'keywords': ['공작기계', '산업기계', '전력기기', '건설기계', '중장비', '펌프', '발전기', '모터', '변압기', '플랜트', '중공업', '내화물', '석도강판', '농기계', '산업용기계', '산업장비', '공정장비', '제조장비', '기계부품', '기계설비'],
        'products': ['공작기계', '건설기계', '발전기', '내화물', '석도강판', '농기계'],
        'sub_sectors': {
            'INDUSTRIAL_MACH': ['공작기계', '산업기계'],
            'POWER_EQUIP': ['전력기기', '발전기', '변압기'],
            'CONSTRUCTION_MACH': ['건설기계', '중장비']
        }
    },
    'SEC_ENT': {
        'keywords': ['엔터테인먼트', '미디어', '콘텐츠', 'K-POP', 'K팝', '아이돌', '앨범', '음악', '영화', '드라마', 'OTT', '방송', '연예'],
        'products': ['음악', '앨범', '영화', '드라마', 'OTT'],
        'sub_sectors': {
            'K_POP': ['K-POP', 'K팝', '아이돌', '앨범'],
            'CONTENT': ['영화', '드라마', '콘텐츠'],
            'MEDIA': ['OTT', '방송', '미디어']
        }
    },
    # ⭐ 토스증권 77개 기업 분석 기반 키워드 업데이트 (2024-12-24)
    'SEC_COSMETIC': {
        'keywords': [
            # Top 키워드 (토스증권 분석)
            '화장품', '기초화장품', '색조화장품', '스킨케어', '화장품소재',
            'OEM', 'ODM', 'OEM/ODM', 
            # 기존 키워드
            '뷰티', '코스메틱', '메이크업', '화장품제조',
            # 추가 키워드
            '글로벌수출', '글로벌유통', '외주생산', '생활용품'
        ],
        'products': ['화장품', '기초화장품', '색조화장품', '스킨케어', '메이크업'],
        'sub_sectors': {
            'COSMETIC_BRAND': ['화장품', '뷰티', '코스메틱', '스킨케어', '기초화장품', '색조화장품'],
            'OEM_ODM': ['OEM', 'ODM', 'OEM/ODM', '화장품제조']
        }
    },
    # ⭐ 토스증권 섬유/의류 95개 기업 분석 기반 신설 (2024-12-24)
    'SEC_FASHION': {
        'keywords': [
            # 의류
            '의류', '패션', '섬유', '의복', '봉제', '어패럴', '가먼트',
            '니트', '우븐', '원단', '직물', '원사', '방적', '방직',
            # 서브카테고리
            '스포츠웨어', '아웃도어', '캐주얼', '정장', '란제리', '내의',
            '여성복', '남성복', '아동복', '유아복', '신사복',
            # 소재
            '폴리에스터', '나일론', '면사', '모직', '합성섬유', '기능성섬유',
            '스판덱스', '합성피혁', '피혁',
        ],
        'products': ['의류', '직물', '원단', '니트', '우븐', '스웨터', '셔츠', '바지', '자켓', '코트'],
        'sub_sectors': {
            'FASHION_BRAND': ['브랜드', '패션', '캐주얼', '스포츠웨어', '아웃도어', '럭셔리브랜드'],
            'FASHION_OEM': ['OEM', 'ODM', '봉제', '외주가공', '수출중심'],
            'TEXTILE': ['섬유', '원사', '직물', '방적', '방직', '염색', '면사'],
            # FASHION_RETAIL 제거: 백화점/유통은 SEC_RETAIL, 패션브랜드 유통은 FASHION_BRAND
        }
    },
    'SEC_TRAVEL': {
        'keywords': ['여행', '항공', '카지노', '면세', '호텔', '관광', '레저', '리조트', '여행사', '항공사'],
        'products': ['항공', '호텔', '면세', '카지노'],
        'sub_sectors': {
            'AIRLINE': ['항공', '항공사'],
            'HOTEL_RESORT': ['호텔', '리조트'],
            'CASINO_DUTYFREE': ['카지노', '면세']
        }
    },
    'SEC_BIO': {
        'keywords': ['바이오', '제약', '신약', '바이오의약', '백신', '바이오시밀러', 'CMO', 'CDMO', '임상', '신약개발'],
        'products': ['바이오', '제약', '신약', '백신'],
        'sub_sectors': {
            'PHARMA': ['제약', '신약'],
            'BIOPHARMA': ['바이오', '바이오의약'],
            'CMO_CDMO': ['CMO', 'CDMO']
        }
    },
    'SEC_MEDDEV': {
        'keywords': ['의료기기', '임플란트', '미용기기', '진단기기', '수술기기', '의료장비', '치과기기', '안과기기'],
        'products': ['의료기기', '임플란트', '진단기기'],
        'sub_sectors': {
            'IMPLANT': ['임플란트'],
            'DIAGNOSTIC': ['진단기기', '의료장비'],
            'AESTHETIC': ['미용기기', '치과기기']
        }
    },
    'SEC_CONSUMER': {
        'keywords': ['가구', '인테리어', '렌탈', '가전렌탈', '소비재', '생활용품', '주방용품', '침구'],
        'products': ['가구', '인테리어', '렌탈'],
        'sub_sectors': {
            'FURNITURE': ['가구', '인테리어'],
            'RENTAL': ['렌탈', '가전렌탈']
        }
    },
    'SEC_FOOD': {
        'keywords': ['음식료', '식품', '음료', '식품제조', '가공식품', '냉동식품', '유제품', '농산물', '축산물', '수산물', '담배', '생필품'],
        'products': ['음식료', '식품', '음료', '가공식품', '냉동식품', '유제품', '담배'],
        'sub_sectors': {
            'FOOD_BEVERAGE': ['음식료', '식품', '음료', '가공식품', '냉동식품', '유제품']
        }
    },
    'SEC_UTIL': {
        'keywords': ['전기', '가스', '수도', '열', '열공급', '발전', '송전', '배전', '전력', '원자력', '신재생에너지', '태양광', '풍력', '환경'],
        'products': ['전기', '가스', '수도', '열', '전력', '발전'],
        'sub_sectors': {
            'POWER': ['전기', '전력', '발전', '송전', '배전', '원자력'],
            'GAS_WATER': ['가스', '수도', '열', '열공급'],
            'RENEWABLE': ['신재생에너지', '태양광', '풍력'],
            'ENVIRONMENT': ['환경']
        }
    }
}

# Value Chain 위치 키워드
VALUE_CHAIN_KEYWORDS = {
    'UPSTREAM': ['원재료', '부품', '소재', '조달', '매입', '공급', '업스트림'],
    'MIDSTREAM': ['제조', '생산', '가공', '조립', '패키징', '중간'],
    'DOWNSTREAM': ['판매', '유통', '고객', '매출', '서비스', '다운스트림', '최종']
}


def classify_sector_rule_based(
    company_detail: CompanyDetail,
    company_name: Optional[str] = None,
    ticker: Optional[str] = None
) -> Tuple[Optional[str], Optional[str], Optional[str], str, Optional[Dict]]:
    """
    Rule-based 섹터 분류 (P0 개선: 매출 비중 부스팅 적용)
    
    Args:
        company_detail: CompanyDetail 객체
        company_name: 회사명 (선택)
        ticker: 종목코드 (선택, entity_type_classifier 호출용)
    
    Returns:
        (major_sector, sub_sector, value_chain, confidence, boosting_log)
    """
    if not company_detail:
        return None, None, None, "LOW", {
            'classification_method': 'RULE_BASED',
            'classification_meta': {
                'entity_type': 'REGULAR',
                'primary_sector_source': 'HOLD',
                'revenue_quality': 'BAD',
                'neutral_ratio': 0.0,
                'top1_score': 0.0,
                'margin': 0.0,
                'error': 'NO_COMPANY_DETAIL'
            }
        }
    
    # ========================================================================
    # 🆕 P0: 대기업 특별 처리 (시장 대표성 우선) - 최우선 순위!
    # 🆕 P0.5: Override를 ticker 우선으로 변경 (회사명 변형에 안정적)
    # 🆕 P0 추가: 거대 제조사는 entity_type을 REGULAR로 강제 (오탐 방지)
    # ========================================================================
    override_key_used = None
    override_sector_info = None
    
    # 1. Ticker 우선 확인 (안정적)
    if ticker and ticker in MAJOR_COMPANY_SECTORS_BY_TICKER:
        sector, sub, vc, conf = MAJOR_COMPANY_SECTORS_BY_TICKER[ticker]
        override_key_used = 'ticker'
        override_sector_info = (sector, sub, vc, conf)
        logger.info(f"[대기업 특별처리] {company_name} ({ticker}) → {sector}/{sub} (시장 대표성, ticker 기반)")
    # 2. 회사명 기반 확인 (보조용, 하위 호환성)
    elif company_name:
        for major_company, (sector, sub, vc, conf) in MAJOR_COMPANY_SECTORS.items():
            if major_company in company_name or company_name == major_company:
                override_key_used = 'name'
                override_sector_info = (sector, sub, vc, conf)
                logger.info(f"[대기업 특별처리] {company_name} → {sector}/{sub} (시장 대표성, name 기반)")
                break
    
    if override_sector_info:
        sector, sub, vc, conf = override_sector_info
        # 🆕 P0-4: 하드코딩 override 메타데이터 저장
        # 🆕 P0 추가: entity_type을 REGULAR로 강제 (거대 제조사 오탐 방지)
        # 🆕 A) override_hit 정의/저장 일관성: override_reason이 있으면 항상 override_hit=True
        boosting_log = {
            'classification_method': 'MAJOR_OVERRIDE',
            'classification_meta': {
                'entity_type': 'REGULAR',  # 🆕 거대 제조사는 항상 REGULAR
                'primary_sector_source': 'OVERRIDE',  # 🆕 정규화: 대문자
                'revenue_quality': 'UNKNOWN',
                'neutral_ratio': 0.0,
                'top1_score': 0.0,  # 0.0~1.0 스케일
                'margin': 0.0,  # 0.0~1.0 스케일
                'override': {  # 🆕 A) 한 덩어리로 고정
                    'hit': True,
                    'reason': 'MAJOR_COMPANY_HARDCODED',
                    'source': 'RULE_OVERRIDE',
                    'key_used': override_key_used  # 🆕 P0.5: ticker|name 저장
                },
                'override_reason': 'MAJOR_COMPANY_HARDCODED',  # 하위 호환성
                'override_hit': True,  # 하위 호환성
                'override_key_used': override_key_used,  # 🆕 P0.5: ticker|name 저장
                'override_sector': sector,
                'override_sub_sector': sub,
                'override_value_chain': vc,
                'override_confidence': conf,
                'entity_type_forced': True  # 🆕 entity_type 강제 설정 플래그
            }
        }
        return sector, sub, vc, conf, boosting_log
    
    # 회사명 기반 특별 처리 (우선순위 높음 - 먼저 체크)
    company_name_lower = company_name.lower() if company_name else ""
    
    # 디버깅: 회사명 확인
    logger.debug(f"[Sector Classifier] 회사명: {company_name}, Lower: {company_name_lower}")
    
    # 🆕 P1-4: 금융사 감지 일원화 (단일 함수 사용)
    from app.services.financial_company_detector import detect_financial_company
    
    is_financial, is_financial_holding, financial_confidence = detect_financial_company(
        company_name=company_name,
        ticker=None,  # ticker는 여기서 사용하지 않음
        business_summary=company_detail.biz_summary if company_detail else None,
        keywords=company_detail.keywords if company_detail else None
    )
    
    # 🆕 P0-1: 사업형 지주회사 감지 (entity_type 기반)
    # 🆕 P0 추가: entity_type_classifier를 먼저 호출하여 정확한 entity_type 확인
    is_business_holding = False
    entity_type_from_classifier_early = None
    
    # 🆕 P0 추가: SK이노베이션 특별 처리 (회사명에 홀딩스가 없어도 지주회사로 인식)
    if ticker == '096770' or (company_name and 'SK이노베이션' in company_name):
        is_business_holding = True
        entity_type_from_classifier_early = 'HOLDING_BUSINESS'
        logger.debug(f"[Sector Classifier] SK이노베이션 특별 처리: 사업형 지주회사로 인식")
    elif company_detail:
        # entity_type_classifier에서 이미 판정된 정보 활용
        # 여기서는 회사명 패턴으로 간단히 감지 (정확한 판정은 entity_type_classifier에서)
        if company_name_lower and ('홀딩스' in company_name_lower or '홀딩' in company_name_lower):
            # 금융지주가 아닌 일반 지주회사 후보
            if not is_financial_holding:
                is_business_holding = True
                logger.debug(f"[Sector Classifier] 사업형 지주회사 후보 감지: {company_name_lower}")
    
    # 🆕 P0-1: 지주회사 통합 플래그 (금융지주 또는 사업형 지주회사)
    is_holding_company = is_financial_holding or is_business_holding
    
    # 🆕 금융지주/사업형 지주회사는 즉시 return하지 않고, revenue_by_segment 기반으로 섹터 결정
    # entity_type은 classify_sector()에서 별도로 처리됨
    
    # 은행 특별 처리
    if '은행' in company_name_lower and '지주' not in company_name_lower:
        return 'SEC_BANK', 'BANK', 'DOWNSTREAM', 'HIGH', None
    
    # 증권 특별 처리
    if '증권' in company_name_lower:
        return 'SEC_SEC', 'SECURITIES', 'DOWNSTREAM', 'HIGH', None
    
    # 보험 특별 처리
    if '보험' in company_name_lower or '생명' in company_name_lower:
        if '생명' in company_name_lower:
            return 'SEC_INS', 'LIFE', 'DOWNSTREAM', 'HIGH', None
        else:
            return 'SEC_INS', 'NON_LIFE', 'DOWNSTREAM', 'HIGH', None
    
    # 텍스트 수집
    text_parts = []
    if company_detail.biz_summary:
        text_parts.append(company_detail.biz_summary.lower())
    if company_detail.products:
        products_text = ' '.join([str(p) for p in company_detail.products]).lower()
        text_parts.append(products_text)
    if company_detail.keywords:
        keywords_text = ' '.join([str(k) for k in company_detail.keywords]).lower()
        text_parts.append(keywords_text)
    if company_name:
        text_parts.append(company_name.lower())
    
    # 금융사 특별 처리: financial_value_chain이 있으면 금융사
    if company_detail.financial_value_chain:
        # 금융사 섹터 우선순위 조정
        financial_keywords_in_name = []
        if company_name: 
            name_lower = company_name.lower()
            if '금융지주' in name_lower or '지주' in name_lower:
                financial_keywords_in_name.append('금융지주')
            if '은행' in name_lower:
                financial_keywords_in_name.append('은행')
            if '증권' in name_lower:
                financial_keywords_in_name.append('증권')
            if '보험' in name_lower or '생명' in name_lower:
                financial_keywords_in_name.append('보험')
            if '카드' in name_lower:
                financial_keywords_in_name.append('카드')
        
        # 금융사 키워드 가중치 증가
        for keyword in financial_keywords_in_name:
            text_parts.append(keyword * 3)  # 가중치 3배
    
    combined_text = ' '.join(text_parts)
    
    # ========================================================================
    # 🆕 P0: 변수 초기화 (스코프 에러 방지)
    # ========================================================================
    # 🆕 P0-1: revenue_by_segment 변수 초기화 (함수 시작 부분에서 무조건 초기화)
    revenue_by_segment = {}
    revenue_data = {}
    has_revenue_data = False
    segments_count = 0
    sum_pct = 0.0
    neutral_ratio = 0.0
    revenue_quality = "BAD"
    quality_reason = "NO_REVENUE_DATA"
    
    # company_detail에서 revenue_by_segment 안전하게 읽기
    if company_detail and hasattr(company_detail, 'revenue_by_segment'):
        raw_revenue = company_detail.revenue_by_segment
        # 타입 체크 및 변환
        if raw_revenue is None:
            revenue_data = {}
        elif isinstance(raw_revenue, dict):
            revenue_data = raw_revenue
        elif isinstance(raw_revenue, str):
            # JSON 문자열인 경우 파싱 시도
            try:
                import json
                revenue_data = json.loads(raw_revenue) if raw_revenue else {}
                if not revenue_data:
                    quality_reason = "JSON_EMPTY"
            except json.JSONDecodeError:
                revenue_data = {}
                quality_reason = "JSON_PARSE_FAIL"  # 🆕 P0-C: JSON 파싱 실패 명시
            except Exception:
                revenue_data = {}
                quality_reason = "JSON_PARSE_FAIL"
        else:
            revenue_data = {}
            quality_reason = "INVALID_TYPE"
        
        revenue_by_segment = revenue_data  # 별칭 (하위 호환성)
        has_revenue_data = bool(revenue_data and isinstance(revenue_data, dict) and len(revenue_data) > 0)
    
    # ========================================================================
    # 🆕 P0 개선: 매출 비중 기반 부스팅 (마진 체크 포함)
    # ========================================================================
    revenue_scores, revenue_audit = calculate_revenue_sector_scores(revenue_by_segment)
    
    # 🆕 P0: revenue_quality 계산
    total_pct = sum(p for p in revenue_data.values() if isinstance(p, (int, float))) if revenue_data else 0.0
    
    # Neutral 비율 계산
    neutral_pct = sum(
        p for seg, p in revenue_data.items()
        if isinstance(p, (int, float)) and is_neutral_segment(seg)
    ) if revenue_data else 0.0
    neutral_ratio = (neutral_pct / total_pct * 100) if total_pct > 0 else 0.0
    
    revenue_quality, quality_meta = calculate_revenue_quality(
        revenue_data, total_pct, neutral_ratio
    )
    
    # 🆕 P0: revenue_quality 기반 preset 가중치 적용
    w_revenue, w_keyword, w_product = calculate_dynamic_weights(
        revenue_by_segment,  # 🆕 초기화된 변수 사용
        revenue_quality=revenue_quality,
        quality_meta=quality_meta
    )
    coverage = revenue_audit.get('coverage', 0)  # Coverage-A 값 미리 가져오기
    
    # Audit Trail 로깅
    if revenue_scores:
        logger.debug(
            f"[{company_name}] 매출비중 점수: {revenue_scores}, "
            f"가중치: R={w_revenue}, K={w_keyword}, P={w_product}, "
            f"마진: {revenue_audit.get('margin', 0):.1f}%, 보너스 적용: {revenue_audit.get('bonus_applied', False)}"
        )
    
    # ========================================================================
    # 섹터별 통합 점수 계산
    # ========================================================================
    sector_scores = {}
    sector_details = {}  # Audit Trail용
    
    for sector_code, sector_info in SECTOR_KEYWORDS.items():
        # 🆕 P0-2: SEC_MACH 덤핑 방지 게이트
        if sector_code == 'SEC_MACH':
            # SEC_MACH는 기계 고유 단서가 최소 1개 이상 필요
            has_mach_keyword = any(kw.lower() in combined_text for kw in SEC_MACH_REQUIRED_KEYWORDS)
            if not has_mach_keyword:
                # 기계 고유 단서 없으면 SEC_MACH 점수 계산 스킵
                continue
        
        # 키워드 매칭 (정규화: 최소 2개 이상 매칭 요구)
        keyword_matches = sum(1 for kw in sector_info['keywords'] if kw.lower() in combined_text)
        keyword_score = min(1.0, keyword_matches / max(len(sector_info['keywords']), 1)) if keyword_matches >= 2 else keyword_matches * 0.1
        
        # 제품 매칭 (정규화)
        product_matches = 0
        if company_detail.products:
            for product in company_detail.products:
                product_lower = str(product).lower()
                if any(pk.lower() in product_lower for pk in sector_info['products']):
                    product_matches += 1
        product_count = len(company_detail.products) if company_detail.products else 1
        product_score = min(1.0, product_matches / max(product_count, 1))
        
        # 매출 비중 점수
        revenue_score = revenue_scores.get(sector_code, 0.0)
        
        # 🆕 P0-2: SEC_MACH 추가 검증 - 매출 비중이 '제조' 같은 중립 세그먼트만이면 제외
        if sector_code == 'SEC_MACH' and revenue_score > 0:
            # 매출 비중에서 기계 고유 단서 확인
            revenue_has_mach_keyword = False
            if revenue_by_segment:  # 🆕 초기화된 변수 사용
                for seg, pct in revenue_by_segment.items():
                    if isinstance(pct, (int, float)) and pct > 0:
                        seg_lower = str(seg).lower()
                        if any(kw in seg_lower for kw in SEC_MACH_REQUIRED_KEYWORDS):
                            revenue_has_mach_keyword = True
                            break
            # 기계 고유 단서 없으면 매출 점수 무효화
            if not revenue_has_mach_keyword:
                revenue_score = 0.0
        
        # 🎯 통합 점수 계산
        total_score = (
            revenue_score * w_revenue +
            keyword_score * w_keyword +
            product_score * w_product
        )
        
        if total_score > 0:
            sector_scores[sector_code] = total_score
            sector_details[sector_code] = {
                'total': total_score,
                'revenue': revenue_score,
                'keyword': keyword_score,
                'product': product_score
            }
    
    # 🆕 P0 추가: SK이노베이션 특별 처리 (sector_scores가 비어있어도 Primary Sector 설정)
    if not sector_scores:
        if ticker == '096770' or (company_name and 'SK이노베이션' in company_name):
            # SK이노베이션은 석유/화학 사업을 하므로 SEC_CHEM으로 강제 설정
            # 🆕 P0-Override: Override 발생 시 confidence는 HIGH로 강제 설정 (논리 모순 해결)
            logger.info(f"[{company_name}] SK이노베이션 특별 처리: sector_scores가 비어있어도 SEC_CHEM으로 설정 (confidence: HIGH)")
            return 'SEC_CHEM', 'PETROCHEM', 'MIDSTREAM', 'HIGH', {
                'classification_method': 'RULE_BASED_OVERRIDE',
                'classification_meta': {
                    'entity_type': 'BIZ_HOLDCO',  # entity_type_classifier에서 설정됨
                    'primary_sector_source': 'OVERRIDE',
                    'revenue_quality': revenue_quality if 'revenue_quality' in locals() else 'BAD',
                    'neutral_ratio': neutral_ratio if 'neutral_ratio' in locals() else 0.0,
                    'top1_score': 0.5,
                    'margin': 0.0,
                    'override_hit': True,
                    'override_reason': 'SK이노베이션 특별 처리: 석유/화학 사업',
                    'override': {
                        'hit': True,
                        'reason': 'SK이노베이션 특별 처리: 석유/화학 사업',
                        'source': 'RULE_OVERRIDE'
                    }
                }
            }
        
        return None, None, None, "LOW", {
            'classification_method': 'RULE_BASED',
            'classification_meta': {
                'entity_type': 'REGULAR',
                'primary_sector_source': 'HOLD',
                'revenue_quality': revenue_quality if 'revenue_quality' in locals() else 'BAD',
                'neutral_ratio': neutral_ratio if 'neutral_ratio' in locals() else 0.0,
                'top1_score': 0.0,
                'margin': 0.0,
                'error': 'NO_SECTOR_SCORES'
            }
        }
    
    # 🆕 P0-1: holding이면 revenue_top_sector를 primary_sector로 설정 (익스포저 기반)
    # holding의 정체성은 entity_type으로 유지, primary_sector는 익스포저 기반
    # 🆕 품질 게이트: revenue_quality가 OK 또는 WARN일 때만 적용 (BAD면 keyword/summary로)
    if is_holding_company and revenue_scores and len(revenue_scores) > 0 and revenue_quality in ('OK', 'WARN'):
        revenue_sorted = sorted(revenue_scores.items(), key=lambda x: x[1], reverse=True)
        if revenue_sorted and revenue_sorted[0][1] > 0:
            revenue_top_sector = revenue_sorted[0][0]
            # revenue_top_sector가 sector_scores에 있으면 우선 사용
            if revenue_top_sector in sector_scores:
                major_sector = revenue_top_sector
                best_score = sector_scores[revenue_top_sector]
                best_details = sector_details.get(revenue_top_sector, {})
                logger.info(f"[{company_name}] Holding 회사: revenue_top_sector={revenue_top_sector}를 primary_sector로 설정 (익스포저 기반, quality={revenue_quality})")
            else:
                # revenue_top_sector가 sector_scores에 없으면 기존 로직 사용
                sorted_sectors = sorted(
                    sector_scores.items(), 
                    key=lambda x: (x[1], revenue_scores.get(x[0], 0)),
                    reverse=True
                )
                major_sector = sorted_sectors[0][0]
                best_score = sorted_sectors[0][1]
                best_details = sector_details.get(major_sector, {})
    elif is_holding_company and revenue_quality == 'BAD':
        logger.warning(f"[{company_name}] Holding 회사: revenue_quality=BAD이므로 revenue 기반 primary 설정 스킵, keyword/summary 기반으로 진행")
        
        # 🆕 P0 추가: SK이노베이션 특별 처리 (Primary Sector 설정)
        if ticker == '096770' or (company_name and 'SK이노베이션' in company_name):
            # SK이노베이션은 석유/화학 사업을 하므로 SEC_CHEM으로 설정
            if 'SEC_CHEM' in sector_scores:
                major_sector = 'SEC_CHEM'
                best_score = sector_scores['SEC_CHEM']
                best_details = sector_details.get('SEC_CHEM', {})
                logger.info(f"[{company_name}] SK이노베이션 특별 처리: Primary Sector를 SEC_CHEM으로 설정 (키워드/제품 기반)")
            elif 'SEC_ENERGY' in sector_scores:
                major_sector = 'SEC_ENERGY'
                best_score = sector_scores['SEC_ENERGY']
                best_details = sector_details.get('SEC_ENERGY', {})
                logger.info(f"[{company_name}] SK이노베이션 특별 처리: Primary Sector를 SEC_ENERGY로 설정 (키워드/제품 기반)")
            else:
                # 키워드/제품 기반으로도 없으면 SEC_CHEM으로 강제 설정
                major_sector = 'SEC_CHEM'
                best_score = 0.5  # 기본 점수
                best_details = {}
                logger.info(f"[{company_name}] SK이노베이션 특별 처리: Primary Sector를 SEC_CHEM으로 강제 설정 (키워드/제품 매칭 없음)")
        else:
            # 기존 로직 사용
            sorted_sectors = sorted(
                sector_scores.items(), 
                key=lambda x: (x[1], revenue_scores.get(x[0], 0)),
                reverse=True
            )
            major_sector = sorted_sectors[0][0] if sorted_sectors else None
            best_score = sorted_sectors[0][1] if sorted_sectors else 0.0
            best_details = sector_details.get(major_sector, {}) if major_sector else {}
    else:
        # 최고 점수 섹터 선택 (Tie-break: 매출 비중 높은 쪽)
        # 🆕 P0 추가: SK이노베이션 특별 처리 (is_holding_company가 False이거나 다른 경로일 때)
        if ticker == '096770' or (company_name and 'SK이노베이션' in company_name):
            # SK이노베이션은 석유/화학 사업을 하므로 SEC_CHEM으로 설정
            if 'SEC_CHEM' in sector_scores:
                major_sector = 'SEC_CHEM'
                best_score = sector_scores['SEC_CHEM']
                best_details = sector_details.get('SEC_CHEM', {})
                logger.info(f"[{company_name}] SK이노베이션 특별 처리: Primary Sector를 SEC_CHEM으로 설정 (키워드/제품 기반)")
            elif 'SEC_ENERGY' in sector_scores:
                major_sector = 'SEC_ENERGY'
                best_score = sector_scores['SEC_ENERGY']
                best_details = sector_details.get('SEC_ENERGY', {})
                logger.info(f"[{company_name}] SK이노베이션 특별 처리: Primary Sector를 SEC_ENERGY로 설정 (키워드/제품 기반)")
            elif not sector_scores:
                # sector_scores가 비어있으면 SEC_CHEM으로 강제 설정
                major_sector = 'SEC_CHEM'
                best_score = 0.5
                best_details = {}
                logger.info(f"[{company_name}] SK이노베이션 특별 처리: Primary Sector를 SEC_CHEM으로 강제 설정 (sector_scores 비어있음)")
            else:
                # 기존 로직 사용
                sorted_sectors = sorted(
                    sector_scores.items(), 
                    key=lambda x: (x[1], revenue_scores.get(x[0], 0)),
                    reverse=True
                )
                major_sector = sorted_sectors[0][0] if sorted_sectors else 'SEC_CHEM'  # 기본값 SEC_CHEM
                best_score = sorted_sectors[0][1] if sorted_sectors else 0.5
                best_details = sector_details.get(major_sector, {}) if major_sector else {}
        else:
            sorted_sectors = sorted(
                sector_scores.items(), 
                key=lambda x: (x[1], revenue_scores.get(x[0], 0)),  # 1차: total, 2차: revenue
                reverse=True
            )
            
            major_sector = sorted_sectors[0][0] if sorted_sectors else None
            best_score = sorted_sectors[0][1] if sorted_sectors else 0.0
            best_details = sector_details.get(major_sector, {}) if major_sector else {}
    
    # Audit Trail 로깅
    logger.debug(f"[{company_name}] 최종 섹터: {major_sector} (total={best_score:.3f}, R={best_details.get('revenue', 0):.3f}, K={best_details.get('keyword', 0):.3f}, P={best_details.get('product', 0):.3f})")
    
    # Sub-sector 결정
    sub_sector = None
    if major_sector in SECTOR_KEYWORDS:
        sub_sector_scores = {}
        for sub_code, sub_keywords in SECTOR_KEYWORDS[major_sector]['sub_sectors'].items():
            sub_score = sum(1 for kw in sub_keywords if kw.lower() in combined_text)
            if sub_score > 0:
                sub_sector_scores[sub_code] = sub_score
        
        if sub_sector_scores:
            sub_sector = max(sub_sector_scores.items(), key=lambda x: x[1])[0]
    
    # 🆕 P0-1: 금융지주/사업형 지주회사 sub_sector 결정 (revenue_by_segment 기반)
    if is_holding_company and revenue_scores and len(revenue_scores) > 0:
        # revenue_by_segment에서 가장 높은 비중의 섹터로 sub_sector 결정
        revenue_sorted = sorted(revenue_scores.items(), key=lambda x: x[1], reverse=True)
        if revenue_sorted and revenue_sorted[0][1] > 0:
            revenue_top_sector = revenue_sorted[0][0]
            
            # 금융지주: SEC_BANK -> BANK, SEC_INS -> LIFE/NON_LIFE 등
            if is_financial_holding:
                if revenue_top_sector == 'SEC_BANK':
                    sub_sector = 'BANK'
                elif revenue_top_sector == 'SEC_INS':
                    # 생명/손해 구분은 키워드로
                    if '생명' in combined_text:
                        sub_sector = 'LIFE'
                    else:
                        sub_sector = 'NON_LIFE'
                elif revenue_top_sector == 'SEC_SEC':
                    sub_sector = 'SECURITIES'
                elif revenue_top_sector == 'SEC_CARD':
                    sub_sector = 'CARD'
                else:
                    sub_sector = 'FINANCIAL_HOLDING'  # 기본값
            # 사업형 지주회사: 매출 비중 1위 섹터의 sub_sector 사용
            elif is_business_holding:
                # 일반 섹터 키워드 매칭으로 sub_sector 결정
                if major_sector in SECTOR_KEYWORDS:
                    sub_sector_scores = {}
                    for sub_code, sub_keywords in SECTOR_KEYWORDS[major_sector]['sub_sectors'].items():
                        sub_score = sum(1 for kw in sub_keywords if kw.lower() in combined_text)
                        if sub_score > 0:
                            sub_sector_scores[sub_code] = sub_score
                    
                    if sub_sector_scores:
                        sub_sector = max(sub_sector_scores.items(), key=lambda x: x[1])[0]
    
    # 🆕 P1-1/P1-3: Value Chain 결정 (Revenue Segment 기반 + 후보 저장)
    # 기존 단순 키워드 매칭 대신 Rule-based Confidence 계산 사용
    value_chain_candidates = []
    try:
        from app.services.value_chain_classifier import classify_value_chain_rule_based
        value_chain, vc_confidence, vc_candidates = classify_value_chain_rule_based(
            company_detail, major_sector, company_name
        )
        value_chain_candidates = vc_candidates if vc_candidates else []
        
        # Confidence가 낮으면 기존 방식 Fallback
        if value_chain is None or vc_confidence < 0.3:
            # 기존 방식 Fallback
            for vc_type, vc_keywords in VALUE_CHAIN_KEYWORDS.items():
                if any(kw.lower() in combined_text for kw in vc_keywords):
                    value_chain = vc_type
                    break
    except ImportError:
        # value_chain_classifier가 없으면 기존 방식 사용
        value_chain = None
        for vc_type, vc_keywords in VALUE_CHAIN_KEYWORDS.items():
            if any(kw.lower() in combined_text for kw in vc_keywords):
                value_chain = vc_type
                break
    except Exception as e:
        # 반환값 개수가 맞지 않을 수 있으므로 예외 처리
        logger.warning(f"[{company_name}] Value Chain 분류 오류: {e}")
        value_chain = None
        value_chain_candidates = []
    
    # ========================================================================
    # 🆕 P0 개선: Confidence 결정 (매출 비중 기반 강화)
    # ========================================================================
    revenue_score = best_details.get('revenue', 0)
    total_score = best_details.get('total', best_score)
    coverage = revenue_audit.get('coverage', 0)  # Coverage-A 값 가져오기
    
    # 데이터 품질 체크
    # 🆕 revenue_data는 이미 함수 초기에 초기화됨
    # has_revenue_data도 이미 초기화됨
    if revenue_data and isinstance(revenue_data, dict):
        total_pct = sum(p for p in revenue_data.values() if isinstance(p, (int, float)))
        data_quality_ok = 80 <= total_pct <= 120  # 합계가 80-120% 범위면 OK
    else:
        data_quality_ok = True  # 데이터 없으면 패널티 없음
    
    # Confidence 결정 (GPT/Gemini 피드백 반영 + Coverage-A 페널티)
    # 🆕 Coverage-A는 HOLD 트리거가 아니라 confidence 페널티로만 사용
    base_confidence = None
    if revenue_score >= 0.50:  # 매출 비중 50% 이상 → 압도적
        base_confidence = "HIGH"
    elif revenue_score >= 0.25 and total_score >= 0.5:  # 매출 25%+ & 총점 0.5+
        base_confidence = "HIGH" if data_quality_ok else "MEDIUM"
    elif total_score >= 0.5:  # 키워드/제품만으로 높은 점수
        base_confidence = "MEDIUM"
    elif total_score >= 0.3:
        base_confidence = "LOW"
    else:
        base_confidence = "LOW"
    
    # 🆕 Coverage-A 페널티 적용 (HOLD 트리거 아님)
    if has_revenue_data and coverage < 20.0:
        # Coverage-A가 낮으면 confidence 한 단계 다운
        if base_confidence == "HIGH":
            confidence = "MEDIUM"
        elif base_confidence == "MEDIUM":
            confidence = "LOW"
        else:
            confidence = base_confidence
        logger.debug(f"[{company_name}] Coverage-A 페널티: {base_confidence} → {confidence} (coverage: {coverage:.1f}%)")
    else:
        confidence = base_confidence
    
    # Top1 score와 margin 계산 (HOLD 조건 체크 전에 먼저 계산)
    # 🆕 스케일 일관성: top1_score와 margin은 0.0~1.0 범위 (normalized score)
    # revenue_scores는 이미 0.0~1.0 스케일로 정규화되어 있음
    top1_score = revenue_score  # best_details의 revenue는 이미 top1 (0.0~1.0)
    top2_score = 0.0
    if revenue_scores and len(revenue_scores) > 0:
        sorted_scores = sorted(revenue_scores.items(), key=lambda x: x[1], reverse=True)
        if len(sorted_scores) > 0:
            top1_score = sorted_scores[0][1]  # 0.0~1.0 스케일
        if len(sorted_scores) > 1:
            top2_score = sorted_scores[1][1]  # 0.0~1.0 스케일
    margin = top1_score - top2_score  # 0.0~1.0 스케일
    
    # 🆕 GPT/Gemini 피드백: HOLD 정책 재설계
    # HOLD는 "진짜 위험"일 때만: top1 score 낮고, margin 낮고, 신호 약함
    hold_reason_code = None
    if confidence == "LOW":
        # 추가 검증: 키워드/제품 매칭 확인
        has_strong_keyword = best_details.get('keyword', 0) >= 0.3
        has_strong_product = best_details.get('product', 0) >= 0.3
        
        # 🆕 HOLD 조건: top1 score < 0.25 AND margin < 0.05 AND 신호 약함
        if top1_score < 0.25 and margin < 0.05 and not has_revenue_data and not has_strong_keyword and not has_strong_product:
            hold_reason_code = 'HOLD_LOW_CONF'
            logger.info(f"[{company_name}] HOLD 정책 적용: {hold_reason_code} - Top1 score 낮음({top1_score:.3f}), margin 낮음({margin:.3f}), 신호 약함")
            # 🆕 HOLD 반환 시에도 classification_meta 저장
            hold_boosting_log = {
                'classification_method': 'HOLD',
                'classification_meta': {
                    'entity_type': 'REGULAR',
                    'primary_sector_source': 'HOLD',
                    'revenue_quality': revenue_quality,
                    'neutral_ratio': neutral_ratio,
                    'top1_score': top1_score,
                    'margin': margin,
                    'hold_reason': hold_reason_code,  # 하위 호환성
                    'hold_reason_code': hold_reason_code  # 🆕 필수: hold_reason_code 저장
                }
            }
            if quality_meta:
                hold_boosting_log['classification_meta']['segments_count'] = quality_meta.get('segments_count', 0)
                hold_boosting_log['classification_meta']['sum_pct'] = quality_meta.get('sum_pct', 0.0)
                hold_boosting_log['classification_meta']['quality_reason'] = quality_meta.get('quality_reason', 'OK')
            
            # 🆕 P0 개선: HOLD 케이스에서도 sector_evidence 저장
            # 🆕 revenue_by_segment와 has_revenue_data는 이미 함수 초기에 초기화됨
            hold_boosting_log['classification_meta']['sector_evidence'] = {
                'revenue_quality': revenue_quality,
                'quality_reason': quality_meta.get('quality_reason', 'OK') if quality_meta else 'NO_DATA',
                'has_revenue_data': has_revenue_data,
                'primary_sector_source': 'HOLD',
                'segments_count': quality_meta.get('segments_count', 0) if quality_meta else 0,
                'sum_pct': quality_meta.get('sum_pct', 0.0) if quality_meta else 0.0
            }
            
            # 🆕 선택-2: HOLD라도 fallback sector candidate 저장
            if revenue_scores and len(revenue_scores) > 0:
                sorted_sectors = sorted(revenue_scores.items(), key=lambda x: x[1], reverse=True)
                candidates = []
                for sector_code, score in sorted_sectors[:3]:
                    if score > 0:
                        candidates.append({
                            'sector': sector_code,
                            'score': float(score),
                            'source': 'revenue'
                        })
                if candidates:
                    hold_boosting_log['classification_meta']['sector_candidates'] = candidates
            
            # 🆕 필수-1: HOLD 반환 전에도 entity_type_classifier 호출
            if ticker:
                try:
                    from app.db import SessionLocal
                    from app.models.stock import Stock
                    from app.services.entity_type_classifier import classify_entity_type
                    db = SessionLocal()
                    stock = db.query(Stock).filter(Stock.ticker == ticker).first()
                    if stock:
                        entity_type_from_classifier, entity_conf, entity_meta = classify_entity_type(stock, company_detail)
                        if entity_type_from_classifier:
                            if entity_type_from_classifier == 'HOLDING_PURE':
                                hold_boosting_log['classification_meta']['entity_type'] = 'HOLDCO'
                            elif entity_type_from_classifier == 'HOLDING_BUSINESS':
                                hold_boosting_log['classification_meta']['entity_type'] = 'BIZ_HOLDCO'
                            elif entity_type_from_classifier == 'HOLDING_FINANCIAL':
                                hold_boosting_log['classification_meta']['entity_type'] = 'FINANCIAL_HOLDING'
                            elif entity_type_from_classifier == 'OPERATING':
                                hold_boosting_log['classification_meta']['entity_type'] = 'REGULAR'
                            else:
                                hold_boosting_log['classification_meta']['entity_type'] = entity_type_from_classifier
                            hold_boosting_log['classification_meta']['entity_type_confidence'] = entity_conf
                            
                            # 🆕 P0: override_hit 전달 보장
                            if isinstance(entity_meta, dict):
                                override_hit = entity_meta.get('override_hit', False)
                                override_reason = entity_meta.get('override_reason')
                                if override_hit or override_reason:
                                    hold_boosting_log['classification_meta']['override_hit'] = True
                                    if override_reason:
                                        hold_boosting_log['classification_meta']['override_reason'] = override_reason
                                    if 'override' in entity_meta:
                                        hold_boosting_log['classification_meta']['override'] = entity_meta['override']
                            
                            # 🆕 P0: entity_type_evidence 전달
                            if isinstance(entity_meta, dict) and 'evidence' in entity_meta:
                                hold_boosting_log['classification_meta']['entity_type_evidence'] = entity_meta.get('evidence', [])
                    db.close()
                except Exception as e:
                    logger.warning(f"[{company_name}] HOLD 반환 시 entity_type_classifier 호출 실패: {e}")
            
            # 🆕 P0: SK이노베이션 특별 처리 (HOLD 경로)
            if ticker == '096770' or (company_name and 'SK이노베이션' in company_name):
                # 🆕 P0-Override: Override 발생 시 confidence는 HIGH로 강제 설정 (논리 모순 해결)
                logger.info(f"[{company_name}] HOLD 경로에서 SK이노베이션 특별 처리: Primary Sector를 SEC_CHEM으로 설정 (confidence: HIGH)")
                hold_boosting_log['classification_meta']['entity_type'] = 'BIZ_HOLDCO'
                hold_boosting_log['classification_meta']['override_hit'] = True
                hold_boosting_log['classification_meta']['override_reason'] = 'SK이노베이션 특별 처리: 중간지주회사 (석유/화학 직접 사업 + 다수 자회사 보유)'
                hold_boosting_log['classification_meta']['override'] = {
                    'hit': True,
                    'reason': 'SK이노베이션 특별 처리: 중간지주회사',
                    'source': 'RULE_OVERRIDE'
                }
                hold_boosting_log['classification_meta']['hold_reason_code'] = hold_reason_code  # 원래 HOLD 사유는 메타에 저장
                # Primary Sector를 SEC_CHEM으로 설정하고 confidence는 HIGH로 설정 (Override는 확정값)
                return 'SEC_CHEM', 'PETROCHEM', None, 'HIGH', hold_boosting_log
            
            return None, None, None, f"HOLD:{hold_reason_code}", hold_boosting_log
        elif top1_score < 0.25 and margin < 0.05 and not has_revenue_data:
            hold_reason_code = 'HOLD_UNMAPPED_REVENUE_HIGH'
            logger.info(f"[{company_name}] HOLD 정책 적용: {hold_reason_code} - Top1 score 낮음({top1_score:.3f}), margin 낮음({margin:.3f}), 매출 데이터 없음")
            # 🆕 HOLD 반환 시에도 classification_meta 저장
            hold_boosting_log = {
                'classification_method': 'HOLD',
                'classification_meta': {
                    'entity_type': 'REGULAR',  # entity_type_classifier 결과로 덮어씌워질 수 있음
                    'primary_sector_source': 'HOLD',
                    'revenue_quality': revenue_quality,
                    'neutral_ratio': neutral_ratio,
                    'top1_score': top1_score,
                    'margin': margin,
                    'hold_reason': hold_reason_code,  # 하위 호환성
                    'hold_reason_code': hold_reason_code  # 🆕 필수: hold_reason_code 저장
                }
            }
            if quality_meta:
                hold_boosting_log['classification_meta']['segments_count'] = quality_meta.get('segments_count', 0)
                hold_boosting_log['classification_meta']['sum_pct'] = quality_meta.get('sum_pct', 0.0)
                hold_boosting_log['classification_meta']['quality_reason'] = quality_meta.get('quality_reason', 'OK')
            
            # 🆕 선택-2: HOLD라도 fallback sector candidate 저장
            if revenue_scores and len(revenue_scores) > 0:
                sorted_sectors = sorted(revenue_scores.items(), key=lambda x: x[1], reverse=True)
                candidates = []
                for sector_code, score in sorted_sectors[:3]:
                    if score > 0:
                        candidates.append({
                            'sector': sector_code,
                            'score': float(score),
                            'source': 'revenue'
                        })
                if candidates:
                    hold_boosting_log['classification_meta']['sector_candidates'] = candidates
            
            # 🆕 필수-1: HOLD 반환 전에도 entity_type_classifier 호출
            if ticker:
                try:
                    from app.db import SessionLocal
                    from app.models.stock import Stock
                    from app.services.entity_type_classifier import classify_entity_type
                    db = SessionLocal()
                    stock = db.query(Stock).filter(Stock.ticker == ticker).first()
                    if stock:
                        entity_type_from_classifier, entity_conf, entity_meta = classify_entity_type(stock, company_detail)
                        if entity_type_from_classifier:
                            if entity_type_from_classifier == 'HOLDING_PURE':
                                hold_boosting_log['classification_meta']['entity_type'] = 'HOLDCO'
                            elif entity_type_from_classifier == 'HOLDING_BUSINESS':
                                hold_boosting_log['classification_meta']['entity_type'] = 'BIZ_HOLDCO'
                            elif entity_type_from_classifier == 'HOLDING_FINANCIAL':
                                hold_boosting_log['classification_meta']['entity_type'] = 'FINANCIAL_HOLDING'
                            elif entity_type_from_classifier == 'OPERATING':
                                hold_boosting_log['classification_meta']['entity_type'] = 'REGULAR'
                            else:
                                hold_boosting_log['classification_meta']['entity_type'] = entity_type_from_classifier
                            hold_boosting_log['classification_meta']['entity_type_confidence'] = entity_conf
                            hold_boosting_log['classification_meta']['entity_type_evidence'] = entity_meta.get('evidence', [])
                            
                            # 🆕 P0: override_hit 전달 보장
                            if isinstance(entity_meta, dict):
                                override_hit = entity_meta.get('override_hit', False)
                                override_reason = entity_meta.get('override_reason')
                                if override_hit or override_reason:
                                    hold_boosting_log['classification_meta']['override_hit'] = True
                                    if override_reason:
                                        hold_boosting_log['classification_meta']['override_reason'] = override_reason
                                    if 'override' in entity_meta:
                                        hold_boosting_log['classification_meta']['override'] = entity_meta['override']
                    db.close()
                except Exception as e:
                    logger.warning(f"[{company_name}] HOLD 반환 시 entity_type_classifier 호출 실패: {e}")
            
            # 🆕 P0: SK이노베이션 특별 처리 (HOLD 경로)
            if ticker == '096770' or (company_name and 'SK이노베이션' in company_name):
                # 🆕 P0-Override: Override 발생 시 confidence는 HIGH로 강제 설정 (논리 모순 해결)
                logger.info(f"[{company_name}] HOLD 경로에서 SK이노베이션 특별 처리: Primary Sector를 SEC_CHEM으로 설정 (confidence: HIGH)")
                hold_boosting_log['classification_meta']['entity_type'] = 'BIZ_HOLDCO'
                hold_boosting_log['classification_meta']['override_hit'] = True
                hold_boosting_log['classification_meta']['override_reason'] = 'SK이노베이션 특별 처리: 중간지주회사 (석유/화학 직접 사업 + 다수 자회사 보유)'
                hold_boosting_log['classification_meta']['override'] = {
                    'hit': True,
                    'reason': 'SK이노베이션 특별 처리: 중간지주회사',
                    'source': 'RULE_OVERRIDE'
                }
                hold_boosting_log['classification_meta']['hold_reason_code'] = hold_reason_code  # 원래 HOLD 사유는 메타에 저장
                # Primary Sector를 SEC_CHEM으로 설정하고 confidence는 HIGH로 설정 (Override는 확정값)
                return 'SEC_CHEM', 'PETROCHEM', None, 'HIGH', hold_boosting_log
            
            return None, None, None, f"HOLD:{hold_reason_code}", hold_boosting_log
    
    # primary_sector_source 결정 (정규화: 대문자 고정)
    # 🆕 값 정규화: REVENUE | KEYWORD | OVERRIDE | HOLD | LLM | UNKNOWN
    primary_sector_source = 'HOLD'
    if major_sector:
        if revenue_scores and major_sector in revenue_scores and revenue_scores[major_sector] > 0:
            primary_sector_source = 'REVENUE'
        elif best_details.get('keyword', 0) > 0 or best_details.get('product', 0) > 0:
            primary_sector_source = 'KEYWORD'
        else:
            primary_sector_source = 'UNKNOWN'
    
    # Audit Trail
    logger.debug(f"[{company_name}] Confidence: {confidence} (revenue={revenue_score:.3f}, total={total_score:.3f}, quality_ok={data_quality_ok})")
    
    # 🆕 필수-1: entity_type_classifier 결과를 classification_meta.entity_type에 항상 반영
    # entity_type_classifier를 소스 오브 트루스로 사용 (회사명 패턴은 보조 신호)
    # boosting_log 생성 전에 먼저 호출하여 entity_type을 확정
    entity_type_from_classifier = None
    entity_type_confidence = 0.0
    entity_type_evidence = []
    
    # ticker가 있으면 Stock 조회하여 entity_type_classifier 호출
    stock_for_entity = None
    if ticker:
        try:
            from app.db import SessionLocal
            from app.models.stock import Stock
            from app.services.entity_type_classifier import classify_entity_type
            db = SessionLocal()
            stock_for_entity = db.query(Stock).filter(Stock.ticker == ticker).first()
            if stock_for_entity:
                entity_type_from_classifier, entity_type_confidence, entity_meta = classify_entity_type(stock_for_entity, company_detail)
                entity_type_evidence = entity_meta.get('evidence', [])
            db.close()
        except Exception as e:
            logger.warning(f"[{company_name}] entity_type_classifier 호출 실패: {e}")
    
    # boosting_log 생성 (기본값)
    # 🆕 B) entity_type 판정과 저장 일관성: MAJOR_COMPANY_SECTORS는 이미 boosting_log를 생성하고 return하므로
    # 여기서는 boosting_log가 비어있을 수 있음 (MAJOR_COMPANY_SECTORS가 실행되지 않은 경우)
    boosting_log = {
        'classification_method': 'RULE_BASED',
        'classification_meta': {}
    }
    
    # 🆕 B) entity_type 판정과 저장 일관성: entity_type_classifier 결과가 반드시 classification_meta.entity_type으로 들어가야 함
    # entity_type_classifier 결과를 우선 사용 (소스 오브 트루스)
    # 🆕 P0 추가: MAJOR_COMPANY_SECTORS에서 이미 entity_type=REGULAR로 강제된 경우는 덮어쓰지 않음
    # 주의: MAJOR_COMPANY_SECTORS는 boosting_log를 먼저 생성하고 return하므로, 
    # 여기서는 boosting_log가 비어있을 수 있음 (MAJOR_COMPANY_SECTORS가 실행되지 않은 경우)
    entity_type_forced = False
    if boosting_log and boosting_log.get('classification_meta', {}).get('entity_type_forced', False):
        entity_type_forced = True
    
    if entity_type_from_classifier and not entity_type_forced:
        # HOLDING_PURE, HOLDING_BUSINESS, HOLDING_FINANCIAL 등을 매핑
        if entity_type_from_classifier == 'HOLDING_PURE':
            boosting_log['classification_meta']['entity_type'] = 'HOLDCO'
        elif entity_type_from_classifier == 'HOLDING_BUSINESS':
            boosting_log['classification_meta']['entity_type'] = 'BIZ_HOLDCO'
        elif entity_type_from_classifier == 'HOLDING_FINANCIAL':
            boosting_log['classification_meta']['entity_type'] = 'FINANCIAL_HOLDING'
        elif entity_type_from_classifier == 'OPERATING':
            boosting_log['classification_meta']['entity_type'] = 'REGULAR'
        else:
            boosting_log['classification_meta']['entity_type'] = entity_type_from_classifier
        boosting_log['classification_meta']['entity_type_confidence'] = entity_type_confidence
        boosting_log['classification_meta']['entity_type_evidence'] = entity_type_evidence
        
        # 🆕 A) override_hit 정의/저장 일관성: override_reason이 있으면 항상 override_hit=True
        if isinstance(entity_type_evidence, dict):
            override_hit = entity_type_evidence.get('override_hit', False)
            override_reason = entity_type_evidence.get('override_reason', '')
            
            # override_reason이 있으면 override_hit을 True로 강제
            if override_reason and not override_hit:
                override_hit = True
                logger.warning(f"[{company_name}] override_reason이 있는데 override_hit이 False - True로 수정")
            
            boosting_log['classification_meta']['override_hit'] = override_hit
            if override_reason:
                boosting_log['classification_meta']['override_reason'] = override_reason
                # 한 덩어리로 고정
                boosting_log['classification_meta']['override'] = {
                    'hit': override_hit,
                    'reason': override_reason,
                    'source': 'RULE_OVERRIDE'
                }
        
        logger.debug(f"[{company_name}] entity_type_classifier 결과: {entity_type_from_classifier} (confidence={entity_type_confidence:.2f})")
    elif entity_type_forced:
        logger.debug(f"[{company_name}] entity_type이 MAJOR_COMPANY_SECTORS에서 REGULAR로 강제됨 (entity_type_classifier 결과 무시)")
    else:
        # entity_type_classifier 결과가 없으면 기존 로직 사용 (하위 호환성)
        if is_holding_company:
            if is_financial_holding:
                boosting_log['classification_meta']['entity_type'] = 'FINANCIAL_HOLDING'
            elif is_business_holding:
                boosting_log['classification_meta']['entity_type'] = 'BIZ_HOLDCO'
            else:
                boosting_log['classification_meta']['entity_type'] = 'HOLDCO'
        else:
            boosting_log['classification_meta']['entity_type'] = 'REGULAR'
        logger.debug(f"[{company_name}] entity_type_classifier 미사용, 기존 로직으로 entity_type 설정")
    
    # 2. primary_sector_source
    boosting_log['classification_meta']['primary_sector_source'] = primary_sector_source
    
    # 3. revenue_quality
    boosting_log['classification_meta']['revenue_quality'] = revenue_quality
    
    # 4. neutral_ratio
    boosting_log['classification_meta']['neutral_ratio'] = neutral_ratio
    
    # 5. top1_score
    boosting_log['classification_meta']['top1_score'] = top1_score
    
    # 6. margin
    boosting_log['classification_meta']['margin'] = margin
    
    # 추가: quality_meta 정보
    if quality_meta:
        boosting_log['classification_meta']['segments_count'] = quality_meta.get('segments_count', 0)
        boosting_log['classification_meta']['sum_pct'] = quality_meta.get('sum_pct', 0.0)
        boosting_log['classification_meta']['quality_reason'] = quality_meta.get('quality_reason', 'OK')
    
    # 🆕 P0 개선: sector_evidence 강화
    has_revenue_data = bool(revenue_by_segment and isinstance(revenue_by_segment, dict) and len(revenue_by_segment) > 0)
    boosting_log['classification_meta']['sector_evidence'] = {
        'revenue_quality': revenue_quality,
        'quality_reason': quality_meta.get('quality_reason', 'OK') if quality_meta else 'NO_DATA',
        'has_revenue_data': has_revenue_data,
        'primary_sector_source': primary_sector_source,
        'segments_count': quality_meta.get('segments_count', 0) if quality_meta else 0,
        'sum_pct': quality_meta.get('sum_pct', 0.0) if quality_meta else 0.0
    }
    
    # 🆕 선택-2: HOLD라도 fallback sector candidate를 meta에 남기기
    # (눈검/수동 승인/override를 위한 힌트)
    if not major_sector or primary_sector_source == 'HOLD':
        if revenue_scores and len(revenue_scores) > 0:
            # 상위 2-3개 섹터를 candidate로 저장
            sorted_sectors = sorted(revenue_scores.items(), key=lambda x: x[1], reverse=True)
            candidates = []
            for sector_code, score in sorted_sectors[:3]:  # 상위 3개
                if score > 0:
                    candidates.append({
                        'sector': sector_code,
                        'score': float(score),  # 0.0~1.0 스케일
                        'source': 'revenue'
                    })
            if candidates:
                boosting_log['classification_meta']['sector_candidates'] = candidates
        elif sector_scores and len(sector_scores) > 0:
            # revenue_scores가 없으면 sector_scores에서 상위 후보 추출
            sorted_sectors = sorted(sector_scores.items(), key=lambda x: x[1], reverse=True)
            candidates = []
            for sector_code, score in sorted_sectors[:3]:  # 상위 3개
                if score > 0:
                    candidates.append({
                        'sector': sector_code,
                        'score': float(score),  # 0.0~1.0 스케일
                        'source': 'keyword_product'
                    })
            if candidates:
                boosting_log['classification_meta']['sector_candidates'] = candidates
    
    # 🆕 P0-1: 금융지주 정보를 boosting_log에 추가
    if is_financial_holding:
        boosting_log['classification_meta']['is_financial_holding'] = True
        boosting_log['classification_meta']['financial_holding_detected_by'] = 'company_name'
        if revenue_scores and len(revenue_scores) > 0:
            revenue_sorted = sorted(revenue_scores.items(), key=lambda x: x[1], reverse=True)
            if revenue_sorted:
                boosting_log['classification_meta']['revenue_top_sector'] = revenue_sorted[0][0]
                boosting_log['classification_meta']['revenue_top_pct'] = revenue_sorted[0][1] * 100
    
    # 🆕 P0-5: financial_company_detector 결과 저장
    if is_financial or is_financial_holding:
        boosting_log['classification_meta']['financial_detector_result'] = {
            'is_financial': is_financial,
            'is_financial_holding': is_financial_holding,
            'confidence': financial_confidence
        }
    
    # 🆕 P0-인사이트: 투자 인사이트용 최소 근거 저장 (exposure_top3, evidence_snippets)
    # exposure_top3: revenue_by_segment에서 상위 3개 세그먼트/비중
    if revenue_by_segment and isinstance(revenue_by_segment, dict) and len(revenue_by_segment) > 0:
        sorted_segments = sorted(
            revenue_by_segment.items(), 
            key=lambda x: x[1] if isinstance(x[1], (int, float)) else 0, 
            reverse=True
        )
        exposure_top3 = []
        for segment, pct in sorted_segments[:3]:
            if isinstance(pct, (int, float)) and pct > 0:
                exposure_top3.append({
                    'segment': str(segment),
                    'pct': float(pct)
                })
        if exposure_top3:
            boosting_log['classification_meta']['exposure_top3'] = exposure_top3
    
    # evidence_snippets: biz_summary에서 섹터/밸류체인 판단에 사용한 근거 문장 1~2줄
    # (현재는 간단히 키워드/제품 기반으로 판단한 경우를 기록)
    evidence_snippets = []
    if best_details:
        if best_details.get('keyword', 0) > 0:
            keywords_used = best_details.get('keywords_used', [])
            if keywords_used:
                evidence_snippets.append(f"키워드: {', '.join(keywords_used[:3])}")
        if best_details.get('product', 0) > 0:
            products_used = best_details.get('products_used', [])
            if products_used:
                evidence_snippets.append(f"제품: {', '.join(products_used[:3])}")
    if company_detail and company_detail.biz_summary:
        biz_summary = str(company_detail.biz_summary)
        # 섹터 관련 키워드가 포함된 문장 추출 (간단한 버전)
        if major_sector:
            sector_keywords_map = {
                'SEC_CHEM': ['화학', '석유', '정유', '화학제품'],
                'SEC_SEMI': ['반도체', '칩', '웨이퍼', '메모리'],
                'SEC_AUTO': ['자동차', '차량', '모터', '엔진'],
                'SEC_BATTERY': ['배터리', '전지', '셀', '리튬'],
            }
            keywords = sector_keywords_map.get(major_sector, [])
            if keywords:
                # biz_summary에서 키워드가 포함된 첫 문장 찾기
                sentences = biz_summary.split('。')[:5]  # 처음 5개 문장만
                for sentence in sentences:
                    if any(kw in sentence for kw in keywords):
                        if len(sentence) <= 200:  # 너무 길지 않은 문장만
                            evidence_snippets.append(sentence.strip()[:200])
                            break
    
    if evidence_snippets:
        boosting_log['classification_meta']['evidence_snippets'] = evidence_snippets[:2]  # 최대 2개
    
    # 🆕 인과 구조 저장 (decision_trace) - 투자 인사이트용
    decision_trace = {}
    
    # 1. entity_type 결정 과정
    entity_type_result = boosting_log['classification_meta'].get('entity_type', 'REGULAR')
    entity_evidence = []
    if is_holding_company:
        if is_financial_holding:
            entity_evidence.append('financial_holding_detected')
        elif is_business_holding:
            entity_evidence.append('business_holding_detected')
        else:
            entity_evidence.append('holding_company_detected')
    if ticker == '096770' or (company_name and 'SK이노베이션' in company_name):
        entity_evidence.append('special_override:SK이노베이션')
    
    decision_trace['entity_type'] = {
        'result': entity_type_result,
        'evidence': entity_evidence if entity_evidence else ['default:REGULAR']
    }
    
    # 2. revenue 품질 결정 과정 + 매핑 근거 (🆕 P0-2: decision_trace에 매칭 근거 저장)
    decision_trace['revenue'] = {
        'quality': revenue_quality,
        'reason': quality_reason,
        'has_data': has_revenue_data,
        'segments_count': segments_count,
        'sum_pct': sum_pct
    }
    
    # 🆕 P0-2: revenue_mapping 상세 정보 저장 (매칭 근거 포함)
    if revenue_audit and 'segment_mapping' in revenue_audit:
        decision_trace['revenue_mapping'] = []
        for segment, mapping_info in revenue_audit['segment_mapping'].items():
            decision_trace['revenue_mapping'].append({
                'segment_raw': mapping_info.get('segment_raw', segment),
                'segment_norm': mapping_info.get('segment_norm', ''),
                'pct': mapping_info.get('pct', 0),
                'matched_keyword': mapping_info.get('matched_keyword', ''),
                'matched_sector': mapping_info.get('sector', ''),
                'match_rule': mapping_info.get('match_rule', ''),  # exact/contains
                'match_method': mapping_info.get('match_method', ''),  # normalized/raw
            })
    
    # 🆕 P0-3: unmapped_top 저장 (매핑 실패한 세그먼트 Top3)
    if revenue_audit and 'unmapped_top' in revenue_audit:
        decision_trace['unmapped_top'] = revenue_audit['unmapped_top']
    
    # 🆕 P0.6-2: 배터리 누락 설명 추가 (텍스트 신호 기반)
    # 텍스트 신호(biz_summary/keywords/products) vs revenue_by_segment 부재로 감지
    battery_keywords = ['배터리', '전지', '2차전지', '리튬', '셀', '양극재', '음극재', '전해액', 
                        'Battery', 'battery', 'SK온', '에스케이온']
    has_battery_text_signal = False
    battery_text_evidence = []
    
    # 1. biz_summary에서 배터리 키워드 검색
    if company_detail and company_detail.biz_summary:
        biz_summary = str(company_detail.biz_summary).lower()
        for kw in battery_keywords:
            if kw.lower() in biz_summary:
                has_battery_text_signal = True
                battery_text_evidence.append(f"biz_summary: {kw}")
                break
    
    # 2. keywords에서 배터리 키워드 검색
    if not has_battery_text_signal and company_detail and company_detail.keywords:
        keywords_str = ' '.join([str(k) for k in company_detail.keywords]).lower()
        for kw in battery_keywords:
            if kw.lower() in keywords_str:
                has_battery_text_signal = True
                battery_text_evidence.append(f"keywords: {kw}")
                break
    
    # 3. products에서 배터리 키워드 검색
    if not has_battery_text_signal and company_detail and company_detail.products:
        products_str = ' '.join([str(p) for p in company_detail.products]).lower()
        for kw in battery_keywords:
            if kw.lower() in products_str:
                has_battery_text_signal = True
                battery_text_evidence.append(f"products: {kw}")
                break
    
    # 4. revenue_by_segment에 배터리 관련 토큰이 없는지 확인
    has_battery_revenue = False
    if revenue_by_segment:
        for segment in revenue_by_segment.keys():
            segment_lower = str(segment).lower()
            if any(kw.lower() in segment_lower for kw in battery_keywords):
                has_battery_revenue = True
                break
    
    # 5. entity_type이 BIZ_HOLDCO이거나 consolidated_structure_score 높은지 확인
    entity_type = boosting_log['classification_meta'].get('entity_type', '')
    is_holding_or_consolidated = (
        entity_type in ['BIZ_HOLDCO', 'HOLDING_BUSINESS'] or
        boosting_log['classification_meta'].get('consolidated_structure_score', 0) > 0.5
    )
    
    # 6. 배터리 누락 감지 조건: 텍스트 신호 있음 + revenue 없음 + (지주사 또는 연결 구조)
    if has_battery_text_signal and not has_battery_revenue and is_holding_or_consolidated:
        decision_trace['battery_missing'] = {
            'text_signals': battery_text_evidence,
            'revenue_segments_checked': list(revenue_by_segment.keys()) if revenue_by_segment else [],
            'entity_type': entity_type,
            'explanation': '배터리 관련 키워드가 텍스트 데이터에 존재하나 매출 세그먼트에는 포함되지 않았습니다. 자회사 데이터 또는 연결 재무제표 미포함 가능성이 있습니다. value_chain 키워드(2차전지, 배터리)로 보완 가능합니다.',
            'supplement_method': 'value_chain_keywords',
            'coverage_scope': 'parent_only_or_unknown'
        }
    
    # 3. sector 결정 과정 (🆕 P0.5: 후보 Top3 + final_reason + confidence_band)
    candidates_top3 = []
    candidates_with_scores = []
    
    # revenue_scores 우선, 없으면 sector_scores
    if revenue_scores and len(revenue_scores) > 0:
        sorted_revenue = sorted(revenue_scores.items(), key=lambda x: x[1], reverse=True)
        for sector_code, score in sorted_revenue[:3]:
            if score > 0:
                candidates_top3.append(sector_code)
                candidates_with_scores.append({
                    'sector': sector_code,
                    'score': float(score),
                    'source': 'revenue'
                })
    elif sector_scores and len(sector_scores) > 0:
        sorted_sector = sorted(sector_scores.items(), key=lambda x: x[1], reverse=True)
        for sector_code, score in sorted_sector[:3]:
            if score > 0:
                candidates_top3.append(sector_code)
                candidates_with_scores.append({
                    'sector': sector_code,
                    'score': float(score),
                    'source': 'keyword_product'
                })
    
    # final_reason 결정 (유저 아웃풋용)
    final_reason = 'unknown'
    if primary_sector_source == 'OVERRIDE':
        final_reason = 'override'
    elif primary_sector_source == 'REVENUE':
        if revenue_quality == 'OK':
            final_reason = 'revenue_ok'
        elif revenue_quality == 'WARN':
            final_reason = 'revenue_warn_but_used'
        else:
            final_reason = 'revenue_bad_but_used'
    elif primary_sector_source == 'KEYWORD':
        final_reason = 'keyword_consensus'
    elif primary_sector_source == 'HOLD':
        final_reason = 'hold_no_decision'
    else:
        final_reason = f'source_{primary_sector_source.lower()}'
    
    # confidence_band 결정 (유저 아웃풋용)
    confidence_band = 'UNKNOWN'
    confidence_why = ''
    
    if confidence == 'HIGH':
        if override_hit:
            confidence_band = 'HIGH_OVERRIDE'
            confidence_why = 'Override로 확정된 값'
        else:
            confidence_band = 'HIGH_MODEL'
            if revenue_quality == 'OK' and top1_score >= 0.5:
                confidence_why = '매출 데이터 OK + 모델 신뢰도 높음'
            elif top1_score >= 0.5 and margin >= 0.1:
                confidence_why = '모델 신뢰도 높음 + 차이 큼'
            else:
                confidence_why = '모델 신뢰도 높음'
    elif confidence == 'MED':
        confidence_band = 'MED_MODEL'
        if revenue_quality == 'WARN':
            confidence_why = '매출 데이터 경고 + 모델 신뢰도 중간'
        elif top1_score < 0.5:
            confidence_why = '모델 신뢰도 중간'
        else:
            confidence_why = '모델 신뢰도 중간'
    elif confidence == 'LOW':
        confidence_band = 'LOW_MODEL'
        if revenue_quality == 'BAD':
            confidence_why = '매출 데이터 없음 + 모델 신뢰도 낮음'
        elif top1_score < 0.25:
            confidence_why = '모델 신뢰도 낮음'
        else:
            confidence_why = '모델 신뢰도 낮음'
    elif confidence and confidence.startswith('HOLD:'):
        confidence_band = 'HOLD'
        hold_reason_code = confidence.split(':', 1)[1] if ':' in confidence else 'UNKNOWN'
        if hold_reason_code == 'HOLD_UNMAPPED_REVENUE_HIGH':
            confidence_why = '매출 데이터 없음 (재수집 필요)'
        elif hold_reason_code == 'HOLD_LOW_CONF':
            confidence_why = '모델 신뢰도 매우 낮음'
        else:
            confidence_why = f'HOLD: {hold_reason_code}'
    
    # 🆕 P0.6-B: Top1/Top2 근접 시 듀얼 섹터 정보 추가 (유저 아웃풋용)
    dual_sector_info = None
    dual_sector_enabled = False
    dual_rule_version = 'v1.0'  # margin ≤ 5% OR top2_pct ≥ 30%
    
    if revenue_audit and 'top1' in revenue_audit and 'top2' in revenue_audit:
        top1_sector = revenue_audit.get('top1')
        top2_sector = revenue_audit.get('top2')
        top1_pct = revenue_audit.get('top1_pct', 0)
        top2_pct = revenue_audit.get('top2_pct', 0)
        margin_pct = revenue_audit.get('margin', 0)
        
        # 🆕 P0.6-2: 듀얼 섹터 조건 - margin ≤ 5% OR top2_pct ≥ 30%
        # 이유: margin만으로 하면 "둘 다 작을 때"도 듀얼이 될 수 있음
        if (margin_pct <= 5.0 or top2_pct >= 30.0) and top1_sector and top2_sector and top1_pct > 0 and top2_pct > 0:
            dual_sector_enabled = True
            dual_sector_info = {
                'primary': top1_sector,
                'primary_pct': top1_pct,
                'secondary': top2_sector,
                'secondary_pct': top2_pct,
                'margin': margin_pct,
                'reason': 'top1_top2_close' if margin_pct <= 5.0 else 'top2_significant',
                'rule_version': dual_rule_version,
                'enabled': True
            }
    
    # 🆕 P0.6-3: 복합기업 카드 텍스트 생성 규칙
    card_text = None
    if dual_sector_enabled and dual_sector_info:
        primary_sector_name = {
            'SEC_CHEM': '석유화학',
            'SEC_ENERGY': '정유/연료',
            'SEC_SEMI': '반도체',
            'SEC_AUTO': '자동차',
            'SEC_BATTERY': '배터리',
            'SEC_BIO': '바이오',
            'SEC_IT': 'IT',
            'SEC_GAME': '게임',
        }.get(dual_sector_info['primary'], dual_sector_info['primary'])
        
        secondary_sector_name = {
            'SEC_CHEM': '석유화학',
            'SEC_ENERGY': '정유/연료',
            'SEC_SEMI': '반도체',
            'SEC_AUTO': '자동차',
            'SEC_BATTERY': '배터리',
            'SEC_BIO': '바이오',
            'SEC_IT': 'IT',
            'SEC_GAME': '게임',
        }.get(dual_sector_info['secondary'], dual_sector_info['secondary'])
        
        card_text = (
            f"{primary_sector_name}({dual_sector_info['primary_pct']:.0f}%) + "
            f"{secondary_sector_name}({dual_sector_info['secondary_pct']:.0f}%) "
            f"기반 복합 {primary_sector_name}·{secondary_sector_name} 기업"
        )
    else:
        # 비듀얼 섹터
        top2_info = candidates_with_scores[1] if len(candidates_with_scores) > 1 else None
        if top2_info:
            top2_sector_name = {
                'SEC_CHEM': '화학',
                'SEC_ENERGY': '에너지',
                'SEC_SEMI': '반도체',
                'SEC_AUTO': '자동차',
                'SEC_BATTERY': '배터리',
                'SEC_BIO': '바이오',
                'SEC_IT': 'IT',
                'SEC_GAME': '게임',
            }.get(top2_info.get('sector', ''), top2_info.get('sector', 'N/A'))
            
            card_text = (
                f"주력: {major_sector}({top1_score*100:.0f}%) — "
                f"2위: {top2_sector_name}({top2_info.get('score', 0)*100:.0f}%)"
            )
        else:
            card_text = f"주력: {major_sector}({top1_score*100:.0f}%)"
    
    decision_trace['sector'] = {
        'candidates': candidates_top3,  # 하위 호환성
        'candidates_top3': candidates_with_scores,  # 🆕 P0.5: 점수 포함 Top3
        'final': major_sector,
        'source': primary_sector_source,
        'confidence': confidence,
        'top1_score': top1_score,
        'margin': margin,
        'final_reason': final_reason,  # 🆕 P0.5: 최종 결정 이유
        'confidence_band': confidence_band,  # 🆕 P0.5: 신뢰도 밴드
        'confidence_why': confidence_why,  # 🆕 P0.5: 신뢰도 이유
        'dual_sector': dual_sector_info,  # 🆕 P0.6-B: 듀얼 섹터 정보
        'dual_sector_enabled': dual_sector_enabled,  # 🆕 P0.6-2: 듀얼 섹터 활성화 여부
        'dual_rule_version': dual_rule_version,  # 🆕 P0.6-2: 규칙 버전
        'card_text': card_text  # 🆕 P0.6-3: 복합기업 카드 텍스트
    }
    
    # 4. override 여부
    override_hit = boosting_log['classification_meta'].get('override_hit', False)
    if override_hit:
        decision_trace['override'] = {
            'hit': True,
            'reason': boosting_log['classification_meta'].get('override_reason', ''),
            'source': boosting_log['classification_meta'].get('primary_sector_source', ''),
            'key_used': boosting_log['classification_meta'].get('override_key_used', 'unknown')  # 🆕 P0.5: ticker|name 저장
        }
    
    boosting_log['classification_meta']['decision_trace'] = decision_trace
    
    # 🆕 P0-Override: Override 발생 시 confidence는 HIGH로 강제 설정 (논리 모순 해결)
    override_hit = boosting_log.get('classification_meta', {}).get('override_hit', False)
    if override_hit and major_sector:
        # Override가 발생하고 major_sector가 설정되었으면 confidence는 HIGH로 강제
        confidence = 'HIGH'
        logger.info(f"[{company_name}] Override 발생: confidence를 HIGH로 강제 설정 (major_sector: {major_sector})")
    
    return major_sector, sub_sector, value_chain, confidence, boosting_log


def classify_sector_llm(
    llm_handler: LLMHandler,
    company_detail: CompanyDetail,
    company_name: Optional[str] = None
) -> Tuple[Optional[str], Optional[str], Optional[str], str, Optional[Dict]]:
    """
    LLM 기반 섹터 분류 (Fallback)
    
    Args:
        llm_handler: LLMHandler 객체
        company_detail: CompanyDetail 객체
        company_name: 회사명 (선택)
    
    Returns:
        (major_sector, sub_sector, value_chain, confidence, boosting_log)
    """
    # LLM 입력 텍스트 구성
    text_parts = []
    if company_detail.biz_summary:
        text_parts.append(f"사업 요약: {company_detail.biz_summary}")
    if company_detail.products:
        text_parts.append(f"주요 제품: {', '.join([str(p) for p in company_detail.products[:10]])}")
    if company_name:
        text_parts.append(f"회사명: {company_name}")
    
    input_text = '\n'.join(text_parts)
    
    prompt = f"""
    다음 기업 정보를 분석하여 투자 섹터를 분류해주세요.
    
    {input_text}
    
    다음 JSON 형식으로만 응답하세요:
    {{
        "major_sector": "SEC_SEMI | SEC_IT | SEC_AUTO | SEC_BATTERY | SEC_CHEMICAL | SEC_STEEL | SEC_CONSTRUCTION | SEC_BANK | SEC_INS | SEC_SEC | SEC_CARD | SEC_HOLDING | SEC_ELECTRONICS | SEC_MEDICAL | SEC_RETAIL | SEC_TELECOM | SEC_DISCRETIONARY | SEC_STAPLE | SEC_INDUSTRIAL | SEC_UTILITIES | SEC_TIRE",
        "sub_sector": "세부 섹터 코드 (예: MEMORY, EV, SOFTWARE, FOOD_BEVERAGE 등)",
        "value_chain": "UPSTREAM | MIDSTREAM | DOWNSTREAM",
        "confidence": "HIGH | MEDIUM | LOW"
    }}
    
    섹터 코드는 반드시 위에 나열된 것 중 하나를 사용하세요.
    """
    
    try:
        # LLM 호출 (간단한 텍스트 생성)
        response = llm_handler.llm.invoke([
            {"role": "system", "content": "You are a financial analyst. Classify companies into investment sectors. Output JSON only."},
            {"role": "user", "content": prompt}
        ])
        
        import json
        import json_repair
        content = response.content.replace("```json", "").replace("```", "").strip()
        
        try:
            result = json.loads(content)
        except:
            result = json.loads(json_repair.repair_json(content))
        
        major_sector = result.get('major_sector')
        sub_sector = result.get('sub_sector')
        value_chain = result.get('value_chain')
        confidence = result.get('confidence', 'MEDIUM')
        
        boosting_log = {
            'classification_method': 'LLM',
            'classification_meta': {
                'entity_type': 'REGULAR',
                'primary_sector_source': 'LLM',  # 🆕 정규화: 대문자
                'revenue_quality': 'UNKNOWN',
                'neutral_ratio': 0.0,
                'top1_score': 0.0,  # 0.0~1.0 스케일
                'margin': 0.0  # 0.0~1.0 스케일
            }
        }
        return major_sector, sub_sector, value_chain, confidence, boosting_log
        
    except Exception as e:
        logger.error(f"LLM 섹터 분류 실패: {e}")
        return None, None, None, "LOW", {
            'classification_method': 'LLM',
            'classification_meta': {
                'entity_type': 'REGULAR',
                'primary_sector_source': 'LLM',  # 🆕 정규화: 대문자
                'revenue_quality': 'UNKNOWN',
                'neutral_ratio': 0.0,
                'top1_score': 0.0,  # 0.0~1.0 스케일
                'margin': 0.0,  # 0.0~1.0 스케일
                'error': str(e)
            }
        }


def classify_sector(
    db: Session,
    ticker: str,
    llm_handler: Optional[LLMHandler] = None,
    use_llm: bool = False
) -> Optional[Dict[str, Any]]:
    """
    섹터 분류 (하이브리드: Rule-based 우선, LLM Fallback)
    🆕 GPT 피드백: Entity Type 분류 및 classification_meta 저장 포함
    
    Args:
        db: DB 세션
        ticker: 종목코드
        llm_handler: LLMHandler 객체 (LLM 사용 시)
        use_llm: LLM Fallback 사용 여부
    
    Returns:
        {
            'major_sector': str,
            'sub_sector': str,
            'value_chain': str,
            'confidence': str,
            'classification_method': str,
            'classification_meta': dict  # 🆕 Entity Type 정보 포함
        } 또는 None
    """
    # CompanyDetail 조회
    company_detail = db.query(CompanyDetail).filter(
        CompanyDetail.ticker == ticker
    ).first()
    
    if not company_detail:
        logger.warning(f"[{ticker}] CompanyDetail 데이터 없음")
        return None
    
    # Stock 조회 (회사명) - Raw SQL 사용
    from app.utils.stock_query import get_stock_by_ticker_safe
    stock = get_stock_by_ticker_safe(db, ticker)
    company_name = stock.stock_name if stock else None
    
    # 🆕 GPT 피드백: Entity Type 분류 (Soft entity_type)
    classification_meta = {}
    if stock:
        from app.services.entity_type_classifier import classify_entity_type, update_classification_meta
        entity_type, entity_conf, entity_meta = classify_entity_type(stock, company_detail)
        classification_meta = update_classification_meta(
            None, entity_type, entity_conf, entity_meta
        )
    
    # 1차: Rule-based 분류
    major_sector, sub_sector, value_chain, confidence, boosting_log = classify_sector_rule_based(
        company_detail, company_name, ticker=ticker
    )
    
    # 🆕 P0-1: 금융지주 정보를 classification_meta에 추가 (boosting_log에서 가져옴)
    if boosting_log and boosting_log.get('classification_meta'):
        financial_holding_info = boosting_log['classification_meta'].get('is_financial_holding')
        if financial_holding_info:
            if not classification_meta:
                classification_meta = {}
            classification_meta['is_financial_holding'] = True
            classification_meta['financial_holding_info'] = financial_holding_info
    
    # 🆕 P0-4: boosting_log가 있으면 classification_meta에 병합
    if boosting_log and boosting_log.get('classification_meta'):
        classification_meta.update(boosting_log['classification_meta'])
        if boosting_log.get('classification_method'):
            classification_method = boosting_log['classification_method']
        else:
            classification_method = "RULE_BASED"
    else:
        classification_method = "RULE_BASED"
    
    # 🆕 GPT 피드백: HOLD 정책 - Confidence가 HOLD면 None 반환
    if confidence and confidence.startswith('HOLD:'):
        hold_reason_code = confidence.split(':', 1)[1] if ':' in confidence else 'HOLD_UNKNOWN'
        logger.info(f"[{ticker}] HOLD 정책 적용: 섹터 분류 보류 (사유: {hold_reason_code})")
        classification_meta['hold_reason_code'] = hold_reason_code
        
        # 🆕 P0: SK이노베이션 특별 처리 (HOLD 경로)
        if ticker == '096770' or (company_name and 'SK이노베이션' in company_name):
            # 🆕 P0-Override: Override 발생 시 confidence는 HIGH로 강제 설정 (논리 모순 해결)
            logger.info(f"[{ticker}] HOLD 경로에서 SK이노베이션 특별 처리: Primary Sector를 SEC_CHEM으로 설정 (confidence: HIGH)")
            classification_meta['entity_type'] = 'BIZ_HOLDCO'
            classification_meta['override_hit'] = True
            classification_meta['override_reason'] = 'SK이노베이션 특별 처리: 중간지주회사 (석유/화학 직접 사업 + 다수 자회사 보유)'
            classification_meta['override'] = {
                'hit': True,
                'reason': 'SK이노베이션 특별 처리: 중간지주회사',
                'source': 'RULE_OVERRIDE'
            }
            classification_meta['hold_reason_code'] = hold_reason_code  # 원래 HOLD 사유는 메타에 저장
            # Primary Sector를 SEC_CHEM으로 설정하고 confidence는 HIGH로 설정 (Override는 확정값)
            return {
                'major_sector': 'SEC_CHEM',
                'sub_sector': 'PETROCHEM',
                'value_chain': None,
                'confidence': 'HIGH',  # 🆕 Override는 확정값이므로 HIGH
                'hold_reason_code': hold_reason_code,  # 원래 HOLD 사유는 메타에 저장
                'classification_method': 'RULE_BASED_OVERRIDE',
                'classification_meta': classification_meta
            }
        
        return {
            'major_sector': None,
            'sub_sector': None,
            'value_chain': None,
            'confidence': 'HOLD',
            'hold_reason_code': hold_reason_code,  # 🆕 HOLD 사유 코드
            'classification_method': 'RULE_BASED_HOLD',
            'classification_meta': classification_meta
        }
    
    # 2차: LLM Fallback (Rule-based 실패 또는 confidence가 LOW인 경우)
    if (not major_sector or confidence == "LOW") and use_llm and llm_handler:
        logger.info(f"[{ticker}] Rule-based 분류 실패/저신뢰, LLM Fallback 시도")
        llm_major, llm_sub, llm_vc, llm_conf, llm_boosting_log = classify_sector_llm(
            llm_handler, company_detail, company_name
        )
        
        if llm_major:
            major_sector = llm_major
            sub_sector = llm_sub
            value_chain = llm_vc
            confidence = llm_conf
            classification_method = "LLM"
            # 🆕 LLM boosting_log 병합
            if llm_boosting_log and llm_boosting_log.get('classification_meta'):
                if not classification_meta:
                    classification_meta = {}
                classification_meta.update(llm_boosting_log['classification_meta'])
    
    if not major_sector:
        logger.warning(f"[{ticker}] 섹터 분류 실패")
        return None
    
    return {
        'major_sector': major_sector,
        'sub_sector': sub_sector,
        'value_chain': value_chain,
        'confidence': confidence,
        'classification_method': classification_method,
        'classification_meta': classification_meta  # 🆕 GPT 피드백: Entity Type 정보 포함
    }


# 제품 → 섹터 매핑 테이블 (Secondary 섹터 탐지용)
PRODUCT_TO_SECTOR_MAP = {
    # SEC_SEMI
    'DRAM': ('SEC_SEMI', 'MEMORY'),
    'NAND': ('SEC_SEMI', 'MEMORY'),
    'HBM': ('SEC_SEMI', 'MEMORY'),
    'DDR': ('SEC_SEMI', 'MEMORY'),
    'LPDDR': ('SEC_SEMI', 'MEMORY'),
    'SSD': ('SEC_SEMI', 'MEMORY'),
    '메모리': ('SEC_SEMI', 'MEMORY'),
    '반도체': ('SEC_SEMI', 'MEMORY'),
    '파운드리': ('SEC_SEMI', 'FOUNDRY'),
    '웨이퍼': ('SEC_SEMI', 'FOUNDRY'),
    '칩': ('SEC_SEMI', 'MEMORY'),
    'SoC': ('SEC_SEMI', 'FOUNDRY'),
    'AP': ('SEC_SEMI', 'FOUNDRY'),
    
    # SEC_ELECTRONICS
    '스마트폰': ('SEC_ELECTRONICS', 'IT_PARTS'),
    'TV': ('SEC_ELECTRONICS', 'CONSUMER'),
    '냉장고': ('SEC_ELECTRONICS', 'CONSUMER'),
    '세탁기': ('SEC_ELECTRONICS', 'CONSUMER'),
    '에어컨': ('SEC_ELECTRONICS', 'CONSUMER'),
    '모니터': ('SEC_ELECTRONICS', 'IT_DEVICE'),
    'PC': ('SEC_ELECTRONICS', 'IT_DEVICE'),
    '노트북': ('SEC_ELECTRONICS', 'IT_DEVICE'),
    '카메라모듈': ('SEC_ELECTRONICS', 'CAMERA'),
    '카메라': ('SEC_ELECTRONICS', 'CAMERA'),
    '디스플레이': ('SEC_ELECTRONICS', 'DISPLAY'),
    'OLED': ('SEC_ELECTRONICS', 'DISPLAY'),
    'LCD': ('SEC_ELECTRONICS', 'DISPLAY'),
    '패널': ('SEC_ELECTRONICS', 'DISPLAY'),
    'MLCC': ('SEC_ELECTRONICS', 'MLCC'),
    'PCB': ('SEC_ELECTRONICS', 'PCB'),
    
    # SEC_BATTERY
    '양극재': ('SEC_BATTERY', 'CATHODE'),
    '음극재': ('SEC_BATTERY', 'ANODE'),
    '전해액': ('SEC_BATTERY', 'MATERIAL'),
    '분리막': ('SEC_BATTERY', 'MATERIAL'),
    '배터리셀': ('SEC_BATTERY', 'CELL'),
    '배터리팩': ('SEC_BATTERY', 'CELL'),
    '2차전지': ('SEC_BATTERY', 'CELL'),
    
    # SEC_AUTO
    '자동차': ('SEC_AUTO', 'OEM'),
    '전기차': ('SEC_AUTO', 'EV'),
    'EV': ('SEC_AUTO', 'EV'),
    '자동차부품': ('SEC_AUTO', 'PARTS'),
    '전장': ('SEC_AUTO', 'EV_PARTS'),
    '모터': ('SEC_AUTO', 'EV_PARTS'),
    '인버터': ('SEC_AUTO', 'EV_PARTS'),
    
    # SEC_SHIP
    '조선': ('SEC_SHIP', 'SHIPBUILDING'),
    '선박': ('SEC_SHIP', 'SHIPBUILDING'),
    '해운': ('SEC_SHIP', 'SHIPPING'),
    '엔진': ('SEC_SHIP', 'ENGINE'),
    
    # SEC_IT
    '소프트웨어': ('SEC_IT', 'SOFTWARE'),
    '플랫폼': ('SEC_IT', 'PLATFORM'),
    '게임': ('SEC_IT', 'GAME'),
    '클라우드': ('SEC_IT', 'CLOUD'),
    'SaaS': ('SEC_IT', 'SOFTWARE'),
    '보안': ('SEC_IT', 'SECURITY'),
    'SI': ('SEC_IT', 'SI'),
    'AI': ('SEC_IT', 'AI_SOFT'),
    
    # SEC_BIO
    '제약': ('SEC_BIO', 'PHARMA'),
    '신약': ('SEC_BIO', 'BIO_NEW'),
    '바이오': ('SEC_BIO', 'BIO_NEW'),
    '바이오시밀러': ('SEC_BIO', 'BIOSIMILAR'),
    'CDMO': ('SEC_BIO', 'CDMO'),
    '의료기기': ('SEC_BIO', 'MED_DEVICE'),
    '임플란트': ('SEC_BIO', 'MED_DEVICE'),
    '미용': ('SEC_BIO', 'AESTHETIC'),
    
    # SEC_CHEM
    '정유': ('SEC_CHEM', 'REFINERY'),
    '석유화학': ('SEC_CHEM', 'PETROCHEM'),
    '특수화학': ('SEC_CHEM', 'SPECIALTY'),
    '비료': ('SEC_CHEM', 'AGRI'),
    
    # SEC_STEEL
    '철강': ('SEC_STEEL', 'STEEL'),
    '강판': ('SEC_STEEL', 'STEEL'),
    '구리': ('SEC_STEEL', 'NON_FERROUS'),
    '알루미늄': ('SEC_STEEL', 'NON_FERROUS'),
    '파이프': ('SEC_STEEL', 'PIPE'),
    
    # SEC_DEFENSE
    '무기': ('SEC_DEFENSE', 'WEAPON'),
    '방산': ('SEC_DEFENSE', 'WEAPON'),
    '항공기': ('SEC_DEFENSE', 'AEROSPACE'),
    '우주': ('SEC_DEFENSE', 'AEROSPACE'),
    
    # SEC_MACH
    '건설기계': ('SEC_MACH', 'CONSTRUCTION_MACH'),
    '전력망': ('SEC_MACH', 'POWER_GRID'),
    '로봇': ('SEC_MACH', 'FACTORY_AUTO'),
    '자동화': ('SEC_MACH', 'FACTORY_AUTO'),
    
    # SEC_CONST
    '건설': ('SEC_CONST', 'EPC'),
    '토목': ('SEC_CONST', 'CIVIL'),
    '플랜트': ('SEC_CONST', 'PLANT'),
    '건축자재': ('SEC_CONST', 'BUILDING_MAT'),
    
    # SEC_ENT
    'K-POP': ('SEC_ENT', 'KPOP'),
    'KPOP': ('SEC_ENT', 'KPOP'),
    '드라마': ('SEC_ENT', 'CONTENT'),
    '콘텐츠': ('SEC_ENT', 'CONTENT'),
    '광고': ('SEC_ENT', 'AD'),
    'OTT': ('SEC_ENT', 'OTT'),
    
    # SEC_COSMETIC (토스증권 77개 기업 분석 반영)
    '화장품': ('SEC_COSMETIC', 'BRAND'),
    '뷰티': ('SEC_COSMETIC', 'BRAND'),
    '코스메틱': ('SEC_COSMETIC', 'BRAND'),
    '스킨케어': ('SEC_COSMETIC', 'BRAND'),
    '기초화장품': ('SEC_COSMETIC', 'BRAND'),
    '색조화장품': ('SEC_COSMETIC', 'BRAND'),
    '화장품소재': ('SEC_COSMETIC', 'OEM'),
    '화장품제조': ('SEC_COSMETIC', 'OEM'),
    'OEM': ('SEC_COSMETIC', 'OEM'),
    'ODM': ('SEC_COSMETIC', 'ODM'),
    'OEM/ODM': ('SEC_COSMETIC', 'OEM'),
    
    # SEC_FASHION (토스증권 섬유/의류 95개 기업 기반 신설)
    '의류': ('SEC_FASHION', 'FASHION_BRAND'),
    '패션': ('SEC_FASHION', 'FASHION_BRAND'),
    '섬유': ('SEC_FASHION', 'TEXTILE'),
    '봉제': ('SEC_FASHION', 'FASHION_OEM'),
    '니트': ('SEC_FASHION', 'FASHION_OEM'),
    '원단': ('SEC_FASHION', 'TEXTILE'),
    '직물': ('SEC_FASHION', 'TEXTILE'),
    '방적': ('SEC_FASHION', 'TEXTILE'),
    '방직': ('SEC_FASHION', 'TEXTILE'),
    '아웃도어': ('SEC_FASHION', 'FASHION_BRAND'),
    '스포츠웨어': ('SEC_FASHION', 'FASHION_BRAND'),
    '란제리': ('SEC_FASHION', 'FASHION_BRAND'),
    '내의': ('SEC_FASHION', 'FASHION_BRAND'),
    '여성복': ('SEC_FASHION', 'FASHION_BRAND'),
    '남성복': ('SEC_FASHION', 'FASHION_BRAND'),
    '신사복': ('SEC_FASHION', 'FASHION_BRAND'),
    '캐주얼': ('SEC_FASHION', 'FASHION_BRAND'),
    
    # SEC_RETAIL
    '마트': ('SEC_RETAIL', 'DEPARTMENT'),
    '편의점': ('SEC_RETAIL', 'CVS'),
    '온라인쇼핑': ('SEC_RETAIL', 'E_COMMERCE'),
    '이커머스': ('SEC_RETAIL', 'E_COMMERCE'),
    '면세점': ('SEC_RETAIL', 'DUTYFREE'),
    
    # SEC_FOOD
    '식품': ('SEC_FOOD', 'FOOD'),
    '음료': ('SEC_FOOD', 'BEVERAGE'),
    '담배': ('SEC_FOOD', 'TOBACCO'),
    '사료': ('SEC_FOOD', 'FEED'),
    
    # SEC_TRAVEL
    '항공': ('SEC_TRAVEL', 'AIRLINE'),
    '여행': ('SEC_TRAVEL', 'TRAVEL'),
    '카지노': ('SEC_TRAVEL', 'CASINO'),
    '호텔': ('SEC_TRAVEL', 'HOTEL'),
    
    # SEC_BANK
    '은행': ('SEC_BANK', 'BANK'),
    '저축은행': ('SEC_BANK', 'SAVINGS'),
    
    # SEC_SEC
    '증권': ('SEC_SEC', 'SECURITIES'),
    '투자은행': ('SEC_SEC', 'INV_BANK'),
    
    # SEC_INS
    '생명보험': ('SEC_INS', 'LIFE'),
    '손해보험': ('SEC_INS', 'NON_LIFE'),
    
    # SEC_CARD
    '카드': ('SEC_CARD', 'CARD'),
    '신용카드': ('SEC_CARD', 'CARD'),
    '캐피탈': ('SEC_CARD', 'CAPITAL'),
    '리츠': ('SEC_CARD', 'REITs'),
    
    # SEC_HOLDING (지주회사 키워드 분석 기반)
    '지주': ('SEC_HOLDING', 'INDUSTRIAL_HOLDING'),
    '지주회사': ('SEC_HOLDING', 'INDUSTRIAL_HOLDING'),
    '금융지주': ('SEC_HOLDING', 'FINANCIAL_HOLDING'),
    '홀딩스': ('SEC_HOLDING', 'INDUSTRIAL_HOLDING'),
    '홀딩': ('SEC_HOLDING', 'INDUSTRIAL_HOLDING'),
    '배당금수익': ('SEC_HOLDING', 'INDUSTRIAL_HOLDING'),
    '배당수익': ('SEC_HOLDING', 'INDUSTRIAL_HOLDING'),
    '임대수익': ('SEC_HOLDING', 'INDUSTRIAL_HOLDING'),
    '브랜드사용료': ('SEC_HOLDING', 'INDUSTRIAL_HOLDING'),
    '로열티수익': ('SEC_HOLDING', 'INDUSTRIAL_HOLDING'),
    
    # SEC_TELECOM
    '통신': ('SEC_TELECOM', 'TELECOM'),
    '이동통신': ('SEC_TELECOM', 'TELECOM'),
    '5G': ('SEC_TELECOM', 'TELECOM'),
    
    # SEC_UTIL
    '전기': ('SEC_UTIL', 'POWER_GEN'),
    '전력': ('SEC_UTIL', 'POWER_GEN'),
    '가스': ('SEC_UTIL', 'GAS'),
    '수도': ('SEC_UTIL', 'ENV'),
    '환경': ('SEC_UTIL', 'ENV'),
}


def detect_secondary_sectors(
    products: List[str],
    primary_sector: Optional[Dict[str, str]] = None,
    revenue_by_segment: Optional[Dict[str, float]] = None,
    max_secondary_sectors: int = 3,
    min_revenue_threshold: float = 10.0
) -> List[Dict[str, str]]:
    """
    제품 리스트를 기반으로 Secondary 섹터 탐지 (매출 비중 필터링 지원)
    
    Args:
        products: 제품 리스트 (예: ["DRAM", "스마트폰", "TV"])
        primary_sector: Primary 섹터 정보 (중복 제거용) {"major": "SEC_SEMI", "sub": "MEMORY"}
        revenue_by_segment: 사업부문별 매출 비중 (예: {"DX부문": 58.1, "DS부문": 36.9})
        max_secondary_sectors: 최대 Secondary 섹터 수 (기본값: 3)
        min_revenue_threshold: 최소 매출 비중 임계값 (%, 기본값: 10.0)
    
    Returns:
        secondary_sectors: [{"major": "SEC_SEMI", "sub": "MEMORY", "source_product": "DRAM", "revenue_pct": 36.9}, ...]
    """
    if not products:
        return []
    
    # 매출 비중 기반 필터링 (있으면 적용)
    filtered_products = products
    segment_to_revenue = {}
    
    if revenue_by_segment and isinstance(revenue_by_segment, dict) and len(revenue_by_segment) > 0:
        # 매출 비중 상위 세그먼트만 선택
        sorted_segments = sorted(
            revenue_by_segment.items(),
            key=lambda x: float(x[1]) if isinstance(x[1], (int, float)) else 0,
            reverse=True
        )
        
        # 최소 비중 이상인 세그먼트만 선택
        top_segments = [
            seg for seg, pct in sorted_segments
            if isinstance(pct, (int, float)) and pct >= min_revenue_threshold
        ][:max_secondary_sectors + 1]  # Primary 포함해서 +1
        
        # 세그먼트명 → 매출비중 매핑
        for seg, pct in sorted_segments:
            segment_to_revenue[seg.lower()] = pct
        
        # 해당 세그먼트에 속하는 제품만 필터링
        if top_segments:
            filtered_products = []
            for product in products:
                product_lower = str(product).lower()
                for seg in top_segments:
                    seg_lower = seg.lower()
                    # 세그먼트명이 제품에 포함되거나, 제품이 세그먼트에 포함
                    if seg_lower in product_lower or product_lower in seg_lower:
                        filtered_products.append(product)
                        break
                    # 일반적인 매핑 (DX→가전/스마트폰, DS→반도체)
                    if 'dx' in seg_lower and any(kw in product_lower for kw in ['tv', '모니터', '냉장고', '스마트폰', '가전']):
                        filtered_products.append(product)
                        break
                    if 'ds' in seg_lower and any(kw in product_lower for kw in ['dram', 'nand', 'ssd', 'hbm', '반도체', '메모리']):
                        filtered_products.append(product)
                        break
            
            # 필터링 결과가 없으면 원본 사용
            if not filtered_products:
                filtered_products = products
    
    secondary_sectors = []
    detected_sectors = set()  # 중복 방지
    
    primary_key = None
    if primary_sector:
        primary_key = f"{primary_sector.get('major')}_{primary_sector.get('sub', '')}"
    
    for product in filtered_products:
        if not product:
            continue
        
        product_str = str(product).strip()
        if not product_str:
            continue
        
        # 제품명 정규화 (대소문자 통일, 공백 제거)
        product_normalized = product_str.lower().strip()
        
        # 직접 매칭 시도
        sector_info = PRODUCT_TO_SECTOR_MAP.get(product_str)
        if not sector_info:
            # 정규화된 버전으로 시도
            sector_info = PRODUCT_TO_SECTOR_MAP.get(product_normalized)
        if not sector_info:
            # 부분 매칭 시도 (제품명에 키워드 포함)
            for keyword, mapped_sector in PRODUCT_TO_SECTOR_MAP.items():
                if keyword.lower() in product_normalized or product_normalized in keyword.lower():
                    sector_info = mapped_sector
                    break
        
        if sector_info:
            major, sub = sector_info
            sector_key = f"{major}_{sub}"
            
            # Primary 섹터와 중복 제거
            if primary_key and sector_key == primary_key:
                continue
            
            # 이미 탐지된 섹터와 중복 제거
            if sector_key not in detected_sectors:
                sector_entry = {
                    "major": major,
                    "sub": sub,
                    "source_product": product_str
                }
                
                # 매출 비중 정보 추가 (있으면)
                for seg_name, pct in segment_to_revenue.items():
                    if seg_name in product_normalized or product_normalized in seg_name:
                        sector_entry["revenue_pct"] = pct
                        break
                
                secondary_sectors.append(sector_entry)
                detected_sectors.add(sector_key)
    
    # 최대 개수 제한
    return secondary_sectors[:max_secondary_sectors]

