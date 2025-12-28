import hashlib
import html
import re
from typing import List, Optional


WHITESPACE_PATTERN = re.compile(r'\s+')
MULTI_NEWLINE_PATTERN = re.compile(r'\n\s*\n\s*\n+')
REPORTER_PATTERN = re.compile(
    r'\((?:서울|부산|인천|대구|대전|광주|울산|제주|수원|성남|[가-힣\s]*?)'
    r'=?[^\)]*(기자|특파원|뉴스|연합뉴스|뉴시스|뉴스1)[^\)]*\)',
    re.IGNORECASE,
)
BRACKET_META_PATTERN = re.compile(r'\[[^]]*(기자|특가|이벤트|쿠폰)[^]]*\]')
HTML_TAG_PATTERN = re.compile(r'<[^>]+>')
NON_PRINTABLE_PATTERN = re.compile(r'[\x00-\x08\x0b-\x0c\x0e-\x1f]')

AD_KEYWORDS = [
    '특가', '최저가', '단독 특가', '단독 최저가', '최대 할인', '쿠폰', '사은품',
    '구매', '구독', '주문', '쇼핑', '패키지', '무료 증정', '무료 체험', '한정 수량',
    '단독 혜택', '입점', '입점 기념', '라이브 커머스', '타임세일', '광고성 내용',
    '조선몰', '단독 최저가로 소개합니다', '구매 혜택', '스폰서', '협찬',
]

AD_PATTERNS = [
    re.compile(r'본\s*기사\s*(?:는|에서는)?.*광고성\s*내용', re.IGNORECASE),
    re.compile(r'이\s*기사는\s*광고\s*성격', re.IGNORECASE),
    re.compile(r'독자\s*혜택.*최저가', re.IGNORECASE),
    re.compile(r'단독\s*특가로\s*소개합니다', re.IGNORECASE),
]

# ============================================
# RSS 피드 언론사명 목록 (메타데이터 제거용)
# ============================================
NEWS_SOURCE_NAMES = frozenset({
    # 정식명 + 약칭
    '매일경제', '매경',
    '한국경제', '한경',
    '연합뉴스', '연합',
    '아시아경제', '아시아',
    '이데일리',
    '전자신문', 'ET뉴스',
    '경향신문', '경향',
    'WOWTV', '와우티비',
    '서울경제',
    '이투데이',
    '파이낸셜뉴스', '파이낸셜',
    '연합인포맥스', '인포맥스', 'einfomax',  # ✅ 중요
    '해럴드저널', '해럴드',
    '한겨레신문', '한겨레',
    '동아일보', '동아',
    '뉴시스',
    '조선일보', '조선',
    'JTBC',
})

# ============================================
# 컴파일된 정규식 패턴 (키워드 추출용 텍스트 정제)
# ============================================

# 1단계: 고정형 구조 제거 (저작권 정보)
COPYRIGHT_PATTERN_HTML = re.compile(
    r'<저작권자[^>]*>.*?AI\s*학습\s*및\s*활용\s*금지[^>]*>',
    re.IGNORECASE | re.DOTALL
)
COPYRIGHT_PATTERN_TEXT = re.compile(
    r'저작권자\s*\(c\)\s*[^,]*,\s*무단전재\s*및\s*재배포\s*금지[^.]*\s*AI\s*학습\s*및\s*활용\s*금지',
    re.IGNORECASE
)
COPYRIGHT_SHORT = re.compile(r'무단전재.*?금지|AI\s*학습.*?금지', re.IGNORECASE)

# 2단계: 메타데이터 제거 (기자 정보, 날짜)
REPORTER_DATE_PATTERN = re.compile(
    r'^[가-힣\s]+\s*기자\s*\|\s*입력\s*\d{4}\.\d{2}\.\d{2}.*?수정\s*\d{4}\.\d{2}\.\d{2}.*?$',
    re.MULTILINE
)
REPORTER_PATTERN = re.compile(r'[가-힣]{2,4}\s*(?:기자|특파원)\s*[|｜]')
DATE_TIME_PATTERN = re.compile(
    r'(?:입력|수정)\s*\d{4}\.\d{2}\.\d{2}\s*\d{2}:\d{2}|\d{4}\.\s*\d{1,2}\.\s*\d{1,2}\.\s*오[전후]\s*\d{1,2}:\d{2}:\d{2}'
)
EMAIL_PATTERN = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')

# 3단계: 이미지 캡션 및 대괄호 내용 제거
IMAGE_CAPTION_PATTERN = re.compile(r'\[(?:촬영|사진|제공)[^\]]*\]')
META_BRACKET_PATTERN = re.compile(
    r'\[[^\]]*(?:기자|특파원|뉴스|연합|인포맥스|촬영|제공)[^\]]*\]',
    re.IGNORECASE
)

# 4단계: 괄호 내용 제거 (위치 정보, 소속)
LOCATION_PATTERN = re.compile(
    r'\([가-힣]+=[^)]*(?:연합|인포맥스|뉴스|뉴시스)[^)]*\)',
    re.IGNORECASE
)
REPORTER_PAREN_PATTERN = re.compile(r'\([가-힣\s]*(?:기자|특파원)[^)]*\)')
# 언론사명이 포함된 괄호 제거를 위한 동적 패턴 생성 함수 필요

# 5단계: 본사 정보 및 기타 노이즈
HEADQUARTERS_PATTERN = re.compile(r'[가-힣]+(?:본사|지사|센터)\s*$', re.MULTILINE)
SCREEN_NUM_PATTERN = re.compile(r'화면\s*\d{4,}')
URL_PATTERN = re.compile(r'https?://[^\s]+')

# 언론사명 제거를 위한 동적 패턴 (긴 이름 4글자 이상)
# 컴파일 시점에는 생성 불가하므로 함수 내에서 생성


def clean_text(text: str) -> str:
    """
    텍스트 정리 (불필요한 공백, 줄바꿈 제거)
    
    Args:
        text: 원본 텍스트
    
    Returns:
        정리된 텍스트
    """
    if not text:
        return ""
    
    text = NON_PRINTABLE_PATTERN.sub(" ", text)
    text = WHITESPACE_PATTERN.sub(" ", text)
    text = MULTI_NEWLINE_PATTERN.sub("\n\n", text)
    return text.strip()


def normalize_article_text(text: str) -> str:
    """
    기사 제목/본문을 유사도 계산에 적합하도록 정규화.
    HTML 엔티티, 기자명, 불필요한 괄호 내용을 제거하고 소문자화한다.
    """
    if not text:
        return ""

    text = html.unescape(text)
    text = HTML_TAG_PATTERN.sub(" ", text)
    text = REPORTER_PATTERN.sub(" ", text)
    text = BRACKET_META_PATTERN.sub(" ", text)
    text = text.replace('’', "'").replace('“', '"').replace('”', '"')
    text = clean_text(text)
    return text.lower()


def extract_keywords(text: str, max_keywords: int = 10) -> List[str]:
    """
    텍스트에서 간단한 키워드 추출 (기본 구현)
    
    Args:
        text: 원본 텍스트
        max_keywords: 최대 키워드 수
    
    Returns:
        키워드 리스트
    """
    words = re.findall(r'\b[가-힣]{2,}\b', text or "")
    word_freq = {}
    for word in words:
        word_freq[word] = word_freq.get(word, 0) + 1
    
    sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
    return [word for word, _ in sorted_words[:max_keywords]]


def is_probable_advertorial(
    title: Optional[str] = None,
    summary: Optional[str] = None,
    body: Optional[str] = None,
) -> bool:
    """
    광고성 기사 여부 추정.
    특정 키워드/패턴이 다수 등장하면 광고성으로 간주한다.
    """
    combined = " ".join(
        filter(None, [title or "", summary or "", body or ""])
    )
    normalized = normalize_article_text(combined)

    if not normalized:
        return False

    for pattern in AD_PATTERNS:
        if pattern.search(normalized):
            return True

    keyword_hits = sum(1 for keyword in AD_KEYWORDS if keyword in normalized)
    if keyword_hits >= 2:
        return True

    # 특정 키워드가 제목과 본문에 동시에 등장하면 광고 가능성이 큼
    if title and summary:
        title_norm = normalize_article_text(title)
        summary_norm = normalize_article_text(summary)
        for keyword in AD_KEYWORDS:
            if keyword in title_norm and keyword in summary_norm:
                return True

    return False


def make_article_hash_key(title: Optional[str], image_url: Optional[str]) -> Optional[str]:
    """
    제목과 이미지 URL을 기반으로 기사 고유 해시 생성.
    """
    normalized_title = normalize_article_text(title or "")
    normalized_image = (image_url or "").strip()

    if not normalized_title and not normalized_image:
        return None

    base = f"{normalized_title}::{normalized_image}"
    return hashlib.md5(base.encode("utf-8")).hexdigest()


def remove_news_source_names(text: str) -> str:
    """
    언론사명 제거 (괄호 패턴 + 단독)
    
    Args:
        text: 원본 텍스트
    
    Returns:
        언론사명이 제거된 텍스트
    """
    # 1. 괄호 안의 언론사명 제거
    # 패턴: (서울=연합인포맥스), (연합인포맥스), (인포맥스) 등
    for source_name in NEWS_SOURCE_NAMES:
        # 괄호 안에 언론사명이 포함된 경우 제거
        pattern = re.compile(
            rf'\([^)]*{re.escape(source_name)}[^)]*\)',
            re.IGNORECASE
        )
        text = pattern.sub('', text)
    
    # 2. 긴 언론사명 단독 제거 (4글자 이상만, 오탐 방지)
    for source_name in NEWS_SOURCE_NAMES:
        if len(source_name) >= 4:
            # 단어 경계 확인하여 단독 언론사명만 제거
            pattern = re.compile(
                rf'\b{re.escape(source_name)}\b',
                re.IGNORECASE
            )
            text = pattern.sub('', text)
    
    return text


def clean_text_for_keywords(text: str) -> str:
    """
    키워드 추출용 텍스트 정제 (최적화)
    
    노이즈 제거 우선순위:
    1. 고정형 구조 (저작권, 푸터) - 가장 확실한 패턴
    2. 메타데이터 (기자정보, 날짜) - 정규식 패턴
    3. 이미지 캡션 - 대괄호 패턴
    4. 괄호 내용 - 기자 소속 정보, 언론사명
    5. 본사 정보 및 기타 노이즈
    6. HTML 태그 및 특수 문자
    7. 공백 정리
    
    Args:
        text: 원본 텍스트
    
    Returns:
        정제된 텍스트 (본문 내용만)
    """
    if not text:
        return ""
    
    # ============================================
    # 1단계: 고정형 구조 제거 (가장 확실한 패턴)
    # ============================================
    text = COPYRIGHT_PATTERN_HTML.sub('', text)
    text = COPYRIGHT_PATTERN_TEXT.sub('', text)
    text = COPYRIGHT_SHORT.sub('', text)
    
    # ============================================
    # 2단계: 메타데이터 제거 (기자 정보, 날짜)
    # ============================================
    text = REPORTER_DATE_PATTERN.sub('', text)
    text = REPORTER_PATTERN.sub('', text)
    text = DATE_TIME_PATTERN.sub('', text)
    text = EMAIL_PATTERN.sub('', text)
    
    # ============================================
    # 3단계: 이미지 캡션 및 대괄호 내용 제거
    # ============================================
    text = IMAGE_CAPTION_PATTERN.sub('', text)
    text = META_BRACKET_PATTERN.sub('', text)
    
    # ============================================
    # 4단계: 괄호 내용 제거 (위치 정보, 소속, 언론사명)
    # ============================================
    text = LOCATION_PATTERN.sub('', text)
    text = REPORTER_PAREN_PATTERN.sub('', text)
    
    # 언론사명 제거 (괄호 패턴 + 단독)
    text = remove_news_source_names(text)
    
    # ============================================
    # 5단계: 본사 정보 및 기타 노이즈
    # ============================================
    text = HEADQUARTERS_PATTERN.sub('', text)
    text = SCREEN_NUM_PATTERN.sub('', text)
    text = URL_PATTERN.sub('', text)
    
    # ============================================
    # 6단계: HTML 태그 및 특수 문자 정리
    # ============================================
    text = HTML_TAG_PATTERN.sub('', text)
    text = html.unescape(text)
    text = NON_PRINTABLE_PATTERN.sub(' ', text)
    
    # ============================================
    # 7단계: 공백 정리 (마지막)
    # ============================================
    text = WHITESPACE_PATTERN.sub(' ', text)
    text = MULTI_NEWLINE_PATTERN.sub('\n\n', text)
    text = text.strip()
    
    return text


def filter_keywords_by_metadata(keywords: List[str]) -> List[str]:
    """
    키워드에서 메타데이터 관련 키워드 필터링
    
    Args:
        keywords: 키워드 리스트
    
    Returns:
        필터링된 키워드 리스트
    """
    filtered = []
    
    for kw in keywords:
        # 언론사명 포함 여부 확인
        if any(source_name in kw for source_name in NEWS_SOURCE_NAMES):
            continue
        # 언론사명 자체인 경우
        if kw in NEWS_SOURCE_NAMES:
            continue
        # 메타데이터 관련 키워드
        metadata_keywords = {
            '서울', '부산', '인천', '기자', '특파원', '입력', '수정',
            '저작권', '무단', '재배포', '금지', 'AI', '학습', '활용',
            '촬영', '본사', '화면', '서비스'
        }
        if any(meta_kw in kw for meta_kw in metadata_keywords):
            continue
        filtered.append(kw)
    
    return filtered

