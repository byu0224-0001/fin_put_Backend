"""
Smart PDF Parser (Phase 2.0)

목적: LLM이 헛소리 안 하게 "LLM 입력용 텍스트"를 안정적으로 만든다.
(완벽한 원문 복원이 아니라, 사고 방해 요소 제거가 목적)

핵심 기능:
- pdfplumber 사용 (2단 레이아웃 처리 우수)
- 리포트 유형별 preset (종목/산업 리포트)
- 표/헤더/푸터/컴플라이언스 제거
"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
import re
import logging
from io import BytesIO

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    import pdfplumber
except ImportError:
    raise ImportError("pdfplumber가 설치되지 않았습니다. pip install pdfplumber 실행하세요.")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 리포트 유형별 Default Preset
DEFAULT_COMPANY_PDF_PRESET = {
    "header_ratio": 0.08,      # 상단 8% 제거
    "footer_ratio": 0.90,      # 하단 10% 제거
    "column_split": 0.52,      # 중앙 52% 기준 좌/우 분리
    "min_paragraph_length": 40,  # 최소 문단 길이
    "table_digit_ratio": 0.35,   # 표 판단 기준 (숫자 비율)
    "stop_at_compliance": True,  # 컴플라이언스 섹션 컷오프
    "max_pages": 3              # 최대 페이지 수 (종목 리포트는 앞부분만)
}

DEFAULT_INDUSTRY_PDF_PRESET = {
    "header_ratio": 0.08,
    "footer_ratio": 0.92,
    "column_split": 0.52,
    "min_paragraph_length": 50,
    "table_digit_ratio": 0.40,   # 산업 리포트는 표 밀도가 더 높음
    "stop_at_compliance": True,
    "max_pages": 6              # 산업 리포트는 설명이 더 길다
}

# 컴플라이언스 컷오프 키워드
COMPLIANCE_CUTOFF_KEYWORDS = [
    "Compliance Notice",
    "투자의견",
    "투자등급",
    "법적 책임",
    "면책",
    "지적재산권",
    "본 자료는",
    "투자판단의 참고자료",
    "목표주가 변동",
    "의견 비율 공시",
    "Disclaimer",
    "Appendix"
]

# 표 판단 키워드
TABLE_INDICATOR_KEYWORDS = [
    "단위:",
    "(억원)",
    "YoY",
    "QoQ",
    "EPS",
    "PER",
    "PBR",
    "EV/EBITDA",
    "매출액",
    "영업이익",
    "순이익"
]


def count_digits(text: str) -> int:
    """텍스트 내 숫자 개수"""
    return len(re.findall(r'\d', text))


def is_table_candidate(text: str, digit_ratio_threshold: float = 0.35) -> bool:
    """
    표 후보 판단 휴리스틱
    
    Args:
        text: 검사할 텍스트
        digit_ratio_threshold: 숫자 비율 임계값
    
    Returns:
        표로 판단되면 True
    """
    if not text or len(text) < 20:
        return False
    
    # 1. 숫자 밀도 체크
    digit_ratio = count_digits(text) / len(text) if len(text) > 0 else 0
    if digit_ratio > digit_ratio_threshold:
        return True
    
    # 2. 구분자 밀도 체크
    separator_count = len(re.findall(r'[|—─_:]', text))
    if separator_count > len(text) * 0.1:  # 10% 이상 구분자
        return True
    
    # 3. 라인 평균 길이 체크
    lines = text.split('\n')
    if lines:
        avg_line_length = sum(len(line.strip()) for line in lines) / len(lines)
        if avg_line_length < 15:  # 평균 15자 미만
            return True
    
    # 4. 키워드 포함 체크
    text_lower = text.lower()
    if any(keyword.lower() in text_lower for keyword in TABLE_INDICATOR_KEYWORDS):
        # 키워드가 있고, 숫자 비율도 높으면 표로 판단
        if digit_ratio > 0.25:
            return True
    
    return False


def extract_text_from_pdf_smart(
    pdf_bytes: bytes,
    report_type: str = "company",  # "company" or "industry"
    preset: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Smart PDF 파싱 (pdfplumber 사용)
    
    Args:
        pdf_bytes: PDF 바이너리 데이터
        report_type: 리포트 유형 ("company" or "industry")
        preset: 커스텀 preset (None이면 기본 preset 사용)
    
    Returns:
        {
            "full_text": 전체 텍스트,
            "paragraphs": 문단 리스트,
            "pages": 페이지별 텍스트,
            "total_pages": 총 페이지 수,
            "total_paragraphs": 총 문단 수,
            "parse_quality": "HIGH" | "MEDIUM" | "LOW"
        }
    """
    # Preset 선택
    if preset is None:
        if report_type.lower() == "industry":
            preset = DEFAULT_INDUSTRY_PDF_PRESET.copy()
        else:
            preset = DEFAULT_COMPANY_PDF_PRESET.copy()
    
    try:
        # pdfplumber는 bytes를 직접 받을 수 없으므로 BytesIO로 변환
        pdf_stream = BytesIO(pdf_bytes)
        with pdfplumber.open(pdf_stream) as pdf:
            total_pages = len(pdf.pages)
            max_pages = preset.get("max_pages", total_pages)
            
            full_text = ""
            paragraphs = []
            pages = []
            compliance_cutoff_triggered = False
            
            for page_num, page in enumerate(pdf.pages[:max_pages], 1):
                if compliance_cutoff_triggered:
                    logger.info(f"페이지 {page_num}: 컴플라이언스 섹션 이후로 스킵")
                    break
                
                page_width = page.width
                page_height = page.height
                
                # 헤더/푸터 영역 계산
                header_cutoff = page_height * preset["header_ratio"]
                footer_cutoff = page_height * preset["footer_ratio"]
                column_split = page_width * preset["column_split"]
                
                # 좌/우 컬럼 분리
                left_column_words = []
                right_column_words = []
                
                words = page.extract_words()
                
                for word in words:
                    word_text = word.get("text", "").strip()
                    if not word_text:
                        continue
                    
                    # 헤더/푸터 영역 제외
                    y = word.get("top", 0)
                    if y < header_cutoff or y > footer_cutoff:
                        continue
                    
                    # 좌/우 컬럼 분류
                    x = word.get("x0", 0)
                    if x < column_split:
                        left_column_words.append(word)
                    else:
                        right_column_words.append(word)
                
                # 좌 → 우 순서로 텍스트 결합
                all_words = left_column_words + right_column_words
                
                # 단어를 문장으로 결합
                page_text = ""
                prev_word = None
                for word in all_words:
                    word_text = word.get("text", "")
                    if not word_text:
                        continue
                    
                    if prev_word:
                        # 단어 간 간격 계산
                        x_gap = word.get("x0", 0) - prev_word.get("x1", 0)
                        y_gap = abs(word.get("top", 0) - prev_word.get("top", 0))
                        
                        if y_gap > 5:  # 다음 줄
                            page_text += "\n"
                        elif x_gap > 10:  # 단어 간 간격
                            page_text += " "
                    
                    page_text += word_text
                    prev_word = word
                
                # 컴플라이언스 섹션 체크
                if preset.get("stop_at_compliance", True):
                    page_text_lower = page_text.lower()
                    for keyword in COMPLIANCE_CUTOFF_KEYWORDS:
                        if keyword.lower() in page_text_lower:
                            logger.info(f"페이지 {page_num}: 컴플라이언스 키워드 '{keyword}' 발견, 이후 컷오프")
                            compliance_cutoff_triggered = True
                            break
                
                if compliance_cutoff_triggered:
                    break
                
                # 문단 단위로 분리
                page_paragraphs = []
                for para in page_text.split('\n\n'):
                    para = para.strip()
                    if not para:
                        continue
                    
                    # 최소 길이 체크
                    if len(para) < preset["min_paragraph_length"]:
                        continue
                    
                    # 표 후보 제거
                    if is_table_candidate(para, preset["table_digit_ratio"]):
                        logger.debug(f"페이지 {page_num}: 표로 판단되어 제거 - {para[:50]}...")
                        continue
                    
                    page_paragraphs.append({
                        "page_number": page_num,
                        "text": para
                    })
                
                if page_paragraphs:
                    pages.append({
                        "page_number": page_num,
                        "text": "\n\n".join([p["text"] for p in page_paragraphs])
                    })
                    
                    full_text += "\n\n".join([p["text"] for p in page_paragraphs]) + "\n\n"
                    paragraphs.extend(page_paragraphs)
            
            # 파싱 품질 평가
            total_chars = len(full_text)
            if total_chars < 2000:
                parse_quality = "LOW"
            elif total_chars < 5000:
                parse_quality = "MEDIUM"
            else:
                parse_quality = "HIGH"
            
            logger.info(f"PDF 파싱 완료: {total_pages}페이지 중 {len(pages)}페이지, {len(paragraphs)}개 문단, 품질: {parse_quality}")
            
            return {
                "full_text": full_text.strip(),
                "paragraphs": paragraphs,
                "pages": pages,
                "total_pages": total_pages,
                "parsed_pages": len(pages),
                "total_paragraphs": len(paragraphs),
                "parse_quality": parse_quality,
                "report_type": report_type,
                # 메타데이터 (Ticker Matcher에서 Context-Aware 처리용)
                "metadata": {
                    "report_type": report_type,
                    "parsed_at": datetime.now().isoformat() if 'datetime' in globals() else None
                }
            }
    
    except Exception as e:
        logger.error(f"PDF 파싱 실패: {e}", exc_info=True)
        return {
            "full_text": "",
            "paragraphs": [],
            "pages": [],
            "total_pages": 0,
            "parsed_pages": 0,
            "total_paragraphs": 0,
            "parse_quality": "LOW",
            "error": str(e),
            "parse_fail_reason": "SMART_PARSER_EXCEPTION"  # P0-2: parse_fail_reason 추가
        }


def parse_pdf_file(
    pdf_path: Path,
    report_type: str = "company",
    preset: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    PDF 파일 파싱 (파일 경로 입력)
    
    Args:
        pdf_path: PDF 파일 경로
        report_type: 리포트 유형
        preset: 커스텀 preset
    
    Returns:
        파싱 결과
    """
    with open(pdf_path, 'rb') as f:
        pdf_bytes = f.read()
    
    return extract_text_from_pdf_smart(pdf_bytes, report_type, preset)


if __name__ == "__main__":
    # 테스트 코드
    import sys
    
    if len(sys.argv) < 2:
        print("사용법: python smart_pdf_parser.py <pdf_path> [report_type]")
        sys.exit(1)
    
    pdf_path = Path(sys.argv[1])
    report_type = sys.argv[2] if len(sys.argv) > 2 else "company"
    
    if not pdf_path.exists():
        print(f"파일이 없습니다: {pdf_path}")
        sys.exit(1)
    
    result = parse_pdf_file(pdf_path, report_type)
    
    print(f"\n파싱 결과:")
    print(f"- 총 페이지: {result['total_pages']}")
    print(f"- 파싱된 페이지: {result['parsed_pages']}")
    print(f"- 문단 수: {result['total_paragraphs']}")
    print(f"- 품질: {result['parse_quality']}")
    print(f"- 전체 텍스트 길이: {len(result['full_text'])}자")
    
    # 출력 디렉토리에 저장
    output_dir = project_root / "outputs" / "parsed"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = output_dir / f"{pdf_path.stem}_parsed.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(result['full_text'])
    
    print(f"\n결과 저장: {output_file}")

