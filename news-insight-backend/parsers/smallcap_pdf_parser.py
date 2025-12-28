"""
SmallCap PDF Parser (KIRS 전용)

한국IR협의회 기업분석보고서 전용 파서
- 체크포인트/투자포인트 박스 우선 추출
- 기술분석/시장동향 섹션 분리
- Compliance 노이즈 제거
"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
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

# KIRS 리포트 전용 키워드
CHECKPOINT_KEYWORDS = [
    "체크포인트",
    "투자포인트",
    "투자 포인트",
    "핵심 포인트",
    "주요 포인트",
    "Checkpoint",
    "Investment Points"
]

SECTION_KEYWORDS = {
    "기술분석": ["기술분석", "기술 경쟁력", "기술력", "Technical Analysis"],
    "시장동향": ["시장동향", "시장 전망", "산업 동향", "Market Trend"],
    "재무분석": ["재무분석", "재무 현황", "Financial Analysis"],
    "투자의견": ["투자의견", "투자 등급", "Rating", "목표주가"]
}

COMPLIANCE_CUTOFF_KEYWORDS = [
    "시장경보제도",
    "시장경보제도란",
    "Compliance Notice",
    "저작권",
    "본 자료는",
    "투자판단의 참고자료",
    "면책",
    "법적 책임",
    "Disclaimer",
    "Appendix",
    "발간 History"
]


def extract_checkpoint_box(page) -> Optional[str]:
    """
    1페이지에서 체크포인트 박스 추출
    
    Args:
        page: pdfplumber Page 객체
    
    Returns:
        체크포인트 텍스트 또는 None
    """
    try:
        # 페이지 상단 30% 영역에서 체크포인트 찾기
        page_height = page.height
        checkpoint_zone_top = 0
        checkpoint_zone_bottom = page_height * 0.3
        
        words = page.extract_words()
        checkpoint_texts = []
        
        in_checkpoint_zone = False
        checkpoint_started = False
        
        for word in words:
            y = word.get("top", 0)
            word_text = word.get("text", "").strip()
            
            # 체크포인트 영역 내부인지 확인
            if checkpoint_zone_top <= y <= checkpoint_zone_bottom:
                # 체크포인트 키워드 발견
                if any(keyword in word_text for keyword in CHECKPOINT_KEYWORDS):
                    checkpoint_started = True
                    in_checkpoint_zone = True
                    continue
                
                # 체크포인트 시작 후 텍스트 수집
                if checkpoint_started and in_checkpoint_zone:
                    checkpoint_texts.append(word_text)
            else:
                # 영역을 벗어나면 종료
                if checkpoint_started:
                    break
        
        if checkpoint_texts:
            checkpoint_text = " ".join(checkpoint_texts)
            # 불필요한 키워드 제거
            for keyword in CHECKPOINT_KEYWORDS:
                checkpoint_text = checkpoint_text.replace(keyword, "")
            return checkpoint_text.strip()
        
        return None
    except Exception as e:
        logger.warning(f"체크포인트 추출 실패: {e}")
        return None


def identify_sections(text: str) -> Dict[str, str]:
    """
    텍스트에서 섹션 분리
    
    Returns:
        {"기술분석": "...", "시장동향": "...", ...}
    """
    sections = {}
    
    for section_name, keywords in SECTION_KEYWORDS.items():
        section_text = []
        in_section = False
        
        lines = text.split('\n')
        for i, line in enumerate(lines):
            # 섹션 시작 키워드 찾기
            if any(keyword in line for keyword in keywords):
                in_section = True
                continue
            
            # 다음 섹션 시작 전까지 수집
            if in_section:
                # 다른 섹션 시작 키워드 발견 시 종료
                if any(
                    other_keywords and 
                    any(kw in line for kw in other_keywords) and 
                    other_section != section_name
                    for other_section, other_keywords in SECTION_KEYWORDS.items()
                ):
                    in_section = False
                    break
                
                section_text.append(line)
        
        if section_text:
            sections[section_name] = "\n".join(section_text).strip()
    
    return sections


def remove_compliance_noise(text: str) -> str:
    """
    Compliance 노이즈 제거
    
    Args:
        text: 원본 텍스트
    
    Returns:
        정제된 텍스트
    """
    lines = text.split('\n')
    cleaned_lines = []
    compliance_started = False
    
    for line in lines:
        # Compliance 섹션 시작 감지
        if any(keyword in line for keyword in COMPLIANCE_CUTOFF_KEYWORDS):
            compliance_started = True
            break
        
        if not compliance_started:
            cleaned_lines.append(line)
    
    return "\n".join(cleaned_lines)


def extract_text_from_kirs_pdf(
    pdf_bytes: bytes,
    report_type: str = "company"  # "company" or "industry"
) -> Dict[str, Any]:
    """
    KIRS 리포트 전용 PDF 파싱
    
    Args:
        pdf_bytes: PDF 바이너리 데이터
        report_type: 리포트 유형
    
    Returns:
        {
            "full_text": 전체 텍스트,
            "checkpoint": 체크포인트 텍스트,
            "sections": 섹션별 텍스트,
            "paragraphs": 문단 리스트,
            "total_pages": 총 페이지 수,
            "parse_quality": "HIGH" | "MEDIUM" | "LOW"
        }
    """
    try:
        pdf_stream = BytesIO(pdf_bytes)
        with pdfplumber.open(pdf_stream) as pdf:
            total_pages = len(pdf.pages)
            
            # 1페이지에서 체크포인트 추출
            checkpoint = None
            if total_pages > 0:
                first_page = pdf.pages[0]
                checkpoint = extract_checkpoint_box(first_page)
            
            # 전체 텍스트 추출 (최대 10페이지)
            max_pages = min(10, total_pages)
            full_text = ""
            pages_text = []
            
            for page_num, page in enumerate(pdf.pages[:max_pages], 1):
                page_text = page.extract_text()
                if page_text:
                    pages_text.append(page_text)
                    full_text += page_text + "\n\n"
            
            # Compliance 노이즈 제거
            full_text = remove_compliance_noise(full_text)
            
            # 섹션 분리
            sections = identify_sections(full_text)
            
            # 문단 단위로 분리
            paragraphs = []
            for para in full_text.split('\n\n'):
                para = para.strip()
                if len(para) >= 50:  # 최소 50자 이상
                    paragraphs.append({
                        "text": para,
                        "length": len(para)
                    })
            
            # 품질 평가
            parse_quality = "HIGH"
            if len(paragraphs) < 3:
                parse_quality = "LOW"
            elif len(paragraphs) < 5:
                parse_quality = "MEDIUM"
            
            # 체크포인트가 있으면 품질 향상
            if checkpoint:
                parse_quality = "HIGH"
            
            result = {
                "full_text": full_text,
                "checkpoint": checkpoint,
                "sections": sections,
                "paragraphs": paragraphs,
                "pages": pages_text,
                "total_pages": total_pages,
                "total_paragraphs": len(paragraphs),
                "parse_quality": parse_quality,
                "source": "KIRS"
            }
            
            logger.info(f"KIRS PDF 파싱 완료: {total_pages}페이지, {len(paragraphs)}개 문단, 품질: {parse_quality}")
            
            return result
    
    except Exception as e:
        logger.error(f"KIRS PDF 파싱 실패: {e}", exc_info=True)
        return {
            "full_text": "",
            "checkpoint": None,
            "sections": {},
            "paragraphs": [],
            "pages": [],
            "total_pages": 0,
            "total_paragraphs": 0,
            "parse_quality": "LOW",
            "source": "KIRS",
            "error": str(e)
        }

