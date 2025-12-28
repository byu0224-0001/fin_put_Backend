"""
텍스트 청킹 유틸리티

문단/문장 단위로 텍스트를 정확하게 잘라서 max_chars 이하로 제한
- 문단 단위 우선 (문맥 보존)
- 문장 단위 보완 (kss 라이브러리 사용)
"""
from typing import List, Optional
import logging
import re

logger = logging.getLogger(__name__)

# kss 라이브러리 import (선택적)
try:
    import kss
    HAS_KSS = True
except ImportError:
    HAS_KSS = False
    logger.warning("kss 라이브러리가 설치되지 않았습니다. 기본 문장 분할을 사용합니다.")


def truncate_to_sentences(
    text: str, 
    max_chars: int = 500,
    prefer_paragraphs: bool = True
) -> str:
    """
    텍스트를 문장/문단 단위로 끊어서 max_chars 이하로 제한
    
    전략:
    1. 문단 단위 우선 (prefer_paragraphs=True)
       - 첫 1-2개 문단이 500자 이하면 문단 단위로 사용
    2. 문장 단위 보완
       - 문단 단위로 부족하면 문장 단위로 추가
       - kss 라이브러리 사용 (정확도 높음)
    
    Args:
        text: 원본 텍스트
        max_chars: 최대 문자 수
        prefer_paragraphs: 문단 단위 우선 사용 여부
    
    Returns:
        문장/문단 단위로 잘린 텍스트
    """
    if not text:
        return ""
    
    # 이미 max_chars 이하면 그대로 반환
    if len(text) <= max_chars:
        return text
    
    # 문단 단위 우선 처리
    if prefer_paragraphs:
        result = _truncate_by_paragraphs(text, max_chars)
        if result:
            return result
    
    # 문장 단위로 처리
    return _truncate_by_sentences(text, max_chars)


def _truncate_by_paragraphs(text: str, max_chars: int) -> Optional[str]:
    """
    문단 단위로 텍스트 자르기
    
    DART 보고서는 문단 구조가 명확하고,
    첫 1-2개 문단에 핵심 정보가 있는 경우가 많음
    
    Args:
        text: 원본 텍스트
        max_chars: 최대 문자 수
    
    Returns:
        문단 단위로 잘린 텍스트 (None이면 문장 단위로 처리 필요)
    """
    # 문단 분리 (\n\n 또는 \n\n\n)
    paragraphs = re.split(r'\n\s*\n+', text)
    
    # 빈 문단 제거
    paragraphs = [p.strip() for p in paragraphs if p.strip()]
    
    if not paragraphs:
        return None
    
    # 첫 문단만으로 충분한 경우
    first_para = paragraphs[0]
    if len(first_para) <= max_chars:
        # 첫 문단 + 두 번째 문단도 가능하면 추가
        if len(paragraphs) > 1:
            second_para = paragraphs[1]
            combined = f"{first_para}\n\n{second_para}"
            if len(combined) <= max_chars:
                return combined
        return first_para
    
    # 첫 문단이 max_chars를 초과하는 경우
    # 문단 단위로는 처리 불가, 문장 단위로 처리 필요
    return None


def _truncate_by_sentences(text: str, max_chars: int) -> str:
    """
    문장 단위로 텍스트 자르기
    
    kss 라이브러리를 우선 사용하고,
    없으면 개선된 정규표현식 사용
    
    Args:
        text: 원본 텍스트
        max_chars: 최대 문자 수
    
    Returns:
        문장 단위로 잘린 텍스트
    """
    # kss로 문장 분리
    if HAS_KSS:
        try:
            sentences = kss.split_sentences(text)
        except Exception as e:
            logger.warning(f"kss 문장 분할 실패, 기본 분할 사용: {e}")
            sentences = _split_sentences_fallback(text)
    else:
        sentences = _split_sentences_fallback(text)
    
    if not sentences:
        # 문장 분리 실패 시 단순 자르기 (최후의 수단)
        return text[:max_chars-3] + "..."
    
    # 문장 단위로 누적하여 max_chars 이하로 제한
    result_parts = []
    current_length = 0
    
    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue
        
        # 문장 길이 확인
        sentence_length = len(sentence)
        
        # 공백 추가 시 길이
        separator_length = 1 if result_parts else 0
        total_length = current_length + separator_length + sentence_length
        
        if total_length <= max_chars:
            result_parts.append(sentence)
            current_length = total_length
        else:
            # 한 문장이 max_chars를 초과하는 경우
            if not result_parts:
                # 첫 문장이 너무 긴 경우, 단순 자르기 (최후의 수단)
                return text[:max_chars-3] + "..."
            break
    
    if not result_parts:
        return text[:max_chars-3] + "..."
    
    result = ' '.join(result_parts)
    
    # 마지막 문장이 완전하지 않을 수 있으므로, 문장 부호 확인
    if result and result[-1] not in '.!?。':
        # 마지막 문장이 완전하지 않으면 제거
        if len(result_parts) > 1:
            result_parts = result_parts[:-1]
            result = ' '.join(result_parts)
    
    return result


def _split_sentences_fallback(text: str) -> List[str]:
    """
    Fallback: kss가 없을 때 기본 문장 분할
    
    개선된 정규표현식 사용:
    - 숫자 뒤 점 제외 (예: 3.14)
    - 약어 뒤 점 제외 (Dr., Mr. 등)
    - 한국어 문장 끝 패턴 인식
    
    Args:
        text: 원본 텍스트
    
    Returns:
        문장 리스트
    """
    if not text:
        return []
    
    # 문단 단위로 먼저 분리 (문맥 보존)
    paragraphs = text.split('\n\n')
    
    all_sentences = []
    
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
        
        # 문장 끝 패턴
        # - 숫자 뒤 점 제외: (?<!\d)
        # - 영어 약어 패턴 고려
        # - 한국어/영어 문장 끝: [.!?。]
        
        # 기본 문장 분할 (개선된 패턴)
        # 문장 끝이면서 공백으로 시작하는 다음 문장
        pattern = r'(?<!\d)(?<!\.)[.!?。]\s+(?=[가-힣A-Z])'
        
        sentences = re.split(pattern, para)
        
        # 빈 문장 제거 및 정리
        for sent in sentences:
            sent = sent.strip()
            if sent and len(sent) > 1:  # 최소 길이 체크
                all_sentences.append(sent)
    
    return all_sentences


def split_into_paragraphs(text: str) -> List[str]:
    """
    텍스트를 문단 단위로 분리
    
    Args:
        text: 원본 텍스트
    
    Returns:
        문단 리스트
    """
    if not text:
        return []
    
    # 문단 분리 (\n\n 또는 \n\n\n)
    paragraphs = re.split(r'\n\s*\n+', text)
    
    # 빈 문단 제거 및 정리
    paragraphs = [p.strip() for p in paragraphs if p.strip()]
    
    return paragraphs


def split_into_sentences(text: str) -> List[str]:
    """
    텍스트를 문장 단위로 분리
    
    kss 라이브러리를 우선 사용
    
    Args:
        text: 원본 텍스트
    
    Returns:
        문장 리스트
    """
    if not text:
        return []
    
    # kss 사용
    if HAS_KSS:
        try:
            return kss.split_sentences(text)
        except Exception as e:
            logger.warning(f"kss 문장 분할 실패, 기본 분할 사용: {e}")
            return _split_sentences_fallback(text)
    
    # Fallback
    return _split_sentences_fallback(text)

