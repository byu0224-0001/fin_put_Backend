"""
한국어 문장 분리 (kiwipiepy 사용)

kiwipiepy는 형태소 분석 기반으로 문장 경계를 정확히 인식합니다.
금융 리포트의 복잡한 문장 구조에 최적화.

v2 변경사항:
- kiwipiepy 기반 문장 분리 (정확도 향상)
- Fallback: regex 기반 (kiwipiepy 미설치 시)
"""
import re
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

# kiwipiepy 지연 로딩
_kiwi = None


def _get_kiwi():
    """kiwipiepy 인스턴스 지연 로딩"""
    global _kiwi
    if _kiwi is None:
        try:
            from kiwipiepy import Kiwi
            _kiwi = Kiwi()
            logger.info("kiwipiepy 로드 완료")
        except ImportError:
            logger.warning("kiwipiepy not installed. pip install kiwipiepy")
            _kiwi = False
    return _kiwi if _kiwi else None


def split_sentences(text: str) -> List[str]:
    """
    한국어 문장 분리 (kiwipiepy 사용)
    
    Args:
        text: 원본 텍스트
    
    Returns:
        문장 리스트
    """
    if not text:
        return []
    
    kiwi = _get_kiwi()
    
    if kiwi:
        # kiwipiepy 사용 (정확도 높음)
        try:
            sentences = [sent.text.strip() for sent in kiwi.split_into_sents(text)]
            # 빈 문장 및 너무 짧은 문장 제거
            sentences = [s for s in sentences if s and len(s) > 5]
            return sentences
        except Exception as e:
            logger.warning(f"kiwipiepy 문장 분리 실패, fallback 사용: {e}")
    
    # Fallback: regex 기반 (기존 로직)
    sentences = re.split(r'[.!?]\s+', text)
    sentences = [s.strip() + '.' for s in sentences if s.strip()]
    
    return sentences


def split_sentences_with_positions(text: str) -> List[Dict]:
    """
    문장 분리 + 위치 정보 포함
    
    Returns:
        [{"text": "문장", "start": 0, "end": 10}, ...]
    """
    if not text:
        return []
    
    kiwi = _get_kiwi()
    
    if kiwi:
        try:
            results = []
            for sent in kiwi.split_into_sents(text):
                results.append({
                    "text": sent.text.strip(),
                    "start": sent.start,
                    "end": sent.end
                })
            return [r for r in results if r["text"] and len(r["text"]) > 5]
        except Exception as e:
            logger.warning(f"kiwipiepy 실패: {e}")
    
    # Fallback
    sentences = split_sentences(text)
    return [{"text": s, "start": -1, "end": -1} for s in sentences]

