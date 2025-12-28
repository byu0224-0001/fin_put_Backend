from typing import Dict, List, Optional
from app.services.pipelines.keywords import extract_keywords
from app.services.pipelines.textrank import textrank_extract
from app.services.pipelines.kobart import summarize_kobart
from app.services.pipelines.entities import extract_entities
from app.services.pipelines.sentiment import analyze_sentiment
from app.utils.text_cleaner import clean_text
from app.utils.sentence_split import split_sentences
import logging
import time

logger = logging.getLogger(__name__)

def summarize_text(text: str) -> Optional[Dict]:
    """
    뉴스 기사 요약 (KeyBERT + TextRank + KoBART 하이브리드)
    
    세 가지 조합이 서로의 단점을 보완하는 '안전한 하이브리드 구조'입니다.
    
    파이프라인:
    1. KeyBERT: 의미 기반 키워드 추출
    2. TextRank + KR-SBERT re-ranking: 사실 보존 핵심 문장 추출 (의미 기반 정렬)
    3. KoBART: 사실 보존형 자연스러운 요약 생성
    4. 엔티티 추출: 기업명 데이터 소스 활용 (서버 시작 시 메모리 로딩)
    5. 감성 분석: KR-FinBERT-SC 기반 금융 뉴스 감성 분석
    
    Args:
        text: 기사 본문 텍스트
    
    Returns:
        요약 결과 딕셔너리
        {
            "summary": "요약문",
            "keywords": ["키워드1", "키워드2", ...],
            "entities": {"ORG": [...], "PERSON": [...], "LOCATION": [...]},
            "bullet_points": ["핵심1", "핵심2", ...],
            "sentiment": "positive/negative/neutral"
        }
    """
    try:
        pipeline_start_time = time.time()
        
        if not text or len(text.strip()) < 100:
            logger.warning("텍스트가 너무 짧습니다")
            return None
        
        # 텍스트 정리
        clean_start = time.time()
        cleaned_text = clean_text(text)
        logger.info(f"텍스트 정리 완료: {time.time() - clean_start:.3f}초")
        
        # 1. KeyBERT로 의미 키워드 추출
        keywords_start = time.time()
        logger.info("1단계: KeyBERT 키워드 추출 시작")
        keywords = extract_keywords(cleaned_text, top_n=10)
        keywords_time = time.time() - keywords_start
        
        # 키워드가 없으면 경고 로그
        if not keywords or len(keywords) == 0:
            logger.warning(f"키워드 추출 결과가 비어있습니다. 텍스트 길이: {len(cleaned_text)}자 (시간: {keywords_time:.3f}초)")
        else:
            logger.info(f"키워드 추출 완료: {len(keywords)}개 - {keywords[:5]} (시간: {keywords_time:.3f}초)")
        
        # 2. 하이브리드 방식으로 핵심 문장 추출 (문장 수에 따라 자동 선택)
        # - 짧은 기사 (10개 이하): KR-SBERT만 사용
        # - 긴 기사 (10개 초과): TextRank + KR-SBERT re-ranking
        textrank_start = time.time()
        logger.info("2단계: 핵심 문장 추출 시작 (하이브리드 방식)")
        key_sentences = textrank_extract(cleaned_text, sentence_count=5)
        textrank_time = time.time() - textrank_start
        
        if not key_sentences:
            logger.warning(f"핵심 문장 추출 실패 (시간: {textrank_time:.3f}초)")
            return None
        logger.info(f"핵심 문장 추출 완료: {len(key_sentences)}개 (시간: {textrank_time:.3f}초)")
        
        # 3. KoBART로 생성 요약 (사실 보존형)
        kobart_start = time.time()
        logger.info("3단계: KoBART 요약 시작")
        summary = summarize_kobart(key_sentences)
        kobart_time = time.time() - kobart_start
        
        if not summary:
            logger.warning(f"KoBART 요약 실패, TextRank 문장 사용 (시간: {kobart_time:.3f}초)")
            summary = " ".join(key_sentences)
        else:
            logger.info(f"KoBART 요약 완료: {len(summary)}자 (시간: {kobart_time:.3f}초)")
        
        # 4. 엔티티 추출 (기업명 데이터 소스 활용)
        entities_start = time.time()
        logger.info("4단계: 엔티티 추출 시작")
        entities = extract_entities(cleaned_text)
        entities_time = time.time() - entities_start
        logger.info(f"엔티티 추출 완료: ORG={len(entities.get('ORG', []))}, PERSON={len(entities.get('PERSON', []))}, LOCATION={len(entities.get('LOCATION', []))} (시간: {entities_time:.3f}초)")
        
        # 5. 감성 분석 (KR-FinBERT-SC)
        sentiment_start = time.time()
        logger.info("5단계: 감성 분석 시작 (KR-FinBERT-SC)")
        sentiment = analyze_sentiment(cleaned_text)
        sentiment_time = time.time() - sentiment_start
        
        # 6. Bullet points (요약문을 문장 단위로 분리)
        bullet_start = time.time()
        bullet_points = [s.strip() for s in split_sentences(summary) if s.strip()][:5]
        bullet_time = time.time() - bullet_start
        
        result = {
            "summary": summary,
            "keywords": keywords if keywords else [],  # 빈 배열 보장
            "entities": entities,
            "bullet_points": bullet_points,
            "sentiment": sentiment
        }
        
        total_time = time.time() - pipeline_start_time
        logger.info(f"하이브리드 요약 완료 - 키워드: {len(result['keywords'])}개, 요약: {len(result['summary'])}자")
        logger.info(f"총 소요 시간: {total_time:.3f}초 (키워드: {keywords_time:.3f}초, 문장추출: {textrank_time:.3f}초, KoBART: {kobart_time:.3f}초, 엔티티: {entities_time:.3f}초, 감성: {sentiment_time:.3f}초)")
        
        return result
        
    except Exception as e:
        logger.error(f"하이브리드 요약 실패: {e}", exc_info=True)
        return None
