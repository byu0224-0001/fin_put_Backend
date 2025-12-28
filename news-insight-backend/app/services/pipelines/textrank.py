from summa.summarizer import summarize
from app.utils.sentence_split import split_sentences
from typing import List
import logging
import time

logger = logging.getLogger(__name__)


def get_kr_sbert_model():
    """KR-SBERT 모델 가져오기 (keywords.py의 모델 재사용)"""
    try:
        # keywords.py의 KR-SBERT 모델 재사용 (메모리 절약)
        from app.services.pipelines.keywords import load_kr_sbert_model
        import torch
        
        kr_sbert_model = load_kr_sbert_model()
        device = "cuda" if torch.cuda.is_available() else "cpu"
        
        return kr_sbert_model, device
    except Exception as e:
        logger.error(f"KR-SBERT 모델 가져오기 실패: {e}")
        raise


def kr_sbert_only_extract(text: str, sentence_count: int = 5) -> List[str]:
    """
    KR-SBERT만 사용하여 핵심 문장 추출 (짧은 기사용)
    
    전체 문장에 대해 KR-SBERT로 유사도 계산하여 상위 문장 선택
    짧은 기사(10개 문장 이하)에 최적화
    
    Args:
        text: 원본 텍스트
        sentence_count: 추출할 문장 수 (기본값: 5)
    
    Returns:
        핵심 문장 리스트 (원문 순서)
    """
    try:
        start_time = time.time()
        logger.info("KR-SBERT만 사용하여 문장 추출 시작...")
        
        all_sentences = split_sentences(text)
        total_sentences = len(all_sentences)
        
        if total_sentences == 0:
            logger.warning("문장이 없습니다")
            return []
        
        if total_sentences <= sentence_count:
            # 문장이 이미 충분히 적으면 그대로 반환
            logger.info(f"문장 수가 충분하지 않아 전체 반환: {total_sentences}개")
            return all_sentences[:sentence_count]
        
        # KR-SBERT 모델 가져오기
        kr_sbert_model, device = get_kr_sbert_model()
        
        # 전체 텍스트와 각 문장의 임베딩 계산
        texts_to_encode = [text] + all_sentences
        embeddings = kr_sbert_model.encode(
            texts_to_encode,
            convert_to_tensor=True,
            device=device,
            show_progress_bar=False
        )
        
        # 전체 텍스트 임베딩 (첫 번째)
        text_embedding = embeddings[0:1]
        
        # 각 문장 임베딩
        sentence_embeddings = embeddings[1:]
        
        # 코사인 유사도 계산
        import torch.nn.functional as F
        similarities = F.cosine_similarity(text_embedding, sentence_embeddings).cpu().numpy()
        
        # 유사도 기준으로 인덱스 정렬 (내림차순)
        ranked_indices = sorted(
            range(len(all_sentences)),
            key=lambda i: similarities[i],
            reverse=True
        )
        
        # 상위 문장 선택 (원문 순서 유지)
        top_indices = ranked_indices[:sentence_count]
        top_indices_sorted = sorted(top_indices)  # 원문 순서 보존
        result = [all_sentences[i] for i in top_indices_sorted]
        
        extract_time = time.time() - start_time
        logger.info(f"KR-SBERT 문장 추출 완료: {len(result)}개 (시간: {extract_time:.3f}초)")
        logger.info(f"최고 유사도: {similarities[ranked_indices[0]]:.4f}, 최저 유사도: {similarities[ranked_indices[sentence_count-1]]:.4f}")
        
        return result
        
    except Exception as e:
        logger.error(f"KR-SBERT 문장 추출 실패: {e}", exc_info=True)
        # Fallback: 첫 문장만 반환
        try:
            all_sentences = split_sentences(text)
            return all_sentences[:sentence_count]
        except:
            return []


def textrank_kr_sbert_extract(text: str, sentence_count: int = 5) -> List[str]:
    """
    TextRank + KR-SBERT re-ranking으로 핵심 문장 추출 (긴 기사용)
    
    파이프라인:
    1. TextRank로 후보 문장 추출 (실제 문장 수의 2배)
    2. KR-SBERT로 전체 텍스트와 각 문장의 유사도 계산
    3. 유사도 기준으로 re-ranking하여 상위 문장 선택
    4. 원문 순서 유지하여 반환
    
    TextRank는 원본 문장에서만 뽑기 때문에 사실(Factual) 100% 보장합니다.
    KR-SBERT re-ranking으로 의미적으로 더 중요한 문장을 선택합니다.
    핵심 문장 3~5개만 뽑아 KoBART 입력 제한 → 환각 방지
    
    Args:
        text: 원본 텍스트
        sentence_count: 추출할 문장 수 (기본값: 5)
    
    Returns:
        핵심 문장 리스트 (원문 순서)
    """
    try:
        # 1단계: TextRank로 후보 문장 추출
        textrank_start = time.time()
        all_sentences = split_sentences(text)
        total_sentences = len(all_sentences)
        
        if total_sentences == 0:
            logger.warning("문장이 없습니다")
            return []
        
        # TextRank로 후보 문장 추출 (실제 필요한 수의 2배 추출)
        candidate_ratio = min((sentence_count * 2) / total_sentences, 0.5)
        summary = summarize(text, ratio=candidate_ratio)
        
        if not summary or len(summary.strip()) < 50:
            logger.warning("TextRank 요약 실패, 첫 문장만 반환")
            return all_sentences[:sentence_count]
        
        # TextRank 문장을 원문에서 찾아 매칭
        summary_sentences = split_sentences(summary)
        candidate_sentences = []
        candidate_indices = []
        
        for summary_sentence in summary_sentences:
            for idx, orig_sentence in enumerate(all_sentences):
                # 간단한 유사도 체크
                if (summary_sentence in orig_sentence or 
                    orig_sentence in summary_sentence or
                    len(set(summary_sentence.split()) & set(orig_sentence.split())) > 3):
                    if idx not in candidate_indices:
                        candidate_sentences.append(orig_sentence)
                        candidate_indices.append(idx)
                        break
        
        textrank_time = time.time() - textrank_start
        logger.info(f"TextRank 후보 문장 추출 완료: {len(candidate_sentences)}개 (시간: {textrank_time:.3f}초)")
        
        if len(candidate_sentences) <= sentence_count:
            # 후보가 충분하지 않으면 원문 순서 유지하여 반환
            result = candidate_sentences[:sentence_count]
            logger.info(f"후보 부족, TextRank 결과 그대로 사용: {len(result)}개")
            return result
        
        # 2단계: KR-SBERT로 의미 기반 re-ranking
        rerank_start = time.time()
        logger.info("KR-SBERT re-ranking 시작...")
        
        kr_sbert_model, device = get_kr_sbert_model()
        
        # 전체 텍스트와 각 후보 문장의 임베딩 계산
        texts_to_encode = [text] + candidate_sentences
        embeddings = kr_sbert_model.encode(
            texts_to_encode,
            convert_to_tensor=True,
            device=device,
            show_progress_bar=False
        )
        
        # 전체 텍스트 임베딩 (첫 번째)
        text_embedding = embeddings[0:1]
        
        # 각 후보 문장 임베딩
        sentence_embeddings = embeddings[1:]
        
        # 코사인 유사도 계산
        import torch.nn.functional as F
        similarities = F.cosine_similarity(text_embedding, sentence_embeddings).cpu().numpy()
        
        # 유사도 기준으로 인덱스 정렬 (내림차순)
        ranked_indices = sorted(
            range(len(candidate_sentences)),
            key=lambda i: similarities[i],
            reverse=True
        )
        
        # 상위 문장 선택
        ranked_sentences = [candidate_sentences[i] for i in ranked_indices[:sentence_count]]
        
        # 3단계: 원문 순서 유지 (re-ranking 후에도 원문 순서 보존)
        original_indices = [candidate_indices[i] for i in ranked_indices[:sentence_count]]
        final_sentences = sorted(
            zip(original_indices, ranked_sentences),
            key=lambda x: x[0]
        )
        result = [sentence for _, sentence in final_sentences]
        
        rerank_time = time.time() - rerank_start
        logger.info(f"KR-SBERT re-ranking 완료: {len(result)}개 (시간: {rerank_time:.3f}초)")
        logger.info(f"최고 유사도: {similarities[ranked_indices[0]]:.4f}, 최저 유사도: {similarities[ranked_indices[-1]]:.4f}")
        
        return result
        
    except Exception as e:
        logger.error(f"TextRank + KR-SBERT re-ranking 실패: {e}", exc_info=True)
        # Fallback: TextRank만 사용
        try:
            all_sentences = split_sentences(text)
            return all_sentences[:sentence_count]
        except:
            return []


def textrank_extract(text: str, sentence_count: int = 5) -> List[str]:
    """
    하이브리드 방식으로 핵심 문장 추출
    
    문장 수에 따라 자동으로 최적 방식 선택:
    - 짧은 기사 (10개 문장 이하): KR-SBERT만 사용 (빠르고 정확)
    - 긴 기사 (10개 문장 초과): TextRank + KR-SBERT re-ranking (효율적)
    
    Args:
        text: 원본 텍스트
        sentence_count: 추출할 문장 수 (기본값: 5)
    
    Returns:
        핵심 문장 리스트 (원문 순서)
    """
    try:
        if not text or len(text.strip()) < 100:
            logger.warning("텍스트가 너무 짧습니다")
            return []
        
        # 문장 분리 및 개수 확인
        all_sentences = split_sentences(text)
        total_sentences = len(all_sentences)
        
        if total_sentences == 0:
            logger.warning("문장이 없습니다")
            return []
        
        # 문장 수에 따라 방식 선택 (임계값: 10개)
        SENTENCE_THRESHOLD = 10
        
        if total_sentences <= SENTENCE_THRESHOLD:
            # 짧은 기사: KR-SBERT만 사용
            logger.info(f"짧은 기사 감지 ({total_sentences}개 문장) → KR-SBERT만 사용")
            return kr_sbert_only_extract(text, sentence_count)
        else:
            # 긴 기사: TextRank + KR-SBERT re-ranking
            logger.info(f"긴 기사 감지 ({total_sentences}개 문장) → TextRank + KR-SBERT re-ranking 사용")
            return textrank_kr_sbert_extract(text, sentence_count)
        
    except Exception as e:
        logger.error(f"하이브리드 문장 추출 실패: {e}", exc_info=True)
        # Fallback: 첫 문장만 반환
        try:
            all_sentences = split_sentences(text)
            return all_sentences[:sentence_count]
        except:
            return []
