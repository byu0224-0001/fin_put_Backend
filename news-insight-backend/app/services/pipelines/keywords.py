from keybert import KeyBERT
from sentence_transformers import SentenceTransformer
from typing import List, Set
import torch
import logging

logger = logging.getLogger(__name__)

# 전역 변수로 모델 로드 (최초 1회만)
_kiwi = None
_kr_sbert_model = None
_keybert_model = None
_device = "cuda" if torch.cuda.is_available() else "cpu"


def load_kiwi():
    """Kiwi 형태소 분석기 로드 (최초 1회만)"""
    global _kiwi
    if _kiwi is None:
        logger.info("Kiwi 형태소 분석기 로드 중...")
        try:
            from kiwipiepy import Kiwi
            _kiwi = Kiwi()
            logger.info("Kiwi 형태소 분석기 로드 완료")
        except Exception as e:
            logger.error(f"Kiwi 형태소 분석기 로드 실패: {e}")
            raise
    return _kiwi


def load_kr_sbert_model():
    """KR-SBERT 모델 로드 (최초 1회만)"""
    global _kr_sbert_model
    if _kr_sbert_model is None:
        logger.info("KR-SBERT 모델 로드 중...")
        try:
            # jhgan/ko-sroberta-multitask: 한국어 문장 임베딩 모델
            model_name = "jhgan/ko-sroberta-multitask"
            _kr_sbert_model = SentenceTransformer(model_name)
            _kr_sbert_model.to(_device)
            logger.info(f"KR-SBERT 모델 로드 완료 (device: {_device})")
        except Exception as e:
            logger.error(f"KR-SBERT 모델 로드 실패: {e}")
            raise
    return _kr_sbert_model


def load_keybert_model():
    """KeyBERT 모델 로드 (KR-SBERT 모델 사용)"""
    global _keybert_model
    if _keybert_model is None:
        logger.info("KeyBERT 모델 로드 중...")
        try:
            # KR-SBERT 모델을 KeyBERT에 전달
            kr_sbert = load_kr_sbert_model()
            _keybert_model = KeyBERT(model=kr_sbert)
            logger.info("KeyBERT 모델 로드 완료 (KR-SBERT 백엔드 사용)")
        except Exception as e:
            logger.error(f"KeyBERT 모델 로드 실패: {e}")
            raise
    return _keybert_model


def extract_compound_nouns_with_kiwi(text: str) -> Set[str]:
    """
    Kiwi로 복합명사 추출 (개선 버전)
    
    Kiwi의 extract_words를 사용하여 복합명사로 인식한 단위만 추출
    단순 명사 결합 방식을 제거하여 "분기별도기준당기순이익" 같은 오류 방지
    
    Args:
        text: 원본 텍스트
    
    Returns:
        복합명사 집합 (예: {"당기순이익", "3분기", "현대해상"})
    """
    try:
        kiwi = load_kiwi()
        from kiwipiepy.utils import Stopwords
        
        # 방법 1: extract_words 사용 (Kiwi가 복합명사로 인식한 단위만 추출)
        # 이 방법이 더 정확함
        try:
            results = kiwi.extract_words(
                text,
                min_len=2,
                max_len=5,
                stopwords=Stopwords()
            )
            
            compound_nouns = set()
            for r in results:
                # 복합명사만 추출 (고유명사, 일반명사)
                if r.tag in ['NNG', 'NNP', 'NNB']:
                    # 길이가 너무 긴 경우 제외 (형태소 분석 실패 가능성)
                    if 2 <= len(r.form) <= 15:
                        compound_nouns.add(r.form)
            
            logger.info(f"Kiwi 복합명사 추출 (extract_words): {len(compound_nouns)}개")
            return compound_nouns
            
        except Exception as e:
            logger.warning(f"Kiwi extract_words 실패, 형태소 분석 방식으로 대체: {e}")
            # Fallback: 형태소 분석 방식 (띄어쓰기 유지)
            
            morphemes = kiwi.analyze(text)
            compound_nouns = set()
            
            for morph_list in morphemes:
                tokens = morph_list[0]
                current_noun = []
                
                for i, morph in enumerate(tokens):
                    if morph.tag.startswith('N'):  # 명사
                        current_noun.append(morph.form)
                    else:
                        # 명사가 끝나면 저장 (띄어쓰기 유지)
                        if len(current_noun) == 1:
                            # 단일 명사
                            if 2 <= len(current_noun[0]) <= 15:
                                compound_nouns.add(current_noun[0])
                        elif len(current_noun) > 1:
                            # 복합명사 (띄어쓰기 유지)
                            noun_phrase = ' '.join(current_noun)
                            if len(noun_phrase) <= 20:
                                compound_nouns.add(noun_phrase)
                        current_noun = []
                
                # 문장 끝 처리
                if len(current_noun) == 1:
                    if 2 <= len(current_noun[0]) <= 15:
                        compound_nouns.add(current_noun[0])
                elif len(current_noun) > 1:
                    noun_phrase = ' '.join(current_noun)
                    if len(noun_phrase) <= 20:
                        compound_nouns.add(noun_phrase)
            
            logger.info(f"Kiwi 복합명사 추출 (형태소 분석): {len(compound_nouns)}개")
            return compound_nouns
        
    except Exception as e:
        logger.error(f"Kiwi 복합명사 추출 실패: {e}", exc_info=True)
        return set()


def extract_keywords(text: str, top_n: int = 10) -> List[str]:
    """
    Kiwi 기반 하이브리드 키워드 추출 (최적화 버전)
    
    파이프라인:
    0. 텍스트 정제 (노이즈 제거)
    1. Kiwi 토큰화 + 가벼운 후보 생성 (명사 + phrase) - SBERT 없음
    2. 후보 병합 + 중복 제거
    3. KR-SBERT로 의미 기반 랭킹 (단 1회만!)
    4. 메타데이터 필터링
    5. 최종 top-n 반환
    
    Args:
        text: 원본 텍스트
        top_n: 추출할 키워드 수 (기본값: 10)
    
    Returns:
        키워드 리스트 (의미 기반 우선순위 정렬 완료)
    """
    try:
        if not text or len(text.strip()) < 50:
            logger.warning("텍스트가 너무 짧습니다")
            return []
        
        # 0단계: 텍스트 정제 (노이즈 제거)
        from app.utils.text_cleaner import clean_text_for_keywords, filter_keywords_by_metadata
        logger.info("0단계: 텍스트 정제 시작 (노이즈 제거)")
        cleaned_text = clean_text_for_keywords(text)
        
        if len(cleaned_text.strip()) < 50:
            logger.warning("정제 후 텍스트가 너무 짧습니다")
            return []
        
        logger.info(f"텍스트 정제 완료: {len(text)}자 → {len(cleaned_text)}자")
        
        # 1단계: Kiwi 토큰화 및 가벼운 후보 생성 (SBERT 없음)
        logger.info("1단계: Kiwi 토큰화 및 후보 생성 시작")
        kiwi = load_kiwi()
        
        # Kiwi로 형태소 분석
        morphemes_list = kiwi.analyze(cleaned_text)
        
        noun_candidates = set()  # 단일/복합 명사
        phrase_candidates = set()  # n-gram phrase
        
        # 모든 문장에 대해 처리
        for morphemes in morphemes_list:
            tokens = morphemes[0]  # 형태소 분석 결과
            
            # 명사 후보 추출 및 phrase 생성
            for i, token in enumerate(tokens):
                # 1a. 단일/복합 명사 추출
                if token.tag in ['NNG', 'NNP', 'NNB'] and 2 <= len(token.form) <= 15:
                    noun_candidates.add(token.form)
                
                # 1b. phrase 후보 생성 (2-gram, 3-gram)
                if token.tag in ['NNG', 'NNP', 'NNB', 'VA']:  # 명사 또는 형용사로 시작
                    # 2-gram phrase
                    if i + 1 < len(tokens):
                        next_token = tokens[i + 1]
                        if next_token.tag in ['NNG', 'NNP', 'NNB', 'VA']:
                            phrase_2 = f"{token.form} {next_token.form}"
                            if 4 <= len(phrase_2) <= 20:  # 길이 필터링
                                phrase_candidates.add(phrase_2)
                    
                    # 3-gram phrase
                    if i + 2 < len(tokens):
                        next_token_1 = tokens[i + 1]
                        next_token_2 = tokens[i + 2]
                        if (next_token_1.tag in ['NNG', 'NNP', 'NNB', 'VA'] and
                            next_token_2.tag in ['NNG', 'NNP', 'NNB', 'VA']):
                            phrase_3 = f"{token.form} {next_token_1.form} {next_token_2.form}"
                            if 6 <= len(phrase_3) <= 25:  # 길이 필터링
                                phrase_candidates.add(phrase_3)
        
        logger.info(f"Kiwi 명사 후보: {len(noun_candidates)}개, phrase 후보: {len(phrase_candidates)}개")
        
        # 2단계: 후보 병합 + 중복 제거
        logger.info("2단계: 후보 병합 및 중복 제거")
        all_candidates = list(noun_candidates | phrase_candidates)
        logger.info(f"병합된 후보: {len(all_candidates)}개")
        
        if not all_candidates:
            logger.warning("후보가 없습니다")
            return []
        
        # 3단계: KR-SBERT로 의미 기반 랭킹 (단 1회만!)
        logger.info("3단계: KR-SBERT 의미 기반 랭킹 시작 (단 1회)")
        kr_sbert_model = load_kr_sbert_model()
        
        # 전체 텍스트와 각 후보의 임베딩 계산 (단 1회만!)
        texts_to_encode = [cleaned_text] + all_candidates
        embeddings = kr_sbert_model.encode(
            texts_to_encode,
            convert_to_tensor=True,
            device=_device,
            show_progress_bar=False
        )
        
        # 전체 텍스트 임베딩 (첫 번째)
        text_embedding = embeddings[0:1]
        
        # 각 후보 임베딩
        candidate_embeddings = embeddings[1:]
        
        # 코사인 유사도 계산
        import torch.nn.functional as F
        similarities = F.cosine_similarity(text_embedding, candidate_embeddings).cpu().numpy()
        
        # 유사도 기준으로 정렬 (내림차순)
        ranked_indices = sorted(
            range(len(all_candidates)),
            key=lambda i: similarities[i],
            reverse=True
        )
        
        # 상위 키워드 선택
        ranked_keywords = [all_candidates[i] for i in ranked_indices]
        logger.info(f"KR-SBERT 랭킹 완료: 최고 유사도 {similarities[ranked_indices[0]]:.4f}")
        
        # 4단계: 메타데이터 필터링
        logger.info("4단계: 메타데이터 필터링")
        filtered_keywords = filter_keywords_by_metadata(ranked_keywords)
        
        # 5단계: 최종 top-n 반환
        final_keywords = filtered_keywords[:top_n]
        
        logger.info(f"키워드 추출 완료: {len(final_keywords)}개 - {final_keywords[:5] if final_keywords else '없음'}")
        return final_keywords
        
    except Exception as e:
        logger.error(f"키워드 추출 실패: {e}", exc_info=True)
        # 실패 시 빈 배열 반환
        return []
