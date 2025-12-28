"""
모델 로더 (서버 시작 시 warm-up용)

서버 시작 시 모든 AI 모델을 미리 로드하여
첫 요청 시 발생하는 지연을 방지합니다.
"""
import logging
import time

logger = logging.getLogger(__name__)


def warm_up_models():
    """
    모든 AI 모델을 미리 로드 (warm-up)
    
    서버 시작 시 한 번만 호출하여 첫 요청 시 발생하는 지연을 방지합니다.
    """
    logger.info("=" * 50)
    logger.info("AI 모델 Warm-up 시작...")
    logger.info("=" * 50)
    
    warm_up_start = time.time()
    
    try:
        # 1. Kiwi 형태소 분석기 로드
        kiwi_start = time.time()
        logger.info("[1/4] Kiwi 형태소 분석기 로드 중...")
        from app.services.pipelines.keywords import load_kiwi
        load_kiwi()
        logger.info(f"[1/4] Kiwi 로드 완료 (시간: {time.time() - kiwi_start:.2f}초)")
        
        # 2. KR-SBERT 모델 로드
        sbert_start = time.time()
        logger.info("[2/4] KR-SBERT 모델 로드 중...")
        from app.services.pipelines.keywords import load_kr_sbert_model
        load_kr_sbert_model()
        logger.info(f"[2/4] KR-SBERT 로드 완료 (시간: {time.time() - sbert_start:.2f}초)")
        
        # 3. KoBART 모델 로드
        kobart_start = time.time()
        logger.info("[3/4] KoBART 모델 로드 중...")
        from app.services.pipelines.kobart import load_kobart_model
        load_kobart_model()
        logger.info(f"[3/4] KoBART 로드 완료 (시간: {time.time() - kobart_start:.2f}초)")
        
        # 4. KR-FinBERT-SC 모델 로드
        finbert_start = time.time()
        logger.info("[4/4] KR-FinBERT-SC 모델 로드 중...")
        from app.services.pipelines.sentiment import load_finbert_model
        load_finbert_model()
        logger.info(f"[4/4] KR-FinBERT-SC 로드 완료 (시간: {time.time() - finbert_start:.2f}초)")
        
        total_time = time.time() - warm_up_start
        logger.info("=" * 50)
        logger.info(f"✅ 모든 모델 Warm-up 완료! (총 시간: {total_time:.2f}초)")
        logger.info("=" * 50)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 모델 Warm-up 실패: {e}", exc_info=True)
        logger.error("⚠️ 첫 요청 시 모델 로드로 인한 지연이 발생할 수 있습니다.")
        return False

