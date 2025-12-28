"""
감성 분석 (KR-FinBERT-SC 기반)

KR-FinBERT-SC 모델을 사용한 금융 뉴스 감성 분석
snunlp/KR-FinBert-SC: 한국어 금융 텍스트 감성 분석에 특화된 모델
"""
import logging
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
from typing import Optional
import time

logger = logging.getLogger(__name__)

# 전역 변수로 모델 로드 (최초 1회만)
_tokenizer: Optional[AutoTokenizer] = None
_model: Optional[AutoModelForSequenceClassification] = None
_device = "cuda" if torch.cuda.is_available() else "cpu"


def load_finbert_model():
    """KR-FinBERT-SC 모델 로드 (최초 1회만)"""
    global _tokenizer, _model
    if _tokenizer is None:
        logger.info("KR-FinBERT-SC 모델 로드 중...")
        start_time = time.time()
        try:
            model_name = "snunlp/KR-FinBert-SC"
            
            _tokenizer = AutoTokenizer.from_pretrained(model_name)
            _model = AutoModelForSequenceClassification.from_pretrained(model_name)
            _model.to(_device)
            _model.eval()
            
            load_time = time.time() - start_time
            logger.info(f"KR-FinBERT-SC 모델 로드 완료 (device: {_device}, 시간: {load_time:.2f}초)")
        except Exception as e:
            logger.error(f"KR-FinBERT-SC 모델 로드 실패: {e}")
            raise
    return _tokenizer, _model


def analyze_sentiment(text: str) -> str:
    """
    KR-FinBERT-SC 기반 감성 분석
    
    한국어 금융 뉴스에 특화된 감성 분석 모델을 사용합니다.
    
    Args:
        text: 원본 텍스트
    
    Returns:
        감성 (positive/negative/neutral)
    """
    try:
        if not text or len(text.strip()) < 10:
            return "neutral"
        
        tokenizer, model = load_finbert_model()
        start_time = time.time()
        
        # 텍스트 토크나이징
        inputs = tokenizer(
            text,
            return_tensors="pt",
            max_length=512,
            truncation=True,
            padding=True
        ).to(_device)
        
        # 감성 예측
        with torch.no_grad():
            outputs = model(**inputs)
            predictions = torch.nn.functional.softmax(outputs.logits, dim=-1)
            predicted_class = torch.argmax(predictions, dim=-1).item()
        
        # 라벨 매핑 (0: negative, 1: neutral, 2: positive)
        label_map = {0: "negative", 1: "neutral", 2: "positive"}
        sentiment = label_map.get(predicted_class, "neutral")
        
        inference_time = time.time() - start_time
        logger.info(f"감성 분석 완료: {sentiment} (시간: {inference_time:.3f}초, 점수: {predictions[0].tolist()})")
        
        return sentiment
        
    except Exception as e:
        logger.error(f"감성 분석 실패: {e}", exc_info=True)
        # 실패 시 neutral 반환
        return "neutral"

