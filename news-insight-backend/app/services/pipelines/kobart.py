from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
from typing import List
import torch
import logging

logger = logging.getLogger(__name__)

# 전역 변수로 모델 로드 (최초 1회만)
_tokenizer = None
_model = None
_device = "cuda" if torch.cuda.is_available() else "cpu"

def load_kobart_model():
    """KoBART 모델 로드 (최초 1회만)"""
    global _tokenizer, _model
    if _tokenizer is None:
        logger.info("KoBART 모델 로드 중...")
        try:
            # KoBART 요약 모델 사용
            model_name = "gogamza/kobart-summarization"
            
            _tokenizer = AutoTokenizer.from_pretrained(model_name)
            _model = AutoModelForSeq2SeqLM.from_pretrained(model_name)
            _model.to(_device)
            _model.eval()
            
            logger.info(f"KoBART 모델 로드 완료 (device: {_device})")
        except Exception as e:
            logger.error(f"KoBART 모델 로드 실패: {e}")
            raise
    return _tokenizer, _model

def summarize_kobart(sentences: List[str]) -> str:
    """KoBART 요약"""
    try:
        if not sentences:
            return ""
        
        tokenizer, model = load_kobart_model()
        input_text = "\n".join(sentences)
        
        inputs = tokenizer(
            input_text,
            return_tensors="pt",
            max_length=512,
            truncation=True,
            padding=True
        ).to(_device)
        
        with torch.no_grad():
            outputs = model.generate(
                inputs["input_ids"],
                max_length=200,
                min_length=60,
                num_beams=4,
                temperature=0.1,
                top_p=0.85,
                repetition_penalty=1.2,
                early_stopping=True,
                no_repeat_ngram_size=3
            )
        
        summary = tokenizer.decode(outputs[0], skip_special_tokens=True).strip()
        logger.info(f"KoBART 요약 완료: {len(summary)}자")
        return summary
        
    except Exception as e:
        logger.error(f"KoBART 요약 실패: {e}")
        return " ".join(sentences)

