"""
직접 임베딩 모델 - SentenceTransformer 완전 우회
Meta Tensor 오류를 완전히 회피하기 위해 AutoModel + mean pooling 직접 구현
"""
import logging
import os
import numpy as np
from typing import List, Union, Optional
import torch
from transformers import AutoModel, AutoTokenizer

# ⭐ 모듈 로드 시점에 accelerate 비활성화 (가장 빠른 시점에 설정)
os.environ['TRANSFORMERS_NO_ACCELERATE'] = '1'
os.environ['ACCELERATE_USE_CPU'] = '1'

logger = logging.getLogger(__name__)

class DirectEmbeddingModel:
    """
    SentenceTransformer를 완전히 우회하는 직접 임베딩 모델
    AutoModel + mean pooling으로 임베딩 생성
    """
    
    def __init__(self, model_name: str = "upskyy/kf-deberta-multitask", device: str = None):
        """
        Args:
            model_name: HuggingFace 모델 이름
            device: 사용할 디바이스 ('cuda', 'cpu', None=자동)
        """
        self.model_name = model_name
        self._model = None
        self._tokenizer = None
        self._device = None
        
        # 디바이스 결정
        if device is None:
            if torch.cuda.is_available():
                device = "cuda"
            else:
                device = "cpu"
        
        self._load_model(device)
    
    def _load_model(self, target_device: str):
        """모델 로드 (meta tensor 문제 완전 회피)"""
        import time
        load_start = time.time()
        logger.info(f"🔄 [KF-DeBERTa] 모델 로딩 시작: {self.model_name}")
        
        try:
            # 1. Tokenizer 로드
            tokenizer_start = time.time()
            logger.info(f"  📥 [KF-DeBERTa] Tokenizer 로딩 중...")
            self._tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            tokenizer_time = time.time() - tokenizer_start
            logger.info(f"  ✅ [KF-DeBERTa] Tokenizer 로딩 완료 ({tokenizer_time:.2f}초)")
            
            # 2. 모델 로드 (CPU 로드 후 GPU 이동)
            model_start = time.time()
            logger.info(f"  📥 [KF-DeBERTa] 모델 로딩 중...")
            
            # CPU에 직접 로드 (meta tensor 문제 완전 회피)
            # torch_dtype을 지정하지 않으면 기본 dtype으로 실제 메모리에 로드됨
            self._model = AutoModel.from_pretrained(
                self.model_name,
                device_map=None,  # ⭐ device_map 사용 안 함
                low_cpu_mem_usage=False,
                torch_dtype=None  # ⭐ 명시적으로 None 지정
            )
            
            # ⭐ 모델을 CPU로 명시적으로 이동
            self._model = self._model.to('cpu')
            
            # ⭐ 더미 forward pass로 모든 파라미터를 실제 메모리에 로드
            # 이렇게 하면 meta tensor가 실제 tensor로 변환됨
            try:
                dummy_input = self._tokenizer("test", return_tensors="pt", padding=True, truncation=True)
                dummy_input = {k: v.to('cpu') for k, v in dummy_input.items()}
                with torch.no_grad():
                    _ = self._model(**dummy_input)
                logger.info("  ✅ [KF-DeBERTa] 더미 forward pass로 모든 파라미터를 실제 메모리에 로드 완료")
            except Exception as e:
                logger.warning(f"  ⚠️ [KF-DeBERTa] 더미 forward pass 실패 (무시 가능): {e}")
            
            self._device = "cpu"
            
            # GPU로 이동 (가능한 경우)
            if target_device == "cuda" and torch.cuda.is_available():
                try:
                    torch.cuda.empty_cache()
                    self._model = self._model.to("cuda")
                    self._device = "cuda"
                    logger.info(f"  ✅ [KF-DeBERTa] 모델 GPU 이동 완료")
                except Exception as e:
                    logger.warning(f"  ⚠️ [KF-DeBERTa] GPU 이동 실패, CPU 사용: {e}")
                    self._device = "cpu"
            
            model_time = time.time() - model_start
            logger.info(f"  ✅ [KF-DeBERTa] 모델 로딩 완료 ({model_time:.2f}초, device: {self._device})")
            
            # 3. 평가 모드로 설정
            self._model.eval()
                
        except Exception as e:
            logger.error(f"❌ [KF-DeBERTa] 모델 로딩 실패: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # 모델이 None이 되지 않도록 초기화
            self._model = None
            self._device = None
            raise
        
        total_time = time.time() - load_start
        logger.info(f"✅ [KF-DeBERTa] 전체 로딩 완료 (총 소요 시간: {total_time:.2f}초)")
    
    @property
    def device(self) -> str:
        """현재 디바이스 반환"""
        return self._device
    
    def _mean_pooling(self, model_output, attention_mask):
        """Mean pooling - 어텐션 마스크를 고려한 평균 풀링"""
        token_embeddings = model_output[0]  # 첫 번째 요소가 토큰 임베딩
        input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
        return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)
    
    def encode(
        self, 
        sentences: Union[str, List[str]], 
        batch_size: int = 32,
        convert_to_numpy: bool = True,
        normalize_embeddings: bool = True,
        show_progress_bar: bool = False
    ) -> Union[np.ndarray, torch.Tensor]:
        """
        문장을 임베딩으로 변환
        
        Args:
            sentences: 단일 문장 또는 문장 리스트
            batch_size: 배치 크기
            convert_to_numpy: numpy 배열로 변환 여부
            normalize_embeddings: 임베딩 정규화 여부
            show_progress_bar: 진행 표시줄 표시 여부 (현재 미구현)
        
        Returns:
            임베딩 벡터 (numpy 배열 또는 torch 텐서)
        """
        # 단일 문장 처리
        if isinstance(sentences, str):
            sentences = [sentences]
        
        all_embeddings = []
        
        with torch.no_grad():
            for i in range(0, len(sentences), batch_size):
                batch = sentences[i:i + batch_size]
                
                # 토크나이징
                encoded_input = self._tokenizer(
                    batch,
                    padding=True,
                    truncation=True,
                    max_length=512,
                    return_tensors='pt'
                )
                
                # 디바이스로 이동
                encoded_input = {k: v.to(self._device) for k, v in encoded_input.items()}
                
                # 모델 추론
                model_output = self._model(**encoded_input)
                
                # Mean pooling
                embeddings = self._mean_pooling(model_output, encoded_input['attention_mask'])
                
                # 정규화
                if normalize_embeddings:
                    embeddings = torch.nn.functional.normalize(embeddings, p=2, dim=1)
                
                all_embeddings.append(embeddings.cpu())
        
        # 결과 합치기
        all_embeddings = torch.cat(all_embeddings, dim=0)
        
        if convert_to_numpy:
            return all_embeddings.numpy()
        
        return all_embeddings
    
    def to(self, device: str):
        """디바이스 이동"""
        if device == "cuda" and torch.cuda.is_available():
            self._model = self._model.to("cuda")
            self._device = "cuda"
        else:
            self._model = self._model.to("cpu")
            self._device = "cpu"
        return self
    
    def get_sentence_embedding_dimension(self) -> int:
        """임베딩 차원 반환"""
        return self._model.config.hidden_size


# 전역 모델 캐시
_direct_embedding_model = None

def get_direct_embedding_model(model_name: str = "upskyy/kf-deberta-multitask", device: str = None) -> DirectEmbeddingModel:
    """
    직접 임베딩 모델 로드 (캐싱)
    
    Args:
        model_name: HuggingFace 모델 이름
        device: 사용할 디바이스 ('cuda', 'cpu', None=자동)
    
    Returns:
        DirectEmbeddingModel 인스턴스
    
    Raises:
        Exception: 모델 로드 실패 시
    """
    global _direct_embedding_model
    
    if _direct_embedding_model is None:
        _direct_embedding_model = DirectEmbeddingModel(model_name=model_name, device=device)
    
    return _direct_embedding_model
