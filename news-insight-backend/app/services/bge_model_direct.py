"""
직접 BGE-M3 모델 - FlagEmbedding 완전 우회
Meta Tensor 오류를 완전히 회피하기 위해 AutoModel + CLS pooling 직접 구현
"""
import logging
import os
import numpy as np
from typing import List, Union, Optional, Dict
import torch
from transformers import AutoModel, AutoTokenizer

# ⭐ 모듈 로드 시점에 accelerate 비활성화 (가장 빠른 시점에 설정)
os.environ['TRANSFORMERS_NO_ACCELERATE'] = '1'
os.environ['ACCELERATE_USE_CPU'] = '1'

logger = logging.getLogger(__name__)


class DirectBGEM3Model:
    """
    FlagEmbedding을 완전히 우회하는 직접 BGE-M3 모델
    AutoModel + CLS token pooling으로 임베딩 생성
    """
    
    def __init__(self, model_name: str = "BAAI/bge-m3", device: str = None, use_fp16: bool = True):
        """
        Args:
            model_name: HuggingFace 모델 이름
            device: 사용할 디바이스 ('cuda', 'cpu', None=자동)
            use_fp16: FP16 사용 여부 (GPU에서 권장)
        """
        self.model_name = model_name
        self._model = None
        self._tokenizer = None
        self._device = None
        self.use_fp16 = use_fp16
        
        # 디바이스 결정
        if device is None:
            if torch.cuda.is_available():
                device = "cuda"
            else:
                device = "cpu"
        
        self._load_model(device)
    
    def _load_model(self, target_device: str):
        """모델 로드 (meta tensor 문제 완전 회피, GPU 우선 사용)"""
        import time
        load_start = time.time()
        logger.info(f"🔄 Loading BGE-M3 model: {self.model_name} (Target: {target_device})")
        
        try:
            # 1. Tokenizer 로드
            self._tokenizer = AutoTokenizer.from_pretrained(self.model_name)
            logger.info(f"✅ BGE-M3 Tokenizer loaded: {self.model_name}")
            
            # 2. 모델 로드 (CPU 로드 후 GPU 이동)
            model_start = time.time()
            
            # CPU에 직접 로드 (meta tensor 문제 완전 회피)
            # torch_dtype을 지정하지 않으면 기본 dtype으로 실제 메모리에 로드됨
            # 하지만 여전히 meta tensor 문제가 발생할 수 있으므로, 명시적으로 실제 디바이스에 로드
            import torch.nn as nn
            
            # 모델을 로드하되, 실제 메모리에 로드되도록 강제
            self._model = AutoModel.from_pretrained(
                self.model_name,
                device_map=None,  # ⭐ device_map 사용 안 함
                low_cpu_mem_usage=False,
                torch_dtype=None  # ⭐ 명시적으로 None 지정
            )
            
            # ⭐ 더미 forward pass를 먼저 실행하여 모든 파라미터를 실제 메모리에 로드
            # 이렇게 하면 meta tensor가 실제 tensor로 변환된 후 .to('cpu') 호출 가능
            try:
                dummy_input = self._tokenizer("test", return_tensors="pt", padding=True, truncation=True)
                # 모델이 아직 meta device에 있을 수 있으므로, 입력도 meta device에 맞춤
                with torch.no_grad():
                    _ = self._model(**dummy_input)
                logger.info("✅ [BGE-M3] 더미 forward pass로 모든 파라미터를 실제 메모리에 로드 완료")
            except Exception as e:
                logger.warning(f"⚠️ [BGE-M3] 더미 forward pass 실패, 직접 CPU 이동 시도: {e}")
            
            # 이제 모델을 CPU로 이동 (더미 forward pass 후에는 meta tensor가 실제 tensor로 변환됨)
            try:
                self._model = self._model.to('cpu')
            except (NotImplementedError, RuntimeError) as e:
                logger.error(f"❌ [BGE-M3] CPU 이동 실패: {e}")
                raise
            
            self._device = "cpu"
            
            # GPU로 이동 (가능한 경우)
            if target_device == "cuda" and torch.cuda.is_available():
                try:
                    torch.cuda.empty_cache()
                    # FP16 사용 시 GPU로 이동하면서 dtype 변경
                    if self.use_fp16:
                        self._model = self._model.to("cuda").half()
                    else:
                        self._model = self._model.to("cuda")
                    self._device = "cuda"
                    dtype_str = "float16" if self.use_fp16 else "float32"
                    logger.info(f"✅ BGE-M3 모델 GPU 이동 완료 (dtype={dtype_str})")
                except Exception as e:
                    logger.warning(f"⚠️ BGE-M3 GPU 이동 실패, CPU 사용: {e}")
                    self._device = "cpu"
            
            model_time = time.time() - model_start
            logger.info(f"✅ BGE-M3 Model loaded ({model_time:.2f}초, device: {self._device})")
            
            # 3. 평가 모드로 설정
            self._model.eval()
                
        except Exception as e:
            logger.error(f"❌ Failed to load BGE-M3 model: {e}")
            raise
        
        total_time = time.time() - load_start
        logger.info(f"✅ BGE-M3 전체 로딩 완료 (총 소요 시간: {total_time:.2f}초)")
    
    @property
    def device(self) -> str:
        """현재 디바이스 반환"""
        return self._device
    
    def _cls_pooling(self, model_output, attention_mask=None):
        """CLS token pooling - BGE-M3의 기본 pooling 방식"""
        # [CLS] 토큰의 출력 사용 (첫 번째 토큰)
        return model_output.last_hidden_state[:, 0]
    
    def _mean_pooling(self, model_output, attention_mask):
        """Mean pooling (대안)"""
        token_embeddings = model_output.last_hidden_state
        input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
        return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(input_mask_expanded.sum(1), min=1e-9)
    
    def encode(
        self, 
        sentences: Union[str, List[str]], 
        batch_size: int = 12,
        max_length: int = 8192,
        convert_to_numpy: bool = True,
        normalize_embeddings: bool = True,
        show_progress_bar: bool = False,
        pooling_method: str = "cls"
    ) -> Union[np.ndarray, torch.Tensor, Dict]:
        """
        문장을 임베딩으로 변환
        
        Args:
            sentences: 단일 문장 또는 문장 리스트
            batch_size: 배치 크기
            max_length: 최대 토큰 길이 (BGE-M3는 8192까지 지원)
            convert_to_numpy: numpy 배열로 변환 여부
            normalize_embeddings: 임베딩 정규화 여부
            show_progress_bar: 진행 표시줄 표시 여부 (현재 미구현)
            pooling_method: 풀링 방법 ('cls' 또는 'mean')
        
        Returns:
            임베딩 벡터 (numpy 배열 또는 torch 텐서)
            또는 FlagEmbedding 호환 형식: {'dense_vecs': embeddings}
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
                    max_length=max_length,
                    return_tensors='pt'
                )
                
                # 디바이스로 이동
                encoded_input = {k: v.to(self._device) for k, v in encoded_input.items()}
                
                # 모델 추론
                model_output = self._model(**encoded_input)
                
                # Pooling
                if pooling_method == "cls":
                    embeddings = self._cls_pooling(model_output)
                else:
                    embeddings = self._mean_pooling(model_output, encoded_input['attention_mask'])
                
                # 정규화
                if normalize_embeddings:
                    embeddings = torch.nn.functional.normalize(embeddings, p=2, dim=1)
                
                # Meta tensor 문제 회피: clone 후 CPU로 이동
                # clone()은 실제 데이터를 복사하므로 meta tensor 문제 회피
                try:
                    embeddings_cpu = embeddings.clone().detach().cpu()
                    all_embeddings.append(embeddings_cpu)
                except (NotImplementedError, RuntimeError) as e:
                    # clone()도 실패하는 경우 (매우 드묾): numpy 경유
                    logger.warning(f"⚠️ [BGE-M3] clone() 실패, numpy 경유: {e}")
                    if convert_to_numpy:
                        # numpy로 직접 변환
                        with torch.no_grad():
                            embeddings_np = embeddings.cpu().numpy()
                        all_embeddings.append(torch.from_numpy(embeddings_np))
                    else:
                        raise e
        
        # 결과 합치기
        all_embeddings = torch.cat(all_embeddings, dim=0)
        
        if convert_to_numpy:
            return all_embeddings.numpy()
        
        return all_embeddings
    
    def encode_queries(
        self,
        queries: Union[str, List[str]],
        batch_size: int = 12,
        max_length: int = 8192,
        **kwargs
    ) -> Dict[str, np.ndarray]:
        """
        쿼리 인코딩 (FlagEmbedding 호환)
        
        Returns:
            {'dense_vecs': embeddings}
        """
        embeddings = self.encode(
            queries,
            batch_size=batch_size,
            max_length=max_length,
            convert_to_numpy=True,
            **kwargs
        )
        return {'dense_vecs': embeddings}
    
    def encode_corpus(
        self,
        corpus: Union[str, List[str]],
        batch_size: int = 12,
        max_length: int = 8192,
        **kwargs
    ) -> Dict[str, np.ndarray]:
        """
        코퍼스 인코딩 (FlagEmbedding 호환)
        
        Returns:
            {'dense_vecs': embeddings}
        """
        embeddings = self.encode(
            corpus,
            batch_size=batch_size,
            max_length=max_length,
            convert_to_numpy=True,
            **kwargs
        )
        return {'dense_vecs': embeddings}
    
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
_direct_bge_model = None


def get_direct_bge_model(
    model_name: str = "BAAI/bge-m3",
    device: str = None,
    use_fp16: bool = True
) -> DirectBGEM3Model:
    """
    직접 BGE-M3 모델 로드 (캐싱)
    
    Args:
        model_name: HuggingFace 모델 이름
        device: 사용할 디바이스 ('cuda', 'cpu', None=자동)
        use_fp16: FP16 사용 여부
    
    Returns:
        DirectBGEM3Model 인스턴스
    """
    global _direct_bge_model
    
    if _direct_bge_model is None:
        _direct_bge_model = DirectBGEM3Model(
            model_name=model_name,
            device=device,
            use_fp16=use_fp16
        )
    
    return _direct_bge_model
