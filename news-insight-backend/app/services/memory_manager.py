"""
Memory Manager Service

메모리 모니터링 및 관리, GPU 메모리 관리, 프로세스 재시작 로직
"""
import gc
import logging
import psutil
import os
from typing import Optional, Dict
import torch

logger = logging.getLogger(__name__)


class MemoryManager:
    """메모리 관리 클래스"""
    
    def __init__(self, memory_threshold_mb: int = 8000, gpu_threshold_mb: int = 6000):
        """
        Args:
            memory_threshold_mb: 시스템 메모리 임계값 (MB)
            gpu_threshold_mb: GPU 메모리 임계값 (MB)
        """
        self.memory_threshold_mb = memory_threshold_mb
        self.gpu_threshold_mb = gpu_threshold_mb
    
    def get_memory_usage(self) -> Dict[str, float]:
        """
        현재 메모리 사용량 반환
        
        Returns:
            {
                'system_mb': 시스템 메모리 사용량 (MB),
                'process_mb': 프로세스 메모리 사용량 (MB),
                'gpu_mb': GPU 메모리 사용량 (MB, 사용 가능한 경우)
            }
        """
        process = psutil.Process(os.getpid())
        process_mb = process.memory_info().rss / 1024 / 1024
        
        system_memory = psutil.virtual_memory()
        system_mb = system_memory.used / 1024 / 1024
        
        gpu_mb = None
        if torch.cuda.is_available():
            gpu_mb = torch.cuda.memory_allocated() / 1024 / 1024
        
        return {
            'system_mb': system_mb,
            'process_mb': process_mb,
            'gpu_mb': gpu_mb
        }
    
    def check_memory_pressure(self) -> bool:
        """
        메모리 압박 상태 확인
        
        Returns:
            True if memory pressure detected
        """
        usage = self.get_memory_usage()
        
        # 시스템 메모리 체크
        if usage['system_mb'] > self.memory_threshold_mb:
            logger.warning(
                f"시스템 메모리 압박 감지: {usage['system_mb']:.0f}MB "
                f"(임계값: {self.memory_threshold_mb}MB)"
            )
            return True
        
        # GPU 메모리 체크
        if usage['gpu_mb'] is not None and usage['gpu_mb'] > self.gpu_threshold_mb:
            logger.warning(
                f"GPU 메모리 압박 감지: {usage['gpu_mb']:.0f}MB "
                f"(임계값: {self.gpu_threshold_mb}MB)"
            )
            return True
        
        return False
    
    def cleanup_memory(self, force_gc: bool = True, clear_gpu_cache: bool = True):
        """
        메모리 정리
        
        Args:
            force_gc: 강제 가비지 컬렉션 실행 여부
            clear_gpu_cache: GPU 캐시 클리어 여부
        """
        if force_gc:
            collected = gc.collect()
            logger.debug(f"가비지 컬렉션 완료: {collected}개 객체 수거")
        
        if clear_gpu_cache and torch.cuda.is_available():
            torch.cuda.empty_cache()
            logger.debug("GPU 캐시 클리어 완료")
        
        usage = self.get_memory_usage()
        logger.info(
            f"메모리 정리 후 - 프로세스: {usage['process_mb']:.0f}MB, "
            f"시스템: {usage['system_mb']:.0f}MB"
            + (f", GPU: {usage['gpu_mb']:.0f}MB" if usage['gpu_mb'] else "")
        )
    
    def log_memory_status(self, label: str = ""):
        """
        메모리 상태 로깅
        
        Args:
            label: 로그 라벨
        """
        usage = self.get_memory_usage()
        prefix = f"[{label}] " if label else ""
        
        logger.info(
            f"{prefix}메모리 사용량 - "
            f"프로세스: {usage['process_mb']:.0f}MB, "
            f"시스템: {usage['system_mb']:.0f}MB"
            + (f", GPU: {usage['gpu_mb']:.0f}MB" if usage['gpu_mb'] else "")
        )
    
    def should_restart_process(self) -> bool:
        """
        프로세스 재시작 필요 여부 확인
        
        Returns:
            True if process restart recommended
        """
        usage = self.get_memory_usage()
        
        # 프로세스 메모리가 임계값의 1.5배를 넘으면 재시작 권장
        if usage['process_mb'] > self.memory_threshold_mb * 1.5:
            logger.warning(
                f"프로세스 재시작 권장: 프로세스 메모리 {usage['process_mb']:.0f}MB "
                f"(임계값의 1.5배: {self.memory_threshold_mb * 1.5:.0f}MB)"
            )
            return True
        
        return False


# 전역 인스턴스
memory_manager = MemoryManager()


def cleanup_after_batch():
    """배치 처리 후 메모리 정리 (편의 함수)"""
    memory_manager.cleanup_memory()


def log_memory_usage(label: str = ""):
    """메모리 사용량 로깅 (편의 함수)"""
    memory_manager.log_memory_status(label)

