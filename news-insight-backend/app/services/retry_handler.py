"""
Retry Handler Service

DART API 및 LLM API 호출에 대한 재시도 및 백오프 로직 제공
tenacity 라이브러리 사용
"""
import logging
from typing import Callable, TypeVar, Optional
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    after_log
)
import requests
from openai import RateLimitError, APIError

logger = logging.getLogger(__name__)

T = TypeVar('T')


# DART API 재시도 설정
DART_MAX_ATTEMPTS = 5
DART_WAIT_MIN = 2  # 초
DART_WAIT_MAX = 60  # 초
DART_MULTIPLIER = 2  # 지수 백오프 배수

# LLM API 재시도 설정
LLM_MAX_ATTEMPTS = 3
LLM_WAIT_MIN = 1  # 초
LLM_WAIT_MAX = 30  # 초
LLM_MULTIPLIER = 2  # 지수 백오프 배수


def retry_dart_api(func: Callable[..., T]) -> Callable[..., T]:
    """
    DART API 호출에 대한 재시도 데코레이터
    
    재시도 조건:
    - requests.exceptions.RequestException (네트워크 오류)
    - HTTP 429 (Rate Limit)
    - HTTP 500, 502, 503 (서버 오류)
    
    사용 예:
        @retry_dart_api
        def fetch_report(ticker):
            return dart.list(ticker, ...)
    """
    return retry(
        stop=stop_after_attempt(DART_MAX_ATTEMPTS),
        wait=wait_exponential(
            multiplier=DART_MULTIPLIER,
            min=DART_WAIT_MIN,
            max=DART_WAIT_MAX
        ),
        retry=retry_if_exception_type((
            requests.exceptions.RequestException,
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.SSLError,  # SSL 오류 추가
        )),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )(func)


def retry_llm_api(func: Callable[..., T]) -> Callable[..., T]:
    """
    LLM API 호출에 대한 재시도 데코레이터
    
    재시도 조건:
    - RateLimitError (API Rate Limit)
    - APIError (일반 API 오류)
    - requests.exceptions.RequestException (네트워크 오류)
    
    사용 예:
        @retry_llm_api
        def call_llm(prompt):
            return llm.invoke(...)
    """
    return retry(
        stop=stop_after_attempt(LLM_MAX_ATTEMPTS),
        wait=wait_exponential(
            multiplier=LLM_MULTIPLIER,
            min=LLM_WAIT_MIN,
            max=LLM_WAIT_MAX
        ),
        retry=retry_if_exception_type((
            RateLimitError,
            APIError,
            requests.exceptions.RequestException,
        )),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        after=after_log(logger, logging.INFO),
        reraise=True
    )(func)


def retry_with_custom_config(
    max_attempts: int = 3,
    wait_min: float = 1.0,
    wait_max: float = 30.0,
    multiplier: float = 2.0,
    retry_exceptions: tuple = (Exception,)
) -> Callable:
    """
    커스텀 재시도 설정 데코레이터 팩토리
    
    Args:
        max_attempts: 최대 재시도 횟수
        wait_min: 최소 대기 시간 (초)
        wait_max: 최대 대기 시간 (초)
        multiplier: 지수 백오프 배수
        retry_exceptions: 재시도할 예외 타입 튜플
    
    사용 예:
        @retry_with_custom_config(max_attempts=5, wait_min=2.0)
        def custom_function():
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        return retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(
                multiplier=multiplier,
                min=wait_min,
                max=wait_max
            ),
            retry=retry_if_exception_type(retry_exceptions),
            before_sleep=before_sleep_log(logger, logging.WARNING),
            after=after_log(logger, logging.INFO),
            reraise=True
        )(func)
    return decorator


# Rate Limit 관리 (간단한 토큰 버킷 방식)
class RateLimiter:
    """간단한 Rate Limiter (토큰 버킷 방식)"""
    
    def __init__(self, max_calls: int, period: float):
        """
        Args:
            max_calls: 기간 내 최대 호출 횟수
            period: 기간 (초)
        """
        self.max_calls = max_calls
        self.period = period
        self.calls = []
    
    def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
        """데코레이터로 사용"""
        import time
        
        def wrapper(*args, **kwargs):
            now = time.time()
            # 기간이 지난 호출 기록 제거
            self.calls = [t for t in self.calls if now - t < self.period]
            
            # Rate limit 체크
            if len(self.calls) >= self.max_calls:
                sleep_time = self.period - (now - self.calls[0])
                if sleep_time > 0:
                    logger.info(f"Rate limit 도달. {sleep_time:.2f}초 대기 중...")
                    time.sleep(sleep_time)
                    # 다시 정리
                    now = time.time()
                    self.calls = [t for t in self.calls if now - t < self.period]
            
            # 호출 기록 추가
            self.calls.append(time.time())
            
            return func(*args, **kwargs)
        
        return wrapper


# DART API Rate Limiter (초당 15회 제한 - 병렬 처리 여유 확보)
dart_rate_limiter = RateLimiter(max_calls=15, period=1.0)

# LLM API Rate Limiter (분당 60회 제한)
llm_rate_limiter = RateLimiter(max_calls=60, period=60.0)

