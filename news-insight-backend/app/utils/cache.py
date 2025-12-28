"""
Redis 캐싱 유틸리티
"""
import json
import hashlib
import redis
from typing import Optional, Any
from app.config import settings
import logging

logger = logging.getLogger(__name__)

# Redis 클라이언트 (지연 로딩)
redis_client = None

def get_redis_client():
    """Redis 클라이언트 가져오기 (지연 로딩)"""
    global redis_client
    if redis_client is None:
        try:
            redis_client = redis.Redis.from_url(
                settings.CELERY_BROKER_URL,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            # 연결 테스트
            redis_client.ping()
            logger.info("Redis 클라이언트 연결 성공")
        except Exception as e:
            logger.warning(f"Redis 연결 실패: {e}. 캐싱이 비활성화됩니다.")
            redis_client = False  # False로 설정하여 재시도 방지
    return redis_client if redis_client else None


def get_cache_key(key_prefix: str, *args, **kwargs) -> str:
    """
    캐시 키 생성
    
    Args:
        key_prefix: 키 접두사
        *args, **kwargs: 키를 구성하는 파라미터들
    
    Returns:
        캐시 키 문자열
    """
    # 파라미터를 문자열로 변환하여 해시 생성
    key_str = f"{key_prefix}:{json.dumps(args, sort_keys=True)}:{json.dumps(kwargs, sort_keys=True)}"
    key_hash = hashlib.md5(key_str.encode()).hexdigest()[:8]
    return f"{key_prefix}:{key_hash}"


def get_from_cache(key: str) -> Optional[Any]:
    """
    캐시에서 데이터 가져오기
    
    Args:
        key: 캐시 키
    
    Returns:
        캐시된 데이터 (없으면 None)
    """
    client = get_redis_client()
    if not client:
        return None
    
    try:
        data = client.get(key)
        if data:
            return json.loads(data)
    except Exception as e:
        logger.warning(f"캐시 읽기 실패 ({key}): {e}")
    return None


def set_to_cache(key: str, value: Any, ttl: int = 600):
    """
    캐시에 데이터 저장
    
    Args:
        key: 캐시 키
        value: 저장할 데이터
        ttl: TTL (초 단위, 기본값: 600초 = 10분)
    """
    client = get_redis_client()
    if not client:
        return
    
    try:
        client.setex(key, ttl, json.dumps(value, ensure_ascii=False))
        logger.debug(f"캐시 저장 완료: {key} (TTL: {ttl}초)")
    except Exception as e:
        logger.warning(f"캐시 저장 실패 ({key}): {e}")


def invalidate_cache(key_pattern: str):
    """
    캐시 무효화 (패턴 기반)
    
    Args:
        key_pattern: 키 패턴 (예: "feed:*")
    """
    client = get_redis_client()
    if not client:
        return
    
    try:
        keys = client.keys(key_pattern)
        if keys:
            client.delete(*keys)
            logger.info(f"캐시 무효화 완료: {len(keys)}개 키 삭제")
    except Exception as e:
        logger.warning(f"캐시 무효화 실패 ({key_pattern}): {e}")

