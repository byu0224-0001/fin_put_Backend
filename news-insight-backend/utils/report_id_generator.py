"""
report_id 생성 유틸리티

report_id 생성 규칙 버전 관리 및 안정성 보장
"""
import hashlib
import logging
from utils.text_normalizer import (
    normalize_date_for_report_id,
    canonicalize_url,
    REPORT_ID_GENERATION_VERSION
)

logger = logging.getLogger(__name__)


def generate_report_id(
    broker_name: str = "",
    title: str = "",
    date: str = "",
    url: str = ""
) -> str:
    """
    report_id 생성 (v0.1-rc1 규칙)
    
    ⚠️ 중요: 이 함수의 생성 규칙을 변경하면 기존 report_id와 충돌 가능성이 있습니다.
    변경 시에는 report_uid 같은 별도 키를 사용하거나 마이그레이션 전략이 필요합니다.
    
    Args:
        broker_name: 증권사명
        title: 리포트 제목
        date: 리포트 발행일 (다양한 포맷 가능)
        url: 리포트 URL
    
    Returns:
        32자리 해시값 (report_id)
    """
    # 정규화
    normalized_broker = broker_name.strip() if broker_name else "NA"
    normalized_title = title.strip() if title else "NA"
    normalized_date = normalize_date_for_report_id(date)
    canonicalized_url = canonicalize_url(url)
    
    # v0.1-rc1 규칙: broker_name|title|normalized_date|canonicalized_url
    report_id_string = f"{normalized_broker}|{normalized_title}|{normalized_date}|{canonicalized_url}"
    
    # SHA256 해시 (32자리)
    report_id = hashlib.sha256(report_id_string.encode()).hexdigest()[:32]
    
    logger.debug(f"report_id 생성 (version: {REPORT_ID_GENERATION_VERSION}): {report_id[:16]}...")
    
    return report_id

