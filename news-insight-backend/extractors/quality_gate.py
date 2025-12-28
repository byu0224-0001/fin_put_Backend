"""
Quality Gate (Phase 2.0 P0)

목적: KG 보호 + LLM 비용 최적화
원칙: "가치 판단 ❌ / 안전성 판단 ⭕"

⚠️ 중요: Quality Gate는 LLM 진입 직전에 호출되어야 함
- Ticker/Driver 매칭 후
- Insight 추출 전
"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
import re
import logging

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def evaluate_quality(
    parsed_report: Dict[str, Any],
    ticker: Optional[str] = None,
    driver_code: Optional[str] = None
) -> Dict[str, Any]:
    """
    Quality Gate 평가 (LLM 진입 직전)
    
    ⚠️ 중요: Meaningful Paragraph는 Gate 판단에 사용하지 않음
    - Earnings Preview / Flash Note는 의미 문단 1개가 정상
    - Gate로 쓰면 HOLD 비율 급증
    
    Args:
        parsed_report: 파싱된 리포트 데이터
        ticker: 매칭된 티커 (선택)
        driver_code: 매칭된 드라이버 코드 (선택)
    
    Returns:
        {
            "status": "PASS" | "HOLD" | "DROP",
            "reason": "...",
            "scores": {
                "korean_char_count": 0,
                "meaningful_paragraphs": 0,  # Score로만 사용
                "digit_ratio": 0.0,
                "table_ratio": 0.0
            },
            "hold_metadata": {
                "hold_reason": "LOW_KOREAN" | "AMBIGUOUS_TICKER" | "LOW_QUALITY",
                "retry_eligible": True,
                "retry_after": "parser_v2" | "driver_embedding_v1" | null
            }
        }
    """
    full_text = parsed_report.get("full_text", "")
    paragraphs = parsed_report.get("paragraphs", [])
    
    # 한글 문자 수
    korean_chars = len(re.findall(r'[가-힣]', full_text))
    
    # 의미 있는 문단 수 (50자 이상) - Score로만 사용, Gate 판단에 사용 안 함
    meaningful_paras = [p for p in paragraphs if len(p.get("text", "")) >= 50]
    meaningful_para_count = len(meaningful_paras)
    
    # 숫자 밀도 (표 판단용)
    digit_count = len(re.findall(r'\d', full_text))
    digit_ratio = digit_count / len(full_text) if full_text else 0
    
    # 표 비율 (숫자 밀도 기반)
    table_ratio = digit_ratio if digit_ratio > 0.35 else 0
    
    # Scores (기록용)
    scores = {
        "korean_char_count": korean_chars,
        "meaningful_paragraphs": meaningful_para_count,  # Score로만 사용
        "digit_ratio": digit_ratio,
        "table_ratio": table_ratio
    }
    
    # Hard Drop (극소수) - 한글 절대량 기준
    if korean_chars < 80:
        return {
            "status": "DROP",
            "reason": f"한글 문자 수 부족 (80자 미만: {korean_chars}자)",
            "scores": scores,
            "hold_metadata": None
        }
    
    # PASS / HOLD 결정 (보수적 - 한글 절대량 위주)
    # ⚠️ Meaningful Paragraph는 Gate 판단에 사용하지 않음
    if korean_chars >= 150:
        return {
            "status": "PASS",
            "reason": f"기본 품질 기준 충족 (한글: {korean_chars}자)",
            "scores": scores,
            "hold_metadata": None
        }
    else:
        # HOLD 메타 구조화
        hold_reason = "LOW_KOREAN"
        retry_eligible = True
        retry_after = "parser_v2"  # 파서 개선 후 재처리
        
        # Ticker/Driver 매칭 실패 시 추가 정보
        if not ticker:
            hold_reason = "AMBIGUOUS_TICKER"
            retry_after = "ticker_matcher_v2"
        elif not driver_code:
            hold_reason = "AMBIGUOUS_DRIVER"
            retry_after = "driver_embedding_v1"
        
        return {
            "status": "HOLD",
            "reason": f"품질 미달 (한글: {korean_chars}자, 문단: {meaningful_para_count}개)",
            "scores": scores,
            "hold_metadata": {
                "hold_reason": hold_reason,
                "retry_eligible": retry_eligible,
                "retry_after": retry_after
            }
        }


def check_market_cap_weight(
    ticker: str,
    db: Optional[Any] = None
) -> Dict[str, Any]:
    """
    Market Cap Weight 체크 (선택적 안전장치)
    
    시가총액 상위 20% 기업 → 엄격한 기준
    하위 80% → 완화된 기준
    
    Args:
        ticker: 종목코드
        db: DB 세션 (선택)
    
    Returns:
        {
            "is_top_20": bool,
            "market_cap": int | None,
            "strict_mode": bool
        }
    """
    try:
        if db is None:
            from app.db import SessionLocal
            db = SessionLocal()
            should_close = True
        else:
            should_close = False
        
        try:
            from app.models.stock import Stock
            
            stock = db.query(Stock).filter(Stock.ticker == ticker).first()
            
            if not stock or not stock.market_cap:
                return {
                    "is_top_20": False,
                    "market_cap": None,
                    "strict_mode": False
                }
            
            # 시가총액 상위 20% 기준 (임시: 10조원 이상)
            # TODO: 실제 상위 20% 계산 로직 추가
            top_20_threshold = 10_000_000_000_000  # 10조원
            
            is_top_20 = stock.market_cap >= top_20_threshold
            
            return {
                "is_top_20": is_top_20,
                "market_cap": stock.market_cap,
                "strict_mode": is_top_20
            }
        finally:
            if should_close:
                db.close()
    except Exception as e:
        logger.warning(f"Market Cap 체크 실패: {e}")
        return {
            "is_top_20": False,
            "market_cap": None,
            "strict_mode": False
        }

