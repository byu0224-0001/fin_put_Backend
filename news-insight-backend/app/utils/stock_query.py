# -*- coding: utf-8 -*-
"""
Stock 조회 유틸리티 (DB 컬럼 없어도 작동)
"""
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import text


class SimpleStock:
    """Stock 모델과 호환되는 간단한 객체 (DB 컬럼 없어도 작동)"""
    def __init__(self, ticker, stock_name, market=None, industry_raw=None, synonyms=None, country=None):
        self.ticker = ticker
        self.stock_name = stock_name
        self.market = market
        self.industry_raw = industry_raw
        self.synonyms = synonyms or []
        self.country = country
        self.is_preferred_stock = False  # 기본값
        self.parent_ticker = None  # 기본값


def get_stock_by_ticker_safe(db: Session, ticker: str) -> Optional[SimpleStock]:
    """
    티커로 Stock 정보 조회 (Raw SQL, DB 컬럼 없어도 작동)
    
    Args:
        db: DB 세션
        ticker: 티커 코드
    
    Returns:
        SimpleStock 객체 또는 None
    """
    sql = """
        SELECT ticker, stock_name, market, industry_raw, synonyms, country
        FROM stocks
        WHERE ticker = :ticker
        LIMIT 1
    """
    
    result = db.execute(text(sql), {'ticker': ticker})
    row = result.fetchone()
    
    if row:
        return SimpleStock(
            ticker=row[0],
            stock_name=row[1],
            market=row[2] if len(row) > 2 else None,
            industry_raw=row[3] if len(row) > 3 else None,
            synonyms=row[4] if len(row) > 4 and row[4] else [],
            country=row[5] if len(row) > 5 else 'KR'
        )
    return None

