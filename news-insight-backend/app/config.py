import os
from urllib.parse import quote_plus
from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    """애플리케이션 설정"""
    
    # API 설정
    API_TITLE: str = "News Insight Backend API"
    API_VERSION: str = "1.0.0"
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    
    # PostgreSQL 설정
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "user")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "password")
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT: str = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "newsdb")
    
    @property
    def POSTGRES_URL(self) -> str:
        # UTF-8 인코딩 명시 및 특수 문자 URL 인코딩 (Windows 환경에서 인코딩 문제 방지)
        # 비밀번호에 특수 문자가 있을 경우를 대비해 URL 인코딩 적용
        # 참고: db.py에서는 URL.create()를 사용하여 직접 연결하므로 이 메서드는 사용되지 않을 수 있습니다
        user = quote_plus(self.POSTGRES_USER)
        password = quote_plus(self.POSTGRES_PASSWORD)
        return f"postgresql://{user}:{password}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}?client_encoding=utf8"
    
    # Neo4j 설정
    NEO4J_URI: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "password")
    
    # OpenAI 설정
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    
    # Gemini API 설정
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")
    GEMINI_MODEL: str = os.getenv("GEMINI_MODEL", "gemini-2.0-flash-exp")
    
    # Upstage API 설정
    UPSTAGE_API_KEY: str = os.getenv("UPSTAGE_API_KEY", "")
    
    # DART API 설정
    DART_API_KEY: str = os.getenv("DART_API_KEY", "")
    
    # Finnhub API 설정
    FINNHUB_API_KEY: str = os.getenv("FINNHUB_API_KEY", "")
    
    # Celery & Redis 설정
    CELERY_BROKER_URL: str = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
    CELERY_RESULT_BACKEND: str = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0")
    
    # CORS 설정
    CORS_ORIGINS: List[str] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:8000",
    ]
    
    # 중복 제거 설정
    DEDUPLICATION_ENABLED: bool = os.getenv("DEDUPLICATION_ENABLED", "True").lower() == "true"
    SIMILARITY_THRESHOLD: float = float(os.getenv("SIMILARITY_THRESHOLD", "0.75"))
    ENABLE_BERT: bool = os.getenv("ENABLE_BERT", "True").lower() == "true"
    TFIDF_WEIGHT: float = float(os.getenv("TFIDF_WEIGHT", "0.6"))
    BERT_WEIGHT: float = float(os.getenv("BERT_WEIGHT", "0.4"))
    MAX_SAME_SOURCE: int = int(os.getenv("MAX_SAME_SOURCE", "3"))
    
    # RSS 피드 URL 목록
    RSS_FEEDS: List[str] = [
        # 매일경제
        "https://www.mk.co.kr/rss/30100041/",  # 매경 경제
        "https://www.mk.co.kr/rss/50200011/",  # 매경 증권
        "https://www.mk.co.kr/rss/50100032/",  # 매경 기업경영
        "https://www.mk.co.kr/rss/50300009/",  # 매경 부동산
        
        # 한국경제
        "https://www.hankyung.com/feed/economy",  # 한경 경제
        "https://www.hankyung.com/feed/finance",  # 한경 증권
        "https://www.hankyung.com/feed/realestate",  # 한경 부동산
        "https://www.hankyung.com/feed/it",  # 한경 IT
        "https://www.hankyung.com/feed/politics",  # 한경 정치
        "https://www.hankyung.com/feed/society",  # 한경 사회
        "https://www.hankyung.com/feed/international",  # 한경 국제
        
        # 연합뉴스
        "https://www.yna.co.kr/rss/economy.xml",  # 연합 경제
        "https://www.yna.co.kr/rss/market.xml",  # 연합 시장
        "https://www.yna.co.kr/rss/politics.xml",  # 연합 정치
        "https://www.yna.co.kr/rss/industry.xml",  # 연합 산업
        "https://www.yna.co.kr/rss/society.xml",  # 연합 사회
        "https://www.yna.co.kr/rss/local.xml",  # 연합 전국
        "https://www.yna.co.kr/rss/international.xml",  # 연합 세계
        "https://www.yna.co.kr/rss/northkorea.xml",  # 연합 북한
        
        # 아시아경제
        "https://www.asiae.co.kr/rss/stock.htm",  # 아시아경제 증권
        "https://www.asiae.co.kr/rss/economy.htm",  # 아시아경제 경제
        "https://www.asiae.co.kr/rss/realestate.htm",  # 아시아경제 부동산
        "https://www.asiae.co.kr/rss/industry-IT.htm",  # 아시아경제 산업·IT
        
        # 이데일리
        "http://rss.edaily.co.kr/economy_news.xml",  # 이데일리 경제
        "http://rss.edaily.co.kr/stock_news.xml",  # 이데일리 증권
        "http://rss.edaily.co.kr/finance_news.xml",  # 이데일리 금융
        "http://rss.edaily.co.kr/bondfx_news.xml",  # 이데일리 채권/외환
        
        # 전자신문
        "http://rss.etnews.com/Section901.xml",  # 전자신문 IT
        
        # 경향신문
        "https://www.khan.co.kr/rss/rssdata/economy_news.xml",  # 경향 경제
        
        # WOWTV
        "https://help.wowtv.co.kr/serviceinfo/newsstand/FeedRss/stock",  # WOWTV 증권
        "https://help.wowtv.co.kr/serviceinfo/newsstand/FeedRss/economy",  # WOWTV 경제
        "https://help.wowtv.co.kr/serviceinfo/newsstand/FeedRss/init",  # WOWTV 산업·IT
        
        # 서울경제
        "https://www.sedaily.com/rss/economy",  # 서울경제 경제
        "https://www.sedaily.com/rss/finance",  # 서울경제 증권·금융
        "https://www.sedaily.com/rss/realestate",  # 서울경제 부동산
        
        # 이투데이
        "https://rss.etoday.co.kr/eto/finance_news.xml",  # 이투데이 증권·금융
        "https://rss.etoday.co.kr/eto/land_news.xml",  # 이투데이 부동산
        "https://rss.etoday.co.kr/eto/company_news.xml",  # 이투데이 기업
        "https://rss.etoday.co.kr/eto/global_news.xml",  # 이투데이 글로벌 경제
        
        # 전자신문
        "http://rss.etnews.com/02.xml",  # 전자신문 경제
        
        # 파이낸셜뉴스
        "http://www.fnnews.com/rss/r20/fn_realnews_economy.xml",  # 파이낸셜뉴스 경제
        "http://www.fnnews.com/rss/r20/fn_realnews_stock.xml",  # 파이낸셜뉴스 증권
        "http://www.fnnews.com/rss/r20/fn_realnews_finance.xml",  # 파이낸셜뉴스 금융
        "http://www.fnnews.com/rss/r20/fn_realnews_realestate.xml",  # 파이낸셜뉴스 부동산
        "http://www.fnnews.com/rss/r20/fn_realnews_industry.xml",  # 파이낸셜뉴스 산업
        
        # 연합인포맥스
        "https://news.einfomax.co.kr/rss/S1N15.xml",  # 연합인포맥스 정책/금융
        "https://news.einfomax.co.kr/rss/S1N16.xml",  # 연합인포맥스 채권/외환
        "https://news.einfomax.co.kr/rss/S1N17.xml",  # 연합인포맥스 부동산
        "https://news.einfomax.co.kr/rss/S1N21.xml",  # 연합인포맥스 해외주식
        "https://news.einfomax.co.kr/rss/S1N2.xml",  # 연합인포맥스 증권
        "https://news.einfomax.co.kr/rss/S1N7.xml",  # 연합인포맥스 IB/기업
        
        # 해럴드저널
        "http://www.heraldjournal.co.kr/rss/clickTop.xml",  # 해럴드저널 인기기사
        "http://www.heraldjournal.co.kr/rss/S1N3.xml",  # 해럴드저널 오피니언

        # 한겨레신문
        "http://www.hani.co.kr/rss/economy/",  # 한겨레 경제

        # 동아일보
        "http://rss.donga.com/economy.xml",  # 동아일보 경제

        # 뉴시스
        "https://www.newsis.com/RSS/economy.xml",  # 뉴시스 경제
        "https://www.newsis.com/RSS/bank.xml",  # 뉴시스 금융
        "https://www.newsis.com/RSS/industry.xml",  # 뉴시스 산업

        # 조선일보
        "https://www.chosun.com/arc/outboundfeeds/rss/category/economy/?outputType=xml",  # 조선일보 경제

        # JTBC
        "https://news-ex.jtbc.co.kr/v1/get/rss/section/economy",  # JTBC 경제
    ]
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"  # Windows 환경에서 인코딩 문제 방지
        case_sensitive = False


settings = Settings()
