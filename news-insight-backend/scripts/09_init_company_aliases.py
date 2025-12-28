"""
기업명 별칭 초기 데이터 로딩 스크립트

주요 공급업체의 별칭을 company_aliases 테이블에 등록
"""
import sys
import os
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Windows 환경에서 인코딩 문제 방지
if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from dotenv import load_dotenv
load_dotenv()

import logging
from app.db import SessionLocal
from app.models.company_alias import CompanyAlias
from app.models.stock import Stock
from app.services.entity_resolver import normalize_company_name

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# 주요 공급업체 별칭 데이터
INITIAL_ALIASES = [
    # 삼성전기 관련
    {"alias": "SEMCO", "official": "삼성전기", "ticker": "009150"},
    {"alias": "에스이엠", "official": "삼성전기", "ticker": "009150"},
    {"alias": "Samsung Electro-Mechanics", "official": "삼성전기", "ticker": "009150"},
    {"alias": "Samsung Electro Mechanics", "official": "삼성전기", "ticker": "009150"},
    
    # MediaTek (타이완 상장사, 한국 DB에 없을 수 있음)
    {"alias": "MediaTek", "official": "MediaTek", "ticker": None, "type": "UNLISTED"},
    {"alias": "MTK", "official": "MediaTek", "ticker": None, "type": "UNLISTED"},
    
    # CSOT (중국 상장사)
    {"alias": "CSOT", "official": "CSOT", "ticker": None, "type": "UNLISTED"},
    {"alias": "China Star Optoelectronics Technology", "official": "CSOT", "ticker": None, "type": "UNLISTED"},
    
    # AUO (타이완 상장사)
    {"alias": "AUO", "official": "AUO", "ticker": None, "type": "UNLISTED"},
    {"alias": "AU Optronics", "official": "AUO", "ticker": None, "type": "UNLISTED"},
    
    # SUMCO (일본 상장사)
    {"alias": "SUMCO", "official": "SUMCO", "ticker": None, "type": "UNLISTED"},
    
    # 비에이치
    {"alias": "비에이치", "official": "비에이치", "ticker": "090460"},
    {"alias": "BH", "official": "비에이치", "ticker": "090460"},
    
    # 파트론
    {"alias": "파트론", "official": "파트론", "ticker": "091700"},
    {"alias": "Partron", "official": "파트론", "ticker": "091700"},
    
    # 솔브레인
    {"alias": "솔브레인", "official": "솔브레인", "ticker": "357780"},
    {"alias": "Soulbrain", "official": "솔브레인", "ticker": "357780"},
    
    # Wistron
    {"alias": "Wistron NewEB", "official": "Wistron", "ticker": None, "type": "UNLISTED"},
    {"alias": "WNC", "official": "Wistron", "ticker": None, "type": "UNLISTED"},
]


def init_aliases():
    """별칭 데이터 초기화"""
    db = SessionLocal()
    try:
        added_count = 0
        skipped_count = 0
        
        for alias_data in INITIAL_ALIASES:
            alias_name = normalize_company_name(alias_data['alias'])
            official_name = alias_data['official']
            ticker = alias_data.get('ticker')
            company_type = alias_data.get('type', 'LISTED' if ticker else 'UNLISTED')
            
            # ID 생성
            alias_id = f"{alias_name}_{ticker or 'UNLISTED'}"
            
            # 중복 확인
            existing = db.query(CompanyAlias).filter(CompanyAlias.id == alias_id).first()
            if existing:
                logger.debug(f"별칭 이미 존재: {alias_name}")
                skipped_count += 1
                continue
            
            # 티커 검증 (있는 경우)
            if ticker:
                stock = db.query(Stock).filter(Stock.ticker == ticker).first()
                if not stock:
                    logger.warning(f"티커 {ticker}가 stocks 테이블에 없습니다. 비상장사로 처리합니다.")
                    ticker = None
                    company_type = "UNLISTED"
            
            alias = CompanyAlias(
                id=alias_id,
                alias_name=alias_name,
                official_name=official_name,
                ticker=ticker,
                company_type=company_type,
                confidence="HIGH"
            )
            
            db.add(alias)
            added_count += 1
            logger.debug(f"별칭 추가: {alias_name} -> {official_name} ({ticker or 'UNLISTED'})")
        
        db.commit()
        logger.info(f"✅ 별칭 {added_count}개 추가 완료 (스킵: {skipped_count}개)")
        
    except Exception as e:
        logger.error(f"❌ 별칭 초기화 실패: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("기업명 별칭 초기 데이터 로딩")
    logger.info("=" * 60)
    init_aliases()
    logger.info("=" * 60)
    logger.info("✅ 완료!")
    logger.info("=" * 60)

