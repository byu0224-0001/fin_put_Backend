# -*- coding: utf-8 -*-
"""
Market Cap API 테스트 스크립트
"""

import sys
import os
import codecs
from pathlib import Path

# 인코딩 설정
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')

# 프로젝트 루트 추가
project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from app.db import SessionLocal
from app.routes.scenario import get_affected_companies_by_driver
from app.services.kg_explanation_layer import generate_comparison_output
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_market_cap_in_query():
    """market_cap이 쿼리 결과에 포함되는지 테스트"""
    logger.info("=" * 80)
    logger.info("Market Cap API 테스트 시작")
    logger.info("=" * 80)
    
    db = SessionLocal()
    
    try:
        # 1. get_affected_companies_by_driver 테스트
        logger.info("\n1. get_affected_companies_by_driver 테스트")
        logger.info("-" * 80)
        
        companies = get_affected_companies_by_driver(db, 'OIL_PRICE', limit=10, min_weight=0.5)
        
        logger.info(f"조회된 기업 수: {len(companies)}개")
        
        # market_cap 포함 여부 확인
        has_market_cap = sum(1 for c in companies if c.get('market_cap') is not None)
        logger.info(f"market_cap이 있는 기업: {has_market_cap}개 ({has_market_cap/len(companies)*100:.1f}%)")
        
        # 샘플 출력
        logger.info("\n상위 5개 기업:")
        for i, c in enumerate(companies[:5], 1):
            market_cap = c.get('market_cap')
            market_cap_str = f"{market_cap:,}원 ({market_cap/1e12:.2f}조원)" if market_cap else "없음"
            logger.info(f"  {i}. {c['name']} ({c['ticker']})")
            logger.info(f"     - market_cap: {market_cap_str}")
            logger.info(f"     - weight: {c.get('weight', 0):.3f}")
            logger.info(f"     - mechanism: {c.get('mechanism', 'N/A')}")
        
        # 2. generate_comparison_output 테스트
        logger.info("\n2. generate_comparison_output 테스트")
        logger.info("-" * 80)
        
        comparison = generate_comparison_output('OIL_PRICE', companies, top_n=3)
        
        logger.info(f"변수: {comparison['variable']['name_kr']}")
        logger.info(f"긍정 영향 기업: {len(comparison['positive_impact'])}개")
        logger.info(f"부정 영향 기업: {len(comparison['negative_impact'])}개")
        
        # market_cap_insights 확인
        if comparison.get('market_cap_insights'):
            logger.info("\n✅ market_cap_insights 생성됨:")
            for insight in comparison['market_cap_insights']:
                logger.info(f"  - {insight}")
        else:
            logger.warning("⚠️ market_cap_insights가 생성되지 않았습니다")
        
        # 긍정 영향 기업 중 market_cap 확인
        if comparison['positive_impact']:
            logger.info("\n긍정 영향 기업 (market_cap 포함):")
            for c in comparison['positive_impact'][:3]:
                market_cap = c.get('market_cap')
                market_cap_str = f"{market_cap:,}원 ({market_cap/1e12:.2f}조원)" if market_cap else "없음"
                logger.info(f"  - {c['name']}: {market_cap_str}")
        
        # 부정 영향 기업 중 market_cap 확인
        if comparison['negative_impact']:
            logger.info("\n부정 영향 기업 (market_cap 포함):")
            for c in comparison['negative_impact'][:3]:
                market_cap = c.get('market_cap')
                market_cap_str = f"{market_cap:,}원 ({market_cap/1e12:.2f}조원)" if market_cap else "없음"
                logger.info(f"  - {c['name']}: {market_cap_str}")
        
        logger.info("\n" + "=" * 80)
        logger.info("✅ API 테스트 완료")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ API 테스트 실패: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        db.close()


if __name__ == '__main__':
    success = test_market_cap_in_query()
    sys.exit(0 if success else 1)

