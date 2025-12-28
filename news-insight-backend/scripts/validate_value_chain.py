"""
밸류체인 재분류 정확도 검증 스크립트

샘플 검증 데이터셋을 사용하여 재분류 전/후 정확도를 비교합니다.
"""
import sys
import os
from pathlib import Path
import logging

# 프로젝트 루트 경로 설정
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.services.value_chain_classifier import classify_value_chain_rule_based, classify_value_chain_hybrid

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# 검증 데이터셋 (수동 라벨링 필요)
# =============================================================================

VALIDATION_DATASET = [
    # === 반도체 (SEC_SEMI) ===
    {
        'ticker': '005930',  # 삼성전자
        'expected_vc': 'MIDSTREAM',
        'reason': '반도체 제조 (메모리, 파운드리)',
        'sector': 'SEC_SEMI'
    },
    {
        'ticker': '000660',  # SK하이닉스
        'expected_vc': 'MIDSTREAM',
        'reason': '메모리 반도체 제조',
        'sector': 'SEC_SEMI'
    },
    {
        'ticker': '058470',  # 리노공업
        'expected_vc': 'UPSTREAM',
        'reason': '반도체 소재/장비 공급',
        'sector': 'SEC_SEMI'
    },
    
    # === 자동차 (SEC_AUTO) ===
    {
        'ticker': '005380',  # 현대차
        'expected_vc': 'MIDSTREAM',
        'reason': '완성차 제조',
        'sector': 'SEC_AUTO'
    },
    {
        'ticker': '012330',  # 현대모비스
        'expected_vc': 'UPSTREAM',
        'reason': '자동차 부품 공급',
        'sector': 'SEC_AUTO'
    },
    
    # === 배터리 (SEC_BATTERY) ===
    {
        'ticker': '373220',  # LG에너지솔루션
        'expected_vc': 'MIDSTREAM',
        'reason': '배터리 셀 제조',
        'sector': 'SEC_BATTERY'
    },
    {
        'ticker': '006400',  # 삼성SDI
        'expected_vc': 'MIDSTREAM',
        'reason': '배터리 셀 제조',
        'sector': 'SEC_BATTERY'
    },
    
    # === IT (SEC_IT) ===
    {
        'ticker': '035720',  # 카카오
        'expected_vc': 'DOWNSTREAM',
        'reason': '플랫폼 서비스 (B2C)',
        'sector': 'SEC_IT'
    },
    {
        'ticker': '035420',  # NAVER
        'expected_vc': 'DOWNSTREAM',
        'reason': '플랫폼 서비스 (B2C)',
        'sector': 'SEC_IT'
    },
    
    # === 철강 (SEC_STEEL) ===
    {
        'ticker': '005490',  # POSCO홀딩스
        'expected_vc': 'UPSTREAM',
        'reason': '철강 소재 생산',
        'sector': 'SEC_STEEL'
    },
    
    # === 화학 (SEC_CHEM) ===
    {
        'ticker': '051910',  # LG화학
        'expected_vc': 'UPSTREAM',
        'reason': '화학 소재 생산',
        'sector': 'SEC_CHEM'
    },
    
    # === 유통 (SEC_RETAIL) ===
    {
        'ticker': '069960',  # 현대백화점
        'expected_vc': 'DOWNSTREAM',
        'reason': '소매 유통',
        'sector': 'SEC_RETAIL'
    },
    
    # === 금융 (SEC_FINANCE) ===
    {
        'ticker': '105560',  # KB금융
        'expected_vc': 'MIDSTREAM',
        'reason': '금융 서비스',
        'sector': 'SEC_FINANCE'
    },
    
    # === 바이오 (SEC_BIO) ===
    {
        'ticker': '207940',  # 삼성바이오로직스
        'expected_vc': 'MIDSTREAM',
        'reason': '바이오 의약품 제조',
        'sector': 'SEC_BIO'
    },
    
    # === 건설 (SEC_CONST) ===
    {
        'ticker': '000720',  # 현대건설
        'expected_vc': 'MIDSTREAM',
        'reason': '건설 시공',
        'sector': 'SEC_CONST'
    },
]


def validate_value_chain_reclassification():
    """
    밸류체인 재분류 정확도 검증
    """
    db = SessionLocal()
    
    try:
        logger.info("=" * 60)
        logger.info("밸류체인 재분류 정확도 검증 시작")
        logger.info("=" * 60)
        
        results = []
        correct = 0
        total = 0
        
        for sample in VALIDATION_DATASET:
            ticker = sample['ticker']
            expected_vc = sample['expected_vc']
            sector = sample['sector']
            reason = sample['reason']
            
            # CompanyDetail 조회
            company_detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == ticker
            ).first()
            
            if not company_detail:
                logger.warning(f"[{ticker}] CompanyDetail 없음 - 스킵")
                continue
            
            # 밸류체인 분류 (Rule-based)
            predicted_vc, confidence = classify_value_chain_rule_based(
                company_detail=company_detail,
                sector=sector,
                company_name=company_detail.name
            )
            
            total += 1
            is_correct = predicted_vc == expected_vc
            
            if is_correct:
                correct += 1
                status = "✅"
            else:
                status = "❌"
            
            results.append({
                'ticker': ticker,
                'name': company_detail.name,
                'sector': sector,
                'expected': expected_vc,
                'predicted': predicted_vc,
                'confidence': confidence,
                'correct': is_correct,
                'reason': reason
            })
            
            logger.info(
                f"[{ticker}] {company_detail.name[:15]:15s} | "
                f"예상: {expected_vc:10s} | 예측: {str(predicted_vc):10s} | "
                f"신뢰도: {confidence:.2f} | {status}"
            )
        
        # 결과 요약
        accuracy = correct / total if total > 0 else 0
        
        logger.info("=" * 60)
        logger.info("검증 결과 요약")
        logger.info("=" * 60)
        logger.info(f"총 샘플: {total}")
        logger.info(f"정확: {correct}")
        logger.info(f"오류: {total - correct}")
        logger.info(f"정확도: {accuracy:.2%}")
        logger.info("=" * 60)
        
        # 오류 케이스 상세
        errors = [r for r in results if not r['correct']]
        if errors:
            logger.info("\n오류 케이스 상세:")
            for e in errors:
                logger.info(
                    f"  - [{e['ticker']}] {e['name']}: "
                    f"예상={e['expected']}, 예측={e['predicted']} "
                    f"(이유: {e['reason']})"
                )
        
        return {
            'total': total,
            'correct': correct,
            'accuracy': accuracy,
            'results': results
        }
    
    finally:
        db.close()


if __name__ == "__main__":
    validate_value_chain_reclassification()

