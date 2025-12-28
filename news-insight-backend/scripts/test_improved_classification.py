# -*- coding: utf-8 -*-
"""
P0 개선 사항 테스트:
1. 대기업 특별 처리 (삼성전자 → SEC_SEMI)
2. 마진 체크 로직
3. 확장된 SEGMENT_TO_SECTOR_MAP
"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.services.sector_classifier import (
    classify_sector_rule_based,
    calculate_revenue_sector_scores,
    MAJOR_COMPANY_SECTORS
)

def test_major_companies():
    """대기업 특별 처리 테스트"""
    print("=" * 80)
    print("[1] 대기업 특별 처리 테스트 (MAJOR_COMPANY_SECTORS)")
    print("=" * 80)
    
    db = SessionLocal()
    
    test_companies = [
        ('삼성전자', '005930', 'SEC_SEMI'),
        ('SK하이닉스', '000660', 'SEC_SEMI'),
        ('현대차', '005380', 'SEC_AUTO'),
        ('기아', '000270', 'SEC_AUTO'),
        ('LG에너지솔루션', '373220', 'SEC_BATTERY'),
        ('삼성바이오로직스', '207940', 'SEC_BIO'),
    ]
    
    passed = 0
    failed = 0
    
    for name, ticker, expected_sector in test_companies:
        # DB에서 CompanyDetail 조회
        detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == ticker).first()
        
        if detail:
            major, sub, vc, conf, _ = classify_sector_rule_based(detail, name)
            status = "✅" if major == expected_sector else "❌"
            if major == expected_sector:
                passed += 1
            else:
                failed += 1
            print(f"{status} {name}: {major} (기대: {expected_sector}), confidence: {conf}")
        else:
            print(f"⚠️ {name} ({ticker}): DB에 없음")
    
    print(f"\n결과: {passed}/{passed + failed} 통과")
    db.close()
    return failed == 0


def test_margin_check():
    """마진 체크 테스트"""
    print("\n" + "=" * 80)
    print("[2] 마진 체크 테스트 (top1 vs top2 차이 < 5%일 때 보너스 미적용)")
    print("=" * 80)
    
    # 마진이 넓은 케이스 (보너스 적용 O)
    wide_margin = {
        '건설부문': 50.0,  # 50%
        '상사부문': 30.0,  # 30% → margin = 20%
    }
    
    scores, audit = calculate_revenue_sector_scores(wide_margin)
    print(f"\n[마진 넓음] 건설 50%, 상사 30% (margin: {audit['margin']:.1f}%)")
    print(f"  - 보너스 적용: {audit['bonus_applied']}")
    print(f"  - 점수: {scores}")
    
    # 마진이 좁은 케이스 (보너스 적용 X)
    narrow_margin = {
        '건설부문': 32.0,  # 32%
        '상사부문': 30.0,  # 30% → margin = 2%
    }
    
    scores, audit = calculate_revenue_sector_scores(narrow_margin)
    print(f"\n[마진 좁음] 건설 32%, 상사 30% (margin: {audit['margin']:.1f}%)")
    print(f"  - 보너스 적용: {audit['bonus_applied']}")
    print(f"  - 점수: {scores}")


def test_segment_mapping():
    """확장된 매핑 테스트"""
    print("\n" + "=" * 80)
    print("[3] 확장된 SEGMENT_TO_SECTOR_MAP 테스트")
    print("=" * 80)
    
    test_segments = {
        '화장품': {'expected': 'SEC_COSMETIC', 'pct': 80.0},
        '의료기기': {'expected': 'SEC_BIO', 'pct': 60.0},
        '게임': {'expected': 'SEC_ENT', 'pct': 90.0},
        '물류': {'expected': 'SEC_RETAIL', 'pct': 70.0},
        '솔루션': {'expected': 'SEC_IT', 'pct': 50.0},
        '건강기능식품': {'expected': 'SEC_FOOD', 'pct': 40.0},  # 건강기능식품 → FOOD
    }
    
    for segment, info in test_segments.items():
        scores, audit = calculate_revenue_sector_scores({segment: info['pct']})
        mapped_sector = audit.get('segment_mapping', {}).get(segment, {}).get('sector', 'UNMAPPED')
        status = "✅" if mapped_sector == info['expected'] else "❌"
        print(f"{status} '{segment}' → {mapped_sector} (기대: {info['expected']})")


def test_misclassification_candidates():
    """오분류 후보 테스트"""
    print("\n" + "=" * 80)
    print("[4] 오분류 후보 테스트 (품질 리포트 Top 5)")
    print("=" * 80)
    
    db = SessionLocal()
    
    # 품질 리포트에서 발견된 오분류 후보
    candidates = [
        ('일동홀딩스', '000230', 'SEC_HOLDING', 'SEC_BIO'),  # 의약품 88%
        ('만호제강', '001080', 'SEC_ELECTRONICS', 'SEC_STEEL'),  # 철강 84%
        ('GS건설', '006360', 'SEC_UTIL', 'SEC_CONST'),  # 건설 73%
    ]
    
    for name, ticker, current, expected in candidates:
        detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == ticker).first()
        stock = db.query(Stock).filter(Stock.ticker == ticker).first()
        
        if detail:
            # 새 로직으로 분류
            major, sub, vc, conf, boosting_log = classify_sector_rule_based(detail, stock.stock_name if stock else name)
            
            # 매출 비중 확인
            rev = detail.revenue_by_segment or {}
            rev_str = ', '.join([f"{k}:{v}%" for k, v in list(rev.items())[:3]])
            
            status = "✅" if major == expected else "⚠️"
            print(f"\n{status} {name} ({ticker})")
            print(f"   현재 DB: {current} → 새 분류: {major}")
            print(f"   기대 섹터: {expected}")
            print(f"   매출비중: {rev_str}")
    
    db.close()


if __name__ == '__main__':
    print("\n" + "🧪 " * 20)
    print("P0 개선 사항 테스트")
    print("🧪 " * 20 + "\n")
    
    test_major_companies()
    test_margin_check()
    test_segment_mapping()
    test_misclassification_candidates()
    
    print("\n" + "=" * 80)
    print("✅ 테스트 완료!")
    print("=" * 80)

