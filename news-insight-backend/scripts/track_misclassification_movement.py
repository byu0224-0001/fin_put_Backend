# -*- coding: utf-8 -*-
"""
오분류 Top 20 이동 경로 추적 (GPT 피드백: Before & After 표)

SEC_MACH 블랙홀에 빠졌던 20개 기업이 어디로 안착했는지 추적
"""
import sys
import os
import json

# Windows 환경에서 UTF-8 인코딩 설정
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'
else:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.models.investor_sector import InvestorSector
from app.services.sector_classifier import (
    classify_sector_rule_based,
    calculate_revenue_sector_scores
)

# 오분류 Top 20 (기존 리포트 기준)
MISCLASSIFICATION_TOP20 = [
    {'ticker': '000210', 'name': 'DL', 'current': 'SEC_CONST', 'expected': 'SEC_MACH'},
    {'ticker': '000230', 'name': '일동홀딩스', 'current': 'SEC_HOLDING', 'expected': 'SEC_BIO'},
    {'ticker': '000480', 'name': 'CR홀딩스', 'current': 'SEC_HOLDING', 'expected': 'SEC_MACH'},
    {'ticker': '000650', 'name': '천일고속', 'current': 'SEC_CARD', 'expected': 'SEC_RETAIL'},
    {'ticker': '000680', 'name': 'LS네트웍스', 'current': 'SEC_AUTO', 'expected': 'SEC_FINANCE'},
    {'ticker': '0008Z0', 'name': '에스엔시스', 'current': 'SEC_MACH', 'expected': 'SEC_IT'},
    {'ticker': '001080', 'name': '만호제강', 'current': 'SEC_ELECTRONICS', 'expected': 'SEC_STEEL'},
    {'ticker': '001140', 'name': '국보', 'current': 'SEC_MACH', 'expected': 'SEC_RETAIL'},
    {'ticker': '001250', 'name': 'GS글로벌', 'current': 'SEC_MACH', 'expected': 'SEC_RETAIL'},
    {'ticker': '001540', 'name': '안국약품', 'current': 'SEC_RETAIL', 'expected': 'SEC_BIO'},
    {'ticker': '001620', 'name': '케이비아이동국실업', 'current': 'SEC_AUTO', 'expected': 'SEC_MACH'},
    {'ticker': '001770', 'name': 'SHD', 'current': 'SEC_STEEL', 'expected': 'SEC_MACH'},
    {'ticker': '001810', 'name': '무림SP', 'current': 'SEC_TIRE', 'expected': 'SEC_MACH'},
    {'ticker': '002350', 'name': '넥센타이어', 'current': 'SEC_TIRE', 'expected': 'SEC_AUTO'},
    {'ticker': '002360', 'name': 'SH에너지화학', 'current': 'SEC_CHEM', 'expected': 'SEC_MACH'},
    {'ticker': '002620', 'name': '제일파마홀딩스', 'current': 'SEC_HOLDING', 'expected': 'SEC_BIO'},
    {'ticker': '002870', 'name': '신풍', 'current': 'SEC_AUTO', 'expected': 'SEC_RETAIL'},
    {'ticker': '002900', 'name': 'TYM', 'current': 'SEC_TELECOM', 'expected': 'SEC_MACH'},
    {'ticker': '003030', 'name': '세아제강지주', 'current': 'SEC_HOLDING', 'expected': 'SEC_STEEL'},
    {'ticker': '003280', 'name': '흥아해운', 'current': 'SEC_MACH', 'expected': 'SEC_SHIP'},
]

def track_misclassification_movement():
    """오분류 Top 20 이동 경로 추적"""
    db = SessionLocal()
    
    try:
        print("=" * 80, flush=True)
        print("오분류 Top 20 이동 경로 추적", flush=True)
        print("=" * 80, flush=True)
        
        movement_report = []
        
        for idx, case in enumerate(MISCLASSIFICATION_TOP20, 1):
            ticker = case['ticker']
            name = case['name']
            current_sector = case['current']
            expected_sector = case['expected']
            
            # 현재 DB 상태 조회
            stock = db.query(Stock).filter(Stock.ticker == ticker).first()
            sector = db.query(InvestorSector).filter(
                InvestorSector.ticker == ticker,
                InvestorSector.is_primary == True
            ).first()
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == ticker
            ).order_by(CompanyDetail.updated_at.desc()).first()
            
            if not stock or not detail:
                continue
            
            # 🆕 개선된 로직으로 재분류
            new_sector, new_sub, new_vc, new_conf, _ = classify_sector_rule_based(
                detail, stock.stock_name
            )
            
            # 매출 기반 섹터 계산
            revenue_scores, revenue_audit = calculate_revenue_sector_scores(detail.revenue_by_segment)
            revenue_best_sector = max(revenue_scores.items(), key=lambda x: x[1])[0] if revenue_scores else None
            revenue_best_score = revenue_scores.get(revenue_best_sector, 0) if revenue_best_sector else 0
            
            movement_report.append({
                'ticker': ticker,
                'name': name,
                'before': {
                    'sector': current_sector,
                    'sub_sector': sector.sub_sector if sector else None,
                    'confidence': sector.confidence if sector else None
                },
                'after': {
                    'sector': new_sector,
                    'sub_sector': new_sub,
                    'value_chain': new_vc,
                    'confidence': new_conf
                },
                'revenue_based': {
                    'sector': revenue_best_sector,
                    'score': revenue_best_score
                },
                'expected': expected_sector,
                'status': 'FIXED' if new_sector == expected_sector else ('IMPROVED' if new_sector != current_sector else 'UNCHANGED'),
                'revenue_by_segment': detail.revenue_by_segment
            })
            
            print(f"\n{idx}. {name} ({ticker})", flush=True)
            print(f"   Before: {current_sector} → After: {new_sector} (Confidence: {new_conf})", flush=True)
            print(f"   Revenue-based: {revenue_best_sector} (score: {revenue_best_score:.3f})", flush=True)
            print(f"   Expected: {expected_sector} | Status: {'✅ FIXED' if new_sector == expected_sector else '⚠️ IMPROVED' if new_sector != current_sector else '❌ UNCHANGED'}", flush=True)
        
        # 통계
        fixed_count = sum(1 for m in movement_report if m['status'] == 'FIXED')
        improved_count = sum(1 for m in movement_report if m['status'] == 'IMPROVED')
        unchanged_count = sum(1 for m in movement_report if m['status'] == 'UNCHANGED')
        
        print("\n" + "=" * 80, flush=True)
        print("이동 경로 요약", flush=True)
        print("=" * 80, flush=True)
        print(f"✅ FIXED (예상 섹터로 이동): {fixed_count}개", flush=True)
        print(f"⚠️ IMPROVED (개선되었으나 예상과 다름): {improved_count}개", flush=True)
        print(f"❌ UNCHANGED (변화 없음): {unchanged_count}개", flush=True)
        
        # 섹터별 이동 통계
        sector_movement = {}
        for m in movement_report:
            before = m['before']['sector']
            after = m['after']['sector']
            if before != after:
                key = f"{before} → {after}"
                sector_movement[key] = sector_movement.get(key, 0) + 1
        
        if sector_movement:
            print(f"\n[섹터 이동 패턴]", flush=True)
            sorted_movement = sorted(sector_movement.items(), key=lambda x: x[1], reverse=True)
            for pattern, count in sorted_movement:
                print(f"  {pattern}: {count}개", flush=True)
        
        # 결과 저장
        result = {
            'total_cases': len(movement_report),
            'statistics': {
                'fixed': fixed_count,
                'improved': improved_count,
                'unchanged': unchanged_count
            },
            'sector_movement': sector_movement,
            'movement_details': movement_report
        }
        
        os.makedirs('reports', exist_ok=True)
        output_file = 'reports/misclassification_movement.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n✅ 결과 저장: {output_file}", flush=True)
        print("=" * 80, flush=True)
        
    except Exception as e:
        import traceback
        print(f"\n❌ 오류 발생: {e}", flush=True)
        traceback.print_exc()
        raise
    finally:
        db.close()

if __name__ == '__main__':
    track_misclassification_movement()

